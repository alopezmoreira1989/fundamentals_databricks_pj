"""One-shot script to extract dashboard fixtures via Databricks Connect.

Run from the 60__frontends/61__streamlit/ directory:
    python fetch_fixtures.py

Requires:
- Databricks Connect configured (databricks-connect or databricks-sdk auth)
- Network access to your Databricks workspace

Writes to fixtures/ in this directory.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

from databricks.connect import DatabricksSession

CATALOG = "main"
SCHEMA  = "financials"
FY_YEARS = 10
QUARTERS = 12

OUT_DIR = Path(__file__).parent / "fixtures"
OUT_DIR.mkdir(exist_ok=True)


def main():
    print("Connecting to Databricks...")
    spark = DatabricksSession.builder.getOrCreate()
    print("✓ Connected")

    # 1. Ticker universe (favorites only) — with universe flags + GICS sector + Yahoo industry.
    tickers_df = spark.sql(f"""
        SELECT
            ticker, company, sector, industry,
            country, accounting_standard, reporting_currency, market,
            COALESCE(is_favorite, false) AS is_favorite,
            COALESCE(in_sp500,    false) AS in_sp500,
            COALESCE(in_r3000,    false) AS in_r3000
        FROM {CATALOG}.config.tickers
        WHERE is_favorite = true
        ORDER BY ticker
    """).toPandas()

    if tickers_df.empty:
        raise ValueError("No favorite tickers in main.config.tickers")

    tickers = tickers_df["ticker"].tolist()
    # Native bools so json.dumps(default=str) doesn't stringify numpy bool_.
    ticker_meta = [
        {
            "ticker":      r.ticker,
            "company":     r.company,
            "sector":      r.sector,   # NULL → app maps to "Unknown"
            "industry":    r.industry, # NULL → app maps to "Unknown"
            "country":             r.country             or "",
            "accounting_standard": r.accounting_standard or "",
            "reporting_currency":  r.reporting_currency  or "",
            "market":              r.market              or "US",
            "is_favorite": bool(r.is_favorite),
            "in_sp500":    bool(r.in_sp500),
            "in_r3000":    bool(r.in_r3000),
        }
        for r in tickers_df.itertuples()
    ]
    tickers_sql = ",".join(f"'{t}'" for t in tickers)
    print(f"✓ {len(tickers)} ticker(s): {tickers}")

    # 2. Financials slice (joined with concept_hierarchy).
    print("Fetching financials...")
    financials = spark.sql(f"""
        SELECT
            f.ticker, f.period_type, f.period_end, f.fiscal_year,
            f.stmt, h.section, h.group, f.concept,
            COALESCE(h.display_name, f.concept) AS display_name,
            h.sort_order, f.value
        FROM {CATALOG}.{SCHEMA}.financials f
        LEFT JOIN {CATALOG}.config.concept_hierarchy h
            ON h.stmt = f.stmt AND h.concept = f.concept
        WHERE f.ticker IN ({tickers_sql})
    """).toPandas()

    financials = _trim_recent(financials, ["FY"], FY_YEARS)
    financials = _trim_recent(financials, ["Q1", "Q2", "Q3", "Q4"], QUARTERS)
    print(f"  financials: {len(financials):,} rows")

    # 3. Metrics slice (joined with metrics_hierarchy).
    print("Fetching metrics...")
    # financials_metrics is annual-only (no period_type/period_end columns) —
    # synthesise them to match 51__export_dashboard_data so the app sees FY rows.
    metrics = spark.sql(f"""
        SELECT
            m.ticker,
            'FY' AS period_type,
            MAKE_DATE(m.fiscal_year, 12, 31) AS period_end,
            m.fiscal_year,
            h.category, h.subcategory, m.metric,
            h.unit, h.sort_order, m.value
        FROM {CATALOG}.{SCHEMA}.financials_metrics m
        LEFT JOIN {CATALOG}.config.metrics_hierarchy h
            ON h.metric = m.metric
        WHERE m.ticker IN ({tickers_sql})
    """).toPandas()

    metrics = _trim_recent(metrics, ["FY"], FY_YEARS)
    metrics = _trim_recent(metrics, ["Q1", "Q2", "Q3", "Q4"], QUARTERS)

    # Market Cap (from market_cap_asof — period_end-aligned) as a `Market Cap` metric row — see
    # 50__publish/51__export_dashboard_data.py for rationale. category NULL keeps it out of the
    # detail page's metrics grid but visible to the screener. `period_end` is the real fiscal
    # close (no calendar offset), matching prod after the market_data → market_cap_asof migration.
    import pandas as pd

    # unit mirrors each row's real market_cap_asof.currency (mostly USD, CAD for the
    # USD-quote-mismatch case) — same _unit_expr guard as 51__export_dashboard_data.py, so
    # local fixtures don't silently mislabel Canadian tickers' Market Cap as USD.
    _mca_cols = {f.name for f in spark.table(f"{CATALOG}.{SCHEMA}.market_cap_asof").schema.fields}
    _unit_expr = "LOWER(md.currency)" if "currency" in _mca_cols else "'usd'"
    market_cap = spark.sql(f"""
        SELECT
            md.ticker,
            'FY'                  AS period_type,
            md.period_end         AS period_end,
            md.fiscal_year,
            CAST(NULL AS STRING)  AS category,
            CAST(NULL AS STRING)  AS subcategory,
            'Market Cap'          AS metric,
            {_unit_expr}          AS unit,
            CAST(NULL AS DOUBLE)  AS sort_order,
            md.market_cap         AS value
        FROM {CATALOG}.{SCHEMA}.market_cap_asof md
        WHERE md.ticker IN ({tickers_sql})
          AND md.market_cap IS NOT NULL
    """).toPandas()
    metrics = pd.concat([metrics, market_cap], ignore_index=True)
    print(f"  metrics: {len(metrics):,} rows (incl. {len(market_cap):,} Market Cap)")

    # 3b. FX rates — full daily history (unfiltered by ticker; fx_rates_daily isn't
    # ticker-keyed), mirroring 51__export_dashboard_data.py's dashboard_fx slice so a
    # local "view in USD" toggle has real rates to convert against.
    print("Fetching FX rates...")
    fx_rates = spark.sql(f"""
        SELECT base, quote, pair, date, rate
        FROM {CATALOG}.{SCHEMA}.fx_rates_daily
        ORDER BY base, quote, date
    """).toPandas()
    print(f"  fx_rates: {len(fx_rates):,} rows")

    # 4. Write fixtures.
    data_path   = OUT_DIR / "dashboard_data.parquet"
    metric_path = OUT_DIR / "dashboard_metrics.parquet"
    fx_path     = OUT_DIR / "dashboard_fx.parquet"
    meta_path   = OUT_DIR / "dashboard_meta.json"

    financials.to_parquet(data_path, index=False)
    metrics.to_parquet(metric_path, index=False)
    fx_rates.to_parquet(fx_path, index=False)

    fy_ranges = (
        financials[financials["period_type"] == "FY"]
        .groupby("ticker")["fiscal_year"]
        .agg(["min", "max"])
        .rename(columns={"min": "fy_min", "max": "fy_max"})
        .reset_index()
        .to_dict(orient="records")
    )

    meta = {
        "schema_version":  12,  # kept in sync with 51__export_dashboard_data.py's SCHEMA_VERSION
        "build_timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "tickers":         ticker_meta,
        "fy_ranges":       fy_ranges,
        "row_counts": {
            "financials": len(financials),
            "metrics":    len(metrics),
            "fx_rates":   len(fx_rates),
        },
        "retention": {
            "fy_years":  FY_YEARS,
            "quarters":  QUARTERS,
        },
    }
    meta_path.write_text(json.dumps(meta, indent=2, default=str))

    print(f"\n✓ Fixtures written to {OUT_DIR}/")
    print(f"  {data_path.name}    ({data_path.stat().st_size / 1024:.1f} KB)")
    print(f"  {metric_path.name}  ({metric_path.stat().st_size / 1024:.1f} KB)")
    print(f"  {fx_path.name}      ({fx_path.stat().st_size / 1024:.1f} KB)")
    print(f"  {meta_path.name}    (schema v{meta['schema_version']})")


def _trim_recent(df, period_types: list[str], n_periods: int):
    """Keep only the last N distinct period_end values per ticker for the given period types."""
    import pandas as pd

    mask = df["period_type"].isin(period_types)
    sub = df[mask]
    keep = []
    for _ticker, grp in sub.groupby("ticker"):
        recent_ends = (
            grp[["period_end"]]
            .drop_duplicates()
            .sort_values("period_end", ascending=False)
            .head(n_periods)["period_end"]
            .tolist()
        )
        keep.append(grp[grp["period_end"].isin(recent_ends)])
    rest = df[~mask]
    if keep:
        return pd.concat([rest] + keep, ignore_index=True)
    return rest


if __name__ == "__main__":
    main()

# Databricks notebook source
# MAGIC %md
# MAGIC # 50__publish / 51__export_dashboard_data
# MAGIC
# MAGIC Exports the dashboard-ready slice of `main.financials.*` to three local files on
# MAGIC the driver (`/tmp/`). `52__publish_to_github` uploads them as GitHub Release
# MAGIC assets — together they let the public Streamlit app render without touching
# MAGIC Databricks at runtime.
# MAGIC
# MAGIC **Outputs**
# MAGIC - `/tmp/dashboard_data.parquet`    — long-format financials joined with concept_hierarchy
# MAGIC - `/tmp/dashboard_metrics.parquet` — long-format derived metrics joined with metrics_hierarchy
# MAGIC - `/tmp/dashboard_meta.json`       — build timestamp, ticker list, row counts, schema version
# MAGIC
# MAGIC **Universe:** all tickers that have data in `financials`.
# MAGIC
# MAGIC **Retention window:** last 10 fiscal years (FY) and last 12 quarters per ticker.
# MAGIC
# MAGIC Databricks-only: uses `spark`, `%run`, and writes to driver-local `/tmp/`.

# COMMAND ----------

# MAGIC %run "../00__config/01__tickers"

# COMMAND ----------

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# The schema contract ships in the installable `fundamentals_pipeline` package
# (fundamentals_pipeline/schemas.py). Every environment installs it the same way
# (`pip install -e .`); on Databricks that install happens once in 91__full_pipeline's
# session-dependencies cell, so this %run-included notebook imports it directly — no
# sys.path manipulation.
from fundamentals_pipeline import schemas as _schemas

SCHEMA_VERSION = 11  # +currency (market_cap_asof alignment) + dashboard_fx artifact (full FX history)
FY_YEARS       = 10
QUARTERS       = 12
PRICE_YEARS    = 10                              # daily-price retention window (calendar years)

OUT_DIR        = Path("/tmp")
DATA_PARQUET   = OUT_DIR / "dashboard_data.parquet"
METRIC_PARQUET = OUT_DIR / "dashboard_metrics.parquet"
META_JSON      = OUT_DIR / "dashboard_meta.json"
PRICE_PARQUET  = OUT_DIR / "dashboard_prices.parquet"
BACKTEST_PARQUET = OUT_DIR / "dashboard_backtest.parquet"
FX_PARQUET     = OUT_DIR / "dashboard_fx.parquet"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Resolve ticker universe (all tickers with data in financials)

# COMMAND ----------

# Universe flags (is_favorite / in_sp500 / in_r3000) drive the Streamlit
# screener's universe filter. COALESCE to false so NULLs don't leak through.
tickers_df = spark.sql(f"""
    SELECT
      t.ticker, t.company, t.sector, t.industry, t.has_logo,
      t.description, t.exchange, t.country, t.employees, t.website, t.founded,
      t.accounting_standard, t.reporting_currency,
      COALESCE(t.is_favorite, false) AS is_favorite,
      COALESCE(t.in_sp500,    false) AS in_sp500,
      COALESCE(t.in_r3000,    false) AS in_r3000
    FROM {CATALOG}.config.tickers t
    JOIN (SELECT DISTINCT ticker FROM {CATALOG}.{SCHEMA}.financials) f
      ON f.ticker = t.ticker
    ORDER BY t.ticker
""").toPandas()

if tickers_df.empty:
    raise ValueError("No tickers with financial data found")

tickers = tickers_df["ticker"].tolist()
# Build records with native Python bools — pandas/numpy bool_ would be stringified
# to "True"/"False" by json.dumps(default=str) and break boolean parsing in the app.
ticker_meta = [
    {
        "ticker":      r.ticker,
        "company":     r.company,
        "sector":      r.sector,   # NULL → app maps to "Unknown"
        "industry":    r.industry, # NULL → app maps to "Unknown"
        # has_logo: True hit / False miss / None (probe skipped or errored). Keep it a
        # native bool|None — pd.NA / numpy bool_ would be stringified by json.dumps(default=str).
        "has_logo":    None if pd.isna(r.has_logo) else bool(r.has_logo),
        # Company Overview tab (schema v9). description may be "" — that is fine; NULL → "".
        "description": r.description or "",
        "exchange":    r.exchange    or "",
        "country":     r.country     or "",
        "employees":   None if pd.isna(r.employees) else int(r.employees),
        "website":     r.website     or "",
        "founded":     None if pd.isna(r.founded)   else int(r.founded),
        "accounting_standard": r.accounting_standard or "",
        "reporting_currency":  r.reporting_currency  or "",
        "is_favorite": bool(r.is_favorite),
        "in_sp500":    bool(r.in_sp500),
        "in_r3000":    bool(r.in_r3000),
    }
    for r in tickers_df.itertuples()
]
print(f"✓ Exporting {len(tickers)} ticker(s)")

# Register the export universe as a temp view so the slice queries below semi-join it
# instead of embedding a ~2.5k-literal IN(...) list. The literal list made Catalyst
# planning ~5x slower (≈42s vs ≈13s per query) for an identical result set.
spark.createDataFrame([(t,) for t in tickers], "ticker string").createOrReplaceTempView("_export_universe")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Financials slice (long format + hierarchy)
# MAGIC
# MAGIC LEFT JOIN against `concept_hierarchy` so the Streamlit app gets
# MAGIC `section / group / display_name / sort_order` resolved at write time.
# MAGIC Rows whose `concept` isn't in the hierarchy still flow through with NULL
# MAGIC hierarchy columns — the renderer just won't show them.

# COMMAND ----------

# Precise retention done IN SQL so toPandas() pulls only the final slice (no driver-side
# loop over ~2.5k tickers). DENSE_RANK over DISTINCT period_end — not ROW_NUMBER over rows —
# keeps ALL concept rows for the last N periods regardless of how many concepts a period has
# (the old ROW_NUMBER `* 30` cap was a guess that broke as concept counts grew, and forced an
# over-pull + pandas trim_recent loop that pushed 51 past its 600s timeout). FY and the
# quarterly bucket (Q1..Q4 combined) are ranked independently, matching the previous behaviour:
# last FY_YEARS distinct FY period_ends + last QUARTERS distinct quarter period_ends per ticker.
financials = spark.sql(f"""
    WITH base AS (
      SELECT
        f.ticker, f.period_type, f.period_end, f.fiscal_year,
        f.stmt, f.concept, f.value,
        h.section, h.group, h.sort_order,
        COALESCE(h.display_name, f.concept) AS display_name,
        CASE WHEN f.period_type = 'FY' THEN 'FY' ELSE 'Q' END AS pt_bucket
      FROM {CATALOG}.{SCHEMA}.financials f
      LEFT JOIN {CATALOG}.config.concept_hierarchy h
        ON h.stmt = f.stmt AND h.concept = f.concept
      WHERE f.ticker IN (SELECT ticker FROM _export_universe)
        AND f.period_type IN ('FY', 'Q1', 'Q2', 'Q3', 'Q4')
    ),
    ranked AS (
      SELECT *,
        DENSE_RANK() OVER (
          PARTITION BY ticker, pt_bucket ORDER BY period_end DESC
        ) AS period_rank
      FROM base
    )
    SELECT
      ticker, period_type, period_end, fiscal_year,
      stmt, section, `group`, concept, display_name, sort_order, value
    FROM ranked
    WHERE (pt_bucket = 'FY' AND period_rank <= {FY_YEARS})
       OR (pt_bucket = 'Q'  AND period_rank <= {QUARTERS})
""").toPandas()

print(f"  financials rows: {len(financials):,}")
print(financials.groupby(["ticker", "period_type"]).size().unstack(fill_value=0))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Derived metrics slice (long format + hierarchy)

# COMMAND ----------

metrics = spark.sql(f"""
    WITH ranked AS (
      SELECT
        m.ticker, 'FY' AS period_type,
        MAKE_DATE(m.fiscal_year, 12, 31) AS period_end,
        m.fiscal_year,
        h.category, h.subcategory, m.metric,
        h.unit, h.sort_order, m.value,
        DENSE_RANK() OVER (PARTITION BY m.ticker ORDER BY m.fiscal_year DESC) AS yr_rank
      FROM {CATALOG}.{SCHEMA}.financials_metrics m
      LEFT JOIN {CATALOG}.config.metrics_hierarchy h
        ON h.metric = m.metric
      WHERE m.ticker IN (SELECT ticker FROM _export_universe)
        AND m.fiscal_year BETWEEN 1990 AND 2099
    )
    SELECT ticker, period_type, period_end, fiscal_year,
           category, subcategory, metric, unit, sort_order, value
    FROM ranked
    WHERE yr_rank <= {FY_YEARS}
""").toPandas()

# Market Cap lives in market_cap_asof (period_end-aligned, written by 22) — inject it as a
# `Market Cap` metric row so the Streamlit screener can pivot it like any other metric.
# category/subcategory are NULL on purpose: the detail page's metrics grid filters on
# category.dropna(), so these rows are invisible there but the screener still picks them up.
# `period_end` is the REAL fiscal close and the value is priced as-of it, so — unlike the old
# market_data source — it's on the same fiscal basis as every other FY metric (no 0–11mo offset).
#
# `unit`: market_cap_asof's `currency` column (added alongside the currency-alignment fix,
# schema v11) is each row's REAL native reporting currency (not always USD once Canadian
# tickers exist — see fundamentals_pipeline/fx.py) — read it directly rather than hardcoding
# 'usd'. Defensively falls back to the 'usd' literal for a table that predates that column.
MKT_COLUMNS = ["ticker", "period_type", "period_end", "fiscal_year",
               "category", "subcategory", "metric", "unit", "sort_order", "value"]
try:
    _mca_cols = {f.name for f in spark.table(f"{CATALOG}.{SCHEMA}.market_cap_asof").schema.fields}
    _unit_expr = "LOWER(md.currency)" if "currency" in _mca_cols else "'usd'"
    market_cap = spark.sql(f"""
        WITH ranked AS (
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
            md.market_cap         AS value,
            DENSE_RANK() OVER (PARTITION BY md.ticker ORDER BY md.fiscal_year DESC) AS yr_rank
          FROM {CATALOG}.{SCHEMA}.market_cap_asof md
          WHERE md.ticker IN (SELECT ticker FROM _export_universe)
            AND md.market_cap IS NOT NULL
        )
        SELECT ticker, period_type, period_end, fiscal_year,
               category, subcategory, metric, unit, sort_order, value
        FROM ranked
        WHERE yr_rank <= {FY_YEARS}
    """).toPandas()
except Exception as e:
    print(f"⚠ market_cap_asof unavailable ({e}) — skipping Market Cap rows.")
    market_cap = pd.DataFrame(columns=MKT_COLUMNS)

# Retention now enforced in SQL (last FY_YEARS years per ticker, per source), so no
# driver-side trim needed — just stack the two long-format metric frames.
metrics = pd.concat([metrics, market_cap], ignore_index=True)
print(f"  + Market Cap rows: {len(market_cap):,}")
print(f"  metrics rows: {len(metrics):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3b. Daily price slice (market_prices_daily)
# MAGIC
# MAGIC Daily adjusted/raw close for the export universe, last `PRICE_YEARS` calendar
# MAGIC years. Daily grain only — the Streamlit Price tab resamples to weekly/monthly
# MAGIC client-side, so nothing is pre-aggregated here. `adj_close` drives the chart;
# MAGIC raw `close` is tooltip-only. `value` precision matters (BRK.A trades >$600k), so
# MAGIC both columns stay float64 — no downcast.

# COMMAND ----------

# Resilient read: market_prices_daily may not exist yet (12__fetch_market_data not run
# on full history) or may be empty for this universe. In either case fall back to an
# empty, correctly-typed frame so 52 still finds the file and the app degrades to
# "no data" instead of crashing.
PRICE_COLUMNS = {"ticker": "object", "date": "datetime64[ns]", "close": "float64", "adj_close": "float64"}
try:
    prices = spark.sql(f"""
        SELECT p.ticker, p.date, p.close, p.adj_close
        FROM {CATALOG}.{SCHEMA}.market_prices_daily p
        WHERE p.ticker IN (SELECT ticker FROM _export_universe)
          AND p.date >= ADD_MONTHS(CURRENT_DATE(), -12 * {PRICE_YEARS})
    """).toPandas()
    # volume is a one-line add here (and in PRICE_COLUMNS) if a volume subplot is ever wanted.
    if prices.empty:
        print("⚠️ market_prices_daily returned 0 rows for this universe — writing empty price slice")
except Exception as exc:  # noqa: BLE001 — table absent or unreadable: degrade, don't fail the export
    print(f"⚠️ Could not read market_prices_daily ({type(exc).__name__}: {exc}) — writing empty price slice")
    prices = pd.DataFrame({c: pd.Series(dtype=t) for c, t in PRICE_COLUMNS.items()})

# Keep close/adj_close as float64 (no downcast — large prices lose cents in float32).
prices["close"] = prices["close"].astype("float64")
prices["adj_close"] = prices["adj_close"].astype("float64")

if prices.empty:
    print("  prices rows: 0 (empty slice)")
else:
    print(f"  prices rows: {len(prices):,}")
    print(f"  distinct tickers: {prices['ticker'].nunique():,}")
    print(f"  date range: {prices['date'].min()} → {prices['date'].max()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3c. Backtest slice (backtest_results)
# MAGIC
# MAGIC Equity-curve series per archetype × fiscal_year from `70__backtest/71__run_backtest`.
# MAGIC Resilient: the table may not exist yet (backtester not run) — fall back to an
# MAGIC empty, correctly-typed frame so the app degrades to "no data" instead of crashing.

# COMMAND ----------

BACKTEST_COLUMNS = {
    "archetype": "object", "fiscal_year": "int64",
    "portfolio_return": "float64", "benchmark_return": "float64",
    "portfolio_value": "float64", "benchmark_value": "float64", "n_holdings": "int64",
}
try:
    backtest = spark.sql(f"""
        SELECT archetype, fiscal_year, portfolio_return, benchmark_return,
               portfolio_value, benchmark_value, n_holdings
        FROM {CATALOG}.{SCHEMA}.backtest_results
        ORDER BY archetype, fiscal_year
    """).toPandas()
    if backtest.empty:
        print("⚠️ backtest_results returned 0 rows — writing empty backtest slice")
except Exception as exc:  # noqa: BLE001 — table absent or unreadable: degrade, don't fail the export
    print(f"⚠️ Could not read backtest_results ({type(exc).__name__}: {exc}) — writing empty backtest slice")
    backtest = pd.DataFrame({c: pd.Series(dtype=t) for c, t in BACKTEST_COLUMNS.items()})

if backtest.empty:
    print("  backtest rows: 0 (empty slice)")
else:
    print(f"  backtest rows: {len(backtest):,} ({backtest['archetype'].nunique()} archetype(s))")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3d. FX rates slice (fx_rates_daily)
# MAGIC
# MAGIC **Full daily history** (no retention window, unlike prices) — a future frontend "view
# MAGIC in USD" toggle needs the rate from a HISTORICAL figure's own `period_end`, never today's
# MAGIC spot rate (see `fundamentals_pipeline/fx.py`'s date-anchoring rule), so truncating this
# MAGIC to a recent window would silently break that for older fiscal years. The table itself is
# MAGIC tiny (a handful of currency pairs × ~20 years of daily rates), so exporting it whole
# MAGIC costs nothing. Building the toggle itself is out of scope here — this just publishes the
# MAGIC data it will need.

# COMMAND ----------

FX_COLUMNS = {"base": "object", "quote": "object", "pair": "object",
              "date": "datetime64[ns]", "rate": "float64"}
try:
    fx_rates = spark.sql(f"""
        SELECT base, quote, pair, date, rate
        FROM {CATALOG}.{SCHEMA}.fx_rates_daily
        ORDER BY base, quote, date
    """).toPandas()
    if fx_rates.empty:
        print("⚠️ fx_rates_daily returned 0 rows — writing empty FX slice")
except Exception as exc:  # noqa: BLE001 — table absent or unreadable: degrade, don't fail the export
    print(f"⚠️ Could not read fx_rates_daily ({type(exc).__name__}: {exc}) — writing empty FX slice")
    fx_rates = pd.DataFrame({c: pd.Series(dtype=t) for c, t in FX_COLUMNS.items()})

if fx_rates.empty:
    print("  fx rows: 0 (empty slice)")
else:
    print(f"  fx rows: {len(fx_rates):,} ({fx_rates[['base', 'quote']].drop_duplicates().shape[0]} pair(s))")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write parquet + meta

# COMMAND ----------

# Spark Connect's toPandas() stashes a non-JSON-serializable PlanMetrics object in
# df.attrs (query execution metrics). pandas' to_parquet (PyArrow) then tries
# json.dumps(df.attrs) as file metadata and crashes with
# "TypeError: Object of type PlanMetrics is not JSON serializable". Clear attrs first.
financials.attrs = {}
metrics.attrs = {}
prices.attrs = {}
backtest.attrs = {}
fx_rates.attrs = {}

# Schema contract — fail the run LOUDLY rather than shipping an artifact the public app
# can't read. assert_artifact raises SchemaError naming the offending artifact/column.
_schemas.assert_artifact("dashboard_data", financials)
_schemas.assert_artifact("dashboard_metrics", metrics)
_schemas.assert_artifact("dashboard_prices", prices)
_schemas.assert_artifact("dashboard_backtest", backtest)
_schemas.assert_artifact("dashboard_fx", fx_rates)

financials.to_parquet(DATA_PARQUET, index=False)
metrics.to_parquet(METRIC_PARQUET, index=False)
prices.to_parquet(PRICE_PARQUET, index=False)
backtest.to_parquet(BACKTEST_PARQUET, index=False)
fx_rates.to_parquet(FX_PARQUET, index=False)

# Per-ticker FY range — used by the Streamlit masthead.
fy_ranges = (
    financials[financials["period_type"] == "FY"]
    .groupby("ticker")["fiscal_year"]
    .agg(["min", "max"])
    .rename(columns={"min": "fy_min", "max": "fy_max"})
    .reset_index()
    .to_dict(orient="records")
)

meta = {
    "schema_version":   SCHEMA_VERSION,
    "build_timestamp":  datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "tickers":          ticker_meta,   # list of {"ticker", "company"} dicts
    "fy_ranges":        fy_ranges,
    "row_counts": {
        "financials":   int(len(financials)),
        "metrics":      int(len(metrics)),
        "prices":       int(len(prices)),
        "backtest":     int(len(backtest)),
        "fx":           int(len(fx_rates)),
    },
    "retention": {
        "fy_years":     FY_YEARS,
        "quarters":     QUARTERS,
        "price_years":  PRICE_YEARS,
    },
}
_schemas.assert_meta(meta)
META_JSON.write_text(json.dumps(meta, indent=2, default=str))

print("\n✓ Wrote:")
print(f"  {DATA_PARQUET}   ({DATA_PARQUET.stat().st_size / 1024:.1f} KB)")
print(f"  {METRIC_PARQUET} ({METRIC_PARQUET.stat().st_size / 1024:.1f} KB)")
print(f"  {PRICE_PARQUET}  ({PRICE_PARQUET.stat().st_size / 1024:.1f} KB)")
print(f"  {BACKTEST_PARQUET} ({BACKTEST_PARQUET.stat().st_size / 1024:.1f} KB)")
print(f"  {FX_PARQUET}     ({FX_PARQUET.stat().st_size / 1024:.1f} KB)")
print(f"  {META_JSON}      (schema_version={SCHEMA_VERSION})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Optional — copy to a Volume for local fixture extraction
# MAGIC
# MAGIC When iterating on the Streamlit app locally, the easiest way to grab the
# MAGIC artifacts is via a Unity Catalog Volume mounted in your workspace. Set
# MAGIC `COPY_TO_VOLUME = True` and adjust the path to a volume you can read with
# MAGIC `databricks fs cp`.

# COMMAND ----------

COPY_TO_VOLUME = False
VOLUME_PATH    = "/Volumes/main/financials/_publish"   # must already exist

if COPY_TO_VOLUME:
    dbutils.fs.mkdirs(VOLUME_PATH)
    for f in [DATA_PARQUET, METRIC_PARQUET, PRICE_PARQUET, BACKTEST_PARQUET, FX_PARQUET, META_JSON]:
        dest = f"{VOLUME_PATH}/{f.name}"
        dbutils.fs.cp(f"file:{f}", dest, recurse=False)
        print(f"  ✓ {f.name} → {dest}")
else:
    print("⊘ COPY_TO_VOLUME=False — skipping Volume copy (set True for local dev)")

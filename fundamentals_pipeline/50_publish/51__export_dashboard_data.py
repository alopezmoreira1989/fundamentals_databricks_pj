# Databricks notebook source
# MAGIC %md
# MAGIC # 50_publish / 51__export_dashboard_data
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

# MAGIC %run "../00_config/01__tickers"

# COMMAND ----------

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

SCHEMA_VERSION = 3
FY_YEARS       = 10
QUARTERS       = 12

OUT_DIR        = Path("/tmp")
DATA_PARQUET   = OUT_DIR / "dashboard_data.parquet"
METRIC_PARQUET = OUT_DIR / "dashboard_metrics.parquet"
META_JSON      = OUT_DIR / "dashboard_meta.json"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Resolve ticker universe (all tickers with data in financials)

# COMMAND ----------

# Universe flags (is_favorite / in_sp500 / in_r3000) drive the Streamlit
# screener's universe filter. COALESCE to false so NULLs don't leak through.
tickers_df = spark.sql(f"""
    SELECT
      t.ticker, t.company,
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
        "is_favorite": bool(r.is_favorite),
        "in_sp500":    bool(r.in_sp500),
        "in_r3000":    bool(r.in_r3000),
    }
    for r in tickers_df.itertuples()
]
print(f"✓ Exporting {len(tickers)} ticker(s)")

tickers_sql = ",".join(f"'{t}'" for t in tickers)

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
      WHERE f.ticker IN ({tickers_sql})
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
      WHERE m.ticker IN ({tickers_sql})
        AND m.fiscal_year BETWEEN 1990 AND 2099
    )
    SELECT ticker, period_type, period_end, fiscal_year,
           category, subcategory, metric, unit, sort_order, value
    FROM ranked
    WHERE yr_rank <= {FY_YEARS}
""").toPandas()

# Market Cap lives in market_data (not financials_metrics) — inject it as a
# `Market Cap` metric row so the Streamlit screener can pivot it like any other
# metric. category/subcategory are NULL on purpose: the detail page's metrics
# grid filters on category.dropna(), so these rows are invisible there but the
# screener still picks them up.
# ⚠️ market_data.fiscal_year is the CALENDAR year, while metrics.fiscal_year is
# the FISCAL year — for non-December fiscal-year-end tickers (AAPL/Sep, MSFT/Jun,
# WMT/Jan) there's a known 0–11 month offset. Acceptable for the screener.
market_cap = spark.sql(f"""
    WITH ranked AS (
      SELECT
        md.ticker,
        'FY'                  AS period_type,
        MAKE_DATE(md.fiscal_year, 12, 31) AS period_end,
        md.fiscal_year,
        CAST(NULL AS STRING)  AS category,
        CAST(NULL AS STRING)  AS subcategory,
        'Market Cap'          AS metric,
        'usd'                 AS unit,
        CAST(NULL AS DOUBLE)  AS sort_order,
        md.market_cap         AS value,
        DENSE_RANK() OVER (PARTITION BY md.ticker ORDER BY md.fiscal_year DESC) AS yr_rank
      FROM {CATALOG}.{SCHEMA}.market_data md
      WHERE md.ticker IN ({tickers_sql})
        AND md.market_cap IS NOT NULL
    )
    SELECT ticker, period_type, period_end, fiscal_year,
           category, subcategory, metric, unit, sort_order, value
    FROM ranked
    WHERE yr_rank <= {FY_YEARS}
""").toPandas()

# Retention now enforced in SQL (last FY_YEARS years per ticker, per source), so no
# driver-side trim needed — just stack the two long-format metric frames.
metrics = pd.concat([metrics, market_cap], ignore_index=True)
print(f"  + Market Cap rows: {len(market_cap):,}")
print(f"  metrics rows: {len(metrics):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write parquet + meta

# COMMAND ----------

financials.to_parquet(DATA_PARQUET, index=False)
metrics.to_parquet(METRIC_PARQUET, index=False)

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
    },
    "retention": {
        "fy_years":     FY_YEARS,
        "quarters":     QUARTERS,
    },
}
META_JSON.write_text(json.dumps(meta, indent=2, default=str))

print(f"\n✓ Wrote:")
print(f"  {DATA_PARQUET}   ({DATA_PARQUET.stat().st_size / 1024:.1f} KB)")
print(f"  {METRIC_PARQUET} ({METRIC_PARQUET.stat().st_size / 1024:.1f} KB)")
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
    for f in [DATA_PARQUET, METRIC_PARQUET, META_JSON]:
        dest = f"{VOLUME_PATH}/{f.name}"
        dbutils.fs.cp(f"file:{f}", dest, recurse=False)
        print(f"  ✓ {f.name} → {dest}")
else:
    print("⊘ COPY_TO_VOLUME=False — skipping Volume copy (set True for local dev)")

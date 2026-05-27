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

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/fundamentals_pipeline/00_config/01__tickers"

# COMMAND ----------

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

SCHEMA_VERSION = 2
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

tickers_df = spark.sql(f"""
    SELECT t.ticker, t.company
    FROM {CATALOG}.config.tickers t
    JOIN (SELECT DISTINCT ticker FROM {CATALOG}.{SCHEMA}.financials) f
      ON f.ticker = t.ticker
    ORDER BY t.ticker
""").toPandas()

if tickers_df.empty:
    raise ValueError("No tickers with financial data found")

tickers = tickers_df["ticker"].tolist()
ticker_meta = tickers_df.to_dict(orient="records")
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

financials = spark.sql(f"""
    WITH ranked AS (
      SELECT
        f.ticker, f.period_type, f.period_end, f.fiscal_year,
        f.stmt, f.concept, f.value,
        h.section, h.group, h.sort_order,
        COALESCE(h.display_name, f.concept) AS display_name,
        -- FY / quarterly windows are computed per ticker × period_type
        ROW_NUMBER() OVER (
          PARTITION BY f.ticker, f.period_type
          ORDER BY f.period_end DESC
        ) AS recency_rank
      FROM {CATALOG}.{SCHEMA}.financials f
      LEFT JOIN {CATALOG}.config.concept_hierarchy h
        ON h.stmt = f.stmt AND h.concept = f.concept
      WHERE f.ticker IN ({tickers_sql})
    )
    SELECT
      ticker, period_type, period_end, fiscal_year,
      stmt, section, `group`, concept, display_name, sort_order, value
    FROM ranked
    WHERE
      (period_type = 'FY'  AND recency_rank <= {FY_YEARS} * 30)   -- ~30 concepts/yr cap
      OR
      (period_type IN ('Q1','Q2','Q3','Q4') AND recency_rank <= {QUARTERS} * 30)
""").toPandas()

# The recency_rank cap above is rough (rank counts per row, not per period).
# Apply a precise trim on the pandas side: last N distinct (period_type, period_end)
# groups per ticker, where N = 10 for FY and 12 for quarterly.

def trim_recent(df: pd.DataFrame, period_types: list[str], n_periods: int) -> pd.DataFrame:
    mask = df["period_type"].isin(period_types)
    sub  = df[mask]
    keep_rows = []
    for ticker, sub_t in sub.groupby("ticker"):
        recent_ends = (
            sub_t[["period_end"]]
            .drop_duplicates()
            .sort_values("period_end", ascending=False)
            .head(n_periods)["period_end"]
            .tolist()
        )
        keep_rows.append(sub_t[sub_t["period_end"].isin(recent_ends)])
    return pd.concat([df[~mask]] + keep_rows, ignore_index=True) if keep_rows else df[~mask]

financials = trim_recent(financials, ["FY"], FY_YEARS)
financials = trim_recent(financials, ["Q1", "Q2", "Q3", "Q4"], QUARTERS)

print(f"  financials rows: {len(financials):,}")
print(financials.groupby(["ticker", "period_type"]).size().unstack(fill_value=0))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Derived metrics slice (long format + hierarchy)

# COMMAND ----------

metrics = spark.sql(f"""
    SELECT
      m.ticker, 'FY' AS period_type,
      MAKE_DATE(m.fiscal_year, 12, 31) AS period_end,
      m.fiscal_year,
      h.category, h.subcategory, m.metric,
      h.unit, h.sort_order, m.value
    FROM {CATALOG}.{SCHEMA}.financials_metrics m
    LEFT JOIN {CATALOG}.config.metrics_hierarchy h
      ON h.metric = m.metric
    WHERE m.ticker IN ({tickers_sql})
""").toPandas()

# Trim: last N fiscal years per ticker
metrics = metrics.sort_values("fiscal_year", ascending=False)
metrics = metrics.groupby("ticker").head(FY_YEARS * 40)  # ~40 metrics/yr cap

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

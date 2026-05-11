# Databricks notebook source
# MAGIC %md
# MAGIC # 90_pipelines / 91__full_pipeline
# MAGIC
# MAGIC **Entry point for the Databricks Job.**
# MAGIC Runs the full ingestion → transformation → analysis pipeline in sequence.
# MAGIC
# MAGIC ```
# MAGIC 11__fetch_sec_xbrl       fetch from SEC API → financials_raw
# MAGIC       ↓
# MAGIC 21__clean_and_merge      deduplicate → MERGE into financials
# MAGIC       ↓
# MAGIC 23__wide_tables          pivot → income_statement, balance_sheet, cash_flow
# MAGIC       ↓
# MAGIC 22__derived_metrics      FCF, margins, YoY growth → financials_metrics
# MAGIC       ↓
# MAGIC 31__company_analysis     analysis queries
# MAGIC ```

# COMMAND ----------

# MAGIC %md ## 0. Pipeline parameters
# MAGIC
# MAGIC Override at runtime via Databricks Job parameters:
# MAGIC `{"tickers_override": "AAPL,TSLA", "run_optimization": "false"}`

# COMMAND ----------

tickers_override = dbutils.widgets.get("tickers_override") if dbutils.widgets.getAll() else ""
run_optimization = dbutils.widgets.get("run_optimization") if dbutils.widgets.getAll() else "false"

dbutils.jobs.taskValues.set("tickers_override", tickers_override)
dbutils.jobs.taskValues.set("run_optimization", run_optimization)

print(f"tickers_override : '{tickers_override}' (empty = all active)")
print(f"run_optimization : {run_optimization}")

# COMMAND ----------

# MAGIC %md ## 1. Load config

# COMMAND ----------

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/FA PJ (Basic)/00_config/01__tickers"

# COMMAND ----------

try:
    _ = ACTIVE_TICKERS
    print(f"✓ Config loaded — {len(ACTIVE_TICKERS)} active tickers")
except NameError:
    raise NameError("ACTIVE_TICKERS not defined — check the %run path above.")

from datetime import datetime

pipeline_start = datetime.utcnow()
print(f"Pipeline started at {pipeline_start.isoformat()} UTC")

if tickers_override.strip():
    run_tickers = [t.strip().upper() for t in tickers_override.split(",")]
    print(f"Running override subset: {run_tickers}")
else:
    run_tickers = ACTIVE_TICKERS
    print(f"Running all active tickers: {run_tickers}")

# run_tickers and run_optimization are shared directly via %run namespace
# No need for spark.conf — child notebooks read run_tickers as a Python variable

# COMMAND ----------

# MAGIC %md ## 2. Ingestion — fetch from SEC EDGAR
# MAGIC `financials_raw` — append-only audit log

# COMMAND ----------

print("=" * 55)
print("STEP 1 / 4 — Ingestion")
print("=" * 55)

# COMMAND ----------

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/FA PJ (Basic)/10_ingestion/11__fetch_sec_xbrl"

# COMMAND ----------

# MAGIC %md ## 3. Clean & merge → fact table
# MAGIC `financials` — deduplicated, clean long-format fact table

# COMMAND ----------

print("=" * 55)
print("STEP 2 / 4 — Clean & Merge")
print("=" * 55)

# COMMAND ----------

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/FA PJ (Basic)/20_transformation/21__clean_and_merge"

# COMMAND ----------

# MAGIC %md ## 4. Derived metrics
# MAGIC `financials_metrics` — margins, FCF, YoY growth, leverage

# COMMAND ----------

print("=" * 55)
print("STEP 3 / 4 — Derived Metrics")
print("=" * 55)

# COMMAND ----------

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/FA PJ (Basic)/20_transformation/22__derived_metrics"

# COMMAND ----------

# MAGIC %md ## 5. Analysis
# MAGIC Runs analysis queries — useful for validation after pipeline runs

# COMMAND ----------

print("=" * 55)
print("STEP 4 / 4 — Analysis")
print("=" * 55)

# COMMAND ----------

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/FA PJ (Basic)/30_analysis/31__company_analysis"

# COMMAND ----------

# MAGIC %md ## 7. Pipeline summary

# COMMAND ----------

pipeline_end = datetime.utcnow()
duration     = (pipeline_end - pipeline_start).total_seconds()

print(f"\n{'='*55}")
print(f"  Pipeline completed ✓")
print(f"{'='*55}")
print(f"  Started  : {pipeline_start.isoformat()} UTC")
print(f"  Finished : {pipeline_end.isoformat()} UTC")
print(f"  Duration : {duration:.1f}s")
print(f"  Tickers  : {run_tickers}")
print()

# Row counts for all output tables
for tbl in ["financials_raw", "financials", "financials_metrics"]:
    try:
        n = spark.table(f"{CATALOG}.{SCHEMA}.{tbl}").count()
        print(f"  {CATALOG}.{SCHEMA}.{tbl}: {n:,} rows")
    except Exception:
        pass

# COMMAND ----------

# MAGIC %md ## 8. Optional: optimize Delta tables
# MAGIC Set `run_optimization=true` in Job params to enable. Run at most once a week.

# COMMAND ----------

if run_optimization.lower() == "true":
    print("Running OPTIMIZE + VACUUM...")
    for tbl in ["financials", "financials_metrics"]:
        full = f"{CATALOG}.{SCHEMA}.{tbl}"
        spark.sql(f"OPTIMIZE {full}")
        spark.sql(f"VACUUM {full} RETAIN 168 HOURS")
        print(f"  ✓ {full}")
else:
    print("Optimization skipped (pass run_optimization=true to enable)")

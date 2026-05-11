# Databricks notebook source
# MAGIC %md
# MAGIC # 90_pipelines / 91_full_pipeline
# MAGIC
# MAGIC **Entry point for the Databricks Job.**
# MAGIC Runs the full ingestion → transformation → metrics pipeline in sequence.
# MAGIC
# MAGIC ```
# MAGIC 01_fetch_sec_xbrl        fetch from SEC API → write financials_raw
# MAGIC       ↓
# MAGIC 03_clean_and_merge       deduplicate → MERGE into financials
# MAGIC       ↓
# MAGIC 04_derived_metrics       FCF, margins, YoY growth → financials_metrics
# MAGIC ```
# MAGIC
# MAGIC **To schedule:** Databricks Jobs → Create Job → point to this notebook
# MAGIC Recommended cadence: quarterly (e.g. first Monday of Feb/May/Aug/Nov)

# COMMAND ----------
# MAGIC %md ## 0. Pipeline parameters
# MAGIC
# MAGIC These can be overridden at runtime via Databricks Job parameters:
# MAGIC `databricks jobs run-now --job-id <id> --notebook-params '{"tickers_override": "AAPL,TSLA"}'`

# COMMAND ----------

# Optional: pass a comma-separated list to scrape only specific tickers this run.
# Leave empty ("") to scrape all active tickers from config.
tickers_override = dbutils.widgets.get("tickers_override") if dbutils.widgets.getAll() else ""

run_optimization = dbutils.widgets.get("run_optimization") if dbutils.widgets.getAll() else "false"

# Share these with child notebooks via task values
dbutils.jobs.taskValues.set("tickers_override",  tickers_override)
dbutils.jobs.taskValues.set("run_optimization",  run_optimization)

print(f"tickers_override : '{tickers_override}' (empty = all active)")
print(f"run_optimization : {run_optimization}")

# COMMAND ----------
# MAGIC %md ## 1. Load config

# COMMAND ----------
# MAGIC %md ### ⚠️ Path note
# MAGIC `%run` paths are **relative to this notebook's location** in the Databricks workspace.
# MAGIC If you get a `NameError`, adjust the path below to match your folder structure.
# MAGIC Example: if this notebook is at `FA_PJ/10_ingestion/11__fetch_sec_xbrl`,
# MAGIC the config path should be `../00_config/01__tickers`.

# COMMAND ----------

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/FA PJ (Basic)/00_config/01__tickers"

# COMMAND ----------
# Validate config loaded — if this fails, fix the %run path in the cell above
try:
    _ = ACTIVE_TICKERS
    print(f"✓ Config loaded — {len(ACTIVE_TICKERS)} active tickers")
except NameError:
    raise NameError(
        "ACTIVE_TICKERS not defined — the %run above did not load 00_tickers correctly.\n"
        f"Current path used: '../00_config/01__tickers'\n"
        "Fix: right-click 00_tickers in your workspace → Copy URL/Path, "
        "then adjust the %run path to match."
    )

# COMMAND ----------

from datetime import datetime

pipeline_start = datetime.utcnow()
print(f"Pipeline started at {pipeline_start.isoformat()} UTC")

# Resolve ticker list for this run
if tickers_override.strip():
    run_tickers = [t.strip().upper() for t in tickers_override.split(",")]
    print(f"Running override subset: {run_tickers}")
else:
    run_tickers = ACTIVE_TICKERS
    print(f"Running all active tickers: {run_tickers}")

# Make available to child notebooks
spark.conf.set("pipeline.tickers", ",".join(run_tickers))
spark.conf.set("pipeline.run_optimization", run_optimization)

# COMMAND ----------
# MAGIC %md ## 2. Ingestion — fetch from SEC EDGAR

# COMMAND ----------

print("=" * 55)
print("STEP 1 / 3 — Ingestion")
print("=" * 55)

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/FA PJ (Basic)/10_ingestion/11__fetch_sec_xbrl"

# COMMAND ----------
# MAGIC %md ## 3. Transformation — clean & merge into fact table

# COMMAND ----------

print("=" * 55)
print("STEP 2 / 3 — Transformation")
print("=" * 55)

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/FA PJ (Basic)/20_transformation/21__clean_and_merge"

# COMMAND ----------
# MAGIC %md ## 4. Derived metrics — ratios, margins, YoY growth

# COMMAND ----------

print("=" * 55)
print("STEP 3 / 3 — Derived Metrics")
print("=" * 55)

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/FA PJ (Basic)/20_transformation/22__derived_metrics"

# COMMAND ----------
# MAGIC %md ## 5. Pipeline summary

# COMMAND ----------

pipeline_end = datetime.utcnow()
duration     = (pipeline_end - pipeline_start).total_seconds()

print(f"\n{'='*55}")
print(f"  Pipeline completed")
print(f"{'='*55}")
print(f"  Started  : {pipeline_start.isoformat()} UTC")
print(f"  Finished : {pipeline_end.isoformat()} UTC")
print(f"  Duration : {duration:.1f}s")
print(f"  Tickers  : {run_tickers}")

# Row counts per table
for tbl in [RAW_TABLE, TABLE, "financials_metrics"]:
    try:
        n = spark.table(f"{CATALOG}.{SCHEMA}.{tbl}").count()
        print(f"  {CATALOG}.{SCHEMA}.{tbl}: {n:,} rows")
    except Exception:
        pass

# COMMAND ----------
# MAGIC %md ## 6. Optional: optimize Delta tables
# MAGIC Skipped by default. Set `run_optimization=true` in Job params to enable.
# MAGIC Run at most once a week — it's expensive on large tables.

# COMMAND ----------

if run_optimization.lower() == "true":
    print("Running OPTIMIZE + VACUUM...")
    for tbl in [TABLE, "financials_metrics"]:
        full = f"{CATALOG}.{SCHEMA}.{tbl}"
        spark.sql(f"OPTIMIZE {full} ZORDER BY (ticker, statement, year, concept)")
        spark.sql(f"VACUUM {full} RETAIN 168 HOURS")
        print(f"  ✓ {full}")
else:
    print("Optimization skipped (pass run_optimization=true to enable)")


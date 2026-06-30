# Databricks notebook source
# MAGIC %md
# MAGIC # 30__analysis / 36__run_log_report
# MAGIC
# MAGIC **Read-only** report over the pipeline run-log written by `91__full_pipeline` (step 13c):
# MAGIC `main.config.pipeline_runs` (per-step timings + status) and
# MAGIC `main.config.pipeline_run_coverage` (per-run coverage/freshness snapshot).
# MAGIC
# MAGIC Shows the last `N` runs (duration, step count, failures) and the coverage/freshness
# MAGIC trend over time. Touches no data — pure SELECTs. A Streamlit "pipeline status" view is
# MAGIC a possible follow-up (out of scope here).
# MAGIC
# MAGIC **Databricks-only:** uses `spark` + `display()`.

# COMMAND ----------

# MAGIC %run "../00__config/01__tickers"

# COMMAND ----------

N_RUNS = 20  # how many recent runs to show

_runs_tbl = f"{CATALOG}.config.pipeline_runs"
_cov_tbl  = f"{CATALOG}.config.pipeline_run_coverage"

# COMMAND ----------

# MAGIC %md ## 1. Last N runs — duration, step count, failures

# COMMAND ----------

try:
    run_summary = spark.sql(f"""
        SELECT
            run_id,
            MIN(run_started_at)                              AS run_started_at,
            ROUND(SUM(minutes), 1)                           AS total_minutes,
            COUNT(*)                                         AS n_steps,
            SUM(CASE WHEN status = 'failed'  THEN 1 ELSE 0 END) AS n_failed,
            SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) AS n_skipped
        FROM {_runs_tbl}
        GROUP BY run_id
        ORDER BY run_started_at DESC
        LIMIT {N_RUNS}
    """)
    print(f"Last {N_RUNS} runs from {_runs_tbl}:")
    run_summary.display()
except Exception as e:
    print(f"⚠ Could not read {_runs_tbl} ({type(e).__name__}: {e}). "
          f"Has 91__full_pipeline run since the run-log was added?")

# COMMAND ----------

# MAGIC %md ## 2. Latest run — per-step breakdown (slowest first)

# COMMAND ----------

try:
    latest = spark.sql(f"""
        SELECT step, ROUND(minutes, 2) AS minutes, status, rows_written
        FROM {_runs_tbl}
        WHERE run_id = (
            SELECT run_id FROM {_runs_tbl}
            ORDER BY run_started_at DESC LIMIT 1
        )
        ORDER BY minutes DESC
    """)
    latest.display()
except Exception as e:
    print(f"⚠ Could not read {_runs_tbl}: {e}")

# COMMAND ----------

# MAGIC %md ## 3. Coverage + freshness trend (last N runs)

# COMMAND ----------

try:
    cov_trend = spark.sql(f"""
        SELECT
            run_id, run_started_at,
            total_tickers_ingested,
            favorites_in_metrics, total_favorites, favorites_pct,
            max_filed, staleness_days
        FROM {_cov_tbl}
        ORDER BY run_started_at DESC
        LIMIT {N_RUNS}
    """)
    print(f"Coverage/freshness trend from {_cov_tbl}:")
    cov_trend.display()
except Exception as e:
    print(f"⚠ Could not read {_cov_tbl} ({type(e).__name__}: {e}).")

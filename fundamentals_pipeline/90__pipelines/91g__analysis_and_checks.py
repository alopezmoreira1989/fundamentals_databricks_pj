# Databricks notebook source
# MAGIC %md
# MAGIC # 90__pipelines / 91g__analysis_and_checks
# MAGIC
# MAGIC **Part of the "Financial Analysis Pipeline" multi-task split, Phase 2** — the fan-in point
# MAGIC after the parallel `91d__intrinsic_value` / `91e__backtest` / `91f__forecasting` branch:
# MAGIC this task `depends_on` all three, so it (and everything after it) only starts once ALL
# MAGIC THREE have succeeded. Needed regardless of what each individual check reads — `37` reads
# MAGIC `financials_intrinsic_value` specifically, but the downstream `91h__export_dashboard_data`
# MAGIC exports `dashboard_backtest.parquet`/`dashboard_forecast.parquet` too, so the whole branch
# MAGIC must be complete before export runs, not just the one table this task's own checks happen
# MAGIC to touch.
# MAGIC
# MAGIC Bundles the non-fatal validation stages, run in this specific order (matching the original
# MAGIC single-task `91`'s own ordering):
# MAGIC ```
# MAGIC 31__company_analysis     validation queries
# MAGIC       ↓
# MAGIC 32__coverage_check       favorites made it through the pipeline? (non-fatal)
# MAGIC       ↓
# MAGIC 34__invariants_check     structural-invariant gate (non-fatal)
# MAGIC       ↓
# MAGIC 37__split_adjust_check   split-adjust regression guard (non-fatal)
# MAGIC ```
# MAGIC
# MAGIC **Reads `run_id` from `intrinsic_value` specifically** — an arbitrary but consistent
# MAGIC choice among its three dependencies (all three relay the identical value, originally from
# MAGIC `pipeline_pre22` via `pipeline_metrics`, so it doesn't matter which one this reads from, as
# MAGIC long as it's always the same one).

# COMMAND ----------

import time
from datetime import datetime

STEP_TIMINGS = []

def _record_step(name, t0, status="ok"):
    mins = (time.monotonic() - t0) / 60.0
    STEP_TIMINGS.append({"step": name, "minutes": mins, "status": status})
    print(f"  ⏱ {name}: {mins:.1f} min ({status})")

# COMMAND ----------

# MAGIC %run "../00__config/01__tickers"

# COMMAND ----------

run_id = dbutils.jobs.taskValues.get(
    taskKey="intrinsic_value", key="run_id",
    debugValue=datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"),
)
pipeline_start = datetime.strptime(run_id, "%Y%m%dT%H%M%SZ")
try:
    dbutils.jobs.taskValues.set(key="run_id", value=run_id)  # relay for 91h
except Exception:
    pass
print(f"✓ run_id={run_id} (relayed from intrinsic_value)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analysis

# COMMAND ----------

print("=" * 55)
print("Analysis")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../30__analysis/31__company_analysis"

# COMMAND ----------

_record_step("Analysis", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coverage check
# MAGIC Verifies that all favorite tickers made it through the full pipeline. Hard fails (inside
# MAGIC its own isolated child-notebook run) if >5% of favorites are missing — caught here so it
# MAGIC never aborts this task.

# COMMAND ----------

print("=" * 55)
print("Coverage Check")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

try:
    dbutils.notebook.run(
        "../30__analysis/32__coverage_check",
        timeout_seconds=120,
    )
    coverage_ok = True
except Exception as e:
    print(f"⚠ Coverage check failed: {e}")
    coverage_ok = False

_record_step("Coverage Check", _t0, status="ok" if coverage_ok else "failed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Invariants check
# MAGIC Structural-invariant gate over `main.financials.financials`. Non-fatal.

# COMMAND ----------

print("=" * 55)
print("Invariants Check")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

try:
    dbutils.notebook.run(
        "../30__analysis/34__invariants_check",
        timeout_seconds=300,
    )
    invariants_ok = True
except Exception as e:
    print(f"⚠ Invariants check failed: {e}")
    invariants_ok = False

_record_step("Invariants Check", _t0, status="ok" if invariants_ok else "failed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Split-adjust check
# MAGIC Read-only. Runs inline via `%run` (non-raising by design — see `37`'s own header doc).

# COMMAND ----------

print("=" * 55)
print("Split-Adjust Check")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../30__analysis/37__split_adjust_check"

# COMMAND ----------

split_adjust_ok = bool(globals().get("SPLIT_ADJUST_OK", False))
if not split_adjust_ok:
    print("⚠ Split-adjust check reported issues (non-fatal) — see output above.")
_record_step("Split-Adjust Check", _t0, status="ok" if split_adjust_ok else "failed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## This task's run-log telemetry

# COMMAND ----------

_runs_tbl = f"{CATALOG}.config.pipeline_runs"

try:
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_runs_tbl} (
            run_id         STRING    NOT NULL,
            run_started_at TIMESTAMP NOT NULL,
            step           STRING    NOT NULL,
            minutes        DOUBLE,
            status         STRING,
            rows_written   BIGINT
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true'
        )
    """)

    if STEP_TIMINGS:
        _runs_schema = StructType([
            StructField("run_id",         StringType(),    False),
            StructField("run_started_at", TimestampType(), False),
            StructField("step",           StringType(),    False),
            StructField("minutes",        DoubleType(),    True),
            StructField("status",         StringType(),    True),
        ])
        _runs_records = [
            {"run_id": run_id, "run_started_at": pipeline_start, "step": s["step"],
             "minutes": float(s["minutes"]), "status": s.get("status", "ok")}
            for s in STEP_TIMINGS
        ]
        spark.createDataFrame(_runs_records, schema=_runs_schema) \
            .createOrReplaceTempView("incoming_pipeline_runs_checks")
        spark.sql(f"""
            MERGE INTO {_runs_tbl} AS t
            USING incoming_pipeline_runs_checks AS s
            ON t.run_id = s.run_id AND t.step = s.step
            WHEN MATCHED THEN UPDATE SET
                t.run_started_at = s.run_started_at, t.minutes = s.minutes, t.status = s.status
            WHEN NOT MATCHED THEN INSERT (run_id, run_started_at, step, minutes, status)
                VALUES (s.run_id, s.run_started_at, s.step, s.minutes, s.status)
        """)
        print(f"✓ {len(_runs_records)} step rows → {_runs_tbl} (run_id={run_id})")
except Exception as _e:
    print(f"⚠ Run-log telemetry skipped ({type(_e).__name__}: {_e}) — non-fatal, task already succeeded.")

# COMMAND ----------

print("✓ 91g (analysis_and_checks) complete.")

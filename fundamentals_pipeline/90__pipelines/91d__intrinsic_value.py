# Databricks notebook source
# MAGIC %md
# MAGIC # 90__pipelines / 91d__intrinsic_value
# MAGIC
# MAGIC **Part of the "Financial Analysis Pipeline" multi-task split, Phase 2** — runs
# MAGIC `23__intrinsic_value` as its own Databricks Job Task, in PARALLEL with
# MAGIC `91e__backtest`/`91f__forecasting` (all three depend only on `pipeline_metrics`'s output,
# MAGIC never on each other — confirmed by grep against the real files: neither `71__run_backtest`
# MAGIC nor `24__forecasting` reads `financials_intrinsic_value`, and `23__intrinsic_value` reads
# MAGIC neither `backtest_results` nor `financials_forecast`). Running them concurrently instead
# MAGIC of sequentially is a real wall-clock win, not just finer failure isolation.
# MAGIC
# MAGIC `financials_intrinsic_value` — Graham Number, Graham Revised, DCF and Owner Earnings for
# MAGIC each historical fiscal year and TTM. Requires `22__derived_metrics` and
# MAGIC `12__fetch_market_data` (both already ran, in `pipeline_metrics`/`pipeline_pre22`).
# MAGIC
# MAGIC Relays `run_id` forward (read from `pipeline_metrics`, its direct dependency) so
# MAGIC `91g__analysis_and_checks` — which depends on all three parallel tasks, not just this one
# MAGIC — can read it from whichever of the three it picks (this task, by convention; see `91g`'s
# MAGIC own header doc).

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
    taskKey="pipeline_metrics", key="run_id",
    debugValue=datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"),
)
pipeline_start = datetime.strptime(run_id, "%Y%m%dT%H%M%SZ")
try:
    dbutils.jobs.taskValues.set(key="run_id", value=run_id)  # relay for 91g
except Exception:
    pass
print(f"✓ run_id={run_id} (relayed from pipeline_metrics)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Intrinsic Value

# COMMAND ----------

print("=" * 55)
print("Intrinsic Value")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../20__transformation/23__intrinsic_value"

# COMMAND ----------

_record_step("Intrinsic Value", _t0)

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
            .createOrReplaceTempView("incoming_pipeline_runs_iv")
        spark.sql(f"""
            MERGE INTO {_runs_tbl} AS t
            USING incoming_pipeline_runs_iv AS s
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

print("✓ 91d (intrinsic_value) complete.")

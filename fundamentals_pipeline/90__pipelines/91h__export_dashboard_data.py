# Databricks notebook source
# MAGIC %md
# MAGIC # 90__pipelines / 91h__export_dashboard_data
# MAGIC
# MAGIC **Part of the "Financial Analysis Pipeline" multi-task split, Phase 2** — runs
# MAGIC `51__export_dashboard_data` as its own Databricks Job Task. Writes to `/tmp/` on this
# MAGIC task's own driver, then copies to the `main.financials._publish` Unity Catalog Volume — a
# MAGIC real cross-task-readable location, since `91i__publish_github` (the next task) runs on a
# MAGIC different driver and can't see this one's `/tmp`.

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
    taskKey="analysis_and_checks", key="run_id",
    debugValue=datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"),
)
pipeline_start = datetime.strptime(run_id, "%Y%m%dT%H%M%SZ")
try:
    dbutils.jobs.taskValues.set(key="run_id", value=run_id)  # relay for 91i/91j
except Exception:
    pass
print(f"✓ run_id={run_id} (relayed from analysis_and_checks)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export dashboard data
# MAGIC A failure here aborts this task before reaching the telemetry cell below (→ task marked
# MAGIC FAILED, Job alerting fires) — the Delta tables are already committed by this point, so a
# MAGIC red task flags a stale Release instead of hiding it behind a green one.

# COMMAND ----------

print("=" * 55)
print("Export dashboard data")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../50__publish/51__export_dashboard_data"

# COMMAND ----------

_record_step("Export dashboard data", _t0)

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
            .createOrReplaceTempView("incoming_pipeline_runs_export")
        spark.sql(f"""
            MERGE INTO {_runs_tbl} AS t
            USING incoming_pipeline_runs_export AS s
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

print("✓ 91h (export_dashboard_data) complete.")

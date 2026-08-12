# Databricks notebook source
# MAGIC %md
# MAGIC # 90__pipelines / 91b__pipeline_metrics
# MAGIC
# MAGIC **Second of three Databricks Job Tasks** in the "Financial Analysis Pipeline" multi-task
# MAGIC split (Phase 1 — see `91a__pipeline_pre22`'s header for the full rationale). Isolates
# MAGIC `22__derived_metrics` as its own task: this is where a real production incident happened
# MAGIC (2026-08, a Spark checkpoint/executor-loss error), and it's the step whose retry cost was
# MAGIC most worth isolating — before this split, a failure here forced re-running all of
# MAGIC ingestion and the clean/dedup chain (`91a`'s steps) again on retry.
# MAGIC
# MAGIC `22__derived_metrics` (unlike `24__forecasting`) does NOT read `ACTIVE_TICKERS` anywhere —
# MAGIC confirmed by grep against the real file — so this task needs no `tickers_override` widget
# MAGIC at all, only `CATALOG`/`SCHEMA` from `01__tickers`.
# MAGIC
# MAGIC **Relays `run_id`**: reads it from `91a` (a direct `depends_on` dependency, so
# MAGIC `dbutils.jobs.taskValues.get(taskKey="pipeline_pre22", ...)` is reliable) and re-publishes
# MAGIC it as its own taskValue, so `91c` (which depends on `91b`, not directly on `91a`) can read
# MAGIC the SAME run_id from its own direct dependency — `dbutils.jobs.taskValues.get()` reads
# MAGIC values set by a task's own `depends_on` entries, not arbitrary upstream tasks, so a linear
# MAGIC chain needs each link to relay the value forward rather than every task reaching back to
# MAGIC the original source directly.

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

# `debugValue` lets this task run standalone (outside the Job's task-dependency graph) without
# erroring — falls back to "now" so a lone manual run still gets a usable run_id.
run_id = dbutils.jobs.taskValues.get(
    taskKey="pipeline_pre22", key="run_id",
    debugValue=datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"),
)
pipeline_start = datetime.strptime(run_id, "%Y%m%dT%H%M%SZ")
try:
    dbutils.jobs.taskValues.set(key="run_id", value=run_id)  # relay forward for 91c
except Exception:
    pass
print(f"✓ run_id={run_id} (relayed from pipeline_pre22)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Derived metrics
# MAGIC `financials_metrics` — margins, FCF, YoY growth, leverage, valuation ratios

# COMMAND ----------

print("=" * 55)
print("STEP 7 / 9 — Derived Metrics")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../20__transformation/22__derived_metrics"

# COMMAND ----------

_record_step("Derived Metrics", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. This task's run-log telemetry
# MAGIC Same MERGE-into-`pipeline_runs` idiom as `91a`'s own tail cell — see its header doc.

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
            .createOrReplaceTempView("incoming_pipeline_runs_metrics")
        spark.sql(f"""
            MERGE INTO {_runs_tbl} AS t
            USING incoming_pipeline_runs_metrics AS s
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

print("✓ 91b (metrics) complete — financials_metrics refreshed.")

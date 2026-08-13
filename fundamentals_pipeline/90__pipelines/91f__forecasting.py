# Databricks notebook source
# MAGIC %md
# MAGIC # 90__pipelines / 91f__forecasting
# MAGIC
# MAGIC **Part of the "Financial Analysis Pipeline" multi-task split, Phase 2** — runs
# MAGIC `24__forecasting` as its own Databricks Job Task, in PARALLEL with
# MAGIC `91d__intrinsic_value`/`91e__backtest` — see `91d`'s header doc for why this is safe.
# MAGIC This is the single most expensive step in the whole pipeline (trains 15 LightGBM
# MAGIC quantile regressors + up to 15 binary loss classifiers, cross-sectionally over the full
# MAGIC ~2,600+ ticker universe, every run) — isolating it as its own task means a training
# MAGIC failure here no longer forces re-running `23`/`71` too, and running it in parallel with
# MAGIC them (rather than after) removes it from the critical path entirely when it's not the
# MAGIC slowest of the three.
# MAGIC
# MAGIC `financials_forecast` — requires `22__derived_metrics` and `12__fetch_market_data` (both
# MAGIC already ran). Registers its own `tickers_override` widget directly — the one post-22 stage
# MAGIC that reads `ACTIVE_TICKERS` (confirmed by grep; `23`/`71`/the analysis/checks stages don't).
# MAGIC
# MAGIC Relays `run_id` forward the same way `91d` does — see its header doc.

# COMMAND ----------

import time
from datetime import datetime

STEP_TIMINGS = []

def _record_step(name, t0, status="ok"):
    mins = (time.monotonic() - t0) / 60.0
    STEP_TIMINGS.append({"step": name, "minutes": mins, "status": status})
    print(f"  ⏱ {name}: {mins:.1f} min ({status})")

# COMMAND ----------

dbutils.widgets.text("tickers_override", "", "tickers_override")
tickers_override = dbutils.widgets.get("tickers_override")

# COMMAND ----------

# MAGIC %run "../00__config/01__tickers"

# COMMAND ----------

if tickers_override:
    ACTIVE_TICKERS = [t.strip() for t in tickers_override.split(",") if t.strip()]
    print(f"✓ Override mode — {len(ACTIVE_TICKERS)} tickers: {ACTIVE_TICKERS}")
else:
    tickers_df = spark.table(f"{CATALOG}.config.tickers")
    ACTIVE_TICKERS = [row.ticker for row in tickers_df.select("ticker").collect()]
    print(f"✓ Config loaded — {len(ACTIVE_TICKERS)} tickers from main.config.tickers")

run_id = dbutils.jobs.taskValues.get(
    taskKey="pipeline_metrics", key="run_id",
    debugValue=datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"),
)
pipeline_start = datetime.strptime(run_id, "%Y%m%dT%H%M%SZ")
try:
    dbutils.jobs.taskValues.set(key="run_id", value=run_id)  # relay, in case 91g ever reads from this branch
except Exception:
    pass
print(f"✓ run_id={run_id} (relayed from pipeline_metrics)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Forecasting

# COMMAND ----------

print("=" * 55)
print("Forecasting")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../20__transformation/24__forecasting"

# COMMAND ----------

_record_step("Forecasting", _t0)

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
            .createOrReplaceTempView("incoming_pipeline_runs_forecasting")
        spark.sql(f"""
            MERGE INTO {_runs_tbl} AS t
            USING incoming_pipeline_runs_forecasting AS s
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

print("✓ 91f (forecasting) complete.")

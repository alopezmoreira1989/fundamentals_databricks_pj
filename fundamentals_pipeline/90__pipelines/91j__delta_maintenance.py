# Databricks notebook source
# MAGIC %md
# MAGIC # 90__pipelines / 91j__delta_maintenance
# MAGIC
# MAGIC **Final task of the "Financial Analysis Pipeline" multi-task split, Phase 2.** Runs
# MAGIC `93__delta_maintenance` (OPTIMIZE/VACUUM, self-gated on `run_optimization` — a default run
# MAGIC is a no-op), then writes the run's final `pipeline_run_coverage` snapshot — one row per
# MAGIC whole run, so it belongs on whichever task runs last — and prints the overall pipeline
# MAGIC summary.
# MAGIC
# MAGIC Registers its own `run_optimization` widget directly (needed only by `93`).

# COMMAND ----------

import time
from datetime import datetime

STEP_TIMINGS = []

def _record_step(name, t0, status="ok"):
    mins = (time.monotonic() - t0) / 60.0
    STEP_TIMINGS.append({"step": name, "minutes": mins, "status": status})
    print(f"  ⏱ {name}: {mins:.1f} min ({status})")

# COMMAND ----------

dbutils.widgets.text("run_optimization", "false", "run_optimization")
run_optimization = dbutils.widgets.get("run_optimization")

# COMMAND ----------

# MAGIC %run "../00__config/01__tickers"

# COMMAND ----------

run_id = dbutils.jobs.taskValues.get(
    taskKey="publish_github", key="run_id",
    debugValue=datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"),
)
pipeline_start = datetime.strptime(run_id, "%Y%m%dT%H%M%SZ")
print(f"✓ run_id={run_id} (relayed from publish_github)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta maintenance (OPTIMIZE / VACUUM)
# MAGIC Self-gates on `run_optimization` (this task's own widget, registered above), so a default
# MAGIC run is a no-op. Set `run_optimization=true` in Job params to enable; run at most once a
# MAGIC week.

# COMMAND ----------

# MAGIC %run "./93__delta_maintenance"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline summary

# COMMAND ----------

pipeline_end = datetime.utcnow()
duration     = (pipeline_end - pipeline_start).total_seconds()

print(f"\n{'='*55}")
print("  Pipeline complete ✓ (pipeline_pre22 → ... → pipeline_delta_maintenance)")
print(f"{'='*55}")
print(f"  Started  : {pipeline_start.isoformat()} UTC  (pipeline_pre22's own start)")
print(f"  Finished : {pipeline_end.isoformat()} UTC  (this task's own end)")
print(f"  Duration : {duration:.1f}s ({duration/60:.1f} min)  — total across all tasks")
print()

summary_tables = [
    ("config",      "tickers"),
    ("config",      "concept_hierarchy"),
    ("config",      "metrics_hierarchy"),
    (SCHEMA,        "financials_raw"),
    (SCHEMA,        "financials"),
    (SCHEMA,        "market_data"),
    (SCHEMA,        "market_cap_asof"),
    (SCHEMA,        "financials_metrics"),
    (SCHEMA,        "financials_intrinsic_value"),
]

for schema, tbl in summary_tables:
    full = f"{CATALOG}.{schema}.{tbl}"
    try:
        n = spark.table(full).count()
        print(f"  {full}: {n:,} rows")
    except Exception:
        print(f"  {full}: (not found)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## This task's run-log telemetry + the run's final coverage snapshot
# MAGIC
# MAGIC `pipeline_run_coverage` is ONE row per whole run (not per task), so it belongs here, on
# MAGIC whichever task runs last.

# COMMAND ----------

_runs_tbl = f"{CATALOG}.config.pipeline_runs"
_cov_tbl  = f"{CATALOG}.config.pipeline_run_coverage"

try:
    from pyspark.sql.types import (
        DateType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    # ── pipeline_runs (this task's own steps) ───────────────────────────────────
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
            .createOrReplaceTempView("incoming_pipeline_runs_maint")
        spark.sql(f"""
            MERGE INTO {_runs_tbl} AS t
            USING incoming_pipeline_runs_maint AS s
            ON t.run_id = s.run_id AND t.step = s.step
            WHEN MATCHED THEN UPDATE SET
                t.run_started_at = s.run_started_at, t.minutes = s.minutes, t.status = s.status
            WHEN NOT MATCHED THEN INSERT (run_id, run_started_at, step, minutes, status)
                VALUES (s.run_id, s.run_started_at, s.step, s.minutes, s.status)
        """)
        print(f"✓ {len(_runs_records)} step rows → {_runs_tbl} (run_id={run_id})")

    # ── pipeline_run_coverage (one row per whole run) ───────────────────────────
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_cov_tbl} (
            run_id                 STRING    NOT NULL,
            run_started_at         TIMESTAMP NOT NULL,
            total_tickers_ingested INT,
            total_favorites        INT,
            favorites_in_metrics   INT,
            favorites_pct          DOUBLE,
            max_filed              DATE,
            staleness_days         INT
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true'
        )
    """)

    _cov = spark.sql(f"""
        SELECT
            (SELECT COUNT(DISTINCT ticker) FROM {CATALOG}.{SCHEMA}.financials)                  AS total_tickers_ingested,
            (SELECT COUNT(*) FROM {CATALOG}.config.tickers WHERE is_favorite = true)            AS total_favorites,
            (SELECT COUNT(DISTINCT t.ticker)
               FROM {CATALOG}.config.tickers t
               JOIN (SELECT DISTINCT ticker FROM {CATALOG}.{SCHEMA}.financials_metrics) m
                 ON m.ticker = t.ticker
              WHERE t.is_favorite = true)                                                       AS favorites_in_metrics,
            (SELECT MAX(filed) FROM {CATALOG}.{SCHEMA}.financials_raw)                           AS max_filed,
            DATEDIFF(CURRENT_DATE(), (SELECT MAX(filed) FROM {CATALOG}.{SCHEMA}.financials_raw)) AS staleness_days
    """).collect()[0]

    _tot_fav = int(_cov["total_favorites"] or 0)
    _fav_pct = round(float(_cov["favorites_in_metrics"] or 0) / _tot_fav * 100, 1) if _tot_fav else None

    _cov_schema = StructType([
        StructField("run_id",                 StringType(),    False),
        StructField("run_started_at",         TimestampType(), False),
        StructField("total_tickers_ingested", IntegerType(),   True),
        StructField("total_favorites",        IntegerType(),   True),
        StructField("favorites_in_metrics",   IntegerType(),   True),
        StructField("favorites_pct",          DoubleType(),    True),
        StructField("max_filed",              DateType(),      True),
        StructField("staleness_days",         IntegerType(),   True),
    ])
    _cov_record = [{
        "run_id": run_id, "run_started_at": pipeline_start,
        "total_tickers_ingested": int(_cov["total_tickers_ingested"] or 0),
        "total_favorites": _tot_fav,
        "favorites_in_metrics": int(_cov["favorites_in_metrics"] or 0),
        "favorites_pct": _fav_pct,
        "max_filed": _cov["max_filed"],
        "staleness_days": int(_cov["staleness_days"]) if _cov["staleness_days"] is not None else None,
    }]
    spark.createDataFrame(_cov_record, schema=_cov_schema) \
        .createOrReplaceTempView("incoming_pipeline_run_coverage")
    spark.sql(f"""
        MERGE INTO {_cov_tbl} AS t
        USING incoming_pipeline_run_coverage AS s
        ON t.run_id = s.run_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"✓ coverage snapshot → {_cov_tbl} "
          f"(favorites {_cov['favorites_in_metrics']}/{_tot_fav} = {_fav_pct}%, "
          f"tickers={_cov['total_tickers_ingested']}, staleness={_cov['staleness_days']}d)")
except Exception as _e:
    print(f"⚠ Run-log telemetry skipped ({type(_e).__name__}: {_e}) — non-fatal, task already succeeded.")

# COMMAND ----------

print("✓ Pipeline complete — Delta tables refreshed and dashboard Release published.")

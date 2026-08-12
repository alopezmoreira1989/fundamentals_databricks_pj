# Databricks notebook source
# MAGIC %md
# MAGIC # 90__pipelines / 91c__pipeline_post22
# MAGIC
# MAGIC **Third of three Databricks Job Tasks** in the "Financial Analysis Pipeline" multi-task
# MAGIC split (Phase 1 — see `91a__pipeline_pre22`'s header for the full rationale). Covers
# MAGIC valuation, backtest, forecasting, the non-fatal validation checks, and publish — everything
# MAGIC downstream of `financials_metrics`.
# MAGIC
# MAGIC ```
# MAGIC 23__intrinsic_value             Graham, Graham revised, DCF, Owner Earnings (FY + TTM)
# MAGIC       ↓
# MAGIC 71__run_backtest                investor-archetype screens over history
# MAGIC       ↓
# MAGIC 24__forecasting                 10-year cross-sectional ML scenario forecasts
# MAGIC       ↓
# MAGIC 31__company_analysis            validation queries
# MAGIC       ↓
# MAGIC 32__coverage_check (non-fatal)  favorites made it through the pipeline?
# MAGIC       ↓
# MAGIC 34__invariants_check (non-fatal) structural-invariant gate
# MAGIC       ↓
# MAGIC 37__split_adjust_check (non-fatal) split-adjust regression guard
# MAGIC       ↓
# MAGIC 51__export_dashboard_data       slice + write parquet artifacts to /tmp/
# MAGIC       ↓
# MAGIC 52__publish_to_github           upload artifacts as GitHub Release assets (latest tag)
# MAGIC       ↓
# MAGIC 93__delta_maintenance           OPTIMIZE/VACUUM (self-gated on run_optimization, no-op by default)
# MAGIC ```
# MAGIC
# MAGIC Registers its own `tickers_override` (needed only by `24__forecasting`, the one post-22
# MAGIC stage that reads `ACTIVE_TICKERS`) and `run_optimization` (needed only by `93`) widgets
# MAGIC directly — same "this task IS the top-level notebook, so a direct widget read is reliable"
# MAGIC reasoning as `91a`'s header doc. Reads `run_id` from `91b` (its direct `depends_on`
# MAGIC dependency, which itself relayed the value from `91a`) and writes the run's final
# MAGIC `pipeline_run_coverage` snapshot — one row per whole run, so it belongs on whichever task
# MAGIC runs last.

# COMMAND ----------

import time
from datetime import datetime

STEP_TIMINGS = []

def _record_step(name, t0, status="ok"):
    mins = (time.monotonic() - t0) / 60.0
    STEP_TIMINGS.append({"step": name, "minutes": mins, "status": status})
    print(f"  ⏱ {name}: {mins:.1f} min ({status})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Pipeline parameters (this task's own subset)
# MAGIC
# MAGIC - `tickers_override`: only affects `24__forecasting`'s own `ACTIVE_TICKERS` -- everything
# MAGIC   else in this task processes the whole `financials`/`financials_metrics` tables regardless.
# MAGIC - `run_optimization`: `true` to run OPTIMIZE + VACUUM at the end (via `93`).

# COMMAND ----------

dbutils.widgets.text("tickers_override", "",      "tickers_override")
dbutils.widgets.text("run_optimization", "false", "run_optimization")

tickers_override = dbutils.widgets.get("tickers_override")
run_optimization = dbutils.widgets.get("run_optimization")

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

# `debugValue` lets this task run standalone without erroring.
run_id = dbutils.jobs.taskValues.get(
    taskKey="pipeline_metrics", key="run_id",
    debugValue=datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"),
)
pipeline_start = datetime.strptime(run_id, "%Y%m%dT%H%M%SZ")
print(f"✓ run_id={run_id} (relayed from pipeline_metrics)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Intrinsic Value
# MAGIC `financials_intrinsic_value` — Graham Number, Graham Revised, DCF and Owner Earnings
# MAGIC for each historical fiscal year and TTM (rolling 4 quarters). Requires `22__derived_metrics`
# MAGIC and `12__fetch_market_data` to have run first (both ran in earlier tasks).

# COMMAND ----------

print("=" * 55)
print("STEP 8 / 9 (post-22) — Intrinsic Value")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../20__transformation/23__intrinsic_value"

# COMMAND ----------

_record_step("Intrinsic Value", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9b. Backtest
# MAGIC Applies investor-archetype screens to historical fundamentals and reports forward
# MAGIC returns. Reads `financials_metrics`, `financials_raw`, `market_prices_daily`.

# COMMAND ----------

print("=" * 55)
print("STEP 8b / 9 (post-22) — Backtest")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../70__backtest/71__run_backtest"

# COMMAND ----------

_record_step("Backtest", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9c. Forecasting
# MAGIC `financials_forecast` — 10-year cross-sectional ML scenario forecasts. Requires
# MAGIC `22__derived_metrics` and `12__fetch_market_data` to have run first (both ran earlier).

# COMMAND ----------

print("=" * 55)
print("STEP 8c / 9 (post-22) — Forecasting")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../20__transformation/24__forecasting"

# COMMAND ----------

_record_step("Forecasting", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Analysis
# MAGIC Runs analysis queries — useful for validation after pipeline runs

# COMMAND ----------

print("=" * 55)
print("STEP 9 / 9 (post-22) — Analysis")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../30__analysis/31__company_analysis"

# COMMAND ----------

_record_step("Analysis", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Coverage check
# MAGIC Verifies that all favorite tickers made it through the full pipeline. Hard fails (inside
# MAGIC its own isolated child-notebook run) if >5% of favorites are missing — caught here so it
# MAGIC never aborts this task.

# COMMAND ----------

print("=" * 55)
print("STEP 10 / 9 (post-22) — Coverage Check")
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
# MAGIC ## 11b. Invariants check
# MAGIC Structural-invariant gate over `main.financials.financials`. Non-fatal: a hard fail is
# MAGIC logged and the task continues.

# COMMAND ----------

print("=" * 55)
print("STEP 10b / 9 (post-22) — Invariants Check")
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
# MAGIC ## 11c. Split-adjust check
# MAGIC Read-only. Runs inline via `%run` (non-raising by design — see `37`'s own header doc).

# COMMAND ----------

print("=" * 55)
print("STEP 10c / 9 (post-22) — Split-Adjust Check")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../30__analysis/37__split_adjust_check"

# COMMAND ----------

# `37` sets SPLIT_ADJUST_OK in this task's own shared session (inline %run) AND publishes it via
# taskValues (Phase 0) for any future consumer — this task still just reads the plain global,
# since it's in the SAME session as 37 (inline %run, not a separate task).
split_adjust_ok = bool(globals().get("SPLIT_ADJUST_OK", False))
if not split_adjust_ok:
    print("⚠ Split-adjust check reported issues (non-fatal) — see output above.")
_record_step("Split-Adjust Check", _t0, status="ok" if split_adjust_ok else "failed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Export dashboard data
# MAGIC Written to `/tmp/` on the driver. Consumed by the next step (GitHub publish) in the SAME
# MAGIC task — this pair stays bundled in one task deliberately (Phase 2, not this pass, is where
# MAGIC the driver-local `/tmp` handoff gets replaced with a Unity Catalog Volume so it could
# MAGIC safely split across task boundaries; see the plan).

# COMMAND ----------

print("=" * 55)
print("STEP 11 / 9 (post-22) — Export dashboard data")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../50__publish/51__export_dashboard_data"

# COMMAND ----------

_record_step("Export dashboard data", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Publish to GitHub Release
# MAGIC Uploads the `/tmp/` artifacts as assets on the `latest` GitHub release. Requires the
# MAGIC `github/github_pat` Databricks secret — see `50__publish/README.md`.

# COMMAND ----------

print("=" * 55)
print("STEP 12 / 9 (post-22) — Publish to GitHub")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../50__publish/52__publish_to_github"

# COMMAND ----------

_record_step("Publish to GitHub", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Delta maintenance (OPTIMIZE / VACUUM)
# MAGIC Runs `93__delta_maintenance` inline via `%run`. Self-gates on `run_optimization`
# MAGIC (this task's own widget, registered above), so a default run is a no-op.

# COMMAND ----------

# MAGIC %run "./93__delta_maintenance"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Pipeline summary

# COMMAND ----------

pipeline_end = datetime.utcnow()
duration     = (pipeline_end - pipeline_start).total_seconds()

print(f"\n{'='*55}")
print("  Pipeline complete ✓ (91a → 91b → 91c)")
print(f"{'='*55}")
print(f"  Started  : {pipeline_start.isoformat()} UTC  (pipeline_pre22's own start)")
print(f"  Finished : {pipeline_end.isoformat()} UTC  (this task's own end)")
print(f"  Duration : {duration:.1f}s ({duration/60:.1f} min)  — total across all 3 tasks")
print(f"  Tickers  : {len(ACTIVE_TICKERS):,}")
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
# MAGIC ## 16. This task's run-log telemetry + the run's final coverage snapshot
# MAGIC
# MAGIC Same MERGE-into-`pipeline_runs` idiom as `91a`/`91b`'s own tail cells. Also writes
# MAGIC `pipeline_run_coverage` — ONE row per whole run (not per task), so it belongs here, on
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
            .createOrReplaceTempView("incoming_pipeline_runs_post22")
        spark.sql(f"""
            MERGE INTO {_runs_tbl} AS t
            USING incoming_pipeline_runs_post22 AS s
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

# MAGIC %md
# MAGIC ## 17. Done
# MAGIC Export + publish ran inline above. A failure in either aborts this task before reaching
# MAGIC here (→ this task marked FAILED, Job alerting fires), so getting here means the Delta
# MAGIC tables are refreshed **and** the public GitHub Release is current.

# COMMAND ----------

print("✓ 91c (post-22) complete — Delta tables refreshed and dashboard Release published.")

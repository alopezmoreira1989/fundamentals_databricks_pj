# Databricks notebook source
# MAGIC %md
# MAGIC ## 0a. Session dependencies
# MAGIC `lxml` is required by `13__fetch_dimensional_10k` (XBRL instance parsing) and is
# MAGIC NOT preinstalled on serverless. Install it FIRST — `%pip` restarts the Python
# MAGIC interpreter, so it must run before any state is built. It then persists for the
# MAGIC notebooks pulled in via `%run` (shared session), including `13`.

# COMMAND ----------

# MAGIC %pip install lxml

# COMMAND ----------

# Per-step wall-clock timing. Defined right AFTER the %pip cell because %pip restarts the
# Python interpreter (wiping earlier state). STEP_TIMINGS accumulates {step, minutes} as each
# %run completes; the summary cell at the end prints + persists them. Relies on shared-session
# state surviving across %run cells — the SAME Databricks-only assumption `pipeline_start`
# already depends on. `_record_step` runs only AFTER its %run, so a failed step (which aborts
# the run) is simply absent → abort behaviour is unchanged.
import time

STEP_TIMINGS = []

def _record_step(name, t0, status="ok"):
    # status ∈ {"ok","failed","skipped"} — defaults to "ok" so existing call sites are
    # unchanged. Non-fatal steps (Coverage/Invariants) pass their own status; a fatal step
    # aborts the run before reaching its _record_step, so it never gets an "ok" row.
    mins = (time.monotonic() - t0) / 60.0
    STEP_TIMINGS.append({"step": name, "minutes": mins, "status": status})
    print(f"  ⏱ {name}: {mins:.1f} min ({status})")

# COMMAND ----------

# MAGIC %md
# MAGIC # 90_pipelines / 91__full_pipeline
# MAGIC
# MAGIC **Entry point for the Databricks Job.**
# MAGIC Runs the full ingestion → transformation → analysis pipeline in sequence.
# MAGIC
# MAGIC ```
# MAGIC favorites.json              edit favorites (00_config/favorites.json in the repo)
# MAGIC concept_hierarchy.json      edit concept hierarchy (00_config/ in the repo)
# MAGIC metrics_hierarchy.json      edit derived metrics hierarchy (00_config/ in the repo)
# MAGIC valuation_assumptions.json  edit valuation assumptions (00_config/ in the repo)
# MAGIC       ↓
# MAGIC 02__tickers_master              build main.config.tickers
# MAGIC       ↓
# MAGIC 03__concept_hierarchy_master    build main.config.concept_hierarchy
# MAGIC       ↓
# MAGIC 04__metrics_hierarchy_master    build main.config.metrics_hierarchy
# MAGIC       ↓
# MAGIC 11__fetch_sec_xbrl              fetch from SEC API → financials_raw
# MAGIC       ↓
# MAGIC 12__fetch_market_data           fetch from Yahoo Finance → market_data
# MAGIC       ↓
# MAGIC 21__clean_and_merge             deduplicate → MERGE into financials (FY rows)
# MAGIC       ↓
# MAGIC 21b__derive_quarterly           derive Q1–Q4 standalone rows (Q4 = FY − YTD_Q3 fallback)
# MAGIC       ↓
# MAGIC 22__derived_metrics             margins, FCF, YoY, leverage, valuation ratios
# MAGIC       ↓
# MAGIC 23__intrinsic_value             Graham, Graham revised, DCF, Owner Earnings (FY + TTM)
# MAGIC       ↓
# MAGIC 31__company_analysis            validation queries
# MAGIC       ↓
# MAGIC 51__export_dashboard_data       slice + write parquet artifacts to /tmp/
# MAGIC       ↓
# MAGIC 52__publish_to_github           upload artifacts as GitHub Release assets (latest tag)
# MAGIC ```
# MAGIC
# MAGIC > **Note:** `02__tickers_master`, `03__concept_hierarchy_master` and `04__metrics_hierarchy_master` live in `00_config/`.
# MAGIC > This pipeline assumes `main.config.tickers` already exists — it is built
# MAGIC > manually with `02__tickers_master`. The hierarchies (`concept_hierarchy` and `metrics_hierarchy`)
# MAGIC > are rebuilt automatically on every run from their respective JSONs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Pipeline parameters
# MAGIC
# MAGIC Override at runtime via Databricks Job parameters:
# MAGIC `{"tickers_override": "AAPL,TSLA", "run_optimization": "false"}`
# MAGIC
# MAGIC - `tickers_override`: comma-separated ticker list (overrides `main.config.tickers`)
# MAGIC - `run_optimization`: `true` to run OPTIMIZE + VACUUM at the end
# MAGIC - `force_full_refresh`: `true` to re-ingest ALL tickers ignoring the
# MAGIC   3-day freshness guard in `11__fetch_sec_xbrl` (use this for a backfill
# MAGIC   after a logic change in ingest/merge)
# MAGIC
# MAGIC > The ticker universe (`main.config.tickers`) is maintained manually with
# MAGIC > `02__tickers_master`. The concept and metrics hierarchies
# MAGIC > (`main.config.concept_hierarchy` and `main.config.metrics_hierarchy`)
# MAGIC > are rebuilt automatically on every run from their respective JSONs.

# COMMAND ----------

dbutils.widgets.text("tickers_override",   "",      "tickers_override")
dbutils.widgets.text("run_optimization",   "false", "run_optimization")
dbutils.widgets.text("rebuild_config",     "false", "rebuild_config")
dbutils.widgets.text("force_full_refresh", "false", "force_full_refresh")

tickers_override   = dbutils.widgets.get("tickers_override")
run_optimization   = dbutils.widgets.get("run_optimization")
rebuild_config     = dbutils.widgets.get("rebuild_config")
force_full_refresh = dbutils.widgets.get("force_full_refresh")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load config

# COMMAND ----------

_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../00_config/01__tickers"

# COMMAND ----------

_record_step("Load config", _t0)

# COMMAND ----------

from datetime import datetime

if tickers_override:
    ACTIVE_TICKERS = [t.strip() for t in tickers_override.split(",") if t.strip()]
    print(f"✓ Override mode — {len(ACTIVE_TICKERS)} tickers: {ACTIVE_TICKERS}")
else:
    tickers_df = spark.table(f"{CATALOG}.config.tickers")
    ACTIVE_TICKERS = [row.ticker for row in tickers_df.select("ticker").collect()]
    print(f"✓ Config loaded — {len(ACTIVE_TICKERS)} tickers from main.config.tickers")

if not ACTIVE_TICKERS:
    raise ValueError("No tickers found — run 02__tickers_master first.")

pipeline_start = datetime.utcnow()
print(f"Pipeline started at {pipeline_start.isoformat()} UTC")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Config table status
# MAGIC
# MAGIC `main.config.tickers` must exist before running this pipeline — it is built
# MAGIC manually with `02__tickers_master` (when you edit `favorites.json`).
# MAGIC `main.config.concept_hierarchy` and `main.config.metrics_hierarchy` are rebuilt
# MAGIC automatically in steps 3 and 4.

# COMMAND ----------

# The main.config.tickers rebuild does not run automatically because it makes
# calls to Wikipedia (S&P 500) and iShares (Russell 3000) that rarely change.
#
# To refresh the ticker universe, run 02__tickers_master manually
# (e.g. after editing favorites.json in the repo).
#
# The hierarchies (next steps) are always rebuilt — they are cheap.

if rebuild_config.lower() == "true":
    print("⚠ rebuild_config=true detected, but the ticker rebuild is done")
    print("  manually — run 02__tickers_master separately.")
else:
    print("⊘ Skipping ticker rebuild — using main.config.tickers as-is")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Concept hierarchy
# MAGIC `main.config.concept_hierarchy` — concept hierarchy for grouping and ordering in the dashboard.
# MAGIC
# MAGIC Always rebuilt (cheap — ~48 rows, <5s) to ensure the table
# MAGIC reflects the current state of `concept_hierarchy.json` in the repo.

# COMMAND ----------

print("=" * 55)
print("STEP 1 / 12 — Concept Hierarchy")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../00_config/03__concept_hierarchy_master"

# COMMAND ----------

_record_step("Concept Hierarchy", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Metrics hierarchy
# MAGIC `main.config.metrics_hierarchy` — derived metrics hierarchy (category → subcategory)
# MAGIC for grouping and ordering in the dashboard.
# MAGIC
# MAGIC Always rebuilt (cheap — ~40 rows, <2s) to ensure the table
# MAGIC reflects the current state of `metrics_hierarchy.json` in the repo.

# COMMAND ----------

print("=" * 55)
print("STEP 2 / 12 — Metrics Hierarchy")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../00_config/04__metrics_hierarchy_master"

# COMMAND ----------

_record_step("Metrics Hierarchy", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Ingestion — fetch from SEC EDGAR
# MAGIC `financials_raw` — append-only audit log

# COMMAND ----------

print("=" * 55)
print("STEP 3 / 12 — SEC Ingestion")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../10_ingestion/11__fetch_sec_xbrl"

# COMMAND ----------

_record_step("SEC Ingestion", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5b. Ingestion — dimensional 10-K facts (combined-filers) ⚠️ experimental
# MAGIC `financials_raw` — retrieves annual primary totals for combined-filers
# MAGIC (REIT + Operating Partnership, e.g. SKT) that SEC `companyfacts` omits because they are
# MAGIC dimensional facts. **No-op** if no ticker in `favorites.json` has the
# MAGIC `combined_filer_member` field. Must run AFTER `11` (syncs `scraped_at`) and
# MAGIC BEFORE `21`.

# COMMAND ----------

print("=" * 55)
print("STEP 3b / 12 — Dimensional 10-K (combined-filers)")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../10_ingestion/13__fetch_dimensional_10k"

# COMMAND ----------

_record_step("Dimensional 10-K", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Ingestion — fetch market data from Yahoo Finance
# MAGIC `market_data` — year-end prices + market cap

# COMMAND ----------

print("=" * 55)
print("STEP 4 / 12 — Market Data")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../10_ingestion/12__fetch_market_data"

# COMMAND ----------

_record_step("Market Data", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Clean & merge → fact table
# MAGIC `financials` — deduplicated, clean long-format fact table

# COMMAND ----------

print("=" * 55)
print("STEP 5 / 12 — Clean & Merge")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../20_transformation/21__clean_and_merge"

# COMMAND ----------

_record_step("Clean & Merge", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7b. Derive quarterly rows
# MAGIC `financials` — adds Q1–Q4 standalone rows per concept.
# MAGIC
# MAGIC By `kind`:
# MAGIC - `flow_additive`: Q1–Q3 standalone (~90d) or `YTD_n − YTD_(n-1)`. Q4 standalone if present in the 10-K, otherwise `FY − YTD_Q3`.
# MAGIC - `flow_nonadditive`: standalone only (~90d); NULL if absent.
# MAGIC - `stock`: snapshot at `period_end` (dedup latest `filed`).
# MAGIC
# MAGIC Requires `21__clean_and_merge` to have written the FY rows first.

# COMMAND ----------

print("=" * 55)
print("STEP 6 / 12 — Derive Quarterly")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../20_transformation/21b__derive_quarterly"

# COMMAND ----------

_record_step("Derive Quarterly", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7c. FY-from-quarterly fallback
# MAGIC `financials` — synthesises FY rows (Σ Q1..Q4) **only** for `flow_additive` concepts
# MAGIC that have no real FY (issuers whose annual total is not exposed without a dimension in the
# MAGIC 10-K). Never overwrites a reported FY; `is_derived=True`. Low yield today
# MAGIC (the affected set often lacks Q4), but covers future cases.
# MAGIC
# MAGIC Requires `21b__derive_quarterly` to have written the quarters first.

# COMMAND ----------

print("=" * 55)
print("STEP 6b / 12 — FY from Quarterly (fallback)")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../20_transformation/21e__derive_fy_from_quarterly"

# COMMAND ----------

_record_step("FY from Quarterly", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7d. Dedup Balance Sheet
# MAGIC `financials` — enforces the "one BS snapshot per `(ticker, stmt, concept,
# MAGIC period_end)`" invariant, collapsing mislabelled orphans (fiscal_year/period_type) that MERGE
# MAGIC never deletes. Recurring and idempotent: if no duplicates exist, nothing is rewritten. Must run
# MAGIC after all BS row writers (21, 21b) and before metrics.

# COMMAND ----------

print("=" * 55)
print("STEP 6c / 12 — Dedup Balance Sheet")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../20_transformation/21f__dedup_balance_sheet"

# COMMAND ----------

_record_step("Dedup Balance Sheet", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Derived metrics
# MAGIC `financials_metrics` — margins, FCF, YoY growth, leverage, valuation ratios

# COMMAND ----------

print("=" * 55)
print("STEP 7 / 12 — Derived Metrics")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../20_transformation/22__derived_metrics"

# COMMAND ----------

_record_step("Derived Metrics", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Intrinsic Value
# MAGIC `financials_intrinsic_value` — Graham Number, Graham Revised, DCF and Owner Earnings
# MAGIC for each historical fiscal year and TTM (rolling 4 quarters). Also exposes the
# MAGIC resulting metrics with `(FY)` and `(TTM)` suffixes in `financials_metrics` so the
# MAGIC dashboard can filter them like any other metric.
# MAGIC
# MAGIC Assumptions (WACC, growth, etc.) live in `00_config/valuation_assumptions.json`
# MAGIC with per-ticker overrides. Requires `22__derived_metrics` and `12__fetch_market_data`
# MAGIC to have run first.

# COMMAND ----------

print("=" * 55)
print("STEP 8 / 12 — Intrinsic Value")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../20_transformation/23__intrinsic_value"

# COMMAND ----------

_record_step("Intrinsic Value", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Analysis
# MAGIC Runs analysis queries — useful for validation after pipeline runs

# COMMAND ----------

print("=" * 55)
print("STEP 9 / 12 — Analysis")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../30_analysis/31__company_analysis"

# COMMAND ----------

_record_step("Analysis", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Coverage check
# MAGIC Verifies that all favorite tickers made it through the full pipeline
# MAGIC (config → financials → financials_metrics). Also surfaces any ingestion
# MAGIC failures from the latest run. Hard fails if >5% of favorites are missing.

# COMMAND ----------

print("=" * 55)
print("STEP 10 / 12 — Coverage Check")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

try:
    dbutils.notebook.run(
        "../30_analysis/32__coverage_check",
        timeout_seconds=120,
    )
    coverage_ok = True
except Exception as e:
    print(f"⚠ Coverage check failed: {e}")
    coverage_ok = False

# Non-fatal step (caught above), so record regardless of success.
_record_step("Coverage Check", _t0, status="ok" if coverage_ok else "failed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11b. Invariants check
# MAGIC Structural-invariant gate over `main.financials.financials` (BS dedup uniqueness,
# MAGIC Q-sum rate, two-statement concepts, market_data calendar-year). Mirrors the
# MAGIC `financials-invariants` skill's read-only validator. Non-fatal here: a hard fail is
# MAGIC logged and the pipeline continues (the Delta tables are already committed; a red
# MAGIC invariant flags data to investigate without blocking the dashboard refresh).

# COMMAND ----------

print("=" * 55)
print("STEP 10b / 12 — Invariants Check")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

try:
    dbutils.notebook.run(
        "../30_analysis/34__invariants_check",
        timeout_seconds=300,
    )
    invariants_ok = True
except Exception as e:
    print(f"⚠ Invariants check failed: {e}")
    invariants_ok = False

# Non-fatal step (caught above), so record regardless of success.
_record_step("Invariants Check", _t0, status="ok" if invariants_ok else "failed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Export dashboard data
# MAGIC `dashboard_data.parquet` + `dashboard_metrics.parquet` + `dashboard_meta.json`
# MAGIC written to `/tmp/` on the driver. Consumed by step 13 (GitHub publish).
# MAGIC
# MAGIC Runs **inline via `%run`** (same Spark/driver context, shared `/tmp`) — NOT
# MAGIC `dbutils.notebook.run`. The child-notebook spawn that `dbutils.notebook.run`
# MAGIC requires was stalling on serverless and burning the full `timeout_seconds`
# MAGIC cap (`TIMEDOUT`) even though the export work is only ~3–4 min; inline has no
# MAGIC per-notebook timeout (same fix as `50_publish/53__republish`). If export or
# MAGIC publish raises, the cell aborts the run → Job marked FAILED with the real
# MAGIC error (alerts fire) — the Delta tables are already committed by this point,
# MAGIC so a red run flags a stale Release instead of hiding it behind a green one.

# COMMAND ----------

print("=" * 55)
print("STEP 11 / 12 — Export dashboard data")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../50_publish/51__export_dashboard_data"

# COMMAND ----------

_record_step("Export dashboard data", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Publish to GitHub Release
# MAGIC Uploads the three `/tmp/` artifacts as assets on the `latest` GitHub
# MAGIC release. Skipped if step 12 failed. Requires the `github/github_pat`
# MAGIC Databricks secret — see `50_publish/README.md`.

# COMMAND ----------

print("=" * 55)
print("STEP 12 / 12 — Publish to GitHub")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../50_publish/52__publish_to_github"

# COMMAND ----------

_record_step("Publish to GitHub", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Pipeline summary

# COMMAND ----------

pipeline_end = datetime.utcnow()
duration     = (pipeline_end - pipeline_start).total_seconds()

print(f"\n{'='*55}")
print(f"  Pipeline completed ✓")
print(f"{'='*55}")
print(f"  Started  : {pipeline_start.isoformat()} UTC")
print(f"  Finished : {pipeline_end.isoformat()} UTC")
print(f"  Duration : {duration:.1f}s ({duration/60:.1f} min)")
print(f"  Tickers  : {len(ACTIVE_TICKERS):,}")
print()

summary_tables = [
    ("config",      "tickers"),
    ("config",      "concept_hierarchy"),
    ("config",      "metrics_hierarchy"),
    (SCHEMA,        "financials_raw"),
    (SCHEMA,        "financials"),
    (SCHEMA,        "market_data"),
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
# MAGIC ## 13b. Per-step timings — where the wall-clock went
# MAGIC
# MAGIC Prints the per-step breakdown (slowest first) and persists it to an append-only Delta
# MAGIC history table `pipeline_run_timings`, mirroring the `ingestion_failures` pattern.
# MAGIC `run_ts = pipeline_start` ties all step rows of one run together. The Σ-steps total
# MAGIC should ≈ the wall-clock (the small gap is the untimed `%pip` + banner cells).

# COMMAND ----------

_total_min = sum(s["minutes"] for s in STEP_TIMINGS)
_wall_min  = (datetime.utcnow() - pipeline_start).total_seconds() / 60.0
print(f"\n{'='*55}")
print(f"  PER-STEP TIMINGS")
print(f"{'='*55}")
print(f"  {'Step':<26}{'min':>8}{'% total':>10}")
print(f"  {'-'*44}")
for s in sorted(STEP_TIMINGS, key=lambda x: x["minutes"], reverse=True):
    pct = (s["minutes"] / _total_min * 100) if _total_min else 0.0
    print(f"  {s['step']:<26}{s['minutes']:>8.1f}{pct:>9.0f}%")
print(f"  {'-'*44}")
print(f"  {'Σ steps':<26}{_total_min:>8.1f}")
print(f"  {'Wall-clock (start→now)':<26}{_wall_min:>8.1f}")

# Persist to append-only history (same CREATE-IF-NOT-EXISTS + append idiom as ingestion_failures).
_timings_tbl = f"{CATALOG}.{SCHEMA}.pipeline_run_timings"
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {_timings_tbl} (
        run_ts     TIMESTAMP NOT NULL,
        step       STRING    NOT NULL,
        minutes    DOUBLE,
        n_tickers  INT
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

if STEP_TIMINGS:
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, TimestampType, IntegerType,
    )
    _tm_schema = StructType([
        StructField("run_ts",    TimestampType(), False),
        StructField("step",      StringType(),    False),
        StructField("minutes",   DoubleType(),    True),
        StructField("n_tickers", IntegerType(),   True),
    ])
    _tm_records = [
        {"run_ts": pipeline_start, "step": s["step"],
         "minutes": float(s["minutes"]), "n_tickers": int(len(ACTIVE_TICKERS))}
        for s in STEP_TIMINGS
    ]
    spark.createDataFrame(_tm_records, schema=_tm_schema) \
         .write.mode("append").saveAsTable(_timings_tbl)
    print(f"✓ {len(_tm_records)} step timings → {_timings_tbl}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13c. Run-log telemetry — `main.config.pipeline_runs` + `pipeline_run_coverage`
# MAGIC
# MAGIC Persists this run's per-step timings (with `run_id` + `status`) and a coverage/freshness
# MAGIC snapshot so the run history is queryable over time (read with `30_analysis/36__run_log_report`).
# MAGIC Supersedes the `pipeline_run_timings` write above (kept for back-compat). Pure observability:
# MAGIC wrapped in try/except so a telemetry failure never aborts an otherwise-successful run.
# MAGIC `run_id` = `pipeline_start` stamped `YYYYMMDDThhmmssZ`, tying both tables' rows together.

# COMMAND ----------

_run_id   = pipeline_start.strftime("%Y%m%dT%H%M%SZ")
_runs_tbl = f"{CATALOG}.config.pipeline_runs"
_cov_tbl  = f"{CATALOG}.config.pipeline_run_coverage"

try:
    from pyspark.sql.types import (
        DateType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    # ── pipeline_runs (per-step) ────────────────────────────────────────────────
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
            StructField("rows_written",   LongType(),      True),
        ])
        _runs_records = [
            {"run_id": _run_id, "run_started_at": pipeline_start, "step": s["step"],
             "minutes": float(s["minutes"]), "status": s.get("status", "ok"), "rows_written": None}
            for s in STEP_TIMINGS
        ]
        spark.createDataFrame(_runs_records, schema=_runs_schema).createOrReplaceTempView("incoming_pipeline_runs")
        # MERGE keyed on (run_id, step) → idempotent if this cell is re-executed in-session.
        spark.sql(f"""
            MERGE INTO {_runs_tbl} AS t
            USING incoming_pipeline_runs AS s
            ON t.run_id = s.run_id AND t.step = s.step
            WHEN MATCHED THEN UPDATE SET
                t.run_started_at = s.run_started_at, t.minutes = s.minutes,
                t.status = s.status, t.rows_written = s.rows_written
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"✓ {len(_runs_records)} step rows → {_runs_tbl} (run_id={_run_id})")

    # ── pipeline_run_coverage (one row per run) ─────────────────────────────────
    # Reuses 32's favorites-coverage logic + a financials_raw.filed freshness probe.
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
        "run_id": _run_id, "run_started_at": pipeline_start,
        "total_tickers_ingested": int(_cov["total_tickers_ingested"] or 0),
        "total_favorites": _tot_fav,
        "favorites_in_metrics": int(_cov["favorites_in_metrics"] or 0),
        "favorites_pct": _fav_pct,
        "max_filed": _cov["max_filed"],
        "staleness_days": int(_cov["staleness_days"]) if _cov["staleness_days"] is not None else None,
    }]
    spark.createDataFrame(_cov_record, schema=_cov_schema).createOrReplaceTempView("incoming_pipeline_run_coverage")
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
    print(f"⚠ Run-log telemetry skipped ({type(_e).__name__}: {_e}) — non-fatal, run already succeeded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Delta maintenance (OPTIMIZE / VACUUM)
# MAGIC Runs `93__delta_maintenance` inline via `%run` (shared session — serverless-safe,
# MAGIC avoids the `dbutils.notebook.run` child-notebook stall). `93` self-gates on
# MAGIC `run_optimization` (reused from this session via the shared scope), so a default run is
# MAGIC a **no-op**. Set `run_optimization=true` in Job params to enable; run at most once a
# MAGIC week. Covers `financials_raw`, `financials`, `financials_metrics`,
# MAGIC `financials_intrinsic_value`, `market_prices_daily`, and legacy `market_data`.

# COMMAND ----------

# MAGIC %run "./93__delta_maintenance"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Done
# MAGIC Export + publish ran inline above. A failure in either aborts the run before
# MAGIC reaching here (→ Job marked FAILED), so getting here means the Delta tables are
# MAGIC refreshed **and** the public GitHub Release is current.

# COMMAND ----------

print("✓ Pipeline complete — Delta tables refreshed and dashboard Release published.")
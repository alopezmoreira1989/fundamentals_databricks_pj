# Databricks notebook source
# MAGIC %md
# MAGIC # 90__pipelines / 91a__pipeline_pre22
# MAGIC
# MAGIC **First of three Databricks Job Tasks** in the "Financial Analysis Pipeline" multi-task
# MAGIC split (Phase 1 of the "split 91 into a multi-task job" effort — see the plan/commit history
# MAGIC around 2026-08-13). Was previously the first half of the single monolithic
# MAGIC `91__full_pipeline.py` task; split out so a failure anywhere downstream (transformation,
# MAGIC valuation, forecasting, publish) no longer forces re-ingesting SEC/yfinance data and
# MAGIC re-running the merge/dedup chain — Databricks' native Repair Run skips this task once it
# MAGIC has succeeded.
# MAGIC
# MAGIC **Covers config bootstrap through the clean/dedup chain:**
# MAGIC ```
# MAGIC 02__tickers_master (MANUAL, not run here) → main.config.tickers
# MAGIC       ↓
# MAGIC 03__concept_hierarchy_master    build main.config.concept_hierarchy
# MAGIC       ↓
# MAGIC 04__metrics_hierarchy_master    build main.config.metrics_hierarchy
# MAGIC       ↓
# MAGIC 11__fetch_sec_xbrl              fetch from SEC API → financials_raw
# MAGIC       ↓
# MAGIC 13__fetch_dimensional_10k       combined-filer dimensional facts (usually a no-op)
# MAGIC       ↓
# MAGIC 15__fetch_sec_filings           SEC filing list → dashboard_filings
# MAGIC       ↓
# MAGIC 12__fetch_market_data           fetch from Yahoo Finance → market_prices_daily / fx_rates_daily
# MAGIC       ↓
# MAGIC 21__clean_and_merge             deduplicate → MERGE into financials (FY rows)
# MAGIC       ↓
# MAGIC 21b__derive_quarterly           derive Q1-Q4 standalone rows (Q4 = FY - YTD_Q3 fallback)
# MAGIC       ↓
# MAGIC 21e__derive_fy_from_quarterly   synth FY = SUM(Q) for flow_additive concepts lacking a real FY
# MAGIC       ↓
# MAGIC 21f__dedup_balance_sheet        one BS snapshot per (ticker, concept, period_end)
# MAGIC       ↓
# MAGIC 21g__dedup_flow_orphans         one IS/CF fact per (ticker, concept, period_end, period_type)
# MAGIC       ↓
# MAGIC 21h__shares_diluted_plausibility  delete implausible Shares Diluted / Cover Page values
# MAGIC ```
# MAGIC
# MAGIC **Publishes `run_id` via `dbutils.jobs.taskValues`** (this task's own start timestamp,
# MAGIC stamped `YYYYMMDDThhmmssZ`) so every downstream task ties its own `pipeline_runs` rows to
# MAGIC the SAME run — the Databricks-native cross-task value-passing mechanism, since separate
# MAGIC Job Tasks don't share a Python session/`globals()` the way `%run` cells within one
# MAGIC notebook do. Each downstream task relays the value forward to the next (see `91b`'s own
# MAGIC header doc for why `taskValues.get()` only reads a task's own direct dependencies).
# MAGIC
# MAGIC Registers its own `tickers_override`/`rebuild_config`/`force_full_refresh` widgets
# MAGIC directly (not inherited via globals() from a parent notebook) — this task IS the top-level
# MAGIC notebook Databricks actually submits, so a direct `dbutils.widgets.get()` here is reliable
# MAGIC (the "does NOT reliably read the PARENT's widget under `%run`" gotcha documented in
# MAGIC `11__fetch_sec_xbrl.py` only applies to a NESTED `%run`'d notebook trying to read a value a
# MAGIC parent already registered — not to the top-level notebook registering its own).
# MAGIC `run_optimization` is NOT registered here — only `93__delta_maintenance` (run from
# MAGIC `91j__delta_maintenance`, the last task in the chain) needs it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0a. Session dependencies
# MAGIC Installed FIRST because `%pip` restarts the Python interpreter (any earlier state is
# MAGIC wiped); the result then persists for every notebook pulled in via `%run` for the
# MAGIC REMAINDER of this one task's own session:
# MAGIC - `lxml` — required by `13__fetch_dimensional_10k` (XBRL instance parsing); not preinstalled on serverless.
# MAGIC - `fundamentals_pipeline` (editable, `-e ../..`) — the project package. Installing it here
# MAGIC   once lets `%run` notebooks import it as a normal package — no `sys.path` manipulation.
# MAGIC   (every downstream task is its own separate Job Task with a fresh Python env, so each
# MAGIC   needs this too, or the per-notebook reinstall-on-ImportError fallback added in Phase 0.)
# MAGIC
# MAGIC > **Databricks path note:** the notebook's CWD in a Repo is its own folder
# MAGIC > (`90__pipelines/`), not the repo root — `-e ../..` resolves to the repo root, where
# MAGIC > `pyproject.toml` actually lives.

# COMMAND ----------

# MAGIC %pip install lxml -e ../..

# COMMAND ----------

# Per-step wall-clock timing, scoped to THIS task's own steps only (every downstream task keeps
# its own separate STEP_TIMINGS — no cross-task Python state, same reasoning as the run_id
# taskValues handoff below). Defined right AFTER the %pip cell because %pip restarts the
# interpreter.
import time

STEP_TIMINGS = []

def _record_step(name, t0, status="ok"):
    mins = (time.monotonic() - t0) / 60.0
    STEP_TIMINGS.append({"step": name, "minutes": mins, "status": status})
    print(f"  ⏱ {name}: {mins:.1f} min ({status})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Pipeline parameters
# MAGIC
# MAGIC Override at runtime via Databricks Job parameters:
# MAGIC `{"tickers_override": "AAPL,TSLA", "force_full_refresh": "false"}`
# MAGIC
# MAGIC - `tickers_override`: comma-separated ticker list (overrides `main.config.tickers`)
# MAGIC - `force_full_refresh`: `true` to re-ingest ALL tickers ignoring the
# MAGIC   3-day freshness guard in `11__fetch_sec_xbrl` (use this for a backfill
# MAGIC   after a logic change in ingest/merge)
# MAGIC - `rebuild_config`: read-only preflight print on `main.config.tickers` status — never
# MAGIC   auto-runs `02__tickers_master` (see below)

# COMMAND ----------

dbutils.widgets.text("tickers_override",   "",      "tickers_override")
dbutils.widgets.text("rebuild_config",     "false", "rebuild_config")
dbutils.widgets.text("force_full_refresh", "false", "force_full_refresh")

tickers_override   = dbutils.widgets.get("tickers_override")
rebuild_config     = dbutils.widgets.get("rebuild_config")
force_full_refresh = dbutils.widgets.get("force_full_refresh")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load config

# COMMAND ----------

_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../00__config/01__tickers"

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

# run_id ties this task's pipeline_runs rows to every downstream task's — published via
# taskValues so each one (fresh Python session, no shared globals()) can read the SAME value.
# Wrapped defensively: outside any Job Task context (e.g. an interactive/standalone run)
# taskValues has nothing to attach to and raises.
run_id = pipeline_start.strftime("%Y%m%dT%H%M%SZ")
try:
    dbutils.jobs.taskValues.set(key="run_id", value=run_id)
except Exception:
    pass

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

# `rebuild_config` does NOT auto-run 02__tickers_master. That step is intentionally MANUAL: it
# calls Wikipedia / iShares / yfinance and does a full DROP+overwrite of main.config.tickers
# (incl. a ~3k-ticker yfinance `industry` sweep), which we don't want inside every pipeline run.
# A conditional %run isn't serverless-safe either (dbutils.notebook.exit() in a %run'd notebook
# aborts the PARENT), and this workspace moved off dbutils.notebook.run due to child-notebook
# stalls. So the flag is a reminder + a read-only PREFLIGHT, not an action: when set, it reports
# config.tickers status (incl. whether `industry` is populated) so you know if a manual 02 run +
# 53__republish is needed.
if rebuild_config.lower() == "true":
    print("⚠ rebuild_config=true — 02__tickers_master is a MANUAL step and is NOT auto-run here.")
    print("  To rebuild the universe / populate `industry`: run 00__config/02__tickers_master,")
    print("  then 50__publish/53__republish. Current main.config.tickers status:")
    try:
        _tk = spark.table(f"{CATALOG}.config.tickers")
        _has_industry = "industry" in _tk.columns
        _n = _tk.count()
        _n_ind = _tk.filter("industry IS NOT NULL").count() if _has_industry else 0
        print(f"    rows={_n:,} · industry column={'present' if _has_industry else 'ABSENT'}"
              f" · non-null industry={_n_ind:,}")
        if not _has_industry or _n_ind == 0:
            print("    → industry not populated yet: run 02__tickers_master to fill it.")
    except Exception as _e:
        print(f"    ⚠ could not read {CATALOG}.config.tickers: {_e}")
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
print("STEP 1 / 9 (pre-22) — Concept Hierarchy")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../00__config/03__concept_hierarchy_master"

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
print("STEP 2 / 9 (pre-22) — Metrics Hierarchy")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../00__config/04__metrics_hierarchy_master"

# COMMAND ----------

_record_step("Metrics Hierarchy", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Ingestion — fetch from SEC EDGAR
# MAGIC `financials_raw` — append-only audit log

# COMMAND ----------

print("=" * 55)
print("STEP 3 / 9 (pre-22) — SEC Ingestion")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../10__ingestion/11__fetch_sec_xbrl"

# COMMAND ----------

_record_step("SEC Ingestion", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5b. Ingestion — dimensional 10-K facts (combined-filers) ⚠️ experimental
# MAGIC `financials_raw` — retrieves annual primary totals for combined-filers
# MAGIC (REIT + Operating Partnership, e.g. SKT) that SEC `companyfacts` omits because they are
# MAGIC dimensional facts. **No-op** if no ticker in `favorites.json` has the
# MAGIC `combined_filer_member` field. Must run AFTER `11` (syncs `scraped_at`) and BEFORE `21`.

# COMMAND ----------

print("=" * 55)
print("STEP 3b / 9 (pre-22) — Dimensional 10-K (combined-filers)")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../10__ingestion/13__fetch_dimensional_10k"

# COMMAND ----------

_record_step("Dimensional 10-K", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5c. Ingestion — SEC filing list (Filings tab)
# MAGIC `sec_filings` — 10-K/10-Q form + date + direct document link per ticker, published as
# MAGIC `dashboard_filings` so neither frontend needs its own SEC credentials or live fetch.

# COMMAND ----------

print("=" * 55)
print("STEP 3c / 9 (pre-22) — SEC Filings List")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../10__ingestion/15__fetch_sec_filings"

# COMMAND ----------

_record_step("SEC Filings List", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Ingestion — fetch market data from Yahoo Finance
# MAGIC `market_prices_daily` / `fx_rates_daily` — daily prices + FX rates

# COMMAND ----------

print("=" * 55)
print("STEP 4 / 9 (pre-22) — Market Data")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../10__ingestion/12__fetch_market_data"

# COMMAND ----------

_record_step("Market Data", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Clean & merge → fact table
# MAGIC `financials` — deduplicated, clean long-format fact table

# COMMAND ----------

print("=" * 55)
print("STEP 5 / 9 (pre-22) — Clean & Merge")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../20__transformation/21__clean_and_merge"

# COMMAND ----------

_record_step("Clean & Merge", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7b. Derive quarterly rows
# MAGIC Requires `21__clean_and_merge` to have written the FY rows first.

# COMMAND ----------

print("=" * 55)
print("STEP 6 / 9 (pre-22) — Derive Quarterly")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../20__transformation/21b__derive_quarterly"

# COMMAND ----------

_record_step("Derive Quarterly", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7c. FY-from-quarterly fallback
# MAGIC Requires `21b__derive_quarterly` to have written the quarters first.

# COMMAND ----------

print("=" * 55)
print("STEP 6b / 9 (pre-22) — FY from Quarterly (fallback)")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../20__transformation/21e__derive_fy_from_quarterly"

# COMMAND ----------

_record_step("FY from Quarterly", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7d. Dedup Balance Sheet
# MAGIC Must run after all BS row writers (21, 21b) and before metrics.

# COMMAND ----------

print("=" * 55)
print("STEP 6c / 9 (pre-22) — Dedup Balance Sheet")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../20__transformation/21f__dedup_balance_sheet"

# COMMAND ----------

_record_step("Dedup Balance Sheet", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7e. Dedup Flow Orphans
# MAGIC Must run after all flow writers (21, 21b, 21e) and after 21f, before metrics.

# COMMAND ----------

print("=" * 55)
print("STEP 6d / 9 (pre-22) — Dedup Flow Orphans")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../20__transformation/21g__dedup_flow_orphans"

# COMMAND ----------

_record_step("Dedup Flow Orphans", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7f. Shares Diluted Plausibility
# MAGIC Must run after all quarterly writers (21, 21b, 21e, 21f, 21g). Must run before metrics.

# COMMAND ----------

print("=" * 55)
print("STEP 6e / 9 (pre-22) — Shares Diluted Plausibility")
print("=" * 55)
_t0 = time.monotonic()

# COMMAND ----------

# MAGIC %run "../20__transformation/21h__shares_diluted_plausibility"

# COMMAND ----------

_record_step("Shares Diluted Plausibility", _t0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. This task's run-log telemetry
# MAGIC
# MAGIC Persists THIS task's own per-step rows to `main.config.pipeline_runs`, keyed by the shared
# MAGIC `run_id` — every downstream task appends its own rows the same way, so the full run's step
# MAGIC history is queryable as one logical run once every task has completed. Pure
# MAGIC observability: wrapped in try/except so a telemetry failure never aborts an otherwise-
# MAGIC successful task.

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
            .createOrReplaceTempView("incoming_pipeline_runs_pre22")
        spark.sql(f"""
            MERGE INTO {_runs_tbl} AS t
            USING incoming_pipeline_runs_pre22 AS s
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

print("✓ 91a (pre-22) complete — financials table is clean, deduplicated, and dedup-checked.")

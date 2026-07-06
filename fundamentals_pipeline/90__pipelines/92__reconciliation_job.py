# Databricks notebook source
# MAGIC %md
# MAGIC # 90__pipelines / 92__reconciliation_job — filing reconciliation (Tier A + Tier B/C)
# MAGIC
# MAGIC Thin orchestrator for the **external** reconciliation validator. Runs as a
# MAGIC **standalone Job, monthly / on-demand** — it is intentionally **not** wired into
# MAGIC `91__full_pipeline.py` (the Tier-A linkbase fetch is heavy and the dashboard refresh
# MAGIC must not wait on it).
# MAGIC
# MAGIC 1. `10__ingestion/14__fetch_oracle_statements` — builds the linkbase oracle
# MAGIC    (`main.config.reconciliation_oracle`) for the golden set ∪ `COMBINED_FILERS`.
# MAGIC 2. `30__analysis/35__reconcile_filings` — Tier B/C (universe-wide) + Tier A (golden set)
# MAGIC    → `main.config.reconciliation_findings` (+ `reconciliation_open` view + parquet export).
# MAGIC
# MAGIC **Databricks-only.** `%pip install lxml` is front-loaded here (lxml is not preinstalled on
# MAGIC serverless). Each step runs in an **isolated context via `dbutils.notebook.run`** so each
# MAGIC notebook handles its own `%pip` cleanly (a `%pip` inside a `%run`-ed child would restart the
# MAGIC shared interpreter and wipe state). Both steps are wrapped non-fatally so a red step is
# MAGIC reported without aborting the other.
# MAGIC
# MAGIC > Caveat: `dbutils.notebook.run` spins up an ephemeral context — slower to start on
# MAGIC > serverless (the same reason `91` switched its *export* step to inline `%run`). Acceptable
# MAGIC > for a monthly job; timeouts below are generous.

# COMMAND ----------

# MAGIC %pip install lxml

# COMMAND ----------

import json
import time

ORACLE_TIMEOUT = 3600   # 14 is network-bound (golden set, rate-limited SEC fetch)
RECON_TIMEOUT  = 3600   # 35 joins financials_raw (~80M rows) — Spark-heavy

results = []          # (step, ok, detail)
_recon_summary = None  # JSON string 35 passes back via dbutils.notebook.exit (divergence report)

# COMMAND ----------

# MAGIC %md ## STEP 1 / 2 — Oracle (Tier A source)

# COMMAND ----------

_t0 = time.monotonic()
try:
    dbutils.notebook.run("../10__ingestion/14__fetch_oracle_statements", timeout_seconds=ORACLE_TIMEOUT)
    results.append(("14 oracle", True, f"{(time.monotonic() - _t0) / 60:.1f} min"))
except Exception as e:
    results.append(("14 oracle", False, str(e)[:200]))
    print(f"⚠ Oracle step failed: {e}")

# COMMAND ----------

# MAGIC %md ## STEP 2 / 2 — Reconcile (Tier A + Tier B/C)

# COMMAND ----------

_t0 = time.monotonic()
try:
    _recon_summary = dbutils.notebook.run("../30__analysis/35__reconcile_filings", timeout_seconds=RECON_TIMEOUT)
    results.append(("35 reconcile", True, f"{(time.monotonic() - _t0) / 60:.1f} min"))
except Exception as e:
    results.append(("35 reconcile", False, str(e)[:200]))
    print(f"⚠ Reconcile step failed: {e}")

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

print("=" * 60)
print("  92__reconciliation_job — summary")
print("=" * 60)
for step, ok, detail in results:
    print(f"  {'✓' if ok else '✗'}  {step:<14}  {detail}")

# Surface 35's divergence report at the orchestrator level too — a scheduled run's own output
# should be self-explanatory without drilling into the child notebook run. Non-fatal: parse
# failure or a missing summary (e.g. 35 failed before reaching its exit call) just skips this.
if _recon_summary:
    try:
        _rs = json.loads(_recon_summary)
        print(f"  Divergence vs run_id={_rs['prev_run_id']}: "
              f"{_rs['new_clusters']} new cluster(s), {_rs['grown_clusters']} grown cluster(s) "
              f"({_rs['total_findings']} total open findings)")
        if _rs["new_clusters"] or _rs["grown_clusters"]:
            print("  ⚠ New/growing divergences detected — triage via main.config.reconciliation_open "
                  "(see the external-benchmark-validation skill).")
    except Exception as _e:
        print(f"  (could not parse reconciliation summary: {_e})")
print("=" * 60)

# A failed step is surfaced as a hard error so the Job is marked FAILED (alerts fire). The
# config tables written by a successful step are already committed, so a partial run is safe.
_failed = [s for s, ok, _ in results if not ok]
if _failed:
    raise RuntimeError(f"reconciliation job: {len(_failed)} step(s) failed: {', '.join(_failed)}")
print("✓ Reconciliation job complete.")

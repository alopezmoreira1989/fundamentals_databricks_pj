# Databricks notebook source
# MAGIC %md
# MAGIC # 90__pipelines / 93__delta_maintenance
# MAGIC
# MAGIC **Delta operational hygiene** — compacts (`OPTIMIZE`) and vacuums tombstones
# MAGIC (`VACUUM`) on the pipeline's Delta tables so they don't accrete small files and
# MAGIC stale data files indefinitely. Idempotent and safe to re-run.
# MAGIC
# MAGIC **Gated on `run_optimization`.** When `%run` from `91__full_pipeline`, it reuses the
# MAGIC parent's `run_optimization` variable (shared session); run standalone, it reads its
# MAGIC own widget. Either way, a default run (`run_optimization != "true"`) is a **no-op**.
# MAGIC
# MAGIC **Databricks-only:** uses `spark`, `dbutils`, `%run`, and Unity Catalog three-part
# MAGIC names — it does nothing useful under Databricks Connect without a live session.
# MAGIC
# MAGIC | Operation | Default | Notes |
# MAGIC |---|---|---|
# MAGIC | `OPTIMIZE` | on | Bin-packs small files. For `market_prices_daily` (liquid-clustered) it also compacts clusters. |
# MAGIC | `ZORDER`   | **off** | Left as a guarded no-op — see §3. Not blindly applied. |
# MAGIC | `VACUUM`   | on | `RETAIN 168 HOURS` (7d). Never lowers the retention safety check. |
# MAGIC | raw row-retention | **off** | OPT-IN only — `financials_raw` is append-only by design (§5). |

# COMMAND ----------

# MAGIC %md ### ⚠️ Path note
# MAGIC `%run` paths are relative to this notebook's workspace location. If `CATALOG`/`SCHEMA`
# MAGIC come back undefined, adjust the path to point at `00__config/01__tickers`.

# COMMAND ----------

# MAGIC %run "../00__config/01__tickers"

# COMMAND ----------

# MAGIC %md ## 1. Config — gate, table list, and operation toggles

# COMMAND ----------

# Resolve the gate. If %run from 91 (shared session), `run_optimization` is already defined
# in the parent scope — reuse it. If run standalone, fall back to this notebook's own widget.
try:
    _run_opt = run_optimization                       # noqa: F821 — parent var via %run
except NameError:
    dbutils.widgets.text("run_optimization", "false", "run_optimization")
    _run_opt = dbutils.widgets.get("run_optimization")

RUN_OPTIMIZATION = str(_run_opt).strip().lower() == "true"

# ── Operation toggles ──────────────────────────────────────────────────────────
ENABLE_VACUUM       = True     # VACUUM tombstones older than the retention window
VACUUM_RETAIN_HOURS = 168      # 7 days. Delta blocks < 168h unless the retention check is
                               # disabled — which this notebook deliberately NEVER does (§4).
ENABLE_ZORDER       = False    # See §3 — off by default, correctness + support caveats.
ENABLE_RAW_RETENTION = False   # See §5 — OPT-IN only; financials_raw is append-only.

# Tables to maintain. `clustered=True` marks liquid-clustered tables (CLUSTER BY): they are
# compacted by a plain OPTIMIZE and must NOT be Z-ORDERed. The others are PARTITIONED BY
# (ticker[, stmt]); `zorder_cols` lists the NON-partition columns worth Z-ordering IF it is
# ever enabled (the partition key itself is excluded — Z-ordering on it is redundant).
TABLES = [
    {"name": "financials_raw",             "clustered": False, "zorder_cols": ["concept", "period_end"]},
    {"name": "financials",                 "clustered": False, "zorder_cols": ["concept", "period_end"]},
    {"name": "financials_metrics",         "clustered": False, "zorder_cols": ["metric", "fiscal_year"]},
    {"name": "financials_intrinsic_value", "clustered": False, "zorder_cols": ["method", "scenario", "fiscal_year"]},
    {"name": "market_prices_daily",        "clustered": True,  "zorder_cols": []},  # liquid-clustered
    {"name": "market_data",                "clustered": False, "zorder_cols": []},  # legacy/back-compat
    {"name": "market_cap_asof",            "clustered": False, "zorder_cols": []},  # small, full-overwrite each run by 22
]

print(f"run_optimization = {RUN_OPTIMIZATION}")
print(f"VACUUM           = {ENABLE_VACUUM} (retain {VACUUM_RETAIN_HOURS}h)")
print(f"ZORDER           = {ENABLE_ZORDER}")
print(f"raw retention    = {ENABLE_RAW_RETENTION} (opt-in)")
print(f"tables           = {[t['name'] for t in TABLES]}")

if not RUN_OPTIMIZATION:
    print("\n⊘ run_optimization != 'true' — skipping all maintenance (no-op).")

# COMMAND ----------

# MAGIC %md ## 2. OPTIMIZE (+ optional ZORDER)
# MAGIC
# MAGIC Bin-packs small files. For liquid-clustered tables a plain `OPTIMIZE` also compacts
# MAGIC the clusters. Each table is independent — a failure on one is logged and the rest
# MAGIC continue.

# COMMAND ----------

def _table_exists(fqn: str) -> bool:
    try:
        return spark.catalog.tableExists(fqn)
    except Exception:
        # Older runtimes: fall back to a cheap metadata probe.
        try:
            spark.sql(f"DESCRIBE DETAIL {fqn}")
            return True
        except Exception:
            return False


def _optimize_sql(spec: dict, fqn: str) -> str:
    """Build the OPTIMIZE statement for a table. ZORDER only when enabled AND applicable
    (non-clustered table with declared zorder_cols)."""
    if ENABLE_ZORDER and not spec["clustered"] and spec["zorder_cols"]:
        cols = ", ".join(spec["zorder_cols"])
        return f"OPTIMIZE {fqn} ZORDER BY ({cols})"
    return f"OPTIMIZE {fqn}"


if RUN_OPTIMIZATION:
    for spec in TABLES:
        fqn = f"{CATALOG}.{SCHEMA}.{spec['name']}"
        if not _table_exists(fqn):
            print(f"  ⊘ {fqn} — table absent, skipping")
            continue
        stmt = _optimize_sql(spec, fqn)
        try:
            spark.sql(stmt)
            print(f"  ✓ {stmt}")
        except Exception as e:
            print(f"  ✗ {stmt} — {type(e).__name__}: {e}")
else:
    print("⊘ Skipped (run_optimization != 'true').")

# COMMAND ----------

# MAGIC %md ## 3. ZORDER — why it is OFF by default (guarded no-op)
# MAGIC
# MAGIC `ENABLE_ZORDER = False` above. Reasons it is not blindly applied:
# MAGIC
# MAGIC 1. **Liquid-clustered tables cannot be Z-ordered.** `market_prices_daily` is created
# MAGIC    `CLUSTER BY (ticker, date)`; a plain `OPTIMIZE` already compacts its clusters.
# MAGIC    Issuing `ZORDER` on it errors.
# MAGIC 2. **`ticker` is the partition key** of every other table (`financials*` are
# MAGIC    `PARTITIONED BY (ticker[, stmt])`). Z-ordering on the partition column is
# MAGIC    redundant — data is already segregated by it. The useful keys are the NON-partition
# MAGIC    predicate columns (`concept`/`period_end`, `metric`/`fiscal_year`, …), which is why
# MAGIC    `zorder_cols` deliberately excludes `ticker`.
# MAGIC 3. **Support is unconfirmed on this serverless / Free-Edition workspace.** Before
# MAGIC    flipping `ENABLE_ZORDER = True`, confirm `OPTIMIZE … ZORDER BY` runs here, and run
# MAGIC    it at most weekly (ZORDER is a full rewrite of each touched partition).
# MAGIC
# MAGIC When enabled, §2 emits `OPTIMIZE <t> ZORDER BY (<non-partition cols>)` for the
# MAGIC non-clustered tables and a plain `OPTIMIZE` for the clustered one.

# COMMAND ----------

# MAGIC %md ## 4. VACUUM — delete tombstoned files past the retention window
# MAGIC
# MAGIC Removes data files no longer referenced by the table and older than the retention
# MAGIC window, reclaiming storage. **Guarded:** retention is fixed at 168h (7 days) and this
# MAGIC notebook NEVER disables `spark.databricks.delta.retentionDurationCheck` — so a run can
# MAGIC never accidentally hard-delete recent history or break time-travel/concurrent readers.
# MAGIC Lowering below 168h would require explicitly disabling that safety check, which we refuse.

# COMMAND ----------

if RUN_OPTIMIZATION and ENABLE_VACUUM:
    if VACUUM_RETAIN_HOURS < 168:
        # Defensive: refuse an unsafe retention rather than disabling the safety check.
        raise ValueError(
            f"VACUUM_RETAIN_HOURS={VACUUM_RETAIN_HOURS} < 168h is unsafe; "
            "refusing (would require disabling retentionDurationCheck)."
        )
    for spec in TABLES:
        fqn = f"{CATALOG}.{SCHEMA}.{spec['name']}"
        if not _table_exists(fqn):
            continue
        stmt = f"VACUUM {fqn} RETAIN {VACUUM_RETAIN_HOURS} HOURS"
        try:
            spark.sql(stmt)
            print(f"  ✓ {stmt}")
        except Exception as e:
            print(f"  ✗ {stmt} — {type(e).__name__}: {e}")
elif RUN_OPTIMIZATION:
    print("⊘ VACUUM disabled (ENABLE_VACUUM=False).")
else:
    print("⊘ Skipped (run_optimization != 'true').")

# COMMAND ----------

# MAGIC %md ## 5. OPT-IN: `financials_raw` row-level retention (DISABLED)
# MAGIC
# MAGIC `financials_raw` is **append-only by design** — every scrape appends, and later steps
# MAGIC (`21__clean_and_merge`) dedup downstream by keeping the latest `filed`. The block below
# MAGIC would prune the raw table to only the latest `filed` per
# MAGIC `(ticker, concept, period_end, fp)`, shrinking storage at the cost of **losing the
# MAGIC restatement history** (you could no longer see how a filer revised a prior figure, and
# MAGIC any future logic that wants the as-originally-reported value would break).
# MAGIC
# MAGIC It is **OFF** (`ENABLE_RAW_RETENTION = False`). Do not enable without deciding that the
# MAGIC restatement history is expendable. OPTIMIZE/VACUUM above already reclaim the file-level
# MAGIC bloat without touching row semantics.

# COMMAND ----------

if RUN_OPTIMIZATION and ENABLE_RAW_RETENTION:
    raw_fqn = f"{CATALOG}.{SCHEMA}.financials_raw"
    # Keep only the latest `filed` per natural key; delete older restatements.
    # MERGE-delete via an anti-pattern join (idempotent: re-running deletes nothing new).
    stmt = f"""
        MERGE INTO {raw_fqn} AS t
        USING (
            SELECT ticker, concept, period_end, fp, MAX(filed) AS keep_filed
            FROM {raw_fqn}
            GROUP BY ticker, concept, period_end, fp
        ) AS k
        ON  t.ticker = k.ticker AND t.concept = k.concept
        AND t.period_end <=> k.period_end AND t.fp <=> k.fp
        AND t.filed < k.keep_filed
        WHEN MATCHED THEN DELETE
    """
    print("⚠ ENABLE_RAW_RETENTION=True — pruning financials_raw to latest filed per key.")
    spark.sql(stmt)
    print(f"  ✓ Pruned {raw_fqn}; run OPTIMIZE/VACUUM again to reclaim the freed files.")
else:
    print("⊘ financials_raw row-retention is OPT-IN and disabled (append-only preserved).")

# COMMAND ----------

print("✓ 93__delta_maintenance complete.")

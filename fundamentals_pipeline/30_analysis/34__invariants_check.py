# Databricks notebook source
# MAGIC %md
# MAGIC # 30_analysis / 34__invariants_check
# MAGIC
# MAGIC Post-pipeline **structural-invariant gate** on `main.financials.financials`.
# MAGIC Checks the invariants documented in CLAUDE.md (and in the skill
# MAGIC `.claude/skills/financials-invariants`). This is the "pipeline" twin of the
# MAGIC read-only validator `scripts/check_invariants.py` from that skill: same query logic,
# MAGIC but in notebook format with `raise` on hard failures so the Job marks it as failed.
# MAGIC
# MAGIC **Checks:**
# MAGIC - INV1  Balance Sheet unique per `(ticker, concept, period_end)`            [hard]
# MAGIC - INV2  Q1+Q2+Q3+Q4 ≈ FY — gate by *rate* (not per row)                    [hard if ceiling exceeded]
# MAGIC - INV3  two-statement concepts (Net Income / D&A / SBC) present 2×          [info]
# MAGIC - INV4  TTM ordering by `period_end`                                        [code-level, not observable here → SKIP]
# MAGIC - INV5  `market_data.fiscal_year` is a calendar year (not future)           [hard]
# MAGIC
# MAGIC **Behavior:** prints PASS/FAIL/INFO/SKIP per invariant; `raise RuntimeError`
# MAGIC if any hard check fails. In `91__full_pipeline` the step is wrapped in try/except
# MAGIC (non-fatal): a failure is recorded as ⚠ and the pipeline continues.
# MAGIC
# MAGIC > The healthy Q-sum divergence rate is ~3–4% (irreducible floor, not a
# MAGIC > bug). For a per-concept/year breakdown use the `validate-quarters` skill.

# COMMAND ----------

# MAGIC %run "../00_config/01__tickers"

# COMMAND ----------

FIN = f"{CATALOG}.{SCHEMA}.financials"
MKT = f"{CATALOG}.{SCHEMA}.market_data"

# Q-sum: per-row tolerance and rate ceiling. The ~3–4% baseline is expected; the ceiling
# only fires on a regression. Keep in sync with scripts/check_invariants.py in the skill.
QSUM_TOLERANCE = 0.001     # 0.1% per (ticker, stmt, concept, fy)
QSUM_RATE_CEILING = 0.06   # 6% of checked tuples

results = []   # (status, label, detail) — status in PASS/FAIL/INFO/SKIP
HARD_FAIL = {"FAIL"}


def _scalar(sql):
    return spark.sql(sql).collect()[0][0]


def _table_exists(fqn):
    try:
        spark.sql(f"SELECT 1 FROM {fqn} LIMIT 1").collect()
        return True
    except Exception:
        return False

# COMMAND ----------

# MAGIC %md ## INV1 — Balance Sheet unique per (ticker, concept, period_end)

# COMMAND ----------

dup_groups = _scalar(f"""
    SELECT COUNT(*) FROM (
        SELECT ticker, concept, period_end
        FROM {FIN}
        WHERE stmt = 'Balance Sheet'
        GROUP BY ticker, concept, period_end
        HAVING COUNT(*) > 1
    )
""")
if dup_groups == 0:
    results.append(("PASS", "INV1 BS dedup uniqueness",
                    "Balance Sheet unique by (ticker, concept, period_end)."))
else:
    results.append(("FAIL", "INV1 BS dedup uniqueness",
                    f"{dup_groups:,} (ticker, concept, period_end) groups with >1 BS row. "
                    f"The fix is upstream in 21__clean_and_merge / 21b__derive_quarterly."))

# COMMAND ----------

# MAGIC %md ## INV2 — Q1+Q2+Q3+Q4 ≈ FY (rate gate)

# COMMAND ----------

_row = spark.sql(f"""
    WITH q AS (
        SELECT ticker, stmt, concept, fiscal_year,
               SUM(CASE WHEN period_type IN ('Q1','Q2','Q3','Q4') THEN value END) AS qsum,
               MAX(CASE WHEN period_type = 'FY' THEN value END)                    AS fy
        FROM {FIN}
        WHERE stmt IN ('Income Statement', 'Cash Flow')
          AND concept NOT IN ('EPS Basic', 'EPS Diluted', 'Shares Diluted')
        GROUP BY ticker, stmt, concept, fiscal_year
        HAVING COUNT(DISTINCT period_type) = 5
    )
    SELECT COUNT(*) AS checked,
           SUM(CASE WHEN ABS((qsum - fy) / NULLIF(fy, 0)) > {QSUM_TOLERANCE}
                    THEN 1 ELSE 0 END) AS divergent
    FROM q
""").collect()[0]
_checked, _divergent = int(_row["checked"] or 0), int(_row["divergent"] or 0)
if _checked == 0:
    results.append(("SKIP", "INV2 Q-sum rate gate",
                    "No tuples with FY and all 4 quarters yet."))
else:
    _rate = _divergent / _checked
    _msg = f"{_divergent:,}/{_checked:,} divergent at {QSUM_TOLERANCE:.1%} ({_rate:.2%})."
    if _rate > QSUM_RATE_CEILING:
        results.append(("FAIL", "INV2 Q-sum rate gate",
                        f"{_msg} Exceeds the {QSUM_RATE_CEILING:.0%} ceiling — use validate-quarters "
                        f"for the per-concept/year breakdown."))
    else:
        results.append(("PASS", "INV2 Q-sum rate gate",
                        f"{_msg} Within the ceiling ({QSUM_RATE_CEILING:.0%})."))

# COMMAND ----------

# MAGIC %md ## INV3 — conceptos de dos estados (info)

# COMMAND ----------

_two = spark.sql(f"""
    SELECT concept, COUNT(DISTINCT stmt) AS n_stmts
    FROM {FIN}
    GROUP BY concept
    HAVING COUNT(DISTINCT stmt) > 1
    ORDER BY concept
""").collect()
if not _two:
    results.append(("INFO", "INV3 two-statement concepts",
                    "0 concepts in two statements — Net Income (and often D&A, SBC) expected; "
                    "investigate if it is 0."))
else:
    _names = ", ".join(r["concept"] for r in _two[:8])
    results.append(("INFO", "INV3 two-statement concepts",
                    f"{len(_two)} concept(s) in two statements: {_names}. Every join by concept "
                    f"MUST include `stmt` (otherwise double-counting / false ~100% divergence)."))

# COMMAND ----------

# MAGIC %md ## INV4 — TTM ordering by period_end (code-level)

# COMMAND ----------

# TTM ordering (4 most recent quarters by `period_end`, not `fiscal_year`) is a CODE-level
# invariant in 23__intrinsic_value.py — not observable from the published tables (which
# quarters feed each TTM row is not stored). Verify via code review, not data. We leave
# it as an explicit SKIP to avoid a misleading PASS.
results.append(("SKIP", "INV4 TTM ordering",
                "Code-level invariant (23__intrinsic_value.py orders by period_end); "
                "not observable from the tables — verify via code review."))

# COMMAND ----------

# MAGIC %md ## INV5 — market_data.fiscal_year is a calendar year (not future)

# COMMAND ----------

if not _table_exists(MKT):
    results.append(("SKIP", "INV5 market_data calendar-year", "market_data not available."))
else:
    _max_fy = _scalar(f"SELECT MAX(fiscal_year) FROM {MKT}")
    _cur = _scalar("SELECT YEAR(CURRENT_DATE())")
    if _max_fy is None:
        results.append(("SKIP", "INV5 market_data calendar-year", "market_data has no rows."))
    elif _max_fy > _cur:
        results.append(("FAIL", "INV5 market_data calendar-year",
                        f"max fiscal_year = {_max_fy} > current calendar year {_cur} — "
                        f"calendar/fiscal mislabel or bad join."))
    else:
        results.append(("PASS", "INV5 market_data calendar-year",
                        f"max fiscal_year = {_max_fy} (plausible; actual {_cur})."))

# COMMAND ----------

# MAGIC %md ## Report

# COMMAND ----------

print("=" * 72)
print("financials-invariants — structural check")
print("=" * 72)
for status, label, detail in results:
    print(f"  {status:4}  {label:32}  {detail}")
print()

_hard = [r for r in results if r[0] in HARD_FAIL]
if _hard:
    _summary = "; ".join(f"{lbl}: {dt}" for _, lbl, dt in _hard)
    raise RuntimeError(f"HARD FAIL — {len(_hard)} hard invariant(s): {_summary}")
print("✓ All hard invariants hold.")

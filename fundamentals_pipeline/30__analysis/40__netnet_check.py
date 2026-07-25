# Databricks notebook source
# MAGIC %md
# MAGIC # 30__analysis / 40__netnet_check
# MAGIC
# MAGIC Post-`22__derived_metrics` **regression gate** for the Net-Net Finder's three NCAV
# MAGIC conservatism levels (`NCAV` / `NCAV (Moderate)` / `NCAV (Strict)`, see milestone
# MAGIC "Net-Net Finder (value investing screener mode)", issue #254). Checks two pure
# MAGIC *ordering* invariants that hold by construction of the haircut formulas — no external
# MAGIC ground truth needed, so this is meant as the cheap first pass right after regenerating
# MAGIC `financials_metrics`, before any hand-calculation spot-check against real filings.
# MAGIC
# MAGIC **Checks:**
# MAGIC - NN1  new metric names present at all (else every other check would vacuously SKIP)  [hard]
# MAGIC - NN2  `NCAV (Strict) <= NCAV (Moderate) <= NCAV`, per (ticker, fiscal_year)           [hard]
# MAGIC - NN3  `NCAV Ratio <= NCAV (Moderate) Ratio <= NCAV (Strict) Ratio`, per (ticker, fiscal_year)  [hard]
# MAGIC
# MAGIC **Why these hold:** each stricter level applies a *larger or equal* haircut to every
# MAGIC current-asset line item (see issue #254's percentages), so the estimate can only shrink
# MAGIC as the level gets stricter. Ratio is Market Cap / NCAV — as the denominator shrinks
# MAGIC (staying positive, which is exactly when a ratio is computed at all), the ratio can only
# MAGIC grow. A violation of either means a sign/haircut-percentage bug, not a data-quality
# MAGIC issue — this notebook does NOT validate the formulas against real balance-sheet figures,
# MAGIC only that the three levels are internally consistent with each other.
# MAGIC
# MAGIC **Behavior:** prints PASS/FAIL/SKIP per check; `raise RuntimeError` if any hard check
# MAGIC fails or the new metrics aren't present yet (re-run `22__derived_metrics` first).

# COMMAND ----------

# MAGIC %run "../00__config/01__tickers"

# COMMAND ----------

MET = f"{CATALOG}.{SCHEMA}.financials_metrics"

# Tiny tolerance so an exact-equality edge case (e.g. a ticker with $0 of every haircut-able
# line item, so all three levels land on the identical figure) never reads as a floating-point
# violation.
TOLERANCE = 1e-6

results = []   # (status, label, detail) — status in PASS/FAIL/SKIP
HARD_FAIL = {"FAIL"}

_VALUE_METRICS = ("NCAV", "NCAV (Moderate)", "NCAV (Strict)")
_RATIO_METRICS = ("NCAV Ratio", "NCAV (Moderate) Ratio", "NCAV (Strict) Ratio")


def _scalar(sql):
    return spark.sql(sql).collect()[0][0]


# COMMAND ----------

# MAGIC %md ## NN1 — new metric names present at all

# COMMAND ----------

_present = _scalar(f"""
    SELECT COUNT(DISTINCT metric) FROM {MET}
    WHERE metric IN ('NCAV (Moderate)', 'NCAV (Strict)')
""")
if _present < 2:
    results.append(("FAIL", "NN1 new metrics present",
                     f"Only {_present}/2 of NCAV (Moderate)/NCAV (Strict) found in {MET}. "
                     f"Re-run 22__derived_metrics after landing issue #254 before checking "
                     f"NN2/NN3 — they'd otherwise vacuously SKIP on an empty result set."))
else:
    results.append(("PASS", "NN1 new metrics present",
                     "NCAV (Moderate) and NCAV (Strict) both have at least one row."))

# COMMAND ----------

# MAGIC %md ## NN2 — NCAV (Strict) <= NCAV (Moderate) <= NCAV, per (ticker, fiscal_year)

# COMMAND ----------

if _present < 2:
    results.append(("SKIP", "NN2 value ordering", "Skipped — see NN1."))
else:
    _row = spark.sql(f"""
        WITH pivoted AS (
            SELECT ticker, fiscal_year,
                   MAX(CASE WHEN metric = 'NCAV'           THEN value END) AS ncav_base,
                   MAX(CASE WHEN metric = 'NCAV (Moderate)' THEN value END) AS ncav_moderate,
                   MAX(CASE WHEN metric = 'NCAV (Strict)'   THEN value END) AS ncav_strict
            FROM {MET}
            WHERE metric IN {_VALUE_METRICS}
            GROUP BY ticker, fiscal_year
        )
        SELECT
            COUNT(*) AS checked,
            SUM(CASE
                WHEN NOT (ncav_strict <= ncav_moderate + {TOLERANCE}
                          AND ncav_moderate <= ncav_base + {TOLERANCE})
                THEN 1 ELSE 0
            END) AS violations
        FROM pivoted
        WHERE ncav_base IS NOT NULL AND ncav_moderate IS NOT NULL AND ncav_strict IS NOT NULL
    """).collect()[0]
    _checked, _violations = int(_row["checked"] or 0), int(_row["violations"] or 0)
    if _checked == 0:
        results.append(("SKIP", "NN2 value ordering",
                         "No (ticker, fiscal_year) has all three NCAV levels non-null yet."))
    elif _violations == 0:
        results.append(("PASS", "NN2 value ordering",
                         f"All {_checked:,} checked (ticker, fiscal_year) tuples satisfy "
                         f"Strict <= Moderate <= NCAV."))
    else:
        results.append(("FAIL", "NN2 value ordering",
                         f"{_violations:,}/{_checked:,} tuples violate Strict <= Moderate <= NCAV — "
                         f"check the haircut percentages/signs in 22__derived_metrics.py."))

# COMMAND ----------

# MAGIC %md ## NN3 — NCAV Ratio <= NCAV (Moderate) Ratio <= NCAV (Strict) Ratio, per (ticker, fiscal_year)

# COMMAND ----------

if _present < 2:
    results.append(("SKIP", "NN3 ratio ordering", "Skipped — see NN1."))
else:
    _row = spark.sql(f"""
        WITH pivoted AS (
            SELECT ticker, fiscal_year,
                   MAX(CASE WHEN metric = 'NCAV Ratio'             THEN value END) AS ratio_base,
                   MAX(CASE WHEN metric = 'NCAV (Moderate) Ratio'  THEN value END) AS ratio_moderate,
                   MAX(CASE WHEN metric = 'NCAV (Strict) Ratio'    THEN value END) AS ratio_strict
            FROM {MET}
            WHERE metric IN {_RATIO_METRICS}
            GROUP BY ticker, fiscal_year
        )
        SELECT
            COUNT(*) AS checked,
            SUM(CASE
                WHEN NOT (ratio_base <= ratio_moderate + {TOLERANCE}
                          AND ratio_moderate <= ratio_strict + {TOLERANCE})
                THEN 1 ELSE 0
            END) AS violations
        FROM pivoted
        WHERE ratio_base IS NOT NULL AND ratio_moderate IS NOT NULL AND ratio_strict IS NOT NULL
    """).collect()[0]
    _checked, _violations = int(_row["checked"] or 0), int(_row["violations"] or 0)
    if _checked == 0:
        results.append(("SKIP", "NN3 ratio ordering",
                         "No (ticker, fiscal_year) has all three NCAV ratios non-null yet "
                         "(expected: a stricter level's NCAV often turns <= 0, which leaves its "
                         "own ratio NULL by the existing NCAV Ratio NULL discipline)."))
    elif _violations == 0:
        results.append(("PASS", "NN3 ratio ordering",
                         f"All {_checked:,} checked (ticker, fiscal_year) tuples satisfy "
                         f"NCAV Ratio <= Moderate Ratio <= Strict Ratio."))
    else:
        results.append(("FAIL", "NN3 ratio ordering",
                         f"{_violations:,}/{_checked:,} tuples violate NCAV Ratio <= Moderate Ratio "
                         f"<= Strict Ratio — check the haircut percentages/signs in "
                         f"22__derived_metrics.py."))

# COMMAND ----------

# MAGIC %md ## Report

# COMMAND ----------

print("=" * 72)
print("netnet-check — NCAV level ordering regression gate")
print("=" * 72)
for status, label, detail in results:
    print(f"  {status:4}  {label:24}  {detail}")
print()

_hard = [r for r in results if r[0] in HARD_FAIL]
if _hard:
    _summary = "; ".join(f"{lbl}: {dt}" for _, lbl, dt in _hard)
    raise RuntimeError(f"HARD FAIL — {len(_hard)} check(s): {_summary}")
print("✓ NCAV levels are internally ordering-consistent. This does NOT confirm the formulas "
      "match real balance-sheet figures — pair with a hand-calculation spot-check on a few "
      "known tickers (see the milestone's validation discussion).")

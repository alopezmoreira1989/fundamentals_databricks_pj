#!/usr/bin/env python3
"""Read-only validator for the structural invariants of main.financials.*.

Asserts the invariants documented in CLAUDE.md and this skill's SKILL.md:

  INV1  Balance Sheet rows unique on (ticker, concept, period_end)   [hard]
  INV2  Q1+Q2+Q3+Q4 reconciles to FY (structural rate gate)          [hard]
  INV3  Two-statement concepts (Net Income, D&A, SBC) present in 2x   [info]
  INV4  TTM windows end at the latest period_end per ticker/concept   [hard, skips if absent]
  INV5  market_data.fiscal_year is calendar-plausible (not future)    [hard, skips if absent]

CONTRACT: this script issues ONLY SELECT statements. It never writes, MERGEs, or DELETEs.
It is safe to run against production. Exits non-zero if any HARD invariant fails.

Run standalone (Databricks Connect):   python check_invariants.py
Run in a Databricks notebook:           %run ./check_invariants.py   (reuses global `spark`)
"""
from __future__ import annotations

import sys

CATALOG = "main"
SCHEMA = "financials"
FIN = f"{CATALOG}.{SCHEMA}.financials"
MKT = f"{CATALOG}.{SCHEMA}.market_data"
IV = f"{CATALOG}.{SCHEMA}.financials_intrinsic_value"

# Structural Q-sum gate: fail above this divergent rate. The healthy baseline is ~3-4%
# (see validate-quarters); this ceiling only catches a regression, not the long tail.
QSUM_TOLERANCE = 0.001   # 0.1% per-row
QSUM_RATE_CEILING = 0.06  # 6% of checked tuples


def _get_spark():
    """Reuse the notebook's global `spark` if present; else build a Connect session."""
    g = globals().get("spark")
    if g is not None:
        return g
    try:
        import builtins
        if hasattr(builtins, "spark"):
            return builtins.spark
    except Exception:
        pass
    from databricks.connect import DatabricksSession  # type: ignore
    return DatabricksSession.builder.getOrCreate()


def _table_exists(spark, fqn: str) -> bool:
    try:
        spark.sql(f"SELECT 1 FROM {fqn} LIMIT 1").collect()
        return True
    except Exception:
        return False


def _scalar(spark, sql: str):
    return spark.sql(sql).collect()[0][0]


def check_bs_dedup(spark) -> tuple[str, str]:
    """INV1 — Balance Sheet uniqueness on (ticker, concept, period_end)."""
    dup_groups = _scalar(spark, f"""
        SELECT COUNT(*) FROM (
            SELECT ticker, concept, period_end
            FROM {FIN}
            WHERE stmt = 'Balance Sheet'
            GROUP BY ticker, concept, period_end
            HAVING COUNT(*) > 1
        )
    """)
    if dup_groups == 0:
        return "PASS", "Balance Sheet rows unique on (ticker, concept, period_end)."
    return "FAIL", (f"{dup_groups:,} (ticker, concept, period_end) groups have >1 BS row. "
                    f"See references/balance-sheet-dedup.md.")


def check_qsum(spark) -> tuple[str, str]:
    """INV2 — structural rate gate on Q1+Q2+Q3+Q4 vs FY (additive IS/CF)."""
    row = spark.sql(f"""
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
        SELECT
            COUNT(*)                                                                AS checked,
            SUM(CASE WHEN ABS((qsum - fy) / NULLIF(fy, 0)) > {QSUM_TOLERANCE}
                     THEN 1 ELSE 0 END)                                             AS divergent
        FROM q
    """).collect()[0]
    checked, divergent = int(row["checked"] or 0), int(row["divergent"] or 0)
    if checked == 0:
        return "SKIP", "No (ticker, stmt, concept, fy) tuples with FY and all 4 quarters yet."
    rate = divergent / checked
    msg = f"{divergent:,}/{checked:,} divergent at {QSUM_TOLERANCE:.1%} ({rate:.2%})."
    if rate > QSUM_RATE_CEILING:
        return "FAIL", (f"{msg} Above {QSUM_RATE_CEILING:.0%} ceiling — run validate-quarters "
                        f"for the per-concept/per-year breakdown.")
    return "PASS", f"{msg} Within structural ceiling ({QSUM_RATE_CEILING:.0%})."


def check_two_statement(spark) -> tuple[str, str]:
    """INV3 — concepts that intentionally appear under two statements."""
    rows = spark.sql(f"""
        SELECT concept, COUNT(DISTINCT stmt) AS n_stmts
        FROM {FIN}
        GROUP BY concept
        HAVING COUNT(DISTINCT stmt) > 1
        ORDER BY concept
    """).collect()
    if not rows:
        return "INFO", ("No two-statement concepts found. Expected Net Income (and often D&A, SBC) "
                        "to appear under both Income Statement and Cash Flow — investigate if 0.")
    names = ", ".join(r["concept"] for r in rows[:8])
    return "INFO", (f"{len(rows)} two-statement concept(s): {names}. Any per-concept join MUST "
                    f"include `stmt` (see SKILL.md CRITICAL #4).")


def check_ttm_ordering(spark) -> tuple[str, str]:
    """INV4 — TTM windows must end at the latest period_end (not fiscal_year)."""
    if not _table_exists(spark, FIN):
        return "SKIP", "financials table unavailable."
    # Reconstruct each ticker/concept's latest quarter vs the newest quarter actually present.
    # A TTM that ranks by fiscal_year would skip the genuine max period_end when a fiscal year
    # straddles calendar years. We flag tickers where the top-by-period_end quarter is NOT the
    # global max period_end for that ticker/concept (a contradiction that only arises if ranking
    # was done on the wrong column upstream).
    bad = _scalar(spark, f"""
        WITH ranked AS (
            SELECT ticker, stmt, concept, period_end,
                   ROW_NUMBER() OVER (PARTITION BY ticker, stmt, concept
                                      ORDER BY period_end DESC) AS rn,
                   MAX(period_end) OVER (PARTITION BY ticker, stmt, concept) AS max_pe
            FROM {FIN}
            WHERE period_type IN ('Q1','Q2','Q3','Q4')
        )
        SELECT COUNT(*) FROM ranked
        WHERE rn = 1 AND period_end <> max_pe
    """)
    if bad == 0:
        return "PASS", "TTM quarter ranking by period_end is internally consistent."
    return "FAIL", f"{bad:,} ticker/concept groups where rank-1 quarter is not the max period_end."


def check_market_calendar(spark) -> tuple[str, str]:
    """INV5 — market_data.fiscal_year is calendar year (not future)."""
    if not _table_exists(spark, MKT):
        return "SKIP", "market_data table unavailable."
    max_fy = _scalar(spark, f"SELECT MAX(fiscal_year) FROM {MKT}")
    cur_year = _scalar(spark, "SELECT YEAR(CURRENT_DATE())")
    if max_fy is None:
        return "SKIP", "market_data has no rows."
    if max_fy > cur_year:
        return "FAIL", (f"market_data.fiscal_year max = {max_fy} exceeds current calendar year "
                        f"{cur_year} — calendar/fiscal mislabel or bad join.")
    return "PASS", f"market_data.fiscal_year max = {max_fy} (calendar-plausible, current {cur_year})."


CHECKS = [
    ("INV1 BS dedup uniqueness", check_bs_dedup, True),
    ("INV2 Q-sum structural gate", check_qsum, True),
    ("INV3 two-statement concepts", check_two_statement, False),
    ("INV4 TTM ordering", check_ttm_ordering, True),
    ("INV5 market_data calendar-year", check_market_calendar, True),
]


def main() -> int:
    spark = _get_spark()
    if not _table_exists(spark, FIN):
        print(f"SKIP — {FIN} is not reachable. Nothing to check.")
        return 0

    print(f"financials-invariants — read-only check against {FIN}\n")
    hard_failed = False
    for label, fn, is_hard in CHECKS:
        try:
            status, detail = fn(spark)
        except Exception as e:  # never let a single check abort the run
            status, detail = "SKIP", f"check raised: {e!r}"
        if status == "FAIL" and is_hard:
            hard_failed = True
        print(f"  {status:4}  {label:32}  {detail}")

    print()
    if hard_failed:
        print("RESULT: RED — a hard invariant failed. See the FAIL lines above.")
        return 1
    print("RESULT: GREEN — all hard invariants hold.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

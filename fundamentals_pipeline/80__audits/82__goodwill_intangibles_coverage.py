"""Goodwill / Intangible Assets fill-rate coverage — READ-ONLY baseline.

Standalone analysis script (NOT wired into the 91 chain). No Delta writes, no commits.
Mirrors the fill-rate pattern of 81__cashflow_coverage.py, scoped to the two Balance
Sheet concepts touched by the tag-fallback fix in 01__tickers.py (Intangible Assets now
coalesces IntangibleAssetsNetExcludingGoodwill → FiniteLivedIntangibleAssetsNet →
OtherIntangibleAssetsNet; Goodwill unchanged, single-tag).

RESOLVED 2026-06-21 (full-refresh run 186680724649878): the multi-tag fallback re-ingested
and the lift is measured. Intangible Assets fill 53.1%→72.1% overall (+19.0 pts), 59.1%→74.7%
recent (+15.6 pts) — now ≈ Goodwill (75.5/76.8). Goodwill unchanged (single-tag). The residual
~3pt gap is genuine (filers with goodwill but no separable intangibles). Kept as a re-runnable
read-only coverage check; re-run after future refreshes to watch for regressions.

Denominator anchor: distinct (ticker, fiscal_year) pairs that reported ANY non-null
Balance Sheet FY concept that year (≈ "the filer reported a balance sheet"). Fill-rate
alone is not a bug — many small caps carry no goodwill/intangibles at all; the number to
watch is the DELTA across a refresh, not the absolute level.

Run locally via Databricks Connect (same as test_connection.py):
    python fundamentals_pipeline/80__audits/82__goodwill_intangibles_coverage.py
"""
from __future__ import annotations

RECENT_YEARS = 3
CONCEPTS = ["Goodwill", "Intangible Assets"]


def main() -> None:
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.getOrCreate()
    print("✓ Databricks Connect OK")

    max_fy = spark.sql("""
        SELECT MAX(fiscal_year) m FROM main.financials.financials
        WHERE period_type='FY' AND stmt='Balance Sheet' AND value IS NOT NULL
    """).collect()[0]["m"]
    lo = max_fy - (RECENT_YEARS - 1)
    print(f"✓ max_fy={max_fy}  recent window={lo}..{max_fy}\n")

    rows = spark.sql(f"""
        WITH bs AS (
            SELECT DISTINCT ticker, fiscal_year, concept
            FROM main.financials.financials
            WHERE stmt='Balance Sheet' AND period_type='FY' AND value IS NOT NULL
        ),
        denom_all AS (SELECT COUNT(*) n FROM (SELECT DISTINCT ticker, fiscal_year FROM bs)),
        denom_rec AS (SELECT COUNT(*) n FROM (SELECT DISTINCT ticker, fiscal_year FROM bs WHERE fiscal_year>={lo})),
        num_all AS (SELECT concept, COUNT(*) n FROM bs WHERE concept IN ('Goodwill','Intangible Assets') GROUP BY concept),
        num_rec AS (SELECT concept, COUNT(*) n FROM bs WHERE concept IN ('Goodwill','Intangible Assets') AND fiscal_year>={lo} GROUP BY concept)
        SELECT a.concept,
               a.n AS all_n,  (SELECT n FROM denom_all) AS all_denom,
               ROUND(100.0*a.n/(SELECT n FROM denom_all),1) AS all_pct,
               COALESCE(r.n,0) AS rec_n, (SELECT n FROM denom_rec) AS rec_denom,
               ROUND(100.0*COALESCE(r.n,0)/(SELECT n FROM denom_rec),1) AS rec_pct
        FROM num_all a LEFT JOIN num_rec r USING (concept)
    """).collect()

    fill = {r["concept"]: r.asDict() for r in rows}
    denom_all = rows[0]["all_denom"] if rows else 0
    denom_rec = rows[0]["rec_denom"] if rows else 0

    print(f"Denominator: {denom_all:,} (ticker, FY) BS pairs overall · {denom_rec:,} recent\n")
    print(f"{'Concept':<20} {'overall fill':>22} {'recent fill':>22}")
    print("-" * 66)
    for c in CONCEPTS:
        r = fill.get(c, {"all_n": 0, "all_pct": 0.0, "rec_n": 0, "rec_pct": 0.0})
        all_s = f"{r['all_pct']}% ({r['all_n']:,}/{denom_all:,})"
        rec_s = f"{r['rec_pct']}% ({r['rec_n']:,}/{denom_rec:,})"
        print(f"{c:<20} {all_s:>22} {rec_s:>22}")
    print("\n(RESOLVED 2026-06-21: Intangible fill 53/59%→72/75% after the fallback refresh; "
          "now ≈ Goodwill. Re-run to watch for regressions.)")


if __name__ == "__main__":
    main()

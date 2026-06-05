# Structural invariants (reference)

Companion to `SKILL.md`. Standalone queries for the invariants the validator checks, plus the
reasoning behind each. All queries are read-only.

## 1. TTM = 4 most recent quarters by `period_end` (not `fiscal_year`)

`23__intrinsic_value.py` builds the TTM variant by ranking each ticker's quarters with a window
ordered by `period_end DESC` and taking the top 4. Ordering by `fiscal_year` is wrong because
`market_data.fiscal_year` is calendar year and a fiscal year can straddle two calendar years, so
the "latest fiscal_year" is not always the latest period.

Signature of a regression: a ticker whose TTM-implied coverage excludes its most recent
`period_end` quarter. Probe the raw ranking:

```sql
WITH ranked AS (
  SELECT ticker, concept, period_end, value,
         ROW_NUMBER() OVER (PARTITION BY ticker, stmt, concept
                            ORDER BY period_end DESC) AS rn
  FROM main.financials.financials
  WHERE period_type IN ('Q1','Q2','Q3','Q4')
)
SELECT ticker, concept, MAX(CASE WHEN rn = 1 THEN period_end END) AS latest_q,
       MIN(CASE WHEN rn <= 4 THEN period_end END)                 AS oldest_in_ttm
FROM ranked
GROUP BY ticker, concept
HAVING COUNT(*) >= 4
ORDER BY ticker, concept;
```

`latest_q` must be the genuine max `period_end` for that ticker/concept; the 4-row TTM window
must be contiguous and end at `latest_q`. If a TTM table row references an older window, the
ordering regressed.

## 2. Net Income lives under TWO statements

Net Income (and Depreciation & Amortization, Stock-based Compensation) appear under both
`Income Statement` and `Cash Flow` with identical values. Any grouping/join keyed on `concept`
alone double-counts. Confirm the two-statement concepts:

```sql
SELECT concept, COUNT(DISTINCT stmt) AS n_stmts,
       COLLECT_SET(stmt) AS stmts
FROM main.financials.financials
GROUP BY concept
HAVING COUNT(DISTINCT stmt) > 1
ORDER BY concept;
```

This is the same omission that produces a fake ~100% divergence in [[validate-quarters]] when
`stmt` is dropped from its GROUP BY.

## 3. `market_data.fiscal_year` is calendar year

yfinance returns calendar year-end prices; the column is named `fiscal_year` only for join
convenience. For non-December fiscal-year enders there is a 0–11 month offset against
`financials`. This is acceptable for trend analysis but means a P/E for AAPL FY (Sep-end) pairs a
Sep fundamentals figure with a Dec price. Sanity-check the year range is calendar-plausible:

```sql
SELECT MIN(fiscal_year) AS min_fy, MAX(fiscal_year) AS max_fy, COUNT(*) AS n_rows
FROM main.financials.market_data;
```

`max_fy` should not exceed the current calendar year. A `max_fy` in the future signals a bad
join or a fiscal-vs-calendar mislabel.

## 4. Q4 derivation method (structural)

Q4 must be `FY − YTD_Q3`, not a naive sum. The structural consequence is that, for additive flow
concepts with all four quarters present, `Q1+Q2+Q3+Q4` reconciles to FY by construction. The
validator runs this as a fast pass/fail rate gate; the deep per-concept interpretation (and the
~3–4% healthy baseline) lives in [[validate-quarters]] — do not duplicate that analysis here.

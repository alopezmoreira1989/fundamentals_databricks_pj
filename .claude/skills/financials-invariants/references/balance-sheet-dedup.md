# Balance Sheet dedup invariant (reference)

Migrated from the former `validate-balance-sheet-dedup` skill (v1.0.0). The deterministic
pass/fail lives in `../scripts/check_invariants.py`; this file is the deep-dive for reading
borderline results and the variants a user may ask for.

## The invariant

> SEC re-reports prior snapshots in later 10-Qs. Dedup by `(ticker, concept, period_end)`
> keeping the latest `filed`.  — CLAUDE.md

If dedup worked, every Balance Sheet row in `main.financials.financials` is unique on
`(ticker, concept, period_end)`. Duplicates mean either (a) the merge in
`21__clean_and_merge.py` didn't apply the dedup, or (b) the dedup in
`21b__derive_quarterly.py` regressed when the schema changed.

Income Statement and Cash Flow rows can legitimately have multiple rows per `period_end`
(different `period_type`: Q1/Q2/Q3/Q4/FY) — the check is Balance-Sheet-only.

## Detail query

```sql
SELECT ticker, concept, period_end,
       COUNT(*)                  AS n_rows,
       COLLECT_LIST(value)       AS values,
       COLLECT_LIST(filed)       AS filed_dates,
       COLLECT_LIST(period_type) AS period_types
FROM main.financials.financials
WHERE stmt = 'Balance Sheet'
GROUP BY ticker, concept, period_end
HAVING COUNT(*) > 1
ORDER BY ticker, period_end DESC, concept;
```

## Reading the results

- **Zero rows** → invariant holds. Report in one line.
- **Same `period_end`, different `period_type`** (e.g. `FY` and `Q4` both kept) — expected if the
  merge stores BS snapshots under both labels. Confirm in `21b__derive_quarterly.py` that BS rows
  are intentionally duplicated across period_types; if so, narrow with `AND period_type = 'FY'` and
  re-run.
- **Same `period_end`, same `period_type`, different `filed`** → the "keep latest filed" dedup
  didn't run. Likely culprit: `21__clean_and_merge.py` lost the
  `ROW_NUMBER() OVER (PARTITION BY ticker, concept, period_end ORDER BY filed DESC)` window. The
  `values` column shows two close-but-not-identical numbers (original 10-Q vs restated 10-K/A).
- **Many rows, mixed values** → likely a regression in the `financials_raw → financials` write
  path. Confirm `21__clean_and_merge.py` ran on the latest `scraped_at`
  (`SELECT MAX(scraped_at) FROM main.financials.financials_raw`).

## Variants

- **One ticker**: add `AND ticker = '<X>'`.
- **One concept** (e.g. `'Total Assets'`): add `AND concept = 'Total Assets'`.
- **Count only**: wrap as `SELECT COUNT(*) AS dup_groups FROM (...) sub`.
- **Trace upstream**: run the same group against `main.financials.financials_raw` after filtering
  to a single `scraped_at` and a single `form`; multiple rows there mean SEC actually filed two
  facts for the same anchor (rare, usually `10-K` vs `10-K/A`).

## What NOT to do

- Don't widen to `Income Statement` / `Cash Flow` — `period_type` disambiguates them; duplicates on
  `(ticker, concept, period_end)` are expected there.
- Don't delete a duplicate row directly from `main.financials.financials`. Fix the upstream merge
  and re-run; otherwise the next pipeline pass recreates the dup.
- Don't equate "latest `filed`" with "largest value" — a restatement may correct an overstatement
  downward. Trust `filed`, not magnitude.

---
name: validate-balance-sheet-dedup
description: Spot-check that Balance Sheet rows in main.financials.financials are unique on (ticker, concept, period_end). Surfaces dedup failures where SEC re-reported a prior snapshot in a later 10-Q and the merge kept more than one row. Use after running the pipeline, when a balance-sheet figure looks doubled or stale, or before/after changes to 21__clean_and_merge.py or 21b__derive_quarterly.py.
---

# validate-balance-sheet-dedup

## Purpose

Verify the **Balance Sheet stock-concept dedup invariant** documented in CLAUDE.md:

> SEC re-reports prior snapshots in later 10-Qs. Dedup by `(ticker, concept, period_end)` keeping the latest `filed`.

If dedup worked, every Balance Sheet row in `main.financials.financials` is unique on `(ticker, concept, period_end)`. Duplicates mean either (a) the merge in `21__clean_and_merge.py` didn't apply the dedup, or (b) the dedup in `21b__derive_quarterly.py` regressed when the schema changed.

Income Statement and Cash Flow rows can legitimately have multiple rows per `period_end` (different `period_type`: Q1/Q2/Q3/Q4/FY) — this check is Balance-Sheet-only.

## When to run

- After a full pipeline run that touched ingestion or merge.
- When a user reports a Balance Sheet number looks doubled, stale, or inconsistent with their last filing.
- Before merging changes to `20_transformation/21__clean_and_merge.py` or `21b__derive_quarterly.py`.

## How to run

Run against the same Databricks SQL warehouse the dashboard uses (ask if not obvious).

```sql
SELECT ticker, concept, period_end,
       COUNT(*)                                     AS n_rows,
       COLLECT_LIST(value)                          AS values,
       COLLECT_LIST(filed)                          AS filed_dates,
       COLLECT_LIST(period_type)                    AS period_types
FROM main.financials.financials
WHERE stmt = 'Balance Sheet'
GROUP BY ticker, concept, period_end
HAVING COUNT(*) > 1
ORDER BY ticker, period_end DESC, concept;
```

To run from a notebook cell:

```python
spark.sql(""" ... query above ... """).display()
```

## Reading the results

- **Zero rows** → invariant holds. Report in one line.
- **A handful of rows, same `period_end`, different `period_type`** (e.g. `FY` and `Q4` both kept) — that's expected if the merge stores BS snapshots under both labels. Look at `21b__derive_quarterly.py` to confirm BS rows are intentionally duplicated across period_types; if so, narrow the check by adding `AND period_type = 'FY'` to the WHERE and re-run.
- **A handful of rows, same `period_end`, same `period_type`, different `filed`** → the dedup `keep latest filed` didn't run. Likely culprit: `21__clean_and_merge.py` lost the `ROW_NUMBER() OVER (PARTITION BY ticker, concept, period_end ORDER BY filed DESC)` window. The `values` column will show two close-but-not-identical numbers (the original 10-Q figure vs. the re-stated 10-K/A figure).
- **Many rows, mixed values** → likely a regression in the upstream `raw_full → full_tbl` write path. Check `21__clean_and_merge.py` ran successfully on the latest `scraped_at` (see `latest_scrape = spark.sql(f"SELECT MAX(scraped_at) ...")` pattern in `21b__derive_quarterly.py`).

## Variants the user may ask for

- **One ticker only**: add `AND ticker = '<X>'` to the WHERE clause.
- **Only a specific concept** (e.g. `'Total Assets'`): add `AND concept = 'Total Assets'`.
- **Just count, not detail**: replace the SELECT body with `SELECT COUNT(*) AS dup_groups FROM (...) sub`.
- **Compare against `financials_raw`** to confirm the dup originates upstream: same query against `main.financials.financials_raw` but you must also `GROUP BY filed` is NOT what you want — instead group only by `(ticker, concept, period_end)` after filtering to a single `scraped_at` and a single `form`; multiple rows there mean SEC actually filed two facts for the same anchor (rare, usually `10-K` vs `10-K/A`).

## What NOT to do

- Don't widen to `stmt IN ('Income Statement', 'Cash Flow')` — those use period_type to disambiguate, so duplicates on `(ticker, concept, period_end)` are expected and not a bug.
- Don't "fix" a single-row dup by deleting one row directly from `main.financials.financials`. Fix the upstream merge in `21__clean_and_merge.py` / `21b__derive_quarterly.py` and re-run; otherwise the next pipeline pass will recreate the dup.
- Don't assume `keep latest filed` means "keep largest value". The latest `filed` may have a smaller or negative value (e.g. a restatement that corrected an over-statement). Trust `filed`, not magnitude.

## Related

- [[validate-quarters]] — the flow-concept analogue (Q1+Q2+Q3+Q4 ≈ FY).

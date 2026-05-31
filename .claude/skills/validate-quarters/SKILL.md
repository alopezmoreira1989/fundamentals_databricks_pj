---
name: validate-quarters
description: Verify that Q1+Q2+Q3+Q4 ≈ FY in main.financials.financials (additive Income Statement and Cash Flow concepts), within a rounding tolerance. Lists divergent (ticker, stmt, concept, fiscal_year) rows. Use after running the pipeline, when investigating a suspect ticker, or when changes touch 21__clean_and_merge.py or 21b__derive_quarterly.py.
---

# validate-quarters

## Purpose

Sanity-check that the quarterly decomposition of FY values in `main.financials.financials` reconciles to within rounding error. This is the validation query the README documents but never automates.

Recall the invariant from the project's CLAUDE.md: **Q4 is intentionally derived as `FY − YTD_Q3`** to capture year-end audit adjustments. So `Q1+Q2+Q3+Q4` should equal FY by construction, modulo float rounding. Material divergence indicates either (a) a bug in `21b__derive_quarterly.py`, (b) a stale prune in `21c__prune_quarterly.py`, or (c) an SEC re-statement that didn't merge cleanly.

## When to run

- After a full pipeline run that modified ingestion or transformation.
- When the user asks "is the quarterly data sane?" or names a suspect ticker.
- Before making changes to `20_transformation/21b__derive_quarterly.py` or `21c__prune_quarterly.py` — to capture a baseline.

## How to run

Default tolerance is 0.1% (`0.001`). Default scope is **additive** Income Statement + Cash Flow concepts, grouped **per statement** (Balance Sheet `stock` concepts don't sum and are never checked this way; `flow_nonadditive` per-share/share-count concepts are excluded because they don't sum either). Grouping by `stmt` is mandatory — a concept that lives in two statements (e.g. Net Income, in both IS and CF) would otherwise be double-counted into a fake ~100% divergence.

Run this SQL against the Databricks SQL warehouse the user works against (ask which one if not obvious; the dashboard uses the same one):

```sql
WITH q AS (
    SELECT ticker, stmt, concept, fiscal_year,
           SUM(CASE WHEN period_type IN ('Q1','Q2','Q3','Q4') THEN value END) AS qsum,
           MAX(CASE WHEN period_type = 'FY' THEN value END)                    AS fy
    FROM main.financials.financials
    WHERE stmt IN ('Income Statement', 'Cash Flow')
      -- Exclude flow_nonadditive concepts: per-share / share-count values DON'T sum across
      -- quarters (Q4 is NULL by design), so they'd show huge false rel_diff. Keep this list in
      -- sync with kind='flow_nonadditive' in 00_config/01__tickers.py (STATEMENTS).
      AND concept NOT IN ('EPS Basic', 'EPS Diluted', 'Shares Diluted')
    -- GROUP BY includes `stmt` ON PURPOSE: Net Income (and D&A, SBC, …) appear in BOTH the
    -- Income Statement AND the Cash Flow statement with identical values. Grouping without
    -- `stmt` would SUM Q1..Q4 across both copies (≈2× the real quarter sum) while fy = MAX is
    -- the single value → a spurious ~100% divergence for every such concept. Per-stmt is also
    -- the correct granularity: each statement's quarters should reconcile to its own FY.
    GROUP BY ticker, stmt, concept, fiscal_year
    HAVING COUNT(DISTINCT period_type) = 5  -- has both FY and all 4 Qs
)
SELECT ticker, stmt, concept, fiscal_year, fy, qsum,
       (qsum - fy)                       AS abs_diff,
       (qsum - fy) / NULLIF(fy, 0)       AS rel_diff
FROM q
WHERE ABS((qsum - fy) / NULLIF(fy, 0)) > 0.001  -- 0.1% threshold
ORDER BY ABS(rel_diff) DESC, ticker, fiscal_year DESC;
```

To run from a notebook cell:

```python
spark.sql(""" ... query above ... """).display()
```

## Reading the results

Interpret the **rate** (divergent / checked), not the raw count — the universe is ~400k
(ticker, stmt, concept, fy) tuples.

- **Healthy baseline is ~3–4% divergent at 0.1%** (≈13k rows as of 2026-05-31), roughly flat
  across fiscal years. This is NOT a bug — it's the irreducible long tail: inherently lumpy
  concepts (Income Tax discrete items/true-ups), genuine 10-K/A restatements where old 10-Q YTD
  figures don't reconcile to the restated annual, and concepts with coexisting XBRL synonym tags
  not yet in `CONCEPT_PRIORITY` (Operating Income, Gross Profit, Income Tax…). Report the rate and
  the by-concept / by-year breakdown.
- **A year that spikes well above the others**, or a single concept dominating → regression. Net
  Income, Revenue, Equity and OCF are synonym-priority-pinned and should sit at the low end (~3%);
  if one jumps, suspect a recent change. Likely culprits in order:
  1. `21__clean_and_merge.py` — wrong FY row selected (e.g. a sub-annual `Q_standalone`/`other_Nd`
     shape, or a coexisting synonym tag winning `value desc` for a concept missing from
     `CONCEPT_PRIORITY`).
  2. `21b__derive_quarterly.py` — `kind` mis-tagged (`flow_additive` vs `flow_nonadditive`), or the
     FY-tag pin drawing YTD from a different tag than FY.
  3. `21c__prune_quarterly.py` — pruned a quarter still load-bearing for an old fiscal_year. Check
     `QUARTERLY_WINDOW` vs the row's `fiscal_year`.
- **Concept appears at ~100% divergence** → you almost certainly dropped `stmt` from the GROUP BY
  (double-counting a two-statement concept like Net Income). Re-add it.

## Variants the user may ask for

- **One ticker only**: add `AND ticker = '<X>'` inside the CTE's WHERE.
- **Stricter tolerance**: replace `0.001` with `0.0001` (0.01%).
- **Summary instead of the full list** (recommended — the list is ~13k rows): wrap the query and
  `SELECT COUNT(*)`, plus a `GROUP BY` rollup on concept / fiscal_year / a magnitude bucket
  (`CASE WHEN ABS(rel_diff)>0.05 …`). Always pair the divergent count with the total checked to get
  the rate.
- **Include `flow_nonadditive` concepts** (EPS Basic/Diluted, Shares Diluted): only if explicitly
  asked — they legitimately don't sum (Q4 NULL by design) and will show enormous `rel_diff`. Drop
  the `concept NOT IN (...)` filter to include them.

## What NOT to do

- Don't widen the scope to include `stmt = 'Balance Sheet'` — those are `stock` concepts (snapshots), not flows. They will look "wrong" but aren't.
- Don't drop `stmt` from the GROUP BY — it double-counts two-statement concepts (Net Income, D&A, SBC) into a fake ~100% divergence. (This was the bug in the pre-2026-05-31 version of this query.)
- Don't drop the `flow_nonadditive` exclusion — EPS/shares don't sum and will swamp the results with astronomical `rel_diff`.
- Don't read the raw count as a health verdict — ~3–4% is the normal baseline; judge by per-year/per-concept *shape* (a spike or a single dominant concept), not the absolute number.
- Don't "fix" a divergence by mutating `21b__derive_quarterly.py` to use `Q1+Q2+Q3+Q4` for Q4. Re-read the CLAUDE.md note: Q4 must stay `FY − YTD_Q3` to preserve year-end adjustments.
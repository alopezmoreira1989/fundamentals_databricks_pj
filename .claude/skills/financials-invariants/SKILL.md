---
name: financials-invariants
description: Structural data-integrity invariants of main.financials.financials and the intrinsic-value table, with a read-only validator (scripts/check_invariants.py). Covers Q4 derived as FY minus YTD_Q3 (never summed), TTM built from the 4 most recent quarters ordered by period_end (not fiscal_year), Net Income disambiguated by (stmt, concept), Balance Sheet snapshots deduped by (ticker, concept, period_end) keeping latest filed, and market_data.fiscal_year being calendar year. Use after a pipeline run, when a balance-sheet figure looks doubled or stale, when TTM or fiscal-year values look wrong, or before and after changes to 21__clean_and_merge.py / 21b__derive_quarterly.py. For numeric Q-sum reconciliation use validate-quarters.
metadata:
  author: Alejandro López Moreira
  version: 1.0.0
---

# financials-invariants

## CRITICAL

These invariants are intentional design decisions (CLAUDE.md). Do not "fix" them — verify them.

1. **Q4 = `FY − YTD_Q3`, never `Q1+Q2+Q3+Q4`.** `21b__derive_quarterly.py` derives Q4 by subtracting the YTD-Q3 figure from the annual FY so year-end audit adjustments land in Q4. Q1–Q3 come from standalone SEC reports or YTD deltas based on each concept's `kind`.
2. **TTM = the 4 most recent quarters ordered by `period_end`** (NOT `fiscal_year`). `market_data.fiscal_year` is calendar year, and a ticker's fiscal year can straddle calendar years, so ordering by `fiscal_year` picks the wrong four quarters. `23__intrinsic_value.py` ranks by `period_end`.
3. **Balance Sheet dedup key is `(ticker, concept, period_end)`, keep latest `filed`.** SEC re-reports a prior snapshot as a comparative in later 10-Qs; only the latest `filed` is kept.
4. **Net Income (and D&A, SBC) exist under TWO statements** (Income Statement and Cash Flow) with identical values. Any per-concept grouping or join MUST include `stmt`, or you double-count.
5. **`market_data.fiscal_year` is calendar year**, not fiscal. AAPL/Sep, MSFT/Jun, WMT/Jan carry a 0–11 month offset vs `financials` / `financials_metrics`. Joining the two on `fiscal_year` is intentional but lossy — never "correct" it silently.

This skill is the **structural** gate. For the numeric "do the quarters add up to FY within tolerance" reconciliation (with its ~3–4% healthy baseline), use [[validate-quarters]].

## When to run

- After a full pipeline run that touched ingestion, merge, or quarterly derivation.
- When a Balance Sheet number looks doubled, stale, or inconsistent with the latest filing.
- When TTM-based valuations or fiscal-year alignment look wrong.
- Before and after editing `20__transformation/21__clean_and_merge.py` or `21b__derive_quarterly.py`.

## How to run

Run the read-only validator against the same Databricks SQL warehouse the dashboard uses:

```bash
python .claude/skills/financials-invariants/scripts/check_invariants.py
```

Or from a Databricks notebook cell (the global `spark` is reused automatically):

```python
%run ./.claude/skills/financials-invariants/scripts/check_invariants.py
```

It prints PASS / FAIL / SKIP per invariant and exits non-zero if any hard invariant fails. It only issues SELECTs — no writes, no MERGE, no DELETE. See `scripts/check_invariants.py` for the exact queries. Hard checks: BS dedup uniqueness, Q-sum rate gate, market_data calendar-year. INFO: two-statement concepts. SKIP: TTM ordering (a code-level invariant in `23__intrinsic_value.py`, not observable from the published tables — verify by code review).

This same gate also runs inside the pipeline as **STEP 10b** of `90__pipelines/91__full_pipeline.py` via the notebook twin `30__analysis/34__invariants_check.py` (same queries, notebook form, raises on hard fail; the orchestrator wraps it non-fatally so a red invariant is logged without blocking the dashboard refresh). Keep the two in sync if you change a threshold.

For the full SQL of each check and how to read borderline results, see:

- `references/balance-sheet-dedup.md` — the dedup invariant in depth (migrated from the former `validate-balance-sheet-dedup` skill): variants, how to read same-`period_end` duplicates, and the upstream-vs-merge decision tree.
- `references/invariants.md` — TTM ordering, the two-statement Net Income rule, and the `market_data` calendar-year offset, each with a standalone query.

## Reading the results

- **All PASS** → report in one line.
- **Balance Sheet dedup FAIL** → duplicate `(ticker, concept, period_end)` rows. The fix is upstream in `21__clean_and_merge.py` / `21b__derive_quarterly.py` (the `ROW_NUMBER() OVER (PARTITION BY ticker, concept, period_end ORDER BY filed DESC)` window), then re-run. Do NOT delete rows directly — the next pass recreates them. Details in `references/balance-sheet-dedup.md`.
- **TTM ordering (SKIP)** → not data-checkable. If TTM-based valuations look wrong, review `23__intrinsic_value.py` for `period_end` (not `fiscal_year`) ordering, and use the descriptive probe in `references/invariants.md`.
- **Two-statement check FAIL/INFO** → a concept that should appear under two statements appears under one (or a downstream query dropped `stmt`). Cross-check [[validate-quarters]] (the same omission produces a fake ~100% Q-sum divergence).
- **Q-sum structural gate above ceiling** → escalate to [[validate-quarters]] for the per-concept / per-year breakdown; a single dominant concept usually points at a missing [[xbrl-concept-mapping]] priority/synonym.

## What NOT to do

- Don't widen the dedup check to `Income Statement` / `Cash Flow` — those legitimately have multiple rows per `period_end` (one per `period_type`). The check is Balance-Sheet-only.
- Don't assume "keep latest `filed`" means "keep largest value". A restatement can correct an over-statement downward. Trust `filed`, not magnitude.
- Don't change Q4 to a summation to make a reconciliation pass — see CRITICAL #1.
- Don't make this script write anything. It is read-only by contract; keep it that way.

## Related

- [[validate-quarters]] — numeric Q1+Q2+Q3+Q4 vs FY reconciliation (the interpretive half).
- [[validate-concept-hierarchy]] — verifies the `kind` field that drives Q4 derivation.
- [[xbrl-concept-mapping]] — when a single concept dominates a divergence, the cause is usually a missing tag-priority or synonym mapping.

---
name: validate-concept-hierarchy
description: Cross-check 00_config/concept_hierarchy.json against the STATEMENTS maps in 00_config/01__tickers.py. Reports concepts present in one and missing in the other, statements that disagree, and (against the live table) concepts that ingest into main.financials.financials but have no place in the hierarchy — meaning they won't appear in the dashboard. Use after editing the concept_hierarchy.json or after adding/renaming a concept in 01__tickers.py.
---

# validate-concept-hierarchy

## Purpose

The pipeline has **two parallel concept lists** that must stay in lockstep:

1. `FA PJ (Basic)/00_config/01__tickers.py` — `STATEMENTS = {"Income Statement": INCOME_STATEMENT, "Balance Sheet": BALANCE_SHEET, "Cash Flow": CASH_FLOW}`. Each entry maps a display name to `(xbrl_tag, kind)`. This drives **ingestion** — what gets fetched from SEC XBRL — and the `kind` drives **quarterly derivation** in `21b__derive_quarterly.py`.
2. `FA PJ (Basic)/00_config/concept_hierarchy.json` — drives **dashboard layout**: which concepts appear under which statement, in what order, with what `display_name`.

When these drift:
- A concept in `01__tickers.py` but **missing from concept_hierarchy.json** → ingested into `main.financials.financials` but invisible in the dashboard.
- A concept in concept_hierarchy.json but **missing from 01__tickers.py** → empty row in the dashboard with no data flowing in.
- A concept under the **wrong statement** in concept_hierarchy.json → row appears in the wrong tab.
- A concept in `01__tickers.py` with **no `kind`** (a tuple of length ≠ 2 or kind not in `{flow_additive, flow_nonadditive, stock}`) → `21b__derive_quarterly.py` will silently skip or mis-derive Q1–Q4.

## When to run

- After editing `concept_hierarchy.json` (added/removed/renamed/regrouped concepts).
- After adding or renaming a concept in `01__tickers.py`'s `INCOME_STATEMENT` / `BALANCE_SHEET` / `CASH_FLOW` dicts.
- Before a pipeline run that includes new concepts, to catch typos in either file.

## How to run

### Step 1 — Static check (file ↔ file)

Read both files and build two sets:

- From `FA PJ (Basic)/00_config/01__tickers.py`: parse `INCOME_STATEMENT`, `BALANCE_SHEET`, `CASH_FLOW` (top-level dict literals in that file). For each `{display_name: (xbrl_tag, kind)}` entry, record `(stmt, display_name, kind)`. Flag any entry where the tuple length ≠ 2 or `kind` is not in `{"flow_additive", "flow_nonadditive", "stock"}`.
- From `FA PJ (Basic)/00_config/concept_hierarchy.json`: walk each top-level statement (`"Income Statement"`, `"Balance Sheet"`, `"Cash Flow"`) and each group's `children`. Record every `"concept"` value under `(stmt, concept)`. Ignore `"group"` nodes — they're visual-only (the JSON's `_comment` documents this).

Then compute and report:

1. **In `01__tickers.py` but missing from `concept_hierarchy.json`** → list as `(stmt, concept)`. These get ingested but won't show in the dashboard.
2. **In `concept_hierarchy.json` but missing from `01__tickers.py`** → list as `(stmt, concept)`. These dashboard rows will be empty.
3. **Statement mismatch** — concept appears in both but under a different `stmt` → report `(concept, in_tickers.py: <stmt_a>, in_hierarchy: <stmt_b>)`.
4. **Bad `kind`** — entries in `01__tickers.py` with a missing or invalid `kind` → list as `(stmt, concept, value_seen)`.
5. **Balance Sheet concept tagged non-`stock`** or vice versa → flag as suspicious. All `BALANCE_SHEET` entries should have `kind = "stock"`; all entries in `INCOME_STATEMENT` and `CASH_FLOW` should be `flow_additive` or `flow_nonadditive`.

### Step 2 — Live check (vs. main.financials.financials)

Only run if a Databricks SQL warehouse is available. Skip with a one-line note otherwise.

```sql
SELECT stmt, concept, COUNT(DISTINCT ticker) AS n_tickers, MAX(filed) AS last_filed
FROM main.financials.financials
GROUP BY stmt, concept
ORDER BY stmt, concept;
```

Compare the `(stmt, concept)` set returned against `concept_hierarchy.json`:
- **In live table, missing from `concept_hierarchy.json`** → orphan concepts that the dashboard ignores.
- **In `concept_hierarchy.json`, missing from live table** → dashboard rows that have never been populated (could be a new concept that just hasn't ingested yet, or a typo).

## Reading the results

- **All checks return empty** → report "concept_hierarchy.json and 01__tickers.py are in sync" in one line.
- **Static check finds items, live check skipped** → list the static discrepancies and stop. Don't speculate about live state.
- **Concept in `01__tickers.py` and live table, missing from hierarchy** → almost always a forgotten dashboard update. Offer to draft the JSON entry (need: target statement, group it belongs to, optional `display_name`).
- **Concept in hierarchy and `01__tickers.py`, missing from live table** → either (a) brand-new concept that hasn't ingested yet (check `last_filed` is recent for nearby concepts), or (b) the XBRL tag in `01__tickers.py` doesn't exist for any ticker in the universe (the SEC fetch finds nothing).
- **Statement mismatch** → almost certainly a bug, since the dashboard groups by `stmt`. Fix the side that's wrong; usually the JSON is wrong because `01__tickers.py` is the ingestion contract.
- **Bad `kind`** → blocks `21b__derive_quarterly.py`. Fix before the next pipeline run.

## Variants

- **One statement only**: filter both sets to a single `stmt` (e.g. `"Income Statement"`).
- **Just the static check**: useful in pure-local environments where there's no SQL warehouse handy.
- **Output as a draft JSON snippet** for missing hierarchy entries — useful when the dashboard team will paste-add several at once.

## What NOT to do

- Don't add a concept to `concept_hierarchy.json` without also confirming the matching `(display_name, xbrl_tag, kind)` exists in `01__tickers.py`. The JSON only controls layout; it cannot create data.
- Don't translate the Spanish `_comment` / `_doc` fields in `concept_hierarchy.json`. Per CLAUDE.md, Spanish prose in `00_config/*.json` is intentional.
- Don't normalize `"Revenue (contract)"` away — `21b__derive_quarterly.py` already collapses it to `"Revenue"` before deriving. The two-row presence in `01__tickers.py` is intentional (alternative XBRL tag for newer ASC 606 filers).

## Related

- [[validate-quarters]] — verifies the `kind` field is doing its job once derivation has run.
- [[validate-balance-sheet-dedup]] — verifies the `stock` dedup path is correct.

---
name: validate-concept-hierarchy
description: Cross-check that 00__config/concept_hierarchy.json (dashboard layout) and the STATEMENTS maps in 00__config/01__tickers.py (ingestion contract) stay in sync, with an optional live check against main.financials.financials for orphan concepts. Reports concepts present in one file but not the other, statement mismatches, and invalid kind values. Use to validate or audit the hierarchy, to confirm the two lists are in sync after editing concept_hierarchy.json, or before a pipeline run that adds concepts. To author a new concept, map an XBRL tag, or fix a NULL or zero metric, use xbrl-concept-mapping.
metadata:
  author: Alejandro López Moreira
  version: 1.1.0
---

# validate-concept-hierarchy

## CRITICAL

- This skill **validates**; it does not author. To add/map a concept or fix a NULL/zero metric, use [[xbrl-concept-mapping]] (then run this to confirm the result is in sync).
- `01__tickers.py` is the **ingestion contract** (what gets fetched + the `kind` that drives quarterly derivation). `concept_hierarchy.json` is **layout only** (it cannot create data). When they disagree, the JSON is usually the side to fix.
- Don't translate Spanish `_comment` / `_doc` fields in the JSON — Spanish prose in `00__config/*.json` is intentional (CLAUDE.md).

## Purpose

The pipeline has **two parallel concept lists** that must stay in lockstep:

1. `fundamentals_pipeline/00__config/01__tickers.py` — `STATEMENTS = {"Income Statement": INCOME_STATEMENT, "Balance Sheet": BALANCE_SHEET, "Cash Flow": CASH_FLOW}`. Each entry maps a display name to `(xbrl_tag, kind)` — where `xbrl_tag` is a string for single-tag concepts or a list of fallback tags for multi-tag concepts (debt). This drives **ingestion** and the `kind` drives **quarterly derivation** in `21b__derive_quarterly.py`.
2. `fundamentals_pipeline/00__config/concept_hierarchy.json` — drives **dashboard layout**: which concepts appear under which statement, in what order, with what `display_name`.

When these drift:
- A concept in `01__tickers.py` but **missing from concept_hierarchy.json** → ingested into `main.financials.financials` but invisible in the dashboard.
- A concept in concept_hierarchy.json but **missing from 01__tickers.py** → empty row in the dashboard with no data flowing in.
- A concept under the **wrong statement** in concept_hierarchy.json → row appears in the wrong tab.
- A concept in `01__tickers.py` with **no/invalid `kind`** (kind not in `{flow_additive, flow_nonadditive, stock}`) → `21b__derive_quarterly.py` will silently skip or mis-derive Q1–Q4.

## When to run

- After editing `concept_hierarchy.json` (added/removed/renamed/regrouped concepts).
- After adding or renaming a concept in `01__tickers.py`'s `INCOME_STATEMENT` / `BALANCE_SHEET` / `CASH_FLOW` dicts (the authoring step itself is [[xbrl-concept-mapping]]).
- Before a pipeline run that includes new concepts, to catch typos in either file.

## How to run

### Step 1 — Static check (file ↔ file)

Read both files and build two sets:

- From `fundamentals_pipeline/00__config/01__tickers.py`: parse `INCOME_STATEMENT`, `BALANCE_SHEET`, `CASH_FLOW` (top-level dict literals). For each `{display_name: (xbrl_tag, kind)}` entry, record `(stmt, display_name, kind)`. Note `xbrl_tag` may be a `list` (debt fallback chain) — that's valid. Flag any entry where `kind` is not in `{"flow_additive", "flow_nonadditive", "stock"}`.
- From `fundamentals_pipeline/00__config/concept_hierarchy.json`: walk each top-level statement (`"Income Statement"`, `"Balance Sheet"`, `"Cash Flow"`) and each group's `children`. Record every `"concept"` value under `(stmt, concept)`. Ignore `"group"` nodes — they're visual-only (the JSON's `_comment` documents this).

Then compute and report:

1. **In `01__tickers.py` but missing from `concept_hierarchy.json`** → list as `(stmt, concept)`. These get ingested but won't show in the dashboard.
2. **In `concept_hierarchy.json` but missing from `01__tickers.py`** → list as `(stmt, concept)`. These dashboard rows will be empty.
3. **Statement mismatch** — concept appears in both but under a different `stmt` → report `(concept, in_tickers.py: <stmt_a>, in_hierarchy: <stmt_b>)`.
4. **Bad `kind`** — entries in `01__tickers.py` with a missing or invalid `kind` → list as `(stmt, concept, value_seen)`.
5. **Balance Sheet concept tagged non-`stock`** or vice versa → flag as suspicious. All `BALANCE_SHEET` entries should have `kind = "stock"`; all `INCOME_STATEMENT` / `CASH_FLOW` entries should be `flow_additive` or `flow_nonadditive`.

### Step 2 — Live check (vs. main.financials.financials)

Only run if a Databricks SQL warehouse is available. Skip with a one-line note otherwise.

```sql
SELECT stmt, concept, COUNT(DISTINCT ticker) AS n_tickers, MAX(scraped_at) AS last_scraped
FROM main.financials.financials
GROUP BY stmt, concept
ORDER BY stmt, concept;
```

Compare the `(stmt, concept)` set returned against `concept_hierarchy.json`:
- **In live table, missing from `concept_hierarchy.json`** → orphan concepts that the dashboard ignores.
- **In `concept_hierarchy.json`, missing from live table** → dashboard rows that have never been populated (could be a new concept that just hasn't ingested yet, or a typo).

> Note: the live table stores the canonical concept after synonym collapse (e.g. `Revenue`, not `Revenue (contract)`), because `CONCEPT_SYNONYMS` is applied in `21__clean_and_merge.py`. Compare against canonical names — see [[xbrl-concept-mapping]] for the synonym map.

## Reading the results

- **All checks return empty** → report "concept_hierarchy.json and 01__tickers.py are in sync" in one line.
- **Static check finds items, live check skipped** → list the static discrepancies and stop. Don't speculate about live state.
- **Concept in `01__tickers.py` and live table, missing from hierarchy** → almost always a forgotten dashboard update. Offer to draft the JSON entry (need: target statement, group, optional `display_name`).
- **Concept in hierarchy and `01__tickers.py`, missing from live table** → either (a) brand-new concept that hasn't ingested yet, or (b) the XBRL tag in `01__tickers.py` doesn't exist for any ticker in the universe (the SEC fetch finds nothing).
- **Statement mismatch** → almost certainly a bug, since the dashboard groups by `stmt`. Usually the JSON is wrong because `01__tickers.py` is the ingestion contract.
- **Bad `kind`** → blocks `21b__derive_quarterly.py`. Fix before the next pipeline run.

## Variants

- **One statement only**: filter both sets to a single `stmt`.
- **Just the static check**: useful in pure-local environments without a SQL warehouse.
- **Output as a draft JSON snippet** for missing hierarchy entries.

## What NOT to do

- Don't add a concept to `concept_hierarchy.json` without confirming the matching `(display_name, xbrl_tag, kind)` exists in `01__tickers.py`. The JSON only controls layout.
- Don't translate the Spanish `_comment` / `_doc` fields — intentional per CLAUDE.md.
- Don't normalize `"Revenue (contract)"` away — it's an intentional alternative XBRL tag collapsed to `"Revenue"` via `CONCEPT_SYNONYMS` before derivation.

## Related

- [[xbrl-concept-mapping]] — the authoring half: how to add a concept, the tag-priority / synonym mechanics, the debt fallback chain.
- [[validate-quarters]] — verifies the `kind` field is doing its job once derivation has run.
- [[financials-invariants]] — verifies the `stock` dedup path and other structural invariants.

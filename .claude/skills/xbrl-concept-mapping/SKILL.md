---
name: xbrl-concept-mapping
description: How to add or map an SEC XBRL concept in 00__config/01__tickers.py the right way, using priority-list coalescing wherever multiple tags can carry the same line item, never a bare single-tag lookup that silently returns NULL or zero. Covers the two coalescing mechanisms (debt tag-fallback lists at ingestion, and synonym plus CONCEPT_PRIORITY collapse at merge), the Revenue and Debt canonical patterns, and the debt fallback chain that fixes Debt to Equity reading 0.00x. Use when asked to add a concept, map an XBRL tag, fix a metric that returns NULL or zero, or fix debt to equity. Grounded in 11__fetch_sec_xbrl.py and concept_hierarchy.json. To validate the result is in sync, use validate-concept-hierarchy.
metadata:
  author: Alejandro López Moreira
  version: 1.0.0
---

# xbrl-concept-mapping

## CRITICAL

- **Issuers tag the same economic line item under different us-gaap concepts.** A bare single-tag lookup returns NULL/zero for every issuer that uses a different tag — the classic symptom is one metric blank or `0.00x` for a whole class of companies (AT&T / VZ Debt-to-Equity was `0.00x` because their debt sits under `LongTermDebt`, not `LongTermDebtNoncurrent`).
- There are **two distinct coalescing mechanisms** — pick the right one (full detail in `references/concept-tag-priorities.md`):
  1. **Tag-fallback list at ingestion** (`extract_series_multi` in `11__fetch_sec_xbrl.py`): one display name maps to a *list* of candidate XBRL tags, resolved per period to the highest-priority tag present. **Debt uses this.**
  2. **Synonym + `CONCEPT_PRIORITY` collapse at merge** (`21__clean_and_merge.py`): several *separate* display names, each its own single tag, are collapsed to one canonical name via `CONCEPT_SYNONYMS` and tie-broken by `CONCEPT_PRIORITY`. **Revenue, Net Income, Equity, OCF, Interest Expense use this.**
- After any change, run [[validate-concept-hierarchy]] (file ↔ file sync) and, post-pipeline, [[validate-quarters]] / [[financials-invariants]].
- Single-tag is fine and is the ~99% case — only reach for coalescing when multiple tags genuinely carry the line item.

## The data model

In `fundamentals_pipeline/00__config/01__tickers.py`, `STATEMENTS` maps each statement to a dict of
`display_name: (xbrl_tag, kind)`:

- `xbrl_tag` is a **string** (single tag, the common case) or a **list of strings** (priority-ordered fallback chain, used for debt).
- `kind` is one of `flow_additive`, `flow_nonadditive`, `stock` — it drives quarterly derivation in `21b__derive_quarterly.py` (Balance Sheet = `stock`; Income Statement / Cash Flow = a flow kind).

`SEC_USER_AGENT` (top of the file) must be a real org + email — SEC 403s the placeholder. As of
this writing it is set to a real value; the [[pipeline-preflight]] skill guards regressions.

## Mechanism 1 — Debt fallback chain (tag-list at ingestion)

Used when one display name should pull from whichever of several tags an issuer happens to file.
`extract_series_multi` concatenates the candidate tags' series and, per period
`(fy | period_start | period_end)`, keeps the value from the lowest-index (highest-priority) tag.

Current chains (verbatim from `01__tickers.py`):

```python
"Short-term Debt": (["DebtCurrent",          # corriente de la deuda total (más general)
                     "LongTermDebtCurrent",   # corriente de la deuda LP
                     "ShortTermBorrowings"],  # original
                    "stock"),
"Long-term Debt":  (["LongTermDebtNoncurrent",                   # estándar actual
                     "LongTermDebt",                             # deuda LP total (estilo AT&T)
                     "LongTermDebtAndCapitalLeaseObligations"],  # leases incluidos
                    "stock"),
```

To extend a chain, add the new tag in priority order (most-specific / most-common first). Order
matters: the first tag present for a given period wins.

## Mechanism 2 — Synonym + priority collapse (at merge)

Used when issuers report under genuinely different concepts that should all become one canonical
line. Each variant is its **own** `display_name` with its own single tag, then:

- `CONCEPT_SYNONYMS` maps each variant display name to the canonical (e.g. every `Revenue (…)` to `Revenue`; `Net Income (to common)` and `Net Income (incl NCI)` to `Net Income`).
- `CONCEPT_PRIORITY` assigns each variant an integer (lower = preferred) so that when more than one coexists for the same `(ticker, stmt, fy)`, the dedup in `21__clean_and_merge.py` keeps the preferred one — instead of falling through to `value desc`, which historically picked the wrong synonym.

**Revenue is the canonical example of mechanism 2** (8 variant tags collapsing to `Revenue`,
`Revenues`-first). **Debt is the canonical example of mechanism 1.** Do not conflate them.

The full current contents of both dicts, plus the kind taxonomy, are in
`references/concept-tag-priorities.md`.

## Recipes

### Add a brand-new single-tag concept
1. Add `"Display Name": ("XbrlTag", "<kind>")` to the right statement dict in `01__tickers.py`.
2. Add the concept to `concept_hierarchy.json` under the correct statement/group (it controls layout; it cannot create data).
3. Run [[validate-concept-hierarchy]].

### A metric is NULL or zero for a class of issuers
1. Confirm the line item exists in their filings under a *different* us-gaap tag (SEC company-facts JSON, or compare a peer that works).
2. If it's the same display line: convert the concept to a **mechanism-1 tag list** (add the alternate tag in priority order).
3. If issuers report it as a *different* named concept you already ingest: add a **mechanism-2** `CONCEPT_SYNONYMS` entry and a `CONCEPT_PRIORITY` rank.
4. Re-run ingestion + merge; verify with [[financials-invariants]] / [[validate-quarters]].

### Fix Debt to Equity reading 0.00x
This is the AT&T/VZ symptom — debt under `LongTermDebt`, not `LongTermDebtNoncurrent`. The fallback
chains above already cover it; if a new issuer still shows `0.00x`, find its debt tag and append it
to the relevant chain (mechanism 1). Note: leverage `Total Debt` is left NULL (not 0) when both
debt tags are absent, so a genuinely missing tag reads as absent, not as "debt-free".

## What NOT to do

- Don't add a tag list when the issuers actually use different *named* concepts — that's mechanism 2 (synonyms), not mechanism 1.
- Don't add a `CONCEPT_SYNONYMS` entry without a matching `CONCEPT_PRIORITY` rank when the variant can coexist with the canonical in the same fiscal year — without the rank, dedup falls back to `value desc` and may pick the wrong one.
- Don't translate the Spanish inline comments — intentional per CLAUDE.md.
- Don't edit `concept_hierarchy.json` expecting data to appear; layout cannot create facts.

## Related

- [[validate-concept-hierarchy]] — the deterministic file ↔ file (and live) cross-check after you map a concept.
- [[validate-quarters]] — a single concept dominating a Q-sum divergence usually means a missing priority/synonym here.
- [[financials-invariants]] — structural checks after re-ingestion.

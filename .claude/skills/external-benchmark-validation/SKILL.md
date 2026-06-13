---
name: external-benchmark-validation
description: Reconcile this pipeline's published values against an INDEPENDENT ground truth — primarily the linkbase oracle (each filer's own XBRL presentation linkbase) via the two-tier reconciliation job, and secondarily a manual cross-check against a third party such as Finviz. Use to find wrong/missing-tag coverage gaps, value/scale/sign/period mangling, or to confirm a number against an external source, and to attribute the cause (concept-mapping gap, fiscal-vs-calendar offset, TTM-vs-FY basis, or a genuine definitional difference). This is external cross-validation, distinct from the internal self-consistency checks in financials-invariants and validate-quarters.
metadata:
  author: Alejandro López Moreira
  version: 2.0.0
---

# external-benchmark-validation

## CRITICAL

- This is **external** cross-validation (our value vs an independent source). For **internal**
  self-consistency use [[financials-invariants]] (structural) and [[validate-quarters]] (numeric).
  Don't conflate a definitional gap with a pipeline bug.
- The primary tool is the **linkbase oracle reconciliation job** (`90_pipelines/92__reconciliation_job`)
  — an independent ground truth built from each filer's own XBRL **presentation linkbase**, so a
  discrepancy can never be blamed on the SEC renderer. It produces a findings table + clustered
  parquet that a **human reviews (yes/no)** before any fix is coded.
- A confirmed, unexplained delta on a *class* of issuers is the signal that points at
  [[xbrl-concept-mapping]] (a tag/synonym gap) — the same root cause behind Debt-to-Equity `0.00x`.
- Reconciler is **read-only** against `main.financials.*`; it writes only `main.config.*`.

## Architecture — the two-tier oracle

| Tier | Question | Source | Scope (v1) |
|---|---|---|---|
| **A — coverage / wrong-tag** | Did the app pick the wrong (or no) tag for a statement line? | `reconciliation_oracle` (linkbase: statement-line→tag→value) | **golden set** only |
| **B/C — value / scale / sign / period** | Given the chosen tag, is the value mangled? | `financials_raw` (companyfacts proxy, already ingested) | **universe-wide** |

- **Tier A** needs the per-filing linkbase, so it runs only on the golden set
  (`00_config/reconciliation_golden_set.json` ∪ `COMBINED_FILERS`). `14__fetch_oracle_statements`
  parses `FilingSummary.xml` (`MenuCategory="Statements"`, `Role`→presentation role) + `*_pre.xml`
  (line items + `preferredLabel`) + `*_lab.xml` (labels, `negatedLabel`) + the instance (`*_htm.xml`,
  consolidated context only). It stores the **natural-sign, raw (unscaled)** value and a `negated`
  flag; custom-namespace tags get `tag_namespace='custom'`.
- **Tier B/C** is cheap and universe-wide: the app's value should equal the raw companyfacts fact for
  the (tag, period) it chose — same XBRL. We compare `financials.value` to candidate
  `financials_raw` values at the same `period_end`, bridging the canonical concept to the raw
  pre-synonym label via reverse `CONCEPT_SYNONYMS`. **Derived rows (`is_derived = true`, e.g.
  `Q4 = FY − YTD_Q3`) are excluded** — they are not a single raw fact.

## Files & tables

- `10_ingestion/14__fetch_oracle_statements.py` → `main.config.reconciliation_oracle`
- `30_analysis/35__reconcile_filings.py` → `main.config.reconciliation_findings`
  (+ view `main.config.reconciliation_open`, + `/tmp/reconciliation_findings.parquet`)
- `90_pipelines/92__reconciliation_job.py` — orchestrates 14 then 35 (standalone, monthly/on-demand;
  **not** wired into `91__full_pipeline`)
- `00_config/reconciliation_golden_set.json` — editable Tier-A scope

## issue_class → root cause → fix mechanism

| issue_class | Meaning | Mechanism (deterministic) |
|---|---|---|
| `COVERAGE_GAP` (us-gaap, mapped tag) | oracle has the line; app NULL/absent | `synonym+priority` — mapped tag not landing → [[xbrl-concept-mapping]] |
| `COVERAGE_GAP` (us-gaap, untracked) | us-gaap line not in the concept map | `mechanism-1 tag-list` — add the tag → [[xbrl-concept-mapping]] |
| `COVERAGE_GAP` (`custom`) | filer custom-namespace line | `custom — not mappable` (no us-gaap fix exists) |
| `ORACLE_VALUE_MISMATCH` | app has a value, but it disagrees with the **highest-priority presented** us-gaap tag (wrong synonym picked, or value mangled) — invisible to Tier B/C, which matches the app's own chosen raw fact. (Net Income on the Cash Flow line is excluded — the IS covers its value; the CF line legitimately uses total `ProfitLoss` incl. NCI.) | `synonym+priority` — `CONCEPT_PRIORITY` / inspect `21__clean_and_merge` → [[xbrl-concept-mapping]] |
| `VALUE_MISMATCH` | both present, beyond 0.5% | inspect `21__clean_and_merge` / float precision |
| `SCALE_SIGN` | app ≈ raw × 10ⁿ, or a clean sign flip not explained by `negated` | scale fix in `21__clean_and_merge` |
| `PERIOD_MISALIGN` | app value matches the raw fact at a **different** `period_end` | period anchor in `21__`/`22__` (fiscal/calendar) |
| `ORACLE_PARSE_FAIL` | oracle missing where expected | counts against the **validator**, not the pipeline |

**Severity (deterministic):** `high` if the concept feeds a valuation (the `23__intrinsic_value`
inputs) or the cluster spans ≥25 tickers; `med` if displayed (in `concept_hierarchy`) but
non-valuation; `low` otherwise.

## Procedure — running the reconciliation

1. **Run the job:** `90_pipelines/92__reconciliation_job` as a Databricks Job (or run `14` then `35`
   individually). `14` respects the SEC rate limit + real `SEC_USER_AGENT`; `35` is read-only against
   `financials`/`financials_raw`.
2. **Review the clusters** in `/tmp/reconciliation_findings.parquet` (or
   `main.config.reconciliation_open`). Clustering key is `(issue_class, concept, stmt,
   root_cause_hypothesis)` — 200 tickers missing one concept collapse to one review item.
3. **Decide per cluster (the gate):** confirm whether each is a real bug or a basis/definitional
   difference. Only then code a fix via the mapped mechanism.
4. **Watch `ORACLE_PARSE_FAIL`** — it measures the validator's own false-positive rate. Tier A stays
   golden-set-only until that rate is acceptably low (the **validate-the-validator gate**); only then
   does Tier A go universe-wide (v2).

## Procedure — manual cross-check (Finviz / other, secondary)

Use when sanity-checking one ticker against a public site (the oracle is preferred when available).

1. **Pick the comparison basis.** TTM vs FY, which fiscal year / price date. Our TTM lives in
   `financials_intrinsic_value` and TTM metrics; FY in `financials_metrics`. Finviz fundamentals are
   typically TTM with a live price.
2. **Pull our numbers** for the ticker from `main.financials.financials_metrics` (raw inputs from
   `main.financials.financials` if you need to decompose).
3. **Pull the reference** (record value, period, implied price/date).
4. **Compute relative delta** per field: `abs(ours - ref) / abs(ref)`. Flag above threshold (default
   5%; 1–2% for clean ratios like margins).
5. **Attribute** each flagged field (table below) and report: field, ours, ref, delta %, likely cause,
   basis-difference (expected) vs real discrepancy (actionable).

| Symptom | Likely cause | Where to fix / confirm |
|---|---|---|
| Off by a roughly constant ratio across many fields | TTM vs FY basis mismatch | recompute on the same basis first |
| Valuation multiple off, fundamentals match | price/date basis | market cap is now `period_end`-anchored via `market_prices_daily` ([[financials-invariants]]) |
| One metric NULL/0 for a class of issuers | concept-mapping gap | [[xbrl-concept-mapping]] (debt chain / synonyms) |
| Debt-to-Equity ~0.00x | debt under an untagged variant | [[xbrl-concept-mapping]] debt fallback chain |
| Quarter figure off but annual matches | quarterly derivation | [[validate-quarters]] |
| Definitional (EBITDA, adjusted EPS) | different formula than the reference | document, not a bug |

## What NOT to do

- Don't treat every delta as a defect — confirm basis (TTM/FY, price date, definition) first.
- Don't scale Tier A to the universe before the false-positive gate passes.
- Don't auto-scrape or hammer an external site; pull the few fields you need for one ticker.
- Don't "fix" our number to match a reference whose definition differs — fix only genuine
  mapping/derivation errors, and document definitional differences.
- Don't propose a us-gaap fix for a `custom`-namespace coverage gap (no such fix exists).
- Don't change pipeline code from this skill without the normal plan-first flow
  ([[databricks-notebook]], [[xbrl-concept-mapping]]).

## Related

- [[xbrl-concept-mapping]] — the usual fix when a confirmed delta traces to a tag/synonym gap.
- [[valuation-methods]] — our valuation definitions, to compare against a reference's.
- [[financials-invariants]] / [[validate-quarters]] — internal consistency; run before blaming the source.
- [[databricks-notebook]] — conventions for the notebooks this skill drives.

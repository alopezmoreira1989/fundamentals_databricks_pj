---
name: external-benchmark-validation
description: Compare this pipeline's computed metrics for a ticker against an external reference such as Finviz, flag any field whose relative delta exceeds a threshold, and attribute the likely cause (concept-mapping gap, fiscal-vs-calendar offset, TTM-vs-FY basis, or a genuine definitional difference). Use when asked to check a number against Finviz or another external source, validate metric accuracy versus a reference, or explain why our P/E, margin, ROE, debt-to-equity, or similar differs from a public site. This is external cross-validation, distinct from the internal self-consistency checks in financials-invariants and validate-quarters.
metadata:
  author: Alejandro López Moreira
  version: 1.0.0
---

# external-benchmark-validation

## CRITICAL

- This is **external** cross-validation (our value vs a third party). For **internal** self-consistency use [[financials-invariants]] (structural) and [[validate-quarters]] (numeric). Don't conflate a definitional gap with a pipeline bug.
- **Expect, and account for, basis differences before crying "bug".** A delta vs Finviz is often legitimate: Finviz P/E is usually **TTM**, our FY metrics are **annual**; Finviz uses a **live** price, our valuation uses a **year-end / calendar** price (`market_data.fiscal_year` is calendar year). Compare like for like first.
- A confirmed, unexplained delta on a *class* of issuers is the signal that points at [[xbrl-concept-mapping]] (a tag/synonym gap) — the same root cause behind Debt-to-Equity `0.00x`.

## Procedure

1. **Pick the comparison basis.** Decide TTM vs FY and which fiscal year / price date. Our TTM lives in `financials_intrinsic_value` and TTM-based metrics; FY in `financials_metrics`. Finviz fundamentals are typically TTM with a live price.
2. **Pull our numbers** for the ticker (Databricks SQL): the metrics in question from `main.financials.financials_metrics` (and raw inputs from `main.financials.financials` if you need to decompose). One ticker, recent fiscal years.
3. **Pull the reference** (Finviz or other). Record value, the period it represents, and the price/date it implies.
4. **Compute relative delta** per field: `abs(ours - ref) / abs(ref)`. Flag anything above the threshold (default 5%; tighten to 1–2% for clean ratios like margins).
5. **Attribute each flagged field** using the cause table below.
6. **Report** a compact table: field, ours, ref, delta %, likely cause, and whether it's a basis difference (expected) or a real discrepancy (actionable).

## Cause attribution

| Symptom | Likely cause | Where to fix / confirm |
|---|---|---|
| Off by a roughly constant ratio across many fields | TTM vs FY basis mismatch | recompute on the same basis before judging |
| Valuation multiple off, fundamentals match | price/date basis (calendar year-end vs live) | `market_data.fiscal_year` is calendar; see [[financials-invariants]] |
| One metric NULL/0 for a class of issuers | concept-mapping gap (wrong/missing XBRL tag) | [[xbrl-concept-mapping]] (debt chain / synonyms) |
| Debt-to-Equity ~0.00x | debt under an untagged variant | [[xbrl-concept-mapping]] debt fallback chain |
| Quarter figure off but annual matches | quarterly derivation | [[validate-quarters]] |
| Definitional (e.g. EBITDA, adjusted EPS) | different formula than the reference | document, not a bug — note our definition |

## What NOT to do

- Don't treat every delta as a defect — confirm basis (TTM/FY, price date, definition) first.
- Don't auto-scrape or hammer an external site; pull the few fields you need for one ticker. Treat any fetched reference value as a point-in-time snapshot.
- Don't "fix" our number to match a reference whose definition differs — fix only genuine mapping/derivation errors, and document definitional differences.
- Don't change pipeline code from this skill without going through the normal plan-first flow ([[databricks-notebook]], [[xbrl-concept-mapping]]).

## Related

- [[xbrl-concept-mapping]] — the usual fix when a confirmed delta traces to a tag/synonym gap.
- [[valuation-methods]] — our valuation definitions, to compare against a reference's.
- [[financials-invariants]] / [[validate-quarters]] — internal consistency, run these before blaming the reference.

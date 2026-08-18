# Phase 6.6 (Research) — Can the ESEF/IFRS Taxonomy Replace Part of Our Manual EU Mapping?

**READ-ONLY research. No code changed, no mapping added, no production write, nothing run
(16/21/22/23/51/52 untouched).** This document investigates whether the official ESMA/ESEF
taxonomy infrastructure — presentation, calculation, and definition linkbases; anchoring; labels
— can reduce or replace the manually-curated `EU_CANONICAL_MAPPING` this project has built by
hand across Phases 5.1–6.6.

Every claim is labeled **VERIFIED** (obtained by directly fetching and parsing real ESEF taxonomy
files this session — not from documentation alone), **CITED** (from authoritative external
sources, not independently re-derived), or **INFERRED**.

---

## 1. Executive Summary

**The taxonomy is real, reliably fetchable, and genuinely useful — but for a narrower slice of
our actual problem than the premise of this research assumed.** The concrete, decisive finding,
obtained by fetching and parsing FCC's real definition linkbase this session: **ESEF anchoring
only applies to issuer *extension* concepts anchored to a core IFRS concept — it has no mechanism
for relating two *standard* IFRS concepts to each other.** Almost every real mapping problem this
project has actually hit across Phases 6.0–6.6 (`Revenue` vs. `RevenueFromContractsWithCustomers`,
`TradeReceivables` vs. `CurrentTradeReceivables` vs. `TradeAndOtherCurrentReceivables`, `Equity`
vs. `EquityAttributableToOwnersOfParent`, `ProfitLoss` vs.
`ProfitLossAttributableToOwnersOfParent`) is a standard-concept-to-standard-concept question —
exactly the category anchoring cannot address. **This alone is close to a complete answer to the
research question for the highest-value use case (Tier A/B concept selection): the taxonomy
cannot safely automate it, because the relevant relationship isn't represented anywhere in the
taxonomy at all.**

**Where the taxonomy genuinely helps, verified with real data**: the **presentation linkbase**
reliably classifies which primary statement a concept belongs to, *per filing* — verified
directly against FCC's real filing, where `ifrs-full:ProfitLoss` appears **only** under the
`StatementOfComprehensiveIncome` presentation role, never under `StatementOfCashFlows` (whose own
tree uses different, dedicated reconciliation concepts instead). Had this project consulted
per-filing presentation-linkbase membership instead of building one hand-maintained,
global `_LABEL_TO_STMT_KIND` table, **the Phase 6.3 Net Income statement-misclassification bug
would very plausibly never have happened** — this is the single clearest concrete "the taxonomy
would have helped" finding in this research.

**Anchoring, for the one real extension case fetched (FCC's own
`DeudoresComercialesOtrasCuentasCobrar`, Spanish for "trade debtors and other receivables"),
anchors to the generic `ifrs-full:CurrentAssets` — not to anything Accounts-Receivable-specific**
— a real, concrete demonstration that even where anchoring exists, it is frequently too coarse to
drive automatic normalization to our specific canonical concepts.

**Recommendation: PARTIALLY USEFUL. Do not build a taxonomy-driven or taxonomy-assisted mapping
architecture.** Keep the current manual, evidence-verified `EU_CANONICAL_MAPPING` approach for
concept selection (Tier A/B/C decisions) — it is not "unnecessarily manual"; it is manual because
the taxonomy genuinely does not carry the signal that decision needs. Adopt exactly one narrow,
high-confidence, mechanically-checkable use: **presentation-linkbase-based per-filing statement
classification**, as a validation/cross-check layer against the existing hand-maintained
`STATEMENTS` dict — not a replacement for it, and not attempted in this phase (research only).

---

## 2. Current Mapping Architecture (Part 1)

**VERIFIED**, direct code read, current branch tip (no changes made).

- **`01__tickers.py`**: `STATEMENTS` (`INCOME_STATEMENT`/`BALANCE_SHEET`/`CASH_FLOW`, the single
  canonical model shared by SEC/Canada/EU), `IFRS_FALLBACK_TAGS` (SEC-side ifrs-full 20-F/40-F
  fallback tags — a *different* mechanism from the EU adapter, confirmed in Phase 6.6's own
  implementation work: `16__fetch_eu_xbrl.py` never reads this dict), `CONCEPT_SYNONYMS`
  (post-ingestion alias collapse, e.g. Revenue's ASC-606 variants, Net Income's incl-NCI
  fallback), `CONCEPT_PRIORITY`/`CONCEPT_PRIORITY_BY_STMT` (deterministic tiebreak when multiple
  accepted tags coexist for the same key).
- **`sources/eu_current.py`**: `EU_CANONICAL_MAPPING` — the EU adapter's own, independent
  tag-to-canonical dict (30 canonical concepts / 36 accepted source tags as of Phase 6.6), each
  entry a `MappingDecision` with `status`/`mapping_type`/`source_concept`/free-text `notes`
  citing the real evidence (issuer, filing, value) that justified it.
- **`16__fetch_eu_xbrl.py`**: `extract_source_facts()` filters raw xBRL-JSON facts to
  consolidated (`is_consolidated_fact`), current-period (`is_current_period_fact`) only, before
  `map_source_fact_to_canonical()` ever sees them — dimensional/comparative facts are already
  excluded structurally, upstream of the mapping question this research investigates.

**What the current system knows**: exactly the source tag string, mapped by a human who checked
real fact values across the 8 admitted issuers.

**What it does NOT know**: anything about the tag's *position* in a filing's presentation tree,
its calculation relationships, its label text, or its relationship (if any) to other tags — none
of that metadata is fetched or consulted anywhere in the current pipeline. This gap is exactly
what this research investigates.

---

## 3. What the ESEF/IFRS Taxonomy Actually Provides (Part 2)

**CITED** (ESMA/XBRL International official sources) + **VERIFIED** (real fetches, §4 below).

An IFRS/ESEF XBRL concept carries, in principle: a standard label (and translations — see §4),
documentation, namespace, concept/data type (`monetaryItemType`, etc.), balance attribute
(debit/credit), period type (instant/duration), substitution group, and — via **linkbases**,
separate files from the concept definition itself — presentation relationships (parent-child
display hierarchy, grouped by primary statement), calculation relationships (summation-item
arcs), and definition relationships (including, specifically for ESEF, **anchoring**).

**Anchoring** ([xbrl.org guidance](https://www.xbrl.org/guidance/esef-rules-anchoring-extensions/),
[ESMA32-60-254 ESEF Reporting Manual](https://www.esma.europa.eu/sites/default/files/library/esma32-60-254_esef_reporting_manual.pdf)):
required **only** for issuer-created extension elements, linking an extension to the closest core
taxonomy element that is **wider or narrower in accounting meaning or scope** — explicitly *not*
a claim of equivalence. Per the ESEF Reporting Manual's own 2024 update, an extension should
anchor to a core element sharing the same *data type* (e.g. a `monetaryItemType` extension only
anchors to a `monetaryItemType` core concept). Anchoring relationships are recorded in the
**definition linkbase**, using a dedicated arcrole,
`http://www.esma.europa.eu/xbrl/esef/arcrole/wider-narrower` — confirmed, verbatim, in real data
(§4). There is **no anchoring requirement for notes-block-tagged extensions** (optional there).

**Directly answering Part 2's core question ("can relationships tell us two concepts are
related without falsely implying equivalence")**: yes, structurally — the `wider-narrower`
arcrole is explicit that it is *not* equivalence. But this only ever connects an **extension**
to a **core** concept. There is no XBRL/ESEF mechanism (anchoring or otherwise) that relates two
different **core** ifrs-full concepts to each other — confirmed by inspecting the actual
mechanism's defined scope (CITED) and by finding zero such relationships in real data for the
concepts this project has actually needed to relate (§5).

---

## 4. Real Filing Investigation (Part 4) — the Decisive Evidence

**VERIFIED**, live fetches performed this session against `filings.xbrl.org`, FCC's real
FY2024 ESEF filing (`95980020140005178328-2024-12-31-ESEF-ES-0`).

### 4.1 What the xBRL-JSON our adapter already consumes actually contains

Fetched the exact JSON `16__fetch_eu_xbrl.py`/`sources/eu_current.py` parse today. Top-level
keys: **`documentInfo`** (namespaces + a single reference URL to the extension taxonomy's `.xsd`
— not its content) and **`facts`** (the flat fact list). **No labels, no presentation/
calculation/definition relationships, no anchoring anywhere in this file.** Everything this
research is investigating is structurally absent from the data source our pipeline already
depends on — obtaining any of it requires a categorically different fetch.

### 4.2 The full taxonomy package is real and reliably fetchable — from `filings.xbrl.org` itself

The `documentInfo`-referenced URL (the issuer's own corporate domain,
`fomentodeconstruccionesycontratassa.com`) failed to resolve (`getaddrinfo failed`) when fetched
directly — a real, concrete illustration of a genuine reliability risk in depending on
issuer-hosted taxonomy files at scale. **However**, `filings.xbrl.org` itself mirrors the
**complete original ESEF report package** (confirmed via a real directory listing: a 24MB `.zip`
plus an already-extracted directory tree) under its own, already-proven-reliable domain — the
same one `16__fetch_eu_xbrl.py` already depends on for the facts JSON. Inside, FCC's real
extension taxonomy directory contains exactly the file set XBRL linkbase architecture predicts:

```
95980020140005178328-2024-12-31.xsd          (14K)  -- schema, extension element definitions
95980020140005178328-2024-12-31_cal.xml       (67K)  -- calculation linkbase
95980020140005178328-2024-12-31_def.xml      (174K)  -- definition linkbase (incl. anchoring)
95980020140005178328-2024-12-31_lab-es.xml   (130K)  -- label linkbase (Spanish)
95980020140005178328-2024-12-31_pre.xml      (167K)  -- presentation linkbase
```

**This is a materially heavier fetch** (~550KB of linkbase XML, real XML-namespace parsing, per
filing) **than the current adapter's single ~600KB-ish facts JSON** — not prohibitive, but a
real, non-trivial engineering scope increase per issuer, not a drop-in addition.

### 4.3 The definition linkbase — real anchoring relationships, parsed directly

Parsed FCC's real `_def.xml` (not regex — a proper namespace-aware XML parse). Confirmed
`http://www.esma.europa.eu/xbrl/esef/arcrole/wider-narrower` present, with **38 real anchoring
arcs**. Every single one connects an extension concept (`fomentodeconstruccionesycontratassa_*`)
to a core `ifrs-full_*` concept — **zero** core-to-core relationships found, consistent with §3's
structural claim. Representative real examples (§5 discusses two of these in depth):

```
ifrs-full:CurrentAssets  <—wider—  fomentodeconstruccionesycontratassa:DeudoresComercialesOtrasCuentasCobrar
fomentodeconstruccionesycontratassa:DeudoresComercialesOtrasCuentasCobrar  <—narrower—  ifrs-full:CurrentTaxAssetsCurrent
ifrs-full:Equity  <—wider—  fomentodeconstruccionesycontratassa:FondosPropios
fomentodeconstruccionesycontratassa:FondosPropios  <—narrower—  ifrs-full:TreasuryShares
fomentodeconstruccionesycontratassa:FondosPropios  <—narrower—  ifrs-full:IssuedCapital
ifrs-full:EquityAttributableToOwnersOfParent  <—wider—  fomentodeconstruccionesycontratassa:GananciasAcumuladasYOtrasReservas
ifrs-full:EquityAttributableToOwnersOfParent  <—wider—  fomentodeconstruccionesycontratassa:ResultadosDelEjercicioAtribuidoALaSociedad
```

### 4.4 The label linkbase — real, human-readable, but not structurally actionable

`DeudoresComercialesOtrasCuentasCobrar`'s real Spanish label: **"Deudores comerciales y otras
cuentas a cobrar"** — literally "trade debtors and other accounts receivable." A human reader
recognizes this immediately as Accounts Receivable. But the *label text* is natural language, not
structured taxonomy metadata — using it for automatic mapping would require translation +
fuzzy/semantic text matching (an LLM-adjacent problem), not a deterministic lookup, and reintroduces
exactly the false-positive risk this project's principles (§10 below) explicitly guard against.
The *anchoring* for this same concept (§4.3) — the structured signal — only says "wider: Current
Assets," which is far too coarse to drive an automatic "this is Accounts Receivable" decision on
its own.

### 4.5 The presentation linkbase — real, useful, per-filing statement classification

Parsed FCC's real `_pre.xml`. Six extended-link roles found, each corresponding to exactly one
primary statement or note group:

```
.../roles/StatementOfFinancialPosition          (Balance Sheet)
.../roles/StatementOfComprehensiveIncome          (Income Statement)
.../roles/StatementOfCashFlows                    (Cash Flow)
.../roles/StatementOfChangesInEquity              (equity roll-forward)
.../roles/ProfitOrLossPlaceholder...              (P&L starting point)
.../roles/NotesAndMandatoryItems                  (everything else)
```

**Checked directly which role(s) contain `ifrs-full:ProfitLoss`** (the exact concept at the
center of Phase 6.3's bug): it appears **only** under `StatementOfComprehensiveIncome`. The
`StatementOfCashFlows` role's own presentation tree contains **different** concepts entirely
(`AdjustmentsForReconcileProfitLoss`, `OtherAdjustmentsToReconcileProfitLoss`) — real,
first-hand confirmation of Phase 6.3's own root-cause conclusion that ESEF filings do not
duplicate a Net-Income-family tag across two statement contexts the way SEC filings genuinely do.
**Had `16__fetch_eu_xbrl.py` consulted this real, per-filing presentation membership instead of
the manually-maintained, source-agnostic `_LABEL_TO_STMT_KIND` table (built by literally copying
the SEC-side `STATEMENTS` dict's structure), the bug's root cause — an incorrect assumption of
SEC-style dual-statement tagging — could not have occurred.**

---

## 5. Testing the Theory Against Our Known Problems (Part 5)

### 5a. Case A — Revenue

**INFERRED** from §3's structural finding, not independently re-fetched this pass (both
`ifrs-full:Revenue` and `ifrs-full:RevenueFromContractsWithCustomers` are **standard** IFRS
concepts for every issuer that uses them — confirmed already in Phase 6.0/6.4's own real-data
research, no issuer tags either as an extension). Since anchoring never relates two standard
concepts, it cannot express "these are alternative top-line tags." ISP's/NAI's real top-line
concepts are more instructive: NAI's (`ifrs-full:RentalIncomeFromInvestmentProperty`) is also
standard, not an extension — again outside anchoring's scope. ISP's own real extension
(`isp:InterestIncomeAndSimilarRevenues`, confirmed real in Phase 6.0/6.4's live research) *would*
carry an anchoring relationship in principle — but per §4.3's pattern, that anchor would almost
certainly be to a broad concept (plausibly `ifrs-full:Revenue` itself, or a component of it) that
would say nothing about whether the two are *safely interchangeable* for a corporate Revenue
line — exactly the ambiguity this project's "NULL > questionable value" principle (§13,
Phase 6.0) was built to resist. **Taxonomy metadata cannot safely automate this decision; it
would, at best, help a human reviewer discover the candidate faster.**

### 5b. Case B — Accounts Receivable

**VERIFIED**, §4.3/§4.4 directly. FCC's real receivables-shaped concept
(`DeudoresComercialesOtrasCuentasCobrar`, real label confirms the semantic match) anchors *wider*
to `ifrs-full:CurrentAssets` — the generic bucket, not anything AR-specific — and *narrower*-side
to `ifrs-full:CurrentTaxAssetsCurrent`, an unrelated-looking pairing that would be actively
confusing if consumed mechanically. **This is real, direct evidence that anchoring, even when
present for a concept a human can instantly recognize from its label as "accounts receivable,"
is too coarse to drive automatic mapping to our specific canonical `Accounts Receivable` concept
on its own.**

### 5c. Case C — Total Equity

**VERIFIED, and this is the single clearest negative result in this research.** Phase 6.5's real
collision was between `ifrs-full:Equity` and `ifrs-full:EquityAttributableToOwnersOfParent` —
**both standard, non-extension concepts**. §4.3's real anchoring data confirms structurally that
no anchoring relationship exists (or could exist) between them; anchoring only appears where
FCC's own *extension* (`FondosPropios`) sits, itself anchored wider to `ifrs-full:Equity` and
narrower to component pieces like `TreasuryShares`/`IssuedCapital`. **Anchoring metadata could
not have helped avoid the original bad `CONCEPT_SYNONYMS` collapse — the two concepts genuinely
in collision were never candidates for an anchoring relationship in the first place.** The real
lesson from Phase 6.5 (that "Equity" and "EquityAttributableToOwnersOfParent" are a
parent/subset accounting relationship, not aliases) had to come from accounting domain knowledge
and real production-data cross-referencing — exactly what this project already did, not
something the taxonomy would have surfaced faster or more safely.

### 5d. Case D — Net Income / statement classification

**VERIFIED, §4.5 — the one clearly positive result.** As established above: real presentation-
linkbase role membership for FCC's real filing correctly and unambiguously places
`ifrs-full:ProfitLoss` under the Income Statement role only. This is a genuine, mechanically-
checkable signal the current pipeline does not consult, and consulting it — even just as a
cross-validation step against the existing hand-built `_LABEL_TO_STMT_KIND`, not a replacement
for it — would have caught Phase 6.3's bug before it reached production.

---

## 6. Tier A Concepts — Taxonomy Relationship Availability (Part 6)

| Concept | Current mapping | Taxonomy relationship available? | Useful for discovery? | Safe to auto-normalize? | Manual mapping still required? |
|---|---|---|---|---|---|
| Accounts Payable | 3 real tag variants, `EU_CANONICAL_MAPPING` | None (all 3 are standard concepts; no anchoring between them) | No | No | **Yes** |
| Non-Controlling Interests | 1 tag, new `STATEMENTS` concept | None (standard concept) | No | No | **Yes** (concept selection was the real work) |
| Stock-based Compensation | 1 tag (already existed as a canonical concept — Phase 6.6 correction) | None (standard concept) | No | No | **Yes** |
| Change in Working Capital | 1 tag | None (standard concept) | No | No | **Yes** |
| Income Before Tax | 1 tag | None (standard concept) | No | No | **Yes** |
| EPS Basic / Diluted | 2 tags each (plain + `FromContinuingOperations`) | None between the two variants (both standard) | No | No | **Yes** |
| Finance Income | 1 tag, new `STATEMENTS` concept | None (standard concept); real NET variants (`FinanceIncomeCost`) exist and were correctly excluded by this project's own "NET tags excluded" policy — not something taxonomy metadata flags either | No | No | **Yes** |
| Accounts Receivable | 2 real tag variants | Anchoring exists **only** where an issuer used an extension instead (§5b) — too coarse even then | Marginal (label text only) | No | **Yes** |

**Every single Tier A concept was, and remains, a manual mapping decision.** None of the real
tag-variant relationships this project actually needed (which standard tag is the primary vs.
fallback for the same canonical concept) are represented anywhere in ESEF taxonomy metadata —
because all of them are standard-to-standard relationships, the one category the taxonomy
structurally does not address.

---

## 7. Discovery vs. Classification vs. Normalization (Part 7)

- **Level 1 — Discovery**: **Partially useful.** Label text (§4.4) can help a human reviewer find
  candidate concepts faster ("search real filings for a receivables-shaped label"), but requires
  human judgment and, for non-English filings, translation — not a mechanical filter.
- **Level 2 — Classification (which statement)**: **Genuinely useful, verified real.** Per-filing
  presentation-linkbase role membership reliably classifies a concept's statement — see §5d.
- **Level 3 — Semantic grouping ("related, not interchangeable")**: **Useful only for extension
  concepts.** Anchoring does exactly this, correctly, for extensions — but that is a minority of
  this project's real mapping surface (most of our problem concepts are standard-to-standard).
- **Level 4 — Safe canonical normalization without human review**: **Not supported.** No evidence
  found, across any of the 4 real test cases (§5) or the full Tier A set (§6), that taxonomy
  metadata alone can safely drive an automatic XBRL-concept → canonical-concept decision. Every
  real case this research checked would need a human-reviewed step regardless.
- **Level 5 — Fully automatic mapping for new issuers**: **Not supported**, for the same reason,
  compounded: a new issuer's *own* extension concepts and anchoring choices are unique to that
  issuer and still require the same human-verification discipline this project already applies.

---

## 8. Extension / Anchoring Analysis (Part 8)

**VERIFIED**, §4.3, real data. FCC creates extension concepts for genuinely issuer-specific
presentation choices (e.g. splitting `EquityAttributableToOwnersOfParent` into
`GananciasAcumuladasYOtrasReservas` + `ResultadosDelEjercicioAtribuidoALaSociedad` — retained
earnings and other reserves vs. current-year result, a real, meaningful decomposition specific to
how FCC presents its own equity roll-forward) — and correctly anchors each to the nearest broader
standard concept, per the RTS requirement. **Anchoring answers "is this extension broader,
narrower, or (implicitly) a component of a specific core concept" — never "same meaning."**
Whether an extension can be safely, automatically mapped: **no**, for the same reason §5b/§6
already established — the anchor target is frequently far coarser than what our canonical model
needs (`CurrentAssets`, not `Accounts Receivable`), and using it to auto-populate a specific
canonical concept risks exactly the false-positive Phase 6.0's `EU_CANONICAL_MAPPING` design
(and this project's "NULL > questionable value" principle) was built to prevent.

---

## 9. Architectural Options (Part 9)

| | **A — Current manual mapping** | **B — Taxonomy-assisted discovery** | **C — Hybrid (auto for standard, manual for extensions)** | **D — Fully automatic semantic mapping** |
|---|---|---|---|---|
| Accuracy | High (every entry real-evidence-verified) | High (taxonomy narrows the search, human still decides) | **Not achievable as designed** — see below | Low/unverifiable |
| Maintenance | Manual, but low-volume (evidence-driven, done once per concept) | Slightly more tooling to maintain (a fetch+parse layer), same human decision step | N/A | High (constant false-positive triage) |
| Complexity | Low (a plain dict) | Medium (new fetch/parse pipeline, §4.2's ~550KB/filing) | High, and doesn't solve the actual problem (§6: nothing here is safely automatic) | Very high |
| Databricks compatibility | Already proven (Phase 5.1–6.6) | Feasible (same `%pip`-installable-dependency pattern already used for e.g. LightGBM) but adds real per-filing fetch cost | N/A | Infeasible without a much larger ML/NLP investment this project has no evidence justifies |
| Explainability | High (`MappingDecision.notes` cites real values) | High, if implemented as "assist a human," not "decide" | N/A | Low |
| Regression risk | Low (proven 6 phases running) | Low, if scoped to discovery only | High — Option C's own premise ("automatic for standard concepts") is false per §6 | Very high |
| EU issuer scalability | Proven for 8, same process scales linearly | Could speed up *discovery* for issuers 9–50 | Same false premise blocks this at any scale | Not viable |

**Option C is explicitly evaluated and rejected on the evidence, not dismissed by assumption**:
its premise — that *standard* IFRS concepts can be mapped automatically while only *extensions*
need manual review — is the opposite of what §4–§6 found. The standard concepts are exactly
where this project's real ambiguity has lived (Revenue variants, Equity variants, Net Income
variants, receivables variants); extensions, once anchored, are at least bounded by a real
(if coarse) taxonomy relationship. Building "Option C" as literally specified would automate the
wrong half of the problem.

**Option D (fully automatic) is evaluated and rejected**, consistent with this project's own
explicit instruction not to recommend it "merely because it sounds sophisticated" — no evidence
in this research supports it, and every real case tested argues against it.

---

## 10. Recommendation (Part 14/15/16)

**Do not build Option B, C, or D as a mapping-decision architecture.** Continue Option A (the
current manual, evidence-verified `EU_CANONICAL_MAPPING` process) for all future Tier B/C
concept work and for the next 30–50 EU issuers — it is not "unnecessarily manual"; every real
case this research tested confirms the taxonomy does not carry the signal a Tier A/B/C decision
actually needs.

**One narrow, high-confidence adoption is worth a future, separately-scoped, explicitly-authorized
phase** (not started here): use per-filing **presentation-linkbase role membership** as an
**automated cross-check**, not a mapping source — for every issuer already ingested, verify that
each mapped concept's real presentation-linkbase role matches the `stmt` our `STATEMENTS`
dict/`_LABEL_TO_STMT_KIND` currently assigns it, and flag (not auto-correct) any mismatch for
human review. This would have caught Phase 6.3's bug proactively rather than after a live
production symptom (`Owner Earnings = 0.0`) surfaced it. Scope: read-only validation tooling,
not a change to the merge/normalization pipeline itself.

**What should remain manual**: every canonical-concept selection decision (which standard tag(s)
map to which canonical concept; Tier A/B/C classification) — confirmed, not merely assumed,
across all 8 Tier A concepts and all 4 test cases.

**What could be automated (separately, later, narrowly)**: per-filing statement-classification
cross-validation (§5d/§10 above); potentially, label-text-assisted *discovery* (surfacing
candidate concepts for a human to review faster when researching a new Tier item) — genuinely
useful, never proposed here as a replacement for verification.

**Impact on a future 30–50-issuer expansion**: the manual mapping burden does not grow
per-issuer for concepts already mapped (a new issuer using an already-accepted standard tag needs
zero new work); it grows only for genuinely new tag variants or genuinely new extension concepts,
exactly as it has through Phases 6.0–6.6. The taxonomy does not change this cost structure in any
way this research found.

---

## 11. Risks and Limitations of This Research

- Real linkbase fetches were performed for **one issuer (FCC) only**, for depth rather than
  breadth, given this phase's own scope. The structural claim (anchoring never relates two
  standard concepts) is a property of the ESEF/XBRL specification itself (CITED), not an
  FCC-specific observation, so it generalizes — but the *specific* presentation-role/anchoring
  examples cited are FCC's real data, not independently re-confirmed for ALO/IBE/SGO/FCT/NAI/
  RAND/ISP this pass.
- The ESMA ESEF Toolkit (Arelle) was evaluated from its documentation and confirmed real,
  Python-API-capable, open-source (CITED) — it was **not** actually installed or run this phase
  (research-only scope; installing a full XBRL processor into the Databricks environment would
  itself be a real, separately-scoped engineering decision).
- Calculation-linkbase relationships (summation/component arcs) were not deeply investigated this
  pass beyond confirming their presence — a real, plausible secondary use (e.g. validating that a
  filer's own component breakdown sums to a reported total) that this research flags as
  **UNKNOWN**, not evaluated, rather than claiming it either way.

---

## Final Report

- **Classification: PARTIALLY USEFUL**
- **ESMA taxonomy useful: PARTIALLY** — real, verified value for per-filing statement
  classification (§5d); no verified value for the actual canonical-concept-selection decisions
  this project's manual mapping work has centered on (§5a-c, §6).
- **ESMA Toolkit (Arelle) useful: PARTIALLY** — a real, general-purpose, Python-API-capable XBRL
  processor (CITED), technically capable of extracting everything this research needed; not
  installed or run this phase; a real but non-trivial dependency to add to this pipeline.
- **Current manual mapping still necessary**: YES, confirmed across all 8 Tier A concepts and
  all 4 test cases (§5, §6) — no case found where taxonomy metadata alone would have produced a
  safe automatic mapping.
- **Taxonomy-assisted discovery possible**: YES, marginally (label text, §4.4) — a human-assist
  tool, not an automation mechanism.
- **Safe automatic normalization possible**: NO — confirmed negative across every real case
  tested.
- **Extension concepts**: real, anchored per ESMA's RTS requirement, but the anchor target is
  frequently too coarse (§5b, §8) to drive automatic mapping to this project's specific canonical
  concepts.
- **Tier A impact**: none — every Tier A concept's real mapping decision required (and would
  still require) the same manual, evidence-based process already used.
- **Recommended architecture**: keep Option A (current manual mapping) for all concept-selection
  work; consider a separately-scoped, narrow presentation-linkbase cross-validation tool as
  future work (not started).
- **Expected benefit for 30–50 EU issuers**: no reduction in manual mapping-decision effort;
  potential future reduction in *classification-bug* risk (Phase-6.3-style) if the cross-
  validation tool is later built and run.
- **Production writes: NO**
- **Code changes: NO**
- **Document**: `docs/phase6-6-esef-taxonomy-research.md` (this file)
- **PR**: not yet opened — branch `phase6-6-esef-taxonomy-research`, committed, ready for review.

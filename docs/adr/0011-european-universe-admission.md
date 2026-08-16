# ADR-0011: European universe & admission layer — GLEIF/OpenFIGI-based identity resolution

- **Status:** Proposed
- **Date:** 2026-08-16
- **Deciders:** repo owner

## Context

Phase 5.1 proved the `filings.xbrl.org → EUCurrentSource → financials_raw → financials`
ingestion path works for four real European issuers, but their `(ticker, LEI, MIC)` identity
was hardcoded — manually verified once, not derived by any repeatable process. This does not
scale past a hand-picked pilot: there is no mechanism today for a fifth European listing to
enter the system without a human repeating that manual verification.

The Phase 5.2 research pass (`docs/phase5-2-european-universe-admission-research.md`)
investigated, with live data wherever possible, whether a deterministic identity-resolution
chain exists that could replace the hardcoded pilot list. It found one, and also found two real
ticker collisions confirming why one is necessary — but did **not** find a solved, free,
verified source for European index/universe membership itself. This ADR records the decision
on the identity-resolution architecture; it is explicitly silent on, and does not block on, the
still-open universe-source question.

## Decision

We will adopt the following identity-resolution chain as the target architecture for admitting
new European listings, once implemented (a future phase, not this one):

```
Candidate Listing (ticker, MIC)
        │
        ▼
  OpenFIGI: (ticker, MIC) -> exactly one Common-Stock/Equity instrument?
        │  (ambiguous/none -> reject: IDENTITY_AMBIGUOUS)
        ▼
  GLEIF: ISIN -> LEI  (cross-checked against the LEI's own ISIN set via LEI -> ISIN)
        │  (no match -> reject: NO_LEI)
        ▼
  issuer_id = make_issuer_id("EU_CURRENT", lei)   -- reuses the existing Phase 5.0 function
        │
        ▼
  filings.xbrl.org: does an entity + a real, ingestible filing exist for this LEI?
        │  (no -> reject: NO_ESEF_ENTITY / NO_USABLE_FILING)
        ▼
  ADMITTED  ->  listing_id = MIC:TICKER, issuer_id = EU_CURRENT:LEI
```

Both `GLEIF` (`api.gleif.org`) and `OpenFIGI` (`api.openfigi.com`) are adopted as the
authoritative, automatable identity sources — verified live this session, free, unauthenticated,
and directly tested against all four real pilot issuers (GLEIF resolved FCC's ISIN to its real
LEI and back; OpenFIGI resolved all four pilots' `(ticker, MIC)` pairs to correctly-typed,
single-match equity instruments, and independently surfaced two genuine ticker collisions —
`FCC` also matches an unrelated Vietnamese company, `FCT` matches at least five other unrelated
companies globally). This closes the identity-resolution gap the Phase 5.0 identity model
(`issuer_id`/`listing_id`) needed but did not itself specify a source for.

The `filings.xbrl.org` ESEF-entity-and-filing-existence check becomes the final admission gate,
not merely a downstream ingestion step — a candidate that resolves to a real LEI but has no
usable ESEF filing is correctly `REJECTED`, not admitted with an expectation that ingestion will
silently fail later.

**Explicitly not decided by this ADR**: which universe source supplies the initial candidate
`(ticker, MIC)` list. STOXX's own official constituent lists require a paid license (verified
live); an ETF-holdings-as-free-proxy approach (mirroring this project's own existing Russell
3000/TSX Composite precedent) is the leading candidate but its exact, working download
mechanism was not verified live in this pass. This remains open (see the research doc §4/§21/§22)
and is not blocking acceptance of the identity-resolution decision above, which stands on its
own regardless of which universe source eventually feeds it candidates.

## Consequences

**Easier**: a future admission-layer implementation phase has a concrete, live-tested chain to
build against rather than starting from an unresearched blank slate — every link except the
universe-source one is confirmed working against real data. The two real collisions found this
session are now direct, non-hypothetical regression-test material for that future phase's own
test suite (mirroring how Phase 5.1's real amendment cases became direct fixtures).

**Harder / deferred, on purpose**: nothing in this ADR is implemented — `EUCurrentSource` still
only knows the four hardcoded pilots today. The universe-source gap (§4 of the research doc)
means this ADR alone does not yet answer "how does a new European company enter the system" end
to end — only "once you have a candidate `(ticker, MIC)`, how do you resolve and admit it."
Primary-vs-secondary-listing resolution, corporate-action/identity-change re-verification, and
`MANUAL_REVIEW` operationalization are all explicitly out of scope (see the research doc §11,
§16, §22) and remain unsolved.

## Alternatives considered

- **Fuzzy company-name matching as the primary identity bridge** (ticker/company name →
  best-guess issuer). Rejected outright, per this project's own long-standing principle
  (`identity.py`'s `classify_company_match`, already conservative-by-design for the US/CA case):
  ambiguity must reject or go to NULL, never guess. The two real collisions found this session
  (`FCC`, `FCT`) are direct proof a name/ticker-only approach would misidentify a European
  candidate with unacceptable frequency.
- **Using `filings.xbrl.org`'s `/api/entities` alone as both universe and identity source.**
  Rejected — already established in Phase 4/ADR-0009 research that this endpoint provides no
  ticker/exchange field at all, so it cannot serve as an admission entry point on its own; it
  remains valuable only as the final confirmation gate.
- **Treating STOXX's official data as accessible.** Rejected after live verification this
  session found an explicit paid-license wall — not assumed from prior knowledge, checked
  directly against `stoxx.com/selection-lists`.
- **Deferring the whole ADR until the universe-source question is also solved.** Rejected: the
  identity-resolution chain (GLEIF/OpenFIGI/filings.xbrl.org) is independently valuable,
  independently verified, and independently decidable — coupling it to the unresolved universe
  question would block a real, evidence-backed decision on an unrelated open item.

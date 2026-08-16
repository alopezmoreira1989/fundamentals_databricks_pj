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

`GLEIF` (`api.gleif.org`) is adopted as the **authoritative identifier source** — it is the LEI
system's own official issuing federation, the legal authority for LEI↔ISIN relationships, not
an industry convention. `OpenFIGI` (`api.openfigi.com`) is adopted as the **industry security-
reference / instrument-resolution source** — a widely-relied-upon, free, Bloomberg-run open
symbology service, useful and independently verified, but not a legal or regulatory authority
over instrument identity the way GLEIF is over LEI. This distinction is deliberate: the
architecture leans on GLEIF for the identity claim itself and on OpenFIGI only to resolve which
specific instrument/ISIN a `(ticker, MIC)` pair refers to.

Both were verified live this session, free, unauthenticated, and directly tested against all
four real pilot issuers — **the full chain, `(ticker, MIC) → OpenFIGI → equity ISIN → GLEIF →
LEI`, is now confirmed end-to-end for all four**, not just FCC as an initial pass first found:
Alstom required discovering and fixing a real bug in that first pass (GLEIF paginates
`LEI → ISIN` results at 15/page by default, and Alstom's 36 real ISINs were silently truncated
to 15, none of which was the actual equity — refetching with a larger page size surfaced
`FR0010220475`, confirmed via OpenFIGI as Alstom's real `XPAR:ALO` Common Stock, and confirmed
via GLEIF's reverse `ISIN → LEI` lookup to resolve to the exact same LEI already known from
Phase 5.1). New Amsterdam Invest and Fincantieri's real equity ISINs (`NL0015000CG2`,
`IT0005599938`) were isolated the same way and both independently reproduced their already-known
Phase 5.1 LEIs exactly. OpenFIGI's `micCode` parameter was also directly tested against `MTAA`
(works, resolves Fincantieri correctly) and `XMIL` (returns no result) — independent, third-
source confirmation of ADR-0010's segment-MIC decision, alongside the same result for
`XMAD`/`BMEX` and `XPAR`/`XAMS`.

The `filings.xbrl.org` ESEF-entity-and-filing-existence check becomes the final Generation-1
**fundamentals-eligibility** gate — deliberately kept distinct from **identity resolution**. A
candidate that resolves to a real, confirmed LEI but has no usable ESEF filing has had its
*identity* successfully established (`IDENTITY_RESOLVED`) — it is simply not currently
*ingestible* through `EU_CURRENT` (`NOT_INGESTIBLE` / `NO_ESEF_ENTITY` / `NO_USABLE_FILING`).
For this project's purposes (a fundamentals pipeline with no use for an issuer it can never
source data for), Generation-1 still treats that outcome as a practical `REJECTED` rather than
carrying an identified-but-dataless entity forward — but the two concepts are not conflated in
the model itself, and a future source (e.g. covering Germany/Ireland) could re-admit the same,
already-identified issuer without redoing identity resolution from scratch.

**Explicitly not decided by this ADR, and now explicitly classified `NOT SOLVED`**: which
universe source supplies the initial candidate `(ticker, MIC)` list. Two follow-up research
passes
([docs/phase5-2b-european-universe-source-validation.md](../phase5-2b-european-universe-source-validation.md))
tested every free candidate found and disqualified each in turn, not merely left them
unresolved: STOXX's official "selection-lists" portal requires a paid license (confirmed).
STOXX's separate, free, no-login `CurrentComponents` PDF (`stoxx.com/document/Bookmarks/
CurrentComponents/{SYMBOL}.pdf`) has real, complete data (STOXX Europe 600's PDF has exactly 600
rows — company name, country, sector, weight, no ticker/ISIN/MIC) but was **decisively proven
stale**: checked against two real, dated 2026 quarterly index reviews, it reflects only 4 of 23
real additions and still lists 17 of 21 real deletions. A second, independent STOXX document was
also found stale. The iShares ETF-holdings route (mirroring this project's Russell 3000/TSX
Composite precedent) was tested directly and found blocked: the legacy `.ajax?fileType=csv`/
`json` scraping pattern documented in public tooling no longer works against the current
(Astro-based SPA) `ishares.com` UK site.

This yielded a clean split, worth stating explicitly:
```
identity resolution (this ADR's decision)     READY
ESEF ingestion (Phase 5.1)                     READY
canonical mapping (Phase 5.1)                  READY
universe discovery                              NOT SOLVED  (at the time)
```

**Updated by a third follow-up pass**
([docs/phase5-2c-esma-firds-research.md](../phase5-2c-esma-firds-research.md)): **ESMA FIRDS**
(the EU regulator's own MiFID II/MiFIR reference-data system) was investigated as a replacement
candidate and found `READY WITH CONDITIONS` — a materially better result than STOXX/iShares,
verified live against real ESMA data: a documented, free, unauthenticated machine-to-machine
API; a real equity reference file published the day before this research (confirmed current —
passing precisely the freshness test STOXX's PDF failed); native `ISIN`/issuer-`LEI`/venue-`MIC`
fields (no ticker field, which is a feature for this project's ticker-is-not-identity principle,
not a gap); and all four Phase 5.1 pilots independently re-found by ISIN alone, with LEI and MIC
(including Fincantieri's `MTAA` specifically — now a fourth independent confirmation) matching
exactly. **This ADR's own universe-discovery status updates to**:
```
identity resolution (this ADR's decision)     READY
ESEF ingestion (Phase 5.1)                     READY
canonical mapping (Phase 5.1)                  READY
universe discovery                              READY WITH CONDITIONS  (ESMA FIRDS —
                                                 see phase5-2c for the exact conditions:
                                                 a primary-venue selection filter is
                                                 still undesigned, and the listing_id
                                                 ISIN-vs-ticker keying question this
                                                 finding raises is unresolved)
```
None of this is implemented by this ADR or by the phase5-2c research itself — the
identity-resolution decision above stands on its own regardless, and the universe-source
decision remains a separate, future implementation phase, not decided here.

**Updated by a fourth follow-up pass**
([docs/phase5-2d-firds-primary-listing-and-identity-model.md](../phase5-2d-firds-primary-listing-and-identity-model.md)),
a small, narrowly-scoped pass resolving two of phase5-2c's four open conditions and performing
two lightweight due-diligence checks the repo owner asked for alongside them:

- **Primary-venue selection — now SOLVED, not just designed.** `TradgVnRltdAttrbts/IssrReq =
  true`, tie-broken by earliest `TradgVnRltdAttrbts/FrstTradDt`, resolves all four pilots with no
  remaining ambiguity — including two real ties found in the live data (FCC: `XMAD` vs. the
  Madrid group's own dark-midpoint segment `DMAD`; Fincantieri: `MTAA` vs. a newly-admitted
  Italian MTF `HMTF`), both correctly broken in favor of the already-established Phase 5.1 MIC.
  This rule is documented, not yet implemented in code, and was validated against only two real
  tie cases — flagged in phase5-2d as needing a broader sample before production use.
- **CFI classification — now confirmed from ESMA's own reference, not only ISO 10962 public
  documentation.** ESMA70-145-1090 ("FIRDS CFI validations," a real, versioned workbook
  downloaded and independently parsed) lists the `ES` prefix under its "Equities" classification
  group — the same conclusion Phase 5.2c already reached from public ISO 10962 docs and the four
  pilots' own observed values, now independently corroborated by ESMA's own published validation
  grid. That grid's own Notes sheet states it is derived from ISO 10962:2015 — ESMA is not an
  independent CFI authority, a real and now explicitly documented distinction.
- **`listing_id` keying (`MIC:TICKER` vs. `MIC:ISIN`) — investigated, deliberately not decided.**
  FIRDS' own `RefData` schema has no ticker field at all; its natural composite key, as observed
  directly in the data, is `(ISIN, MIC)`. This is real evidence that `MIC:ISIN` would be the
  better fit for an ISIN-native universe source, consistent with this project's own existing
  `(ticker, market)` — not bare-ticker — identity precedent (`identity.py`). Per explicit
  instruction, this ADR and ADR-0010 are **not** amended by this finding — phase5-2d recommends
  resolving the question before a future phase implements FIRDS-based universe discovery, not
  before this ADR is accepted, since ADR-0011 was already explicitly silent on universe-source
  mechanics by design. **Resolved by a dedicated follow-up**:
  [ADR-0012](0012-listing-identity-key.md) (status Proposed, not yet Accepted) now formally
  decides this question — `listing_id = MIC:ISIN` for new listings, ticker kept as a mutable
  attribute rather than part of the identity — scoped explicitly to new (FIRDS-sourced)
  listings, not a re-key of anything existing. See that ADR, not this one, for the decision
  itself and its evidence.
- **FIRDS terms of use — lightweight check only, not a legal review.** ESMA's general
  reproduction/reuse policy (source-attributed reproduction authorised, no commercial-use
  prohibition found) applies; no FIRDS-specific stricter terms were found.

This pass leaves phase5-2c's remaining two conditions (only the Equity CFI class investigated;
primary-venue rule validated on 4 pilots / 2 tie cases only, not a broad sample) as still-open,
explicitly acknowledged limitations — not silently resolved.

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

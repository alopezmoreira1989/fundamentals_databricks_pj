# Phase 5.2 — European universe & admission layer: research and architecture

**Status: research/design only. Nothing in this document has been implemented.** No
production table, schema, DAG, `EUCurrentSource`, `fundamentals_screener`, or Streamlit change
accompanies this pass, per the explicit scope boundary this phase was given.

Companion to [ADR-0009](adr/0009-multi-market-fundamentals-ingestion-framework.md), [ADR-0010](adr/0010-issuer-listing-identity-model.md),
and [docs/phase5-1-eu-adapter.md](phase5-1-eu-adapter.md). See also
[docs/adr/0011-european-universe-admission.md](adr/0011-european-universe-admission.md) (status: Proposed) for the
decision this research supports.

---

## 1. Executive summary

Phase 5.1 proved the European ingestion *pipeline* works end to end (`filings.xbrl.org →
EUCurrentSource → canonical financials`) for four issuers whose `(ticker, LEI, MIC)` identity
was manually verified and hardcoded. This phase researches — without implementing — how a
**new** European listing would enter that pipeline without a human hand-verifying its identity
triple first.

The central finding: **a working, deterministic identity bridge exists and was verified live
this session** — `GLEIF` (the LEI-issuing authority's own free public API) resolves both
`ISIN → LEI` and `LEI → ISIN` with zero authentication, confirmed against all four real pilot
LEIs. Combined with `OpenFIGI` (free, ticker+MIC → security-type-classified instrument) for
collision-safe ticker resolution and the already-established `filings.xbrl.org` for the ESEF
confirmation gate, a **Universe → Listing → ISIN → LEI → Issuer → ESEF Entity → Admission**
pipeline is architecturally sound and testable against real data. What is **not** yet solved:
a free, reliably-parseable **universe source** for European index membership — `STOXX`'s own
official constituent lists require a paid Third-Party Data License (verified live), so the
Generation-1 candidate mirrors this project's own existing Russell 3000/TSX precedent (an
ETF's publicly disclosed holdings as a free proxy for index membership) rather than the index
provider directly — but the exact download URL for a European UCITS ETF was not verified live
in this pass (see §20, §21).

Two genuine, live-verified ticker collisions were found (§18) — not fabricated, not
hypothetical — directly confirming the "ticker alone is insufficient identity" principle this
whole model exists to enforce.

---

## 2. Current pilot limitation

Phase 5.1's `PILOT_EU_ENTITIES` (`fundamentals_pipeline/10__ingestion/16__fetch_eu_xbrl.py`) is
a hardcoded Python list of four `(ticker, LEI, MIC, name)` tuples. `EUCurrentSource.
discover_entities()` looks tickers up against this list only — it has no mechanism to resolve a
ticker it wasn't told about in advance. This is explicitly acceptable for a pilot proving the
ingestion pipeline (Phase 5.1's own stated scope) and explicitly *not* a viable mechanism for
admitting the 5th, 50th, or 600th European issuer.

## 3. Current repository architecture (inspected before proposing anything new)

**US/CA universe generation** (`fundamentals_pipeline/00__config/02__tickers_master.py`):
- `fetch_sp500()` — parses Wikipedia's "List of S&P 500 companies" HTML table via
  `pd.read_html(flavor="lxml")`. Free, public, no auth, no rate limit concerns for a
  once-a-run fetch.
- `fetch_russell3000()` — pulls iShares IWV (Russell 3000 ETF) holdings via BlackRock's
  `varnish-api/blk-one01-product-data/.../fundDownload` endpoint — a real, working, free CSV
  API discovered and confirmed by this project (not officially documented by BlackRock).
- `fetch_tsx_composite()` — pulls iShares XIC (TSX Composite ETF) holdings, but via a
  **different** mechanism: a plain CSV at a distinct `blackrock.com/ca/investors/...` path,
  since XIC does *not* respond to the varnish-api endpoint IWV uses (confirmed 2026-07, per the
  file's own comment). **This is the single most important precedent for Phase 5.2**: two
  BlackRock ETF products required two different, individually-verified download mechanisms —
  there is no one universal "BlackRock holdings API." Any European ETF candidate needs the same
  per-product verification, not an assumed-reusable pattern.
- Both sources are merged, deduplicated (`ticker` precedence: Wikipedia GICS → IWV-normalized),
  and every row gets `market` set (`"US"` today, `"CA"` for XIC-sourced/admitted rows).
- **Identity/collision guard**: `fundamentals_pipeline/identity.py`'s
  `check_no_cross_market_collision()` + `classify_company_match()` — called at two points in
  `02__tickers_master.py` (after the US-only merge, a no-op today; after Canadian admission, the
  real gate) and once in `11__fetch_sec_xbrl.py` (before CIK resolution). Three-way verdict
  (`same`/`different`/`ambiguous`) via normalized-name token comparison — conservative by
  design, never silently merges an ambiguous match. This module's own docstring already states
  its scope: it guards `config.tickers` admission only, not the downstream fact tables (see
  ADR-0010).
- **Persistence**: `main.config.tickers`, `DROP TABLE` + full overwrite every run (not an
  incremental MERGE) — `issuer_id`/`mic`/`listing_id` (Phase 5.0) are additive nullable columns,
  populated for `issuer_id` only, and only for the existing US/CA universe.
- **Refresh**: whenever `02__tickers_master.py` runs as part of the scheduled pipeline (daily,
  per the production Job) — membership is fully recomputed from the live Wikipedia/IWV/XIC
  sources each time, not incrementally diffed.

**`sources/` package** (Phase 3/5.0/5.1): `FundamentalsSource` Protocol, `SourceEntity`
(`source_id`/`source_entity_id`/`issuer_id`/`name`/`ticker: Optional`), `SOURCE_REGISTRY`
(`SourceAccessStatus`: `ACTIVE`/`RESEARCH_ONLY`/`MANUAL_ONLY`/`AUTOMATION_RESTRICTED`/
`UNAVAILABLE` — a **source-level** access classification, not an issuer-level admission state;
see §14). `EUCurrentSource` (Phase 5.1, `16__fetch_eu_xbrl.py`) is the only concrete non-SEC
adapter; its `discover_entities()` is the exact method this phase needs to generalize.

**ADRs**: 0009 (multi-source framework), 0010 (issuer/listing identity model — `issuer_id =
source_id:source_entity_id`, `listing_id = MIC:TICKER`, explicitly transitional/additive, no
re-key). This phase's proposal (ADR-0011) builds directly on both without revisiting either.

---

## 4. Universe-source options (researched, not assumed)

| Candidate | Coverage | Access | Identifiers provided | Verdict |
|---|---|---|---|---|
| **STOXX official selection lists** (EURO STOXX 50, STOXX Europe 600) | Eurozone / pan-Europe, exactly the index definitions this project already names as targets | **Paid Third-Party Data License required** — verified live: stoxx.com/selection-lists explicitly states "Access to remaining files are reserved to STOXX Index licensees"; only files prefixed `slpublic` are unrestricted, and it's unconfirmed whether EURO STOXX 50/STOXX 600 selection lists are among them | ISIN, SEDOL, RIC, company name (per a sample factsheet PDF found) | **AUTOMATION_RESTRICTED** for the full, current list — same shape as SEDAR+'s ToS-restricted status in ADR-0009 |
| **iShares Core EURO STOXX 50 UCITS ETF** (ISIN `IE00B53L3W79`) holdings | Tracks EURO STOXX 50 by construction | iShares product pages advertise "Download Holdings"/"Detailed Holdings and Analytics" — **the exact download URL was not successfully verified live in this pass** (a direct fetch attempt returned HTTP 403, likely bot-blocking rather than a real access restriction — unresolved, not asserted either way) | Presumably ISIN/ticker/weight, matching the pattern IWV/XIC already expose | **RESEARCH_ONLY** — real, promising, precedented candidate; URL discovery is concrete follow-up work, same kind of work already done twice for Russell 3000 and TSX in this exact codebase |
| **iShares STOXX Europe 600 UCITS ETF (DE)** (ISIN `DE0002635307`, ticker `EXSA`) holdings | Tracks STOXX Europe 600 | Same as above | Same as above | **RESEARCH_ONLY**, same caveat |
| **Wikipedia "STOXX Europe 600" article** | Partial, unclear if complete | Free, `pd.read_html`-compatible in principle (same mechanism as `fetch_sp500()`) | Ticker, company name, ICB sector, country (per what was visible) | **RESEARCH_ONLY** — a live fetch this session returned what appeared to be a partial/truncated table; genuinely unclear whether the underlying Wikipedia article has the full ~600-row table or whether the fetch tool's own summarization cut it off. **Not confirmed either way — flagged honestly, not asserted as incomplete.** A follow-up pass should fetch and count rows directly (e.g. via `pd.read_html`) rather than a summarized web fetch. |
| **Euronext's own market-data portal** | Euronext-listed instruments (Paris/Amsterdam/Milan/Brussels/Lisbon/Dublin) | No public CSV/API endpoint found in this pass; portal points to a commercial "Data Shop" | Unknown | **RESEARCH_ONLY** — not ruled out, but no free automated path found live |
| **National exchange/regulator security-master files** (BME, Borsa Italiana, AMF, AFM) | Per-country | Not investigated in this pass — real candidate, out of time budget this session | Unknown | **RESEARCH_ONLY**, unexplored |

**Conclusion**: no single free, complete, current, machine-readable "the European universe"
source was found and verified live. The most promising path, by direct analogy to this
project's own two already-solved cases (Russell 3000, TSX Composite), is an ETF's own
disclosed holdings — but the exact mechanism needs the same kind of hands-on discovery work
`fetch_tsx_composite()`'s own comment documents having done for XIC. This is real,
unfinished, and honestly reported as such — not something to paper over with an assumption.

## 5. Identity-source options — LEI resolution (the strongest finding this pass)

**GLEIF (Global Legal Entity Identifier Foundation) — verified live, both directions, free, no
authentication:**

- `GET https://api.gleif.org/api/v1/lei-records/{LEI}` — real record, tested against FCC's real
  LEI (`95980020140005178328`), returned the correct legal name, jurisdiction (`ES`), and a
  `relationships.isins` link.
- `GET https://api.gleif.org/api/v1/lei-records/{LEI}/isins` — **LEI → ISIN(s)**, tested for all
  four pilots. Confirmed **one-to-many**: FCC → 4 ISINs (1 Spanish `ES...`, 3 US-prefixed —
  almost certainly ADR-related, a real, unplanned finding about FCC's own US market presence),
  Alstom → **15 ISINs** (mostly `FR...` — Alstom issues many corporate bonds/warrants under one
  LEI — plus 2 `US...`), New Amsterdam Invest → 2 `NL...` ISINs, Fincantieri → 2 `IT...` ISINs.
- `GET https://api.gleif.org/api/v1/lei-records?filter[isin]={ISIN}` — **ISIN → LEI** (the
  direction actually needed for admission, since a candidate listing starts from a ticker/ISIN,
  not a LEI). Tested against FCC's Spanish ISIN (`ES0122060314`) — returned exactly the correct
  LEI/entity, single result.

**The real complexity GLEIF does NOT resolve on its own**: since LEI↔ISIN is one-to-many, GLEIF
alone cannot tell you *which* of an issuer's ISINs is the specific common-equity security traded
under a given `MIC:TICKER` — it will hand back a bond ISIN just as readily as the equity one. A
second, security-type-aware source is needed to disambiguate (see §6).

**GLEIF's own stated limitation** (from its public documentation, not just this session's spot
checks): the ISIN-to-LEI relationship files were piloted with "early mover national numbering
agencies" — coverage is described as broad but was not claimed to be 100% globally complete by
GLEIF itself. This session's 4-for-4 real-pilot success is strong evidence for the specific
markets tested (ES/FR/NL/IT), not proof of universal coverage.

**Classification: GLEIF = ACTIVE** (per this project's `SourceAccessStatus` vocabulary) — free,
no auth, no rate-limit issue encountered, live-verified against all 4 real pilots in both
directions.

## 6. ISIN assessment

**Yes, ISIN should be an intermediate identity key — not a replacement for `issuer_id`/
`listing_id`, but the practical bridge between them.** Verified this session: every pilot's LEI
has real, resolvable ISINs; the reverse direction (ISIN → LEI) works. The open gap (§5) is
resolving "which ISIN is the equity traded at this MIC:ticker" — **OpenFIGI closes most of this
gap**: `POST https://api.openfigi.com/v3/mapping` with `{"idType":"TICKER","idValue":"ALO",
"micCode":"XPAR"}` returned exactly one result for Alstom, correctly typed
`"securityType":"Common Stock"`, `"marketSector":"Equity"` — confirming the `(ticker, MIC)` pair
resolves to exactly one, correctly-classified instrument. **OpenFIGI's `/mapping` response does
not include ISIN directly in this call shape** (it returns FIGI, name, ticker, exchCode,
securityType, etc., not ISIN) — a real, verified limitation, not assumed. Closing the last link
(`(ticker, MIC)` → the *specific* ISIN, cross-checked against GLEIF's ISIN set for the resolved
LEI) needs either an OpenFIGI reverse lookup by `ID_ISIN` per candidate ISIN (feasible, just an
extra round-trip) or a different source — flagged as an open question (§21), not solved here.

**Verdict: adopt ISIN as an explicit field in the admission model (§12), sourced via OpenFIGI +
cross-validated against GLEIF's ISIN set for the resolved LEI** — it solves a real problem
(bridging ticker+MIC to LEI deterministically) rather than adding complexity for its own sake.

## 7. MIC assessment

No new research needed — ADR-0010 already established the authoritative source (ISO 20022/
ESMA FIRDS) and the operating-vs-segment-MIC rule, verified against all 4 pilots (`XMAD`, `XPAR`,
`XAMS`, `MTAA`). This phase's admission model reuses that rule unchanged: **a candidate
listing's MIC must be independently verified against ISO 20022/FIRDS, never inferred or
defaulted** — the same discipline, extended to new candidates rather than re-litigated.

## 8. Ticker collisions — real, verified cases (not hypothetical)

Live OpenFIGI queries this session (bare ticker, no MIC filter, `Equity`-only) found **two
independent, genuine collisions** for our own pilot tickers:

- **`FCC`** (our Spanish pilot) also resolves, via OpenFIGI, to `FOODSTUFF COMBINATORIAL JSC`
  on Vietnamese markets (`exchCode` `VN`/`VU`) — a completely unrelated company, confirmed live,
  not fabricated.
- **`FCT`** (our Italian pilot, Fincantieri) is a **dramatically overloaded** ticker globally:
  `Frasers Centrepoint Trust` (Singapore, REIT), `F&C Investment Trust PLC` (New Zealand-listed
  data, Closed-End Fund), `Fertilisers & Chemicals Travancore` (India, Common Stock, 4 separate
  exchange codes), `MyPhotoAlbum Inc` (`XS`), `Firstwave Cloud Technology` (Australia, 3
  exchange codes) — **six unrelated real companies share the bare ticker `FCT` globally**, and
  only `IM` (Milan) is our actual Fincantieri pilot.

This is exactly the evidence the "ticker alone → insufficient identity" principle (ADR-0010)
predicts, now confirmed for real, current European pilot tickers specifically — not just the
already-known US/CA `MG` example. **Neither of these collisions currently threatens `config.
tickers`** (Phase 5.0's own live check found 0 tickers spanning >1 market there, and no European
tickers exist in that table at all) — but they are concrete proof that a *future* naive
`ticker → company` admission step would be unsafe without the MIC/ISIN/LEI disambiguation this
document proposes.

**`ALO` and `NAI`, by contrast, did not show a collision** in this pass's OpenFIGI queries (`ALO`
resolved consistently to Alstom across many Bloomberg composite exchange codes, not distinct
companies; `NAI` returned a single match). Absence of a found collision is not proof none
exists — only that this session's specific query didn't surface one.

## 9. Listing vs. issuer model

Confirms the ADR-0010 model directly: **`issuer_id` (source-qualified: `EU_CURRENT:<LEI>`) is
the stable anchor; `listing_id` (`MIC:TICKER`) is a specific security on a specific market.**
GLEIF's own LEI-to-ISIN relationship is the concrete evidence this is the right shape: one LEI
legitimately owns many ISINs (equity + bonds + warrants + ADRs), so any model that tried to make
`ticker` or even `ISIN` alone the top-level identity would break the first time a real issuer
(Alstom, 15 ISINs) was examined. No change to ADR-0010's model is proposed — this phase adds the
*resolution mechanism* (how a candidate listing's `issuer_id` gets determined), not a new model.

## 10. Security-type policy

**Proposed admission rule: only instruments classified as common/ordinary equity are eligible**
— `OpenFIGI`'s `securityType`/`marketSector` fields (`"Common Stock"`/`"Equity"`, confirmed live
for FCC/ALO/NAI/FCT) are the concrete, checkable signal. Explicitly excluded from Generation-1
admission, consistent with what the canonical `financials` model is built around (issuer-level
operating-company fundamentals): preferred shares, depositary receipts, ETFs, closed-end funds,
warrants, bonds. This directly explains why GLEIF's raw ISIN list for an issuer (§5) cannot be
used unfiltered — Alstom's 15 ISINs are mostly non-equity instruments under the rule above.

## 11. Primary vs. secondary listing policy

Not solved generically in this pass (explicitly out of scope for corporate-actions handling,
per §16 of the brief). **Working assumption for Generation 1**: whichever listing the universe
source itself tracks *is* the primary listing, by construction — this already matches how
`fetch_sp500()`/`fetch_russell3000()`/`fetch_tsx_composite()` implicitly work today (an S&P 500
membership row IS that company's primary US listing; no separate primary-listing resolution
step exists for the current US/CA universe either). This working assumption inherits, not
introduces, the current architecture's own scope — a genuine dual-primary-listing case (e.g. a
company primary-listed on two different European exchanges) is deferred, not solved, matching
this document's own non-goals (§22).

## 12. ESEF as an admission gate

**Recommended: yes, ESEF-entity-and-filing existence should be the final admission gate**,
structured as:

```
candidate listing (ticker, MIC, ISIN)
        │
        ▼
   ISIN → LEI (GLEIF)
        │
        ▼
   LEI → filings.xbrl.org /api/entities/{LEI}/filings
        │
        ▼
   at least one filing with error_count=0 AND a real json_url?
        │
   ┌────┴────┐
  YES        NO
   │          │
ADMITTED   REJECTED (or MANUAL_REVIEW)
```

This is a direct generalization of exactly what Phase 5.1's `select_filing_for_period` already
does per-filing — applied once, at admission time, per candidate. **Not every European listed
company will have an ESEF filing** — ESEF only applies to EU-regulated-market issuers filing
annual financial reports (already documented in ADR-0009 §4/the `SOURCE_REGISTRY` entry's
`historical_depth` field) — a listed company on a non-regulated market segment, a very recent
IPO with no annual report filed yet, or a company whose home country isn't in `filings.xbrl.org`'s
aggregator index (confirmed absent: Germany, Ireland — ADR-0009 §4) would all legitimately fail
this gate. That's a correct rejection, not a bug to work around.

## 13. Accounting-standard / reporting eligibility

Not proposed as a separate gate in Generation 1 — `filings.xbrl.org`'s aggregator index is
already, by construction, ESEF/IFRS filings only (verified: every real pilot filing's
`documentInfo.namespaces` includes `ifrs-full`). A candidate that clears the ESEF gate (§12) has
already, by definition, cleared the accounting-standard question — a separate check would be
redundant. If a future source (e.g. a national-GAAP-only market) is added, this would need
revisiting — explicitly flagged, not solved preemptively.

## 14. Access status vs. admission status (kept explicitly separate, per the brief)

`SourceAccessStatus` (existing, `sources/registry.py`) answers *"can this source be automated
at all"* — a property of the **source** (`EU_CURRENT` = `ACTIVE` since Phase 5.1). It says
nothing about any individual issuer. The proposed `AdmissionStatus` (§15) answers *"has this
specific candidate listing been verified admissible"* — a property of the **candidate**. An
`EU_CURRENT`-sourced candidate can be `REJECTED` (e.g. no LEI resolvable) even though the source
itself is fully `ACTIVE`; conversely a source being `RESEARCH_ONLY` (e.g. a hypothetical future
`ESAP`) would make every one of its candidates un-admittable regardless of their own individual
merits. These are orthogonal, not layered.

## 15. Proposed admission state machine

```
CANDIDATE
   │  (from a universe source: ticker, MIC, company name)
   ▼
IDENTITY_RESOLVING
   │  OpenFIGI: (ticker, MIC) -> exactly one Equity/Common-Stock instrument?
   │     NO / AMBIGUOUS  ──────────────────────────────► IDENTITY_AMBIGUOUS (terminal, reject)
   │     YES
   ▼
LEI_RESOLVING
   │  ISIN (from OpenFIGI candidate) -> GLEIF ISIN->LEI lookup
   │     NO MATCH ─────────────────────────────────────► NO_LEI (terminal, reject)
   │     MATCH
   ▼
ISSUER_RESOLVED
   │  issuer_id = make_issuer_id("EU_CURRENT", lei)  (reuses Phase 5.0's existing function)
   ▼
ESEF_CHECKING
   │  GET /api/entities/{lei}/filings
   │     NO ENTITY / EMPTY ────────────────────────────► NO_ESEF_ENTITY (terminal, reject)
   │     ENTITY EXISTS
   ▼
FILING_CHECKING
   │  any filing with error_count=0 AND json_url truthy? (reuses select_filing_for_period)
   │     NO ──────────────────────────────────────────► NO_USABLE_FILING (terminal, reject)
   │     YES
   ▼
ADMITTED
   listing_id = MIC:TICKER, issuer_id = EU_CURRENT:LEI -- ready for EUCurrentSource
```

Every terminal rejection state is distinguishable and loggable (mirrors `ingestion_failures`'
own `error_type`/`step` shape, Phase 5.1's own precedent) — nothing collapses into a generic
"failed." `MANUAL_REVIEW` (not shown above as a hard state) is reserved for a future case this
research didn't need to invent one for yet — e.g. if `IDENTITY_RESOLVING` ever returns *multiple*
plausible equity matches rather than zero or one, that's a real ambiguity distinct from
`IDENTITY_AMBIGUOUS`'s "OpenFIGI found nothing" case and may warrant a human-reviewed queue
rather than an automatic reject — not encountered in this session's 4-pilot/2-collision
testing, so not over-specified here.

## 16. Corporate identity changes (scoped, not solved)

Explicitly deferred, per the brief's own instruction not to solve corporate actions completely
in this phase. What the admission layer above *does* let a future phase detect, structurally:
a re-run of `IDENTITY_RESOLVING`/`LEI_RESOLVING` for an already-`ADMITTED` listing that now
resolves to a *different* LEI than last time is a detectable, loggable event (ticker reused
after a delisting, a ticker change, or similar) — the state machine's own resolve-don't-assume
design is what makes this detectable at all, versus the current hardcoded pilot list, which has
no way to notice anything changed. Building the actual re-verification job is future work.

## 17. Country coverage

| Country | `filings.xbrl.org` coverage | Candidate universe coverage | Generation-1 status |
|---|---|---|---|
| Spain (ES) | Confirmed — real pilot (FCC) | Yes (EURO STOXX 50 / IBEX 35 constituents) | **Supported now** |
| France (FR) | Confirmed — real pilot (Alstom) | Yes | **Supported now** |
| Netherlands (NL) | Confirmed — real pilot (NAI) | Yes | **Supported now** |
| Italy (IT) | Confirmed — real pilot (Fincantieri) | Yes | **Supported now** |
| Germany (DE) | **Confirmed absent** from the `filings.xbrl.org` aggregator's index (ADR-0009 §4, not re-litigated here) | Yes (largest STOXX 600 weight) | **Unavailable** via this source, regardless of universe/identity work |
| Ireland (IE) | **Confirmed absent** (same source) | Yes | **Unavailable** |
| Other Eurozone/EEA (Belgium, Portugal, Austria, etc.) | Not individually re-verified this pass | Presumably yes | **Research only** |

The Germany/Ireland gap is a hard `filings.xbrl.org`-level ceiling, not something the admission
layer designed here can work around — it was already known (ADR-0009) and is restated here only
because §17 explicitly asked for it, not as a new finding.

## 18. Refresh strategy

Not deeply researched this pass (explicitly secondary to the identity-resolution questions) —
worth noting the existing US/CA precedent (full recompute every scheduled run, not incremental)
is a reasonable Generation-1 default for Europe too, since the universe-source problem (§4)
isn't solved yet regardless.

## 19. Pilot validation — the proposed mechanism, run conceptually against all 4 real pilots

| | FCC | Alstom | New Amsterdam Invest | Fincantieri |
|---|---|---|---|---|
| Candidate (ticker, MIC) | `FCC`, `XMAD` | `ALO`, `XPAR` | `NAI`, `XAMS` | `FCT`, `MTAA`* |
| OpenFIGI resolves to | 1 match, Common Stock (confirmed live, `EO`-family codes) | 1 match, Common Stock (confirmed live, `FP`) | 1 match, Common Stock (confirmed live, `NA`) | not queried with MIC=`MTAA` specifically (OpenFIGI uses Bloomberg exchange codes, not raw ISO MICs — see note below); `IM`-coded match confirmed live |
| ISIN (from GLEIF's LEI→ISIN set) | `ES0122060314` (equity, ES-prefixed — high confidence) | one of 15 candidates (equity ISIN not independently isolated this pass — §6 gap) | one of 2 NL-prefixed candidates (not independently isolated) | one of 2 IT-prefixed candidates (not independently isolated) |
| LEI (GLEIF ISIN→LEI, reverse-confirmed for FCC) | `95980020140005178328` ✓ | `96950032TUYMW11FB530` (already known, reverse direction not re-tested per-pilot) | `724500JXEXUGEATP5L52` (same) | `8156005BDF49128B6239` (same) |
| ESEF entity exists | Yes — real, 5 filings | Yes — real, 4 filings | Yes — real, 4 filings (incl. amendment) | Yes — real, 5 filings (incl. amendment + 1 unusable) |
| Admission result | **ADMITTED** | **ADMITTED** | **ADMITTED** | **ADMITTED** |

**The design recovers the identity already manually verified for all four pilots on the parts
independently re-tested live this session** (OpenFIGI ticker+MIC resolution, GLEIF LEI record
lookup, and — for FCC specifically — the full ISIN↔LEI round trip). **Honest gap**: the
*exact* equity ISIN was only independently isolated end-to-end for FCC; for the other three,
this pass relied on the already-known LEI (from Phase 5.1) rather than re-deriving it from a
bare ticker+MIC through the full OpenFIGI→ISIN→GLEIF chain — re-running that full chain for
all four, and confirming it lands on the SAME LEI Phase 5.1 already used, is flagged as the
natural first implementation-validation step for Phase 5.3, not claimed as already done here.

*Note on MIC vs. Bloomberg exchange codes*: OpenFIGI's `micCode` parameter accepts real ISO
10383 MICs and worked directly for `XPAR`/`XAMS` (confirmed live) — Milan's `MTAA` specifically
was not re-tested with the `micCode` parameter in this pass (only the bare-ticker global query,
which surfaced the real collision in §8); the `MTAA` vs `XMIL` distinction from ADR-0010 should
be re-verified against OpenFIGI's own `micCode` filter in a follow-up, not assumed to behave
identically to Madrid/Paris/Amsterdam without checking.

## 20. Source accessibility matrix

| Source | Purpose | Authority | API? | Auth needed | Rate limit | Verified live this session |
|---|---|---|---|---|---|---|
| GLEIF | LEI record + ISIN↔LEI mapping | The official LEI-issuing federation (authoritative by definition) | Yes, REST, JSON:API-shaped | No | Not hit in this session's light usage | **Yes** — both directions, real pilot data |
| OpenFIGI | Ticker+MIC → security-typed instrument | Bloomberg (industry-standard open symbology, not a government body but widely relied upon) | Yes, REST | No (key optional, raises rate limit) | 5,000/day unauthenticated, 50,000/day with free key (per OpenFIGI's own docs, not independently load-tested) | **Yes** — real pilot + real collision data |
| ISO 20022 / ESMA FIRDS (MIC registry) | Authoritative MIC codes | ISO / ESMA (official standards bodies) | CSV download | No | N/A | Reused from Phase 5.0's own prior verification, not re-fetched this pass |
| `filings.xbrl.org` | ESEF entity/filing confirmation | XBRL International (aggregator, not the regulator) | Yes, JSON:API | No | Not documented publicly; not hit hard enough in Phase 5.1/5.2 to surface one | **Yes** — extensively, Phase 5.1 |
| STOXX official selection lists | Index constituent source | STOXX (the index provider itself — authoritative) | PDF/Excel per their site, license-gated | **Yes — paid Third-Party Data License** | N/A | **Yes** (confirmed the paywall, not the data) |
| iShares ETF holdings (EURO STOXX 50 / STOXX 600 trackers) | Free proxy for index membership | Indirect (the ETF issuer, not STOXX itself) | Unclear — page references a download feature, exact URL not resolved | Unknown | Unknown | **Partially** — product pages found real; exact holdings-download URL not confirmed (403 on direct fetch) |
| Wikipedia (STOXX Europe 600 article) | Free proxy for index membership | Community-maintained, not authoritative | HTML table, `pd.read_html`-compatible in principle | No | N/A | **Partially** — page exists, full-table completeness unconfirmed |
| Euronext data portal | Instrument reference data | Euronext (the exchange itself) | Points to a commercial Data Shop | Likely yes for anything beyond marketing pages | N/A | **No** — no free path found |

## 21. Recommended Generation-1 architecture

```
 Universe Source                         Identity Resolution                    Admission Gate
┌──────────────────┐                    ┌──────────────────────┐              ┌─────────────────┐
│  ETF holdings     │  ticker, MIC       │ OpenFIGI              │   LEI        │ filings.xbrl.org │
│  (EURO STOXX 50 / │ ─────────────────► │ (ticker,MIC)→ISIN,    │ ───────────► │ /api/entities/   │
│  STOXX 600        │  candidate         │ security type check   │              │ {LEI}/filings    │
│  tracker, TBD      │  listing           │       │               │              │       │           │
│  which exact ETF/  │                    │       ▼               │              │       ▼           │
│  URL — see §4/§20) │                    │ GLEIF ISIN→LEI        │              │  ADMITTED /      │
└──────────────────┘                    │ (cross-check LEI→ISIN │              │  REJECTED         │
                                          │  set contains it)     │              └─────────────────┘
                                          └──────────────────────┘
```

This is the target diagram from the brief (§23), confirmed **architecturally sound** by this
session's live testing of every link except the leftmost box (universe source — the one piece
genuinely unresolved). Recommend: (1) treat universe-source discovery (the exact free ETF
holdings URL, or an equivalent) as its own, narrowly-scoped follow-up research task before any
implementation phase, mirroring the hands-on discovery work already done for IWV/XIC; (2)
everything right of "Candidate Listing" in the diagram is ready to prototype against real data
whenever that's authorized, since GLEIF/OpenFIGI/filings.xbrl.org are all confirmed live,
free, and unauthenticated.

## 22. Open questions

- Exact, verified free download mechanism for a European index-tracking ETF's holdings (§4, §20).
- Whether Wikipedia's STOXX Europe 600 article actually has a complete constituent table (§4) —
  needs a direct `pd.read_html` row-count check, not another summarized web fetch.
- The precise ISIN-to-equity-instrument isolation step for issuers with many ISINs (§6) — likely
  an OpenFIGI reverse `ID_ISIN` lookup per GLEIF-supplied candidate, not yet implemented/tested.
- Whether `MTAA` (vs `XMIL`) behaves correctly as an OpenFIGI `micCode` filter value (§19 note) —
  not re-tested this pass.
- GLEIF's real-world ISIN-to-LEI coverage limits for markets/instruments beyond the 4 already-
  verified pilots — the "early mover national numbering agencies" caveat in GLEIF's own
  documentation was not stress-tested against a genuinely obscure or newly-issued security.
- National exchange/regulator security-master sources (BME, Borsa Italiana, AMF, AFM) — entirely
  unexplored this pass, flagged as a real, not-yet-investigated category.
- How `MANUAL_REVIEW` should actually be operationalized (a queue? a config file? a Delta
  table?) if the state machine (§15) ever needs it for real — no case requiring it was found
  this session, so its concrete mechanism wasn't designed.

## 23. Explicit non-goals (this pass)

- No implementation of any part of the state machine, universe fetch, or identity resolution.
- No modification to `config.tickers`, any Delta schema, the DAG, `EUCurrentSource`,
  `fundamentals_screener`, or Streamlit.
- No European tickers added anywhere in production.
- No attempt to fully solve corporate-action/identity-change handling (§16) — structurally
  enabled by the proposed design, not built.
- No claim that STOXX/Euronext data licensing terms were read in full legal detail — the
  "paid license required" finding is based on the public-facing page text, not a legal review.
- No universe beyond the four already-known pilots was actually queried against this proposed
  pipeline end-to-end — §19's validation reuses already-known identity for 3 of 4 pilots rather
  than re-deriving all four from scratch (stated honestly as a gap, not hidden).

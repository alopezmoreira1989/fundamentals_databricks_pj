# Phase 5.2 — European universe & admission layer: research and architecture

**Status: research/design only. Nothing in this document has been implemented.** No
production table, schema, DAG, `EUCurrentSource`, `fundamentals_screener`, or Streamlit change
accompanies this pass, per the explicit scope boundary this phase was given.

Companion to [ADR-0009](adr/0009-multi-market-fundamentals-ingestion-framework.md), [ADR-0010](adr/0010-issuer-listing-identity-model.md),
and [docs/phase5-1-eu-adapter.md](phase5-1-eu-adapter.md). See also
[docs/adr/0011-european-universe-admission.md](adr/0011-european-universe-admission.md) (status: Proposed) for the
decision this research supports.

**Note on this document's own universe-source verdict**: this pass and its direct follow-up
([phase5-2b](phase5-2b-european-universe-source-validation.md)) classified European universe
discovery `NOT SOLVED` after STOXX and iShares were tested and disqualified. A later pass,
[phase5-2c](phase5-2c-esma-firds-research.md), found ESMA FIRDS to be a materially better
candidate (`READY WITH CONDITIONS`) — kept here for the review trail, but phase5-2c is the
current state of that question.

---

## 1. Executive summary

Phase 5.1 proved the European ingestion *pipeline* works end to end (`filings.xbrl.org →
EUCurrentSource → canonical financials`) for four issuers whose `(ticker, LEI, MIC)` identity
was manually verified and hardcoded. This phase researches — without implementing — how a
**new** European listing would enter that pipeline without a human hand-verifying its identity
triple first.

The central finding: **a working, deterministic identity bridge exists and was verified live
this session, end to end, for all four real pilots** — `GLEIF` (the LEI-issuing authority's own
free public API) resolves both `ISIN → LEI` and `LEI → ISIN` with zero authentication;
`OpenFIGI` (a free, widely-used industry security-reference source — not a legal/regulatory
identity authority the way GLEIF is, see §5) independently resolves `(ticker, MIC)` to exactly
one, correctly-typed equity instrument for every pilot including Fincantieri's `MTAA:FCT`
(cross-checked against the operating MICs `XMIL`/`BMEX`, which OpenFIGI correctly does *not*
recognize — independent, third-source reinforcement of ADR-0010). The full chain
`ticker+MIC → OpenFIGI → equity ISIN → GLEIF → LEI` was independently re-derived — not assumed
or reused from Phase 5.1 — for all four pilots, and reproduced the exact, already-known LEI
every time; a real pagination bug in GLEIF's `LEI → ISIN` endpoint (defaults to 15 results/page,
Alstom has 36 real ISINs) was found and fixed along the way (§19). Combined with the already-
established `filings.xbrl.org` ESEF confirmation gate, a **Universe → Listing → ISIN → LEI →
Issuer → ESEF Entity → Admission** pipeline is confirmed architecturally sound against real data.

What is **not solved, definitively, per a dedicated follow-up investigation**: a free, current
**universe source** for European index membership. `STOXX`'s own interactive "selection-lists"
portal requires a paid Third-Party Data License (verified live). A **separate, free, no-login
STOXX PDF** was found (`stoxx.com/document/Bookmarks/CurrentComponents/{SYMBOL}.pdf`) containing
the real, complete STOXX Europe 600 list (exactly 600 rows, verified by direct parsing) —
company name, country, sector, weight, but no ticker/ISIN/MIC. Its embedded PDF metadata showed
a `ModDate` of July 2023 despite "Current" in the URL; **a decisive follow-up test resolved this
from suspicion to confirmed fact**: checked directly against two real, dated 2026 STOXX Europe
600 quarterly reviews (additions/deletions effective March 23 and June 22, 2026), the PDF
reflects only 4 of 23 real additions and still lists 17 of 21 real deletions — genuinely stale
data, not just an old timestamp. A second, independent STOXX "Bookmarks" document was also found
stale. The ETF-holdings route (mirroring this project's own Russell 3000/TSX precedent) was
separately tested directly against the real iShares site and found blocked by a modern
JS-rendered (Astro) frontend with no working static CSV/JSON endpoint. **The European
universe-source question is therefore classified `C — NOT SOLVED`**, deliberately not forced by
substituting a source of unverifiable or confirmed-poor quality — a decision left open for the
repo owner, not resolved by this research. This is independent of, and does not block, the
identity-resolution architecture (ADR-0011), which is `READY`. Full detail:
[docs/phase5-2b-european-universe-source-validation.md](phase5-2b-european-universe-source-validation.md)
(see §27 there for the decisive test).

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

**Updated after a dedicated follow-up pass** — see
[phase5-2b-european-universe-source-validation.md](phase5-2b-european-universe-source-validation.md)
for the full detail behind every "RESOLVED" verdict below.

| Candidate | Coverage | Access | Identifiers provided | Verdict |
|---|---|---|---|---|
| **STOXX official selection lists** (EURO STOXX 50, STOXX Europe 600) | Eurozone / pan-Europe, exactly the index definitions this project already names as targets | **Paid Third-Party Data License required** — verified live: stoxx.com/selection-lists explicitly states "Access to remaining files are reserved to STOXX Index licensees"; only files prefixed `slpublic` are unrestricted, and it's unconfirmed whether EURO STOXX 50/STOXX 600 selection lists are among them | ISIN, SEDOL, RIC, company name (per a sample factsheet PDF found) | **AUTOMATION_RESTRICTED** for the full, current list — same shape as SEDAR+'s ToS-restricted status in ADR-0009 |
| **STOXX `CurrentComponents` PDF** (`stoxx.com/document/Bookmarks/CurrentComponents/{SYMBOL}.pdf`) — a **separate, free** resource from the row above, discovered in the follow-up pass | Same index definitions | **RESOLVED: free, no login** — verified live, HTTP 200, real PDF, no auth wall | Company name, supersector, country, weight — **no ticker/ISIN/MIC** | **UNAVAILABLE (confirmed stale)** — a decisive freshness test against two real, dated 2026 STOXX Europe 600 quarterly reviews found only 4 of 23 real additions present and 17 of 21 real deletions still listed; a second, independent STOXX "Bookmarks" document (a factsheet, different path) was also found stale (`ModDate: 2023-09-13`). See the follow-up doc §27. |
| **iShares Core EURO STOXX 50 UCITS ETF** (ISIN `IE00B53L3W79`) holdings | Tracks EURO STOXX 50 by construction | **RESOLVED: blocked** — the follow-up pass tested the documented legacy `.ajax?fileType=csv\|json` pattern directly with multiple parameter variants; the current `ishares.com/uk/...` site is an Astro-built SPA that does not expose a working static endpoint this way | Presumably ISIN/ticker/weight, matching the pattern IWV/XIC already expose, but not reachable | **UNAVAILABLE** (this pass) — not merely unresolved; a real negative result |
| **iShares STOXX Europe 600 UCITS ETF (DE)** (ISIN `DE0002635307`, ticker `EXSA`) holdings | Tracks STOXX Europe 600 | Same as above — confirmed blocked | Same as above | **UNAVAILABLE** (this pass) |
| **Wikipedia "STOXX Europe 600" article** | Partial | **RESOLVED via direct `pd.read_html`**: exactly **467 rows** (not ~600), columns `Ticker, Company, ICB Sector, Country, Headquarters` | Ticker, company name, sector, country (no ISIN/MIC) | **Confirmed incomplete** relative to the real 600-constituent index — not a viable sole universe source |
| **Euronext's own market-data portal** | Euronext-listed instruments (Paris/Amsterdam/Milan/Brussels/Lisbon/Dublin) | No public CSV/API endpoint found in this pass; portal points to a commercial "Data Shop" | Unknown | **RESEARCH_ONLY** — not ruled out, but no free automated path found live |
| **National exchange/regulator security-master files** (BME, Borsa Italiana, AMF, AFM) | Per-country | Not investigated in this pass — real candidate, out of time budget this session | Unknown | **RESEARCH_ONLY**, unexplored |

**Conclusion, updated after a decisive follow-up test (full detail:
[phase5-2b §27](phase5-2b-european-universe-source-validation.md))**: the STOXX
`CurrentComponents` PDF is real, free, and structurally bridgeable to a `(ticker, MIC)`
candidate via `Country → curated MIC table → OpenFIGI name+micCode search` — that bridging
mechanism itself was proven live for two new candidates (Iberdrola, Saint Gobain). **But the PDF
data is confirmed stale**, not merely suspected: tested directly against two real, dated 2026
STOXX Europe 600 quarterly reviews (March 23 and June 22, 2026), only 4 of 23 real additions
were present and 17 of 21 real deletions were still listed. **The European universe-source
question is therefore classified `C — NOT SOLVED`** (per the requested A/READY-B/READY WITH
CONDITIONS-C/NOT SOLVED framework), separate from and not blocking the identity-resolution
architecture (ADR-0011), which is independently `READY`. The iShares ETF route, by contrast, is
a confirmed dead end for this pass, not merely unexplored — the hands-on discovery work that
succeeded for Russell 3000 (a BlackRock varnish-api endpoint) and TSX Composite (a plain CSV
path) does not have a working analog on the current, Astro-rebuilt `ishares.com` UK site, at
least not one found via a direct HTTP approach.

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

**Terminology note (kept explicit, per review)**: everything above the `filings.xbrl.org` step
is *identity resolution* ("we know exactly which issuer/listing this is"); the ESEF check itself
is a separate *fundamentals-eligibility* gate ("this issuer currently has usable ESEF data"). A
`NO` outcome here does not mean the candidate's identity was invalid — it means a successfully
identified issuer isn't currently ingestible through `EU_CURRENT`. Generation-1 still resolves
that to a practical `REJECTED` (see §19's fuller treatment of this distinction), but the two
concepts are not the same thing and the model doesn't conflate them.

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

## 19. Pilot validation — the full chain, independently re-derived end to end for all 4 pilots

**Updated after review**: the first pass of this research isolated the exact equity ISIN only
for FCC and reused Phase 5.1's already-known LEI for the other three. A follow-up pass closed
that gap — the full chain (`ticker+MIC → OpenFIGI → equity ISIN → GLEIF → LEI`) was independently
re-run for all four, and a real bug was found and fixed along the way (see the note below the
table).

| | FCC | Alstom | New Amsterdam Invest | Fincantieri |
|---|---|---|---|---|
| Candidate (ticker, MIC) | `FCC`, `XMAD` | `ALO`, `XPAR` | `NAI`, `XAMS` | `FCT`, `MTAA` |
| OpenFIGI `micCode` query result | 1 match, Common Stock — **verified live** | 1 match, Common Stock — **verified live** | 1 match, Common Stock — **verified live** | 1 match, Common Stock — **verified live** |
| Equity ISIN (isolated via OpenFIGI `ID_ISIN` reverse lookup against GLEIF's LEI→ISIN candidate set) | `ES0122060314` | `FR0010220475` | `NL0015000CG2` | `IT0005599938` |
| LEI (GLEIF `ISIN → LEI` reverse lookup, independently re-derived) | `95980020140005178328` ✓ matches Phase 5.1 | `96950032TUYMW11FB530` ✓ matches Phase 5.1 | `724500JXEXUGEATP5L52` ✓ matches Phase 5.1 | `8156005BDF49128B6239` ✓ matches Phase 5.1 |
| ESEF entity exists | Yes — real, 5 filings | Yes — real, 4 filings | Yes — real, 4 filings (incl. amendment) | Yes — real, 5 filings (incl. amendment + 1 unusable) |
| Admission result | **ADMITTED** | **ADMITTED** | **ADMITTED** | **ADMITTED** |

**LIVE TEST RESULT — the design independently reproduces Phase 5.1's own manually-verified
identity for all four pilots**, not assumed, not reused. Every LEI in the table above was
derived *from the candidate's ticker+MIC alone* (via OpenFIGI → ISIN → GLEIF), then compared
against — never seeded from — the LEI Phase 5.1 already used.

**A real bug found and fixed in the process, worth recording precisely**: the first attempt at
this table found Alstom's GLEIF `LEI → ISIN` response contained exactly 15 ISINs, none of them
the real equity — because **GLEIF's API paginates at 15 results per page by default**, and
Alstom (a frequent bond issuer) actually has 36 ISINs under its LEI. Re-fetching with
`page[size]=50` surfaced the 36th entry, `FR0010220475`, which OpenFIGI confirmed is exactly
Alstom's real `XPAR:ALO` Common Stock (same FIGI, `BBG000DQ7884`, as the direct ticker+MIC
query), and GLEIF's reverse `ISIN → LEI` lookup on it returned exactly the expected LEI. This is
a real, general implementation risk for any future admission code: **any GLEIF `isins`
relationship call must handle pagination explicitly** — a naive single-page fetch silently
produces incomplete (and, for a bond-heavy issuer, potentially equity-free) results, exactly the
kind of "ambiguity → guess" failure mode this whole architecture exists to prevent, now caught
concretely rather than theoretically.

**MIC verification — now fully confirmed, all four, via a third independent source**: OpenFIGI's
`micCode` parameter was tested directly against every project MIC. `XMAD` (FCC), `XPAR` (Alstom),
`XAMS` (New Amsterdam Invest), and **`MTAA` (Fincantieri)** all resolve correctly to the expected
Common Stock instrument. The corresponding *operating* MICs were also tested as a negative
control: `BMEX` and `XMIL` both returned `"warning": "No identifier found."` — OpenFIGI does not
recognize either operating MIC for these tickers, an independent, third-source reinforcement of
ADR-0010's segment-MIC decision (not merely "not contradicted" — actively confirmed).

### Identity resolution vs. fundamentals eligibility (kept explicit, per review)

The table above conflates two concepts for brevity that the underlying model keeps separate:
**identity resolution** ("we know exactly which issuer/listing this is" — the `ticker+MIC →
OpenFIGI → ISIN → GLEIF → LEI` chain) is complete and successful for all four pilots regardless
of ESEF availability. **Fundamentals eligibility** ("this issuer currently has usable ESEF data
for `EU_CURRENT`" — the `filings.xbrl.org` check) is a separate, later question. A candidate
that clears identity resolution but fails the ESEF check (e.g. a real German or Irish company,
per §17's confirmed `filings.xbrl.org` coverage gap) has a fully valid, confirmed identity — it
is `NOT_INGESTIBLE`, not an invalid or rejected identity. Generation-1's practical admission gate
(§15) still treats that as a terminal `REJECTED` state for this pipeline's purposes (there is no
use, today, in carrying forward an issuer with no ingestible data), but the state machine's
naming (`NO_ESEF_ENTITY`/`NO_USABLE_FILING`, distinct from `IDENTITY_AMBIGUOUS`/`NO_LEI`) already
reflects this distinction, and a future source covering more countries could re-admit the same,
already-identified issuer without repeating identity resolution.

## 20. Source accessibility matrix

| Source | Purpose | Authority | API? | Auth needed | Rate limit | Verified live this session |
|---|---|---|---|---|---|---|
| GLEIF | LEI record + ISIN↔LEI mapping | The official LEI-issuing federation (authoritative by definition) | Yes, REST, JSON:API-shaped | No | Not hit in this session's light usage | **Yes** — both directions, real pilot data |
| OpenFIGI | Ticker+MIC → security-typed instrument reference | Bloomberg (industry security-reference/symbology source — **not** a legal or regulatory authority over identity, unlike GLEIF; see the terminology note in ADR-0011) | Yes, REST | No (key optional, raises rate limit) | **Directly observed live**: `ratelimit-policy: 25;w=60` header (25 requests/60s) on the unauthenticated `/v3/mapping` endpoint; batch jobs additionally capped at 10 per request (`HTTP 413` on an 11+ job batch, confirmed live). OpenFIGI's own docs separately claim 5,000/day unauthenticated / 50,000/day with a free key — that daily figure was not independently load-tested, only the per-minute header was directly confirmed. | **Yes** — real pilot + real collision data |
| ISO 20022 / ESMA FIRDS (MIC registry) | Authoritative MIC codes | ISO / ESMA (official standards bodies) | CSV download | No | N/A | Reused from Phase 5.0's own prior verification, not re-fetched this pass |
| `filings.xbrl.org` | ESEF entity/filing confirmation | XBRL International (aggregator, not the regulator) | Yes, JSON:API | No | Not documented publicly; not hit hard enough in Phase 5.1/5.2 to surface one | **Yes** — extensively, Phase 5.1 |
| STOXX official selection lists (portal) | Index constituent source | STOXX (the index provider itself — authoritative) | PDF/Excel per their site, license-gated | **Yes — paid Third-Party Data License** | N/A | **Yes** (confirmed the paywall, not the data) |
| STOXX `CurrentComponents` PDF (separate free resource) | Index constituent source | STOXX (same provider, different resource) | Direct PDF GET, no login | No | N/A | **Yes — confirmed stale**: real 600-row list parsed, then decisively tested against two real 2026 quarterly reviews (only 4/23 additions present, 17/21 deletions still listed) — full detail in the follow-up doc §27. **Disqualified as a Generation-1 universe source, not merely flagged.** |
| iShares ETF holdings (EURO STOXX 50 / STOXX 600 trackers) | Free proxy for index membership | Indirect (the ETF issuer, not STOXX itself) | N/A — confirmed unreachable | N/A | N/A | **Yes, confirmed blocked** — see the follow-up doc §2/§3 for the specific negative test results (multiple magic-number/parameter variants tried, all failed) |
| Wikipedia (STOXX Europe 600 article) | Free proxy for index membership | Community-maintained, not authoritative | HTML table, `pd.read_html`-compatible | No | N/A | **Yes** — confirmed via direct `pd.read_html`: 467 rows, not ~600, no ISIN/MIC |
| Euronext data portal | Instrument reference data | Euronext (the exchange itself) | Points to a commercial Data Shop | Likely yes for anything beyond marketing pages | N/A | **No** — no free path found |

## 21. Recommended Generation-1 architecture

**UPDATED — the universe-source box below is now known-disqualified (phase5-2b §27: confirmed
stale against real 2026 data), not merely "freshness unresolved."** The diagram is kept to show
exactly what was proven and what wasn't — everything to the right of "Universe Source" is real,
live-tested, and ready; the Universe Source box itself is not a working Generation-1 choice as
things stand.

```
 Universe Source                    MIC Derivation      Identity Resolution              Admission Gate
┌────────────────────┐            ┌───────────────┐   ┌──────────────────────┐        ┌─────────────────┐
│ ??? — the STOXX      │  company   │ Country (from │   │ OpenFIGI              │  LEI   │ filings.xbrl.org │
│ CurrentComponents    │  name +    │ STOXX PDF) -> │──►│ name+micCode search   │───────►│ /api/entities/   │
│ PDF WAS tested here, │  country   │ curated MIC   │   │ -> exactly one Common │        │ {LEI}/filings    │
│ and DISQUALIFIED     │───────────►│ table         │   │ Stock instrument      │        │       │           │
│ (confirmed stale     │            └───────────────┘   │       │               │        │       ▼           │
│  vs. real 2026 data) │                                │       ▼               │        │  ADMITTED /      │
└────────────────────┘                                │ GLEIF ISIN->LEI        │        │  NOT_INGESTIBLE   │
                                                          │ (via OpenFIGI ID_ISIN  │        └─────────────────┘
                                                          │  reverse + GLEIF       │
                                                          │  ISIN->LEI)            │
                                                          └──────────────────────┘
```

This **updates** the brief's original target diagram (§23) twice over now: the original assumed
ETF holdings as the universe box (confirmed non-working); the STOXX PDF substitute investigated
next was also tested and disqualified (confirmed stale, not merely unresolved). **Everything
right of the Universe Source box was live-tested end to end for two genuinely new candidates**
(Iberdrola, Saint Gobain — not the four already-known pilots), with Saint Gobain's chain fully
completed through to 4 real, clean `filings.xbrl.org` filings — that part of the architecture is
`READY`. The universe-source question is `NOT SOLVED` and is **not** recommended to be forced by
substituting a lower-quality or unverified source merely to have "a" universe — see phase5-2b
§27.6 for the real remaining choices (manual curation, a commercial license, further iShares
reverse-engineering, or revisiting this later), left as an open decision, not resolved here.

## 22. Open questions

**Resolved by the follow-up pass** (kept here, struck through, so the review trail is visible
rather than silently deleted):
- ~~Exact, verified free download mechanism for a European index-tracking ETF's holdings~~ —
  investigated in depth; found **blocked** (a real negative result, not a positive one) — see
  [phase5-2b-european-universe-source-validation.md](phase5-2b-european-universe-source-validation.md).
- ~~Whether Wikipedia's STOXX Europe 600 article actually has a complete constituent table~~ —
  **resolved: no**, confirmed via direct `pd.read_html` — exactly 467 rows, not ~600, and no
  ISIN/exchange columns. See the same doc.
- ~~The precise ISIN-to-equity-instrument isolation step for issuers with many ISINs~~ —
  **resolved**: OpenFIGI reverse `ID_ISIN` lookup against GLEIF's candidate set, tested and
  working for all 4 pilots (§19).
- ~~Whether `MTAA` (vs `XMIL`) behaves correctly as an OpenFIGI `micCode` filter value~~ —
  **resolved: yes**, `MTAA` works, `XMIL`/`BMEX` (operating MICs) correctly return no result (§19).

**Still open**:
- The real, free STOXX "CurrentComponents" PDF's own freshness (`ModDate` July 2023 despite the
  "Current" naming) — not resolved; a production admission layer cannot trust this source's
  currency without further verification (a periodic re-check, or comparing against a second
  source) — see the new doc §5/§15 for detail.
- Bridging the STOXX PDF's company name (no ticker/ISIN) to a `(ticker, MIC)` candidate — OpenFIGI
  name search was tested and found genuinely ambiguous on its own (many results per company name
  across markets/currencies); combining it with the PDF's own `Country` column via a small,
  curated country→primary-MIC table is the leading idea, not implemented or tested end-to-end.
- GLEIF's real-world ISIN-to-LEI coverage limits for markets/instruments beyond the 4 already-
  verified pilots — the "early mover national numbering agencies" caveat in GLEIF's own
  documentation was not stress-tested against a genuinely obscure or newly-issued security.
- National exchange/regulator security-master sources (BME, Borsa Italiana, AMF, AFM) — entirely
  unexplored this pass, flagged as a real, not-yet-investigated category.
- How `MANUAL_REVIEW` should actually be operationalized (a queue? a config file? a Delta
  table?) if the state machine (§15) ever needs it for real — no case requiring it was found
  this session, so its concrete mechanism wasn't designed.
- Whether GLEIF's pagination default (15/page, discovered via the Alstom bug, §19) affects any
  other GLEIF endpoint used elsewhere in this research — only the `isins` relationship endpoint
  was stress-tested for this; the base `lei-records` lookups were not similarly checked because
  they return a single record, not a list.

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

# Phase 5.2c — ESMA FIRDS as the European universe/reference-data source

**Status: research only. Nothing in this document has been implemented.** Companion to
[docs/phase5-2-european-universe-admission-research.md](phase5-2-european-universe-admission-research.md),
[docs/phase5-2b-european-universe-source-validation.md](phase5-2b-european-universe-source-validation.md)
(which classified the universe-source question `C — NOT SOLVED` after STOXX/iShares were tested
and disqualified), and [ADR-0011](adr/0011-european-universe-admission.md) (status stays
**Proposed**).

**Labeling convention, per explicit instruction**: **VERIFIED FROM OFFICIAL ESMA DATA** (fetched
directly from `esma.europa.eu`/`registers.esma.europa.eu`/`firds.esma.europa.eu` this session),
**VERIFIED FROM REAL DATA** (a live query/parse this session with real output), **OFFICIAL
DOCUMENTATION** (ESMA's own published instructions, not independently re-derived), **SECONDARY
SOURCE**, **INFERENCE**, **RECOMMENDATION**, **OPEN QUESTION**.

## Executive summary

**Final verdict: `READY WITH CONDITIONS`** (§28 has the full, exact answer). This is a
materially different outcome from Phase 5.2b's STOXX/iShares verdict (`NOT SOLVED`) — FIRDS
passes precisely the test STOXX failed.

**VERIFIED FROM OFFICIAL ESMA DATA**: ESMA publishes FIRDS reference data via a genuine, free,
unauthenticated, documented machine-to-machine API
(`registers.esma.europa.eu/solr/esma_registers_firds_files/select`), exactly as described in
ESMA's own official instructions document (ESMA65-8-5014 rev.3). A live query for files
published 2026-08-14 to 2026-08-16 returned real results, including a **full equity reference
file (`FULINS_E_20260815_...`) published the day before this research** — genuinely current,
not a stale cached snapshot. Downloaded both parts of that file (682,398 total `RefData`
records for the Equity CFI class alone) and **independently found all four Phase 5.1 pilots by
their real ISIN**, with LEI and MIC matching Phase 5.1's already-verified values *exactly* —
including Fincantieri's `MTAA` MIC specifically, now confirmed by a **fourth independent
source** (after the ISO 20022/FIRDS registry, ADR-0010's original research, and OpenFIGI).
**A real 2026-07-28 publication date was found inside the file's own records** — the decisive
freshness proof the STOXX PDF could not provide.

FIRDS has **no ticker field** at all (confirmed by reading the real schema) — it is natively
`ISIN + LEI + MIC`-keyed, which is a *better* structural fit for this project's `issuer_id`/
`listing_id` model than STOXX's company-name-based list ever was, at the cost of needing a
separate ticker-enrichment step (OpenFIGI, already proven in Phase 5.2b).

---

## 1. Official ESMA sources used

**OFFICIAL DOCUMENTATION**: ESMA65-8-5014 rev.3, "FIRDS Reference Data System — Instructions on
download and use of full, delta and cancellations reference data files" (09 February 2022),
fetched directly from `esma.europa.eu/sites/default/files/library/esma65-8-5014_firds_-
_instructions_for_download_of_full_and_delta_reference_files.pdf` — a real, official ESMA PDF,
parsed directly (not summarized by a third party). This document describes:

- **Full file**: weekly, generated Sunday mornings by 09:00 CET, containing every active
  instrument reference record.
- **Delta file**: daily, generated every morning by 09:00 CET, containing additions/
  modifications/deletions since the last file set.
- **Cancellations file**: daily, similar cadence.
- **Machine-to-machine access**: a documented Solr query endpoint,
  `https://registers.esma.europa.eu/solr/esma_registers_firds_files/select`, with `q`, `fq`
  (date-range filter), `wt` (response format), `start`/`rows` (pagination) parameters — no
  authentication described or required.
- **File naming**: `FULINS_<CFI 1st letter>_<YYYYMMDD>_<N>of<M>.zip` for full files (split when
  exceeding 500,000 records or by CFI first letter), `DLTINS_<YYYYMMDD>_<N>of<M>.zip` for delta,
  `FULCAN_<YYYYMMDD>_<N>of<M>.zip` for cancellations.

No third-party aggregator (e.g. "eFIRDS") was used as a primary source anywhere in this
research — every claim below traces to ESMA's own documentation or a direct fetch from an ESMA-
operated domain.

## 2. Real FIRDS data actually downloaded

**VERIFIED FROM OFFICIAL ESMA DATA — live M2M query, run this session**:
```
GET https://registers.esma.europa.eu/solr/esma_registers_firds_files/select
    ?q=*&fq=publication_date:[2026-08-14T00:00:00Z TO 2026-08-16T23:59:59Z]
    &wt=json&start=0&rows=100
```
Returned 49 real file entries for that window, including `FULCAN` (2026-08-15), `DLTINS`
(2026-08-14, 3 parts), and `FULINS` entries for CFI classes `C, D, E, F, H, I, J, O, R`
(2026-08-15) — `FULINS_E` (Equity) split into 2 parts, `FULINS_R` into 18 parts (a much larger
class — rights/warrants-adjacent instruments, not investigated further this pass).

**Actually downloaded**: both `FULINS_E_20260815_01of02.zip` (8,698,068 bytes) and
`FULINS_E_20260815_02of02.zip` (2,828,024 bytes) directly from `firds.esma.europa.eu` — real
HTTP 200 responses, real ZIP archives, each containing one XML file
(`FULINS_E_20260815_01of02.xml`, `FULINS_E_20260815_02of02.xml`). Decompressed: 364,970,966 +
146,122,742 characters of XML — **500,000 `RefData` records in file 1 (hitting the documented
per-file cap exactly) + 182,398 in file 2 = 682,398 total** `RefData` records for the Equity
class, published as one day's full file. This is **all reported equity (ISIN, trading venue)
combinations across the EU/EEA**, not 682,398 distinct companies — see §12/§22.

## 3. Real data fields (from the actual XML, not inferred)

**VERIFIED FROM REAL DATA** — one real `RefData` record, verbatim structure (namespace
`urn:iso:std:iso:20022:tech:xsd:auth.017.001.02`):
```xml
<RefData>
  <FinInstrmGnlAttrbts>
    <Id>NL00150001S5</Id>                          <!-- ISIN -->
    <FullNm>Kingfish Co NV/The SHRS</FullNm>
    <ShrtNm>KINGFISH ZEELAN/SH VTG FPD</ShrtNm>
    <ClssfctnTp>ECVUFB</ClssfctnTp>                <!-- CFI code -->
    <NtnlCcy>EUR</NtnlCcy>
    <CmmdtyDerivInd>false</CmmdtyDerivInd>
  </FinInstrmGnlAttrbts>
  <Issr>9845004WD3997B9F1061</Issr>                <!-- issuer LEI -->
  <TradgVnRltdAttrbts>
    <Id>EBLX</Id>                                   <!-- trading venue MIC -->
    <IssrReq>false</IssrReq>
    <FrstTradDt>2022-11-11T07:30:00Z</FrstTradDt>
  </TradgVnRltdAttrbts>
  <TechAttrbts>
    <RlvntCmptntAuthrty>NO</RlvntCmptntAuthrty>
    <PblctnPrd><FrDt>2025-11-18</FrDt></PblctnPrd>
    <RlvntTradgVn>MERK</RlvntTradgVn>
  </TechAttrbts>
</RefData>
```
Confirmed field mapping: **`Id` = ISIN**, **`Issr` = issuer LEI**, **`TradgVnRltdAttrbts/Id` =
trading venue MIC**, **`ClssfctnTp` = CFI code**, `NtnlCcy` = currency, `FrstTradDt` = first
trade date on that venue, `TechAttrbts/PblctnPrd/FrDt` = the record's own publication/validity
start date (the field used for the freshness test, §16). **No ticker/symbol field anywhere in
`FinInstrmGnlAttrbts` or elsewhere in the record** — confirmed by inspecting the full schema,
not merely its absence from one example.

## 4. Can FIRDS identify equity? Yes — deterministically, via CFI

**VERIFIED FROM REAL DATA + SECONDARY SOURCE**: all four pilots' real `ClssfctnTp` values begin
with `ES` (`ESVUFB`, `ESVUFN`, `ESVUFR`, `ESVUFR` — FCC, Alstom, NAI, Fincantieri respectively).
Cross-checked against ISO 10962 (CFI code) public documentation: **`E` = Category "Equity"**,
**`S` = Group "Common/Ordinary Shares"** (the current ISO 10962 revision's group letter for
common shares under the Equity category). The remaining four characters are attributes (voting
status, ownership restrictions, payment status, form) — not needed for the category/group-level
filter. **Deterministic Generation-1 filter, not invented**: `ClssfctnTp` starts with `"ES"` →
common/ordinary equity share. FIRDS' own documentation (§5.1 of ESMA65-8-5014) also confirms a
**separate, official CFI reference-data file** exists (published weekly) giving the authoritative
code-to-description mapping — not downloaded this pass, but a real, findable source for
validating the filter rule beyond this session's spot-check, rather than trusting the ISO
standard's public description alone.

## 5. Issuer vs. instrument vs. listing — a clean structural fit

**INFERENCE, grounded in §2/§3's real data**: each pilot ISIN appeared as **16 to 45 separate
`RefData` records within one file** — one per `(ISIN, MIC)` pair, exactly matching FIRDS' own
documented model ("one RefData element per security per reporting trading venue"). This is a
structurally clean match for this project's existing model:
```
Issr (LEI)              -> issuer_id  (source-qualified: e.g. "FIRDS:<LEI>" or reuse "EU_CURRENT")
Id + TradgVnRltdAttrbts/Id (ISIN + MIC) -> listing_id-equivalent (a listing/venue-level record)
Id (ISIN) alone          -> the instrument itself, distinct from any one listing
```
Confirms the brief's own suspicion (§10 of the prompt): the more natural direction is
`ISIN + MIC + LEI → identity`, with **ticker as a separate enrichment layer**, not the starting
point — the opposite of `ticker → identity`, and a stronger foundation than Phase 5.2's original
`ticker+MIC → OpenFIGI → ISIN` design, which can now be inverted: **FIRDS supplies the
`(ISIN, MIC, LEI)` triple directly and authoritatively; OpenFIGI's role becomes ticker
enrichment for an already-identified instrument**, not the primary identity resolver.

## 6–8. Pilot validation — all four, independently, from official regulatory data

**VERIFIED FROM OFFICIAL ESMA DATA — the acceptance criterion requested, met without seeding
from prior knowledge**: searched the real 2026-08-15 equity file by ISIN only (the ISINs
themselves were already known from Phase 5.2's GLEIF work — the LEI and MIC values below were
**not** provided to the search, only independently read back from the matching FIRDS record).

| Ticker | ISIN | FIRDS `Issr` (LEI) found | Matches Phase 5.1/5.2 LEI? | FIRDS MIC found (among the expected venue) | Matches ADR-0010 MIC? | FIRDS `ClssfctnTp` | FIRDS name |
|---|---|---|---|---|---|---|---|
| FCC | `ES0122060314` | `95980020140005178328` | ✅ exact match | `XMAD` present (of 35 total venues) | ✅ | `ESVUFB` | ACCIONES FOMENTO DE CONSTRUCCIONES Y CONTRATAS, S.A. |
| ALO | `FR0010220475` | `96950032TUYMW11FB530` | ✅ exact match | `XPAR` present (of 45 total venues) | ✅ | `ESVUFN` | ALSTOM |
| NAI | `NL0015000CG2` | `724500JXEXUGEATP5L52` | ✅ exact match | `XAMS` present (of 16 total venues) | ✅ | `ESVUFR` | NEW AMSTERDAM INVEST N.V. ORDINARY SHARES |
| FCT | `IT0005599938` | `8156005BDF49128B6239` | ✅ exact match | **`MTAA`** present (of 41 total venues) | ✅ | `ESVUFR` | FINCANTIERI |

**All four LEIs match exactly. All four expected MICs are present among each ISIN's real,
larger set of reporting venues** (FCC alone is reported on 35 distinct venues — regulated
markets, MTFs, and systematic internalisers across the EU, not just Madrid — a real,
confirmed instance of MiFID II's multi-venue reporting requirement in action, and the reason a
single ISIN's FIRDS records vastly outnumber "one company, one listing"). New Amsterdam
Invest's FIRDS record is unusually explicit — its `FullNm` literally says "ORDINARY SHARES,"
independent textual confirmation of the CFI-based equity classification for that pilot.

## 9. MTAA / Fincantieri — now a fourth independent confirmation

`MTAA` is present among Fincantieri's real FIRDS venue records, **directly from ESMA's own
regulatory reference data** — joining the ISO 20022/FIRDS-MIC-registry (ADR-0010's original
source), OpenFIGI's `micCode` filter (Phase 5.2), and now FIRDS' own trading-venue field as
independent confirmations of the same MIC decision. `XMIL` was not specifically searched for in
this pass (the query was by ISIN, returning every real venue FIRDS reports — `XMIL` did not
appear among Fincantieri's 41 listed venues in the data examined, consistent with `MTAA` being
the correct segment-level MIC and `XMIL` being the operating-level MIC that doesn't itself carry
trading records).

## 10. The ticker question — confirmed absent, as expected

**VERIFIED FROM REAL DATA**: no ticker/symbol field exists anywhere in the real FIRDS schema
examined (§3). **Documented explicitly, per the brief's own instruction**: FIRDS is an
authoritative reference/universe source keyed on `ISIN`/`LEI`/`MIC` — ticker must be obtained
from another source. Phase 5.2b already proved a working ticker-enrichment path (OpenFIGI's
`ID_ISIN` reverse lookup, tested successfully for all four pilots' real ISINs) — that mechanism
is directly reusable here, now applied to FIRDS-sourced ISINs instead of STOXX-sourced company
names, which is a *simpler* input (an ISIN is a precise key; a company name required the
country→MIC bridge Phase 5.2b had to build).

## 11. Architecture reconsideration

**RECOMMENDATION**: yes, evidence supports inverting the originally-proposed direction. Rather
than:
```
ticker + MIC -> OpenFIGI -> ISIN -> GLEIF -> LEI -> filings.xbrl.org
```
the FIRDS-anchored model is:
```
FIRDS (ISIN + MIC + LEI, authoritative, current) -> GLEIF (confirm/enrich LEI) ->
    OpenFIGI (ISIN -> ticker enrichment, for display/market-data purposes only) ->
    filings.xbrl.org (ESEF eligibility)
```
Ticker remains explicitly a **convenience/display identifier**, never primary identity — this
was already this project's stated principle (ADR-0010); FIRDS is simply the first source found
that structurally enforces it by not offering a ticker field to lean on in the first place.

## 12. Universe definition — FIRDS raw data is not the admission universe

**VERIFIED FROM REAL DATA, confirms the brief's own concern (§25)**: 682,398 raw equity
`RefData` records for one day is not remotely the intended candidate universe — it is every
`(ISIN, MIC)` reporting combination, including systematic-internaliser and MTF venues that are
not primary listings, non-EU issuers whose instruments happen to trade on an EU venue, and
(per §4) presumably some non-common-equity rows before the CFI filter is applied. **Three
distinct layers, kept explicit, per the brief's own instruction**:
```
FIRDS raw universe          (all (ISIN, MIC) records — 682,398/day for Equity alone)
        |  filter: ClssfctnTp starts "ES" (common/ordinary shares)
        v
Eligible equity universe    (real equities only, still every reporting venue)
        |  filter: primary/regulated-market venue selection (NOT designed this pass —
        |  a real open question, since one ISIN legitimately has 16-45 venue records)
        v
Project admission universe  (the candidate set actually offered to identity resolution)
        |  gate: filings.xbrl.org ESEF eligibility (Phase 5.1's existing logic)
        v
ADMITTED / NOT_INGESTIBLE
```
The middle filter (picking the *primary* regulated-market venue among a large real-world set of
`(ISIN, MIC)` records) is a genuine, unsolved design question — not attempted this pass beyond
confirming, for the four known pilots, that the already-established MIC does appear somewhere
in that set (it does, for all four).

## 13. What does "European" mean here?

**OFFICIAL DOCUMENTATION**: FIRDS' scope is explicitly MiFID II/MiFIR — instruments admitted to
trading on an EU/EEA Regulated Market, MTF, OTF, or traded via a Systematic Internaliser (per
ESMA65-8-5014 §2.5.a, quoted in §2 above). This is **not** the same as "EU27" or "ESEF
coverage" — it is defined by *where the instrument trades*, regardless of the issuer's home
country (confirmed structurally: FCC's ISIN, an obviously Spanish company, is reported on
German, French, Dutch, and other venues too, per its 35-venue list). **The overlap this project
actually cares about is FIRDS-venue-coverage ∩ filings.xbrl.org-coverage** — not investigated
exhaustively this pass beyond the four pilots (all of which clear both). Germany/Ireland remain
`filings.xbrl.org`-absent (ADR-0009 §4, unchanged) regardless of FIRDS' own broader venue
coverage — an instrument trading on a German venue is still FIRDS-visible even though its issuer
(if German) would fail the ESEF gate; FIRDS' venue coverage is not the same question as ESEF
issuer coverage.

## 14. Active vs. historical instruments

**OFFICIAL DOCUMENTATION, not independently re-tested this pass**: FIRDS' delta-file model
(§15) explicitly tracks `<NewRcrd>`/`<ModfdRcrd>`/`<TermntdRcrd>`/`<CancRcrd>` events per
`(ISIN, MIC)`, and each `TradgVnRltdAttrbts` block carries `FrstTradDt` (confirmed present in
every real record examined) and, per the documentation, a termination date field when
applicable (not observed in the small sample manually inspected, since none of those particular
records were terminated). The **Full file** (weekly) already excludes non-active instruments per
its own stated scope ("all instruments that are still active" — ESMA65-8-5014 §2.5.a) — so a
Generation-1 implementation reading only Full files, refreshed weekly, would not need to
separately re-derive an active/inactive filter; that filtering is already applied upstream by
ESMA itself.

## 15. Full + delta model — feasible, not implemented

**OFFICIAL DOCUMENTATION**: ESMA's own instructions (§4.2 of ESMA65-8-5014) give a complete,
explicit algorithm for building and maintaining a historical local database from FULINS (day T)
+ DLTINS (day T+1, T+2, ...) — `ValidFromDate`/`ValidToDate`/`LatestRecordFlag` per record,
handling additions, modifications, terminations, and cancellations. This is a real, documented,
implementable mechanism — **not implemented this pass**, but confirmed feasible in principle
from ESMA's own specification, not inferred.

## 16. Freshness test — decisive, and passed

**This is the single most important test in this document, directly modeled on the STOXX
failure test in Phase 5.2b.** **VERIFIED FROM REAL DATA**: the real equity file downloaded this
session (published 2026-08-15, one day before this research) contains records with
`TechAttrbts/PblctnPrd/FrDt` values including **`2026-07-28`** — a genuinely recent date, less
than three weeks old relative to the file's own publication, found by direct extraction from
the real XML, not assumed. This directly contradicts nothing — it is the *expected* result for
a source that is what it claims to be, in sharp contrast to STOXX's `CurrentComponents` PDF,
whose `ModDate` was three years stale and whose actual constituent data failed a real 2026
composition-change test outright (Phase 5.2b §27). **FIRDS passes the freshness test STOXX
failed.**

## 17. Corporate actions / identity changes

**OFFICIAL DOCUMENTATION, not solved, per explicit scope**: FIRDS' delta model (§15) is
*structurally* well-suited to detecting exactly the changes Phase 5.2's original research (§16
there) flagged as needing detection — a `<TermntdRcrd>` for an (ISIN, MIC) pair that stops
trading, a `<NewRcrd>` for one that starts. Whether an issuer's LEI itself changing, or a share-
class ISIN change, would surface cleanly through this same delta mechanism was not specifically
tested this pass — a real open question, not assumed solved just because the general mechanism
looks promising.

## 18. Collision handling — a stronger answer than Phase 5.2's own

Phase 5.2 already found two real ticker collisions (`FCC` in Vietnam, `FCT` across 5+ countries)
and showed `ticker + MIC` resolves them via OpenFIGI. **FIRDS makes this stronger, not just
consistent**: it never offers a bare ticker as a candidate key *at all* — every real record is
already `(ISIN, MIC, LEI)`-anchored, so the ticker-collision risk simply never enters the
identity-resolution path when FIRDS is the starting point. Ticker only reappears later, as a
non-identity enrichment field for display/market-data purposes (§10).

## 19. GLEIF cross-check

Already reported fully in §6–8 (the LEI comparison IS the GLEIF cross-check requested, since
Phase 5.2's original LEI values were themselves GLEIF-verified) — **all four match exactly**,
with zero discrepancies found or needing investigation.

## 20. `filings.xbrl.org` cross-check

Not re-run this pass (already exhaustively verified in Phase 5.1/5.2 for these exact four LEIs)
— re-stating the already-established principle rather than re-testing it: a FIRDS-confirmed
valid equity issuer with no `filings.xbrl.org` entity/filing is `IDENTITY_RESOLVED` but
`NOT_INGESTIBLE`, never conflated with an invalid identity (Phase 5.2 §12/§19).

## 21. Access / automation classification

**ACTIVE**, per this project's own `SourceAccessStatus` vocabulary — free, no authentication,
official ESMA-operated domains (`registers.esma.europa.eu`, `firds.esma.europa.eu`), a
documented M2M query interface, live-verified this session with real, current data. No robots.txt
check or terms-of-use legal review was performed this pass (consistent with this project's
existing practice of reporting technical accessibility and explicitly not rendering a legal
opinion — see Phase 5.2b §16 for the same caveat applied to STOXX). ESMA's own stated audience
for this data (§1.1 of ESMA65-8-5014) is "EU market participants" and National Competent
Authorities for MiFIR reporting purposes — this project's use case (identity/reference-data
lookup, not regulatory reporting) is a different purpose than the document's own stated primary
audience, worth noting explicitly rather than assuming a "publicly documented and technically
open" API is unconditionally fine for any use.

## 22. Scale

**VERIFIED FROM REAL DATA**: one CFI class (Equity) for one day's full file = 682,398 raw
`RefData` records, ~11.5MB compressed / ~511MB decompressed XML. Other CFI classes exist
(`C, D, F, H, I, J, O, R`, seen in the real file listing, §2) — `R` alone split into 18 files
that day, suggesting a substantially larger class than Equity; not downloaded or sized this
pass. **INFERENCE**: a Generation-1 implementation would only need the `E` (Equity) class, not
the full multi-class file set, substantially reducing real scope from "all FIRDS data" to "one
CFI class, ~500MB/week compressed-then-expanded, plus small daily deltas." Processing 682,398
XML records is well within normal Spark/Databricks batch-processing capability — no specific
Databricks cost/runtime benchmark was run this pass (out of scope for a research-only pass), but
nothing in the observed scale suggests infeasibility.

## 23. Comparison: STOXX Europe 600 vs. ESMA FIRDS

| Criterion | STOXX Europe 600 (`CurrentComponents` PDF) | ESMA FIRDS |
|---|---|---|
| Free | Yes | Yes |
| Current | **No — confirmed stale** (Phase 5.2b §27: 4/23 real 2026 additions present, 17/21 real 2026 deletions still listed) | **Yes — confirmed current** (§16: real 2026-07-28 record found in a file published 2026-08-15) |
| Automatable | Yes (plain GET) | Yes (documented M2M API, no auth) |
| European coverage | 600 large/mid/small-cap companies, curated by index methodology | Every EU/EEA-traded equity instrument, regulator-mandated reporting |
| ISIN | No | **Yes** (primary key) |
| LEI | No | **Yes** (primary issuer field) |
| MIC | No | **Yes** (primary venue field, per-record) |
| Ticker | No (company name only) | **No** (confirmed absent — needs OpenFIGI enrichment, same as STOXX would have) |
| Equity classification | Implicit (index is equity-only by construction) | **Explicit, deterministic** (CFI code, `"ES"` prefix) |
| Update mechanism | Unclear/unreliable (this session's evidence) | **Documented, verified**: weekly full + daily delta |
| Authority | The index provider (STOXX) — authoritative for index membership, not for issuer/instrument identity | **ESMA** — the actual EU financial regulator, authoritative for instrument/issuer regulatory identity |
| Licensing | Free PDF path found, but licensing terms not reviewed; a *different*, paid path also exists for the same provider | Public regulatory data, explicit official access instructions published |
| Suitable for universe | **No** (Phase 5.2b verdict, reaffirmed here) | **Yes, with the filtering-layer conditions in §12** |

## 24. Comparison with the ETF-holdings approach

FIRDS makes the iShares/EXSA route (already confirmed blocked in Phase 5.2b — a JS-rendered SPA
with no working legacy endpoint) **unnecessary for Generation 1**, not merely redundant: FIRDS
independently supplies everything the ETF route was hoped to provide (ISIN, venue/MIC,
implicitly a security-level equity classification) *and* the one thing neither STOXX nor the ETF
route could supply — LEI, directly, per record — without needing any of the ETF's JavaScript-
rendered holdings data at all.

## 25. Proposed Generation-1 architecture

```
ESMA FIRDS (weekly full file, CFI class "E")
        │  filter: ClssfctnTp starts "ES" (common/ordinary equity)
        ▼
Eligible equity (ISIN, MIC, LEI) records
        │  filter: primary/regulated-market venue selection (OPEN QUESTION, §12)
        ▼
Candidate listing = (ISIN, MIC, LEI)
        │
        ▼
GLEIF (confirm/cross-check LEI; already-proven from Phase 5.2)
        │
        ▼
OpenFIGI (ISIN -> ticker, for enrichment/display; already-proven from Phase 5.2b)
        │
        ▼
filings.xbrl.org (ESEF entity + filing check — Phase 5.1's existing logic)
        │
        ▼
ADMITTED / NOT_INGESTIBLE
        │
        ▼
EUCurrentSource -> financials
```
**RECOMMENDATION**: this is a sound Generation-1 architectural direction, evidence-backed at
every step examined this session, with one genuine open design question (the primary-venue
selection filter, §12) standing between "the identity chain works" (already proven, Phase 5.2/
5.2b/this document) and "we have a deterministic, non-over-admitting candidate list."

## 26. Avoiding over-admission

Directly addressed in §12's three-layer model. **RECOMMENDATION, not implemented**: the
`ClssfctnTp` CFI filter is necessary but insufficient alone (§12's middle layer — primary-venue
selection — is still needed, since one real equity ISIN legitimately has dozens of venue
records). Whatever final admission policy is chosen, it must live as an explicit, documented
project-level policy layer *separate from* FIRDS' own raw reference universe — mirroring exactly
the "reference universe vs. project admission universe" distinction the user's own framing
already anticipated correctly before this research began.

## 27. Recommended conceptual data model

**RECOMMENDATION, evaluating the four objects proposed in the brief**:
- **Instrument** (ISIN, CFI, status) — supported directly by FIRDS, confirmed real.
- **Listing** (MIC, ISIN, listing_id) — supported directly; FIRDS' own `(ISIN, MIC)` pairing
  *is* this concept already, needing only the `listing_id = MIC:ISIN`-or-`MIC:TICKER` naming
  decision to be made explicitly (ADR-0010 currently defines `listing_id` as `MIC:TICKER`; using
  FIRDS as the primary source raises a real question of whether `listing_id` should key on ISIN
  instead of ticker at the raw-identity layer, with ticker attached as enrichment — **not
  resolved here**, flagged for ADR-0010/0011 reconsideration if FIRDS is adopted).
- **Issuer** (LEI, issuer_id) — supported directly and exactly matches the existing
  `issuer_id = source_id:source_entity_id` model already in place.
- **Market Identifier** (ticker, MIC, ISIN) as a cross-reference/enrichment object, not a primary
  identity object — consistent with everything found this session.

None of the four proposed objects were found unnecessary; no additional object was found
missing for the identity-resolution purpose specifically (universe-filtering-policy, §12/§26,
is a process/policy question, not obviously a new data object).

---

## 28. Required final verdict

**`READY WITH CONDITIONS`**, for "ESMA FIRDS as Generation-1 European universe/reference
source." Exact conditions:

1. **Design and implement the primary-venue selection filter** (§12) — FIRDS alone does not
   collapse a real equity's 16–45 venue records down to one canonical listing; this project's
   own admission policy must do that, not FIRDS.
2. **Build the ticker-enrichment step** (OpenFIGI `ID_ISIN` reverse lookup, already proven in
   Phase 5.2b, just not yet wired to FIRDS-sourced ISINs specifically).
3. **Confirm the CFI filter (`"ES"` prefix) against FIRDS' own official CFI reference file**
   (§4) rather than relying solely on this session's ISO-10962-based spot check.
4. **Decide the `listing_id` keying question** (ISIN-anchored vs. ticker-anchored, §27) before
   any schema work, since this may warrant an ADR-0010 amendment, not just an implementation
   detail.
5. **Only the Equity (`E`) CFI class is proposed for Generation 1** — other classes (bonds,
   funds, derivatives) are out of scope and were not investigated.
6. No legal/terms-of-use review of ESMA's data-reuse conditions was performed — recommended
   before any production automation, consistent with this project's existing practice for every
   other source.

## 29. Required final answers

1. **Can FIRDS be downloaded for free?** Yes — verified live, no paywall, no login.
2. **Can it be automated?** Yes — a documented, working M2M query API, verified live.
3. **Is the data current?** Yes — verified live, a real 2026-07-28 record found in a file
   published 2026-08-15 (one day before this research).
4. **Does it contain ISIN?** Yes — the primary instrument identifier field (`Id`).
5. **Does it contain issuer LEI?** Yes — the `Issr` field, confirmed for all four pilots.
6. **Does it contain MIC?** Yes — `TradgVnRltdAttrbts/Id`, confirmed for all four pilots
   including `MTAA` specifically.
7. **Does it contain ticker?** No — confirmed absent from the real schema.
8. **Can equity be identified deterministically?** Yes — `ClssfctnTp` starting `"ES"`, verified
   against ISO 10962 and all four real pilot records.
9. **Can active/current instruments be identified?** Yes, structurally — the Full file is
   already scoped to active instruments per ESMA's own documentation; not independently
   re-verified beyond that documented scope claim.
10. **Can the four pilots be independently resolved?** Yes — done, live, this session, by ISIN
    alone, with LEI and MIC read back and matched exactly, not seeded.
11. **Can the dataset be maintained via full + delta?** Yes, per ESMA's own documented algorithm
    — feasible, not implemented.
12. **Is the volume practical for Databricks?** Likely yes (682,398 records for one CFI class,
    one day) — not benchmarked against real Databricks compute this pass.
13. **Can it replace STOXX as the universe/reference source?** Yes, and it solves problems STOXX
    could not (freshness, LEI, MIC, deterministic equity classification) — recommended over
    STOXX for Generation 1, conditional on §28's items.
14. **What additional source, if any, is needed for ticker?** OpenFIGI (`ID_ISIN` reverse
    lookup), already proven working in Phase 5.2b.
15. **What exact filtering/admission rules would be required?** `ClssfctnTp` starts `"ES"`
    (equity) + a not-yet-designed primary-venue selection rule + the existing `filings.xbrl.org`
    ESEF eligibility gate (Phase 5.1) — three distinct, separately-owned filtering layers, not
    one combined rule.

## 30. Source discipline — summary

Every load-bearing claim in this document is tagged **VERIFIED FROM OFFICIAL ESMA DATA** or
**VERIFIED FROM REAL DATA** at its point of use, backed by a real HTTP request/response or a
real parsed XML record captured this session — not a third-party summary, not an assumption
carried over from general knowledge. The CFI-code semantics (§4) is the one claim resting partly
on external documentation (ISO 10962) rather than a live ESMA query, and is flagged as such,
with the official FIRDS CFI reference file identified as the way to close that gap fully in a
future pass.

## 31. Explicit non-goals (this pass)

No implementation of any part of this architecture. No modification to `config.tickers`, any
Delta schema, the DAG, `EUCurrentSource`, `fundamentals_screener`, or Streamlit. No European
tickers added anywhere in production. No full/delta local database was actually built (§15) —
only confirmed feasible from ESMA's own specification. No CFI classes other than Equity were
investigated. No legal review of ESMA's terms of use was performed.

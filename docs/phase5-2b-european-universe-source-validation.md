# Phase 5.2b — European universe source validation: STOXX Europe 600 + iShares EXSA

**Status: research only. Nothing in this document has been implemented.** Companion to
[docs/phase5-2-european-universe-admission-research.md](phase5-2-european-universe-admission-research.md)
and [ADR-0011](adr/0011-european-universe-admission.md) (status stays **Proposed**).

**Labeling convention used throughout, per explicit instruction**: every claim below is tagged
**VERIFIED FACT** (directly observed, reproducible), **LIVE TEST RESULT** (a specific query run
this session with its real output), **RESEARCH FINDING** (established from documentation/public
sources, not independently re-derived), **INFERENCE** (a reasoned conclusion from the above,
not itself directly observed), **RECOMMENDATION**, or **OPEN QUESTION**. Nothing is presented as
verified that wasn't actually checked.

## Executive summary

**UPDATED after a dedicated freshness investigation (§27): the answer to the hypothesis
changes from the original "YES, WITH CONDITIONS" to `C — NOT SOLVED` for the universe-source
question specifically.** The condition flagged in the first pass — resolve whether the free
STOXX PDF is actually current — was tested directly against real, dated 2026 index-review
events and **failed**: the PDF is confirmed stale, not merely suspected stale. See §27 for the
full, decisive test. The rest of this document (§1–§26) is preserved as originally written,
since it remains accurate as a record of what was tried and found — only the final verdict
changes, and it changes because the evidence now supports a stronger conclusion, not because
anything below turned out to be wrong.

A genuinely free, no-login, official STOXX source exists — **not** the paywalled
"selection-lists" portal previously identified, but a separate **`CurrentComponents` PDF**
(`stoxx.com/document/Bookmarks/CurrentComponents/{SYMBOL}.pdf`). **VERIFIED FACT**: fetched
live, HTTP 200, real PDF, and STOXX Europe 600's (`SXXGR`) version has **exactly 600 rows** —
company name, supersector, country, weight — confirmed by direct parsing, not a page-limited
sample. **VERIFIED FACT**: this PDF's own embedded metadata shows `ModDate: 2023-07-12` — over
three years old at the time of this research, despite "Current" in its name and URL. **§27
resolves this from "risk" to "confirmed disqualifying defect"**: the PDF's actual constituent
data does not reflect either of the two real, dated 2026 STOXX Europe 600 quarterly reviews
found and cross-checked this session.

The iShares ETF-holdings route — this project's own established precedent for exactly this kind
of problem (Russell 3000, TSX Composite) — was tested directly and **found blocked**:
**VERIFIED FACT**, multiple direct HTTP attempts against real iShares UK product pages (EUE/
CSX5E, EXSA) using the publicly-documented legacy `.ajax?fileType=csv|json` pattern all failed
(either an unmodified SPA-shell HTML response regardless of query parameters, or a genuine
`HTTP 500`) — the current site is Astro-built (confirmed via `_astro` asset references in the
raw HTML) and does not expose a working static holdings endpoint via this route.

**The most significant new finding this pass**: **LIVE TEST RESULT** — the STOXX PDF's company
name + country, combined with OpenFIGI's `/v3/search` endpoint's `micCode` filter (a
capability not previously tested), resolves cleanly to a single, correctly-typed instrument —
proven end-to-end for **two genuinely new candidates never touched earlier in this session**
(Iberdrola, Saint Gobain — neither is one of the four Phase 5.1 pilots), including a full
`STOXX name → OpenFIGI ticker+MIC → ISIN → GLEIF LEI → filings.xbrl.org entity (4 real, clean
filings)` chain completed for Saint Gobain. This is real evidence the architecture generalizes
beyond the four hardcoded pilots, not just a repeat confirmation of what was already known.

---

## 1. Official STOXX source

**RESEARCH FINDING** (established in Phase 5.2, re-confirmed here): `stoxx.com/selection-lists`
requires a "valid Third-Party Data License" for anything beyond files prefixed `slpublic` — this
remains true and is not contradicted by anything below.

**LIVE TEST RESULT — a separate, free resource**:
```
GET https://www.stoxx.com/download/indices/components/SXXGR.pdf
  -> 302 redirect -> https://www.stoxx.com/document/Bookmarks/CurrentComponents/SXXGR.pdf
  -> HTTP 200, content-type: application/pdf, 81,044 bytes, no login/auth required
```
Retrieval date: 2026-08-16. Same pattern confirmed for `SX5E.pdf` (EURO STOXX 50, HTTP 200) and
`SX5GR.pdf` (STOXX Europe 50 — a *different*, pan-European-not-Eurozone-only index; **VERIFIED
FACT**: `SX5EGR` and `SX5EE` both returned `404` — the correct EURO STOXX 50 symbol is `SX5E`,
not a `SX5E`-prefixed variant, confirmed by trial).

**Content (SXXGR.pdf, VERIFIED FACT via direct `pdfplumber` parsing)**:
- 9 pages, header "STOXX® EUROPE 600 INDEX / Components¹"
- Columns: `Company`, `Supersector`, `Country`, `Weight (%)` — **no ticker, no ISIN, no
  exchange/MIC column**
- **Exactly 600 data rows** (counted programmatically, not estimated)
- Footnote: "¹Based on the last periodic review implementation" — a methodology note, not a
  licensing statement
- **No visible copyright/restrictive-use footer text on any page examined**

**VERIFIED FACT — the freshness problem**: PDF metadata (`pdfplumber`'s `.metadata` property):
`{'Creator': 'JasperReports (Template1_OnlyComponentsList)', 'Producer': 'iText 2.1.7 by 1T3XT',
'ModDate': 'D:20230712112654Z', 'CreationDate': 'D:20230712112654Z'}`. The document was generated
2023-07-12 and, per the identical `ModDate`, has not been regenerated since — despite being
served at a URL path literally named "CurrentComponents." **INFERENCE**: either (a) this
specific cached PDF genuinely hasn't been refreshed in over three years while the URL keeps
serving it, or (b) STOXX periodically regenerates it but this particular fetch hit a stale CDN/
cache layer. Which of the two is true was **not** determined this pass — a second fetch on a
different day, or checking whether other STOXX-index PDFs at the same path pattern show a more
recent `ModDate`, would resolve this. **Not resolved — flagged as a real open question (§15).**

**Update/review frequency**: **RESEARCH FINDING**, not independently verified this pass — STOXX's
publicly known methodology reviews index composition quarterly. Whether the free
`CurrentComponents` PDF tracks that cadence is exactly the open freshness question above.

## 2. iShares ETF source

**Candidates identified** (RESEARCH FINDING, from product pages found live): iShares Core EURO
STOXX 50 UCITS ETF (ticker `EUE`/`CSX5E` depending on share class, product ID `251781`), iShares
STOXX Europe 600 UCITS ETF (DE) (ticker `EXSA`, ISIN `DE0002635307`, product ID `251931`).

**LIVE TEST RESULT — the legacy scraping pattern, tested directly, multiple variations**:
```
GET https://www.ishares.com/uk/individual/en/products/251781/.../{magic}.ajax?fileType=csv&fileName=EUE_holdings&dataType=fund
GET https://www.ishares.com/uk/individual/en/products/251931/.../{magic}.ajax?fileType=csv&fileName=EXSA_holdings&dataType=fund
GET .../{magic}.ajax?tab=all&fileType=json&asOfDate=20260814
```
Tested with 4 different "magic number" path segments documented in public tooling
(`1467271812596`, `1521942788811`, `1506575576011`, `1443182490577`). **Every single attempt
against product 251781 (EUE)** returned HTTP 200 with the exact same 3,330,897-byte response —
the normal SPA-shell HTML page, byte-identical regardless of query parameters, meaning the site
is not routing these query strings to any data endpoint at all. **Every attempt against product
251931 (EXSA)** returned a genuine `HTTP 500` "Error" page. Searched the raw HTML of the 251781
product page for any embedded `.ajax`/CSV/holdings-download link via regex — **zero matches**.
**VERIFIED FACT, not an assumption**: the current `ishares.com/uk/...` site is built with Astro
(confirmed via `_astro/BasicText-...css` asset paths in the served HTML) — a modern static-site/
SPA framework. **INFERENCE**: the actual holdings data is very likely fetched by client-side
JavaScript after page load, via an endpoint not discoverable through a plain HTTP GET of the
initial page — reaching it would require either browser automation (out of scope for this
research pass) or independently reverse-engineering the current API, which was not accomplished
this session.

**A direct product-page fetch attempt earlier in this research also returned `HTTP 403`** for a
different EXSA-adjacent URL — inconsistent behavior across attempts (200/500/403 depending on
exact path) further supports that this is not a stable, documented public API surface.

## 3. Actually downloading the holdings

**Not accomplished.** Per §2, no working CSV/JSON holdings endpoint was found for either iShares
candidate. This is reported as a genuine negative result, not glossed over — no holdings file
was parsed, so no row/field/duplicate analysis exists for the iShares route in this pass.

## 4. Count / coverage

**N/A for the ETF route** (§3). **For the STOXX PDF route**: 600 rows confirmed (§1) — but with
no ticker/ISIN/exchange field, "coverage" in the sense of matching against a security-level
dataset could not be computed directly against the ETF (since that route failed) or against a
security master (out of scope, not investigated). The closest available coverage check is §5's
per-pilot / per-new-candidate resolution success rate.

## 5. Testing the four existing pilots (by company name, since the PDF has no ticker)

**LIVE TEST RESULT** — searched the parsed STOXX Europe 600 PDF rows for each pilot's company
name:

| Pilot | Found in STOXX 600 PDF? | Row (if found) |
|---|---|---|
| FCC (Fomento de Construcciones y Contratas) | **No** | — |
| Alstom | **Yes** | `ALSTOM  Industrial Goods & Services  France  0.08` |
| New Amsterdam Invest | **No** | — |
| Fincantieri | **No** | — |

## 6. New Amsterdam Invest — explicitly not a failure

Per the brief's own instruction: New Amsterdam Invest's absence from STOXX Europe 600 is
**expected, not a defect**. NAI is a small-cap Dutch real-estate investment company — nowhere
near STOXX 600 inclusion thresholds. Its Phase 5.1 admission was a deliberate pilot choice to
exercise the amendment-handling logic (its real `-NL-0`/`-NL-1` filing pair), not a claim that
it belongs to any broad index. **The universe-source question and the "is this a valid pilot"
question are unrelated** — NAI remains a fully valid, already-admitted `EU_CURRENT` issuer
regardless of which universe source(s) a future admission layer uses.

## 7. Security-type filtering

**RESEARCH FINDING / INFERENCE, not directly tested against real ETF holdings** (§3 blocked
this). The STOXX PDF itself contains only equity index constituents by construction (it's an
equity index components list) — no bonds/cash/derivatives rows were observed in the 600 parsed
rows. Had the ETF route worked, OpenFIGI's `securityType`/`marketSector` fields (already proven
live, §12 of the main research doc) would have been the natural filter, exactly as recommended
there — this pass didn't need to re-derive that recommendation, only confirm the ETF data to
apply it to wasn't reachable.

## 8. Ticker quality

**N/A** — the STOXX PDF provides no ticker field at all (§1), and the ETF route was blocked
(§2/§3), so no ticker-format question could be tested this pass.

## 9. Exchange → MIC

**LIVE TEST RESULT, the key positive finding of this pass**: since the STOXX PDF provides
`Country` (not exchange or MIC), the working bridge tested was **`Country` (from STOXX) → a
small, curated country→primary-MIC table (already implicit in ADR-0010: Spain→XMAD,
France→XPAR, Netherlands→XAMS, Italy→MTAA) → `micCode` filter on OpenFIGI's name-search**.
Tested against two genuinely new candidates (neither a Phase 5.1 pilot):

- **Iberdrola** (STOXX PDF: `Country=Spain`) — `POST /v3/search {"query":"IBERDROLA",
  "securityType":"Common Stock","marketSecDes":"Equity","micCode":"XMAD"}` → **exactly one
  result**: `ticker=IBE, exchCode=SQ, FIGI=BBG000BC4FP5, COMMON STOCK`.
- **Saint Gobain** (STOXX PDF: `Country=France`) — same query shape with `micCode=XPAR` →
  **exactly one result**: `ticker=SGO, exchCode=FP, FIGI=BBG000BCCNZ8, name=COMPAGNIE DE SAINT
  GOBAIN`.

**Contrast — a plain name search WITHOUT the `micCode` filter is genuinely ambiguous**: the same
`IBERDROLA` query without `micCode` returned 15+ results across many currency/venue variants
(`IBECHF`, `IBEUSD`, `IBERUB`, `IBE1D`, `1IBEM`, ...) with no obviously-correct single answer.
**This is the negative test case requested in §26**: `ticker/name alone → ambiguous`, but
`name + MIC (derived from the STOXX PDF's own Country column) → exactly one correct instrument`.

## 10. ISIN

**LIVE TEST RESULT**: OpenFIGI's `/v3/search` and `/v3/mapping` responses do not include ISIN
directly (confirmed again this pass, consistent with the main research doc's earlier finding).
For Saint Gobain, the real ISIN (`FR0000125007`, obtained from public knowledge, not derived
from an OpenFIGI field) was independently **confirmed** via OpenFIGI's reverse `ID_ISIN` lookup
— it resolved to the identical FIGI (`BBG000BCCNZ8`) and ticker (`SGO`) already found via the
name+MIC search, cross-validating that this specific ISIN is indeed the correct equity. This
confirms the isolation *method* (§6 of the main doc) generalizes, but does not by itself supply
a way to discover an unknown company's ISIN from nothing but its name+MIC — the working method
remains "GLEIF's `LEI → ISIN` set, cross-checked via OpenFIGI's `ID_ISIN` reverse lookup for
type/venue," applied after the LEI is already known, not before.

## 11. GLEIF cross-check (new candidates, not the already-known pilots)

**LIVE TEST RESULT**:
```
GET https://api.gleif.org/api/v1/lei-records?filter[isin]=FR0000125007
  -> 1 result: LEI=NFONVGN05Z0FMN5PEC35, legalName="COMPAGNIE DE SAINT-GOBAIN"
```
This LEI was **not** seeded from any prior knowledge in this session — it was independently
derived starting from the STOXX PDF's company name alone, through OpenFIGI, to this GLEIF call.
**Acceptance criterion met** for this specific new candidate.

## 12. OpenFIGI cross-check

Already covered in detail above (§9, §10) — `micCode` filtering is confirmed to resolve the
real ticker-collision risk (§26/main doc §8's `FCC`/`FCT` findings) the same way for entirely
new candidates, not just the four pilots. **Terminology, restated per the standing instruction**:
OpenFIGI is used throughout this document as an **industry security-reference / instrument-
resolution source**, never described as "authoritative" — GLEIF alone carries that role for LEI
identity.

## 13. MTAA / Fincantieri

Already fully re-verified in the main research doc's updated §19 (`MTAA` works, `XMIL`/`BMEX`
correctly return no result, for all four pilots including Fincantieri) — not repeated here to
avoid duplicating that finding across two documents; see
[phase5-2-european-universe-admission-research.md §19](phase5-2-european-universe-admission-research.md).

## 14. STOXX vs. ETF membership — which is authoritative for Generation 1

**RECOMMENDATION**: given the ETF route is currently blocked (§2/§3), this question is
provisionally moot — **STOXX's free `CurrentComponents` PDF is the only working candidate found
this pass**, so it would be the de facto universe authority for any Generation-1 implementation,
not by a considered trade-off against the ETF but because the ETF alternative doesn't currently
work. If the ETF route is ever unblocked (e.g. via browser automation or a rediscovered API),
the general principle from the main research doc still applies: an index provider's own official
list is the more authoritative *membership* signal, while an ETF's holdings would mainly add
value as *security-level enrichment* (ticker/ISIN/exchange) the STOXX PDF itself lacks — not
because the STOXX PDF's membership claim is untrustworthy.

## 15. Update / refresh

**OPEN QUESTION, not resolved**: given the `ModDate` freshness concern (§1), any Generation-1
implementation relying on this PDF would need either (a) a periodic freshness check (e.g. alert
if `ModDate` hasn't changed across N consecutive fetches spanning what should be a quarterly
review cycle), or (b) an independent cross-check against a second source before trusting it as
"current." Neither was designed or implemented this pass — flagged, not solved.

## 16. Accessibility

| Source | Free? | Login? | Automatable (technically)? |
|---|---|---|---|
| STOXX `selection-lists` portal | No | Yes (required) | No — paywalled |
| STOXX `CurrentComponents` PDF | **Yes** (confirmed) | **No** | **Yes**, technically (plain HTTP GET, no auth) — but see the explicit caveat below |
| iShares EUE/CSX5E holdings | Unknown (page exists) | Unknown | **No** — legacy pattern confirmed broken this pass |
| iShares EXSA holdings | Unknown (page exists) | Unknown | **No** — same |

**Explicit distinction requested by the brief**: "downloadable in a browser" is **not**
confirmed equivalent to "safe/permitted to automate" for the STOXX PDF. This document reports
the **technical accessibility fact** (no login wall, plain GET succeeds) — it does **not** make
a legal determination that automated, repeated, unattended fetching of this PDF is within
STOXX's terms of use. STOXX's website terms of use were not read or reviewed in this pass. This
is an explicit gap, not an implied "yes it's fine."

## 17. Reproducibility (provenance to persist, if this is ever implemented)

**RECOMMENDATION only, nothing implemented**: minimum fields a future admission layer should
record per universe-refresh run: source URL (including the exact index symbol, e.g. `SXXGR`),
retrieval timestamp, the source document's own `ModDate` (from PDF metadata, exactly as this
pass extracted it — directly actionable for the freshness check in §15), a content hash of the
downloaded file, row count parsed, and the processing run's own timestamp. This mirrors the
provenance fields `main.config.pipeline_runs` already tracks for other pipeline steps — not a
new pattern, just applying the existing one.

## 18. Corporate actions / index changes

**Not solved, per explicit scope.** Structurally, since the PDF is re-fetched (not diffed
incrementally) each time it's used, an index addition/removal would naturally surface as a
row appearing/disappearing on the next fetch — assuming the freshness problem (§15) is actually
resolved so that "next fetch" reflects reality. This is the same "full recompute, not
incremental" pattern already used for the US/CA universe (`02__tickers_master.py`).

## 19. Country coverage

STOXX Europe 600's real, parsed `Country` column values (sample from the 600 rows, VERIFIED
FACT) include at least: Switzerland, Netherlands, Denmark, France, Great Britain, Germany,
Spain, Italy, Sweden, Norway, Finland, Luxembourg, Ireland. Cross-referencing against
`filings.xbrl.org`'s confirmed coverage (ADR-0009 §4, not re-tested here): Great Britain,
Switzerland, Denmark, Sweden, Norway, Finland are **not** ESEF/EU-regulated-market jurisdictions
in the same way as Eurozone/EEA members and would not be expected to have `filings.xbrl.org`
entities regardless of universe-source coverage — Germany and Ireland specifically ARE
EU-regulated but are the confirmed `filings.xbrl.org` aggregator gaps already documented. This
is restated, not newly discovered, but confirms the STOXX 600 list's country mix is broader than
`filings.xbrl.org`'s own coverage regardless of which universe source is used — an inherent
ceiling on what fraction of STOXX 600 could ever be `EU_CURRENT`-ingestible.

## 20. Free / commercial status — final classification

| Source | Classification | Basis |
|---|---|---|
| STOXX `selection-lists` (full/licensed files) | **AUTOMATION_RESTRICTED** | Explicit paid-license wall, page text |
| STOXX `CurrentComponents` PDF | **RESEARCH_ONLY** (not `ACTIVE`) | Technically free/automatable, but legal terms not reviewed and freshness unconfirmed — not yet promotable to a relied-upon source without both being resolved |
| iShares EUE/CSX5E holdings | **UNAVAILABLE** (this pass) | No working access path found |
| iShares EXSA holdings | **UNAVAILABLE** (this pass) | Same |
| GLEIF | **ACTIVE** (already established, main doc) | Free, verified, no auth |
| OpenFIGI | **ACTIVE** (already established, main doc) | Free, verified, no auth, real rate-limit headers observed |
| `filings.xbrl.org` | **ACTIVE** (already established, Phase 5.1) | Free, verified |

## 21. Generation-1 recommendation

**SUPERSEDED by §27 below — the answer changes to `C — NOT SOLVED`** once condition 1 (below)
was actually tested against real 2026 data rather than left as an open question. This section
is kept as originally written for the review trail; §27 is the current, decisive answer.

**Original (first-pass) answer: YES, WITH CONDITIONS.**

The hypothesis (STOXX Europe 600 + iShares EXSA as the Generation-1 universe mechanism) is
**partially confirmed and partially disproven**: the STOXX half works (a genuinely free, current-
enough-to-be-useful, complete 600-row list was retrieved and parsed live); the iShares half does
not (blocked, confirmed negative, not merely unexplored). The **conditions**:

1. Resolve the STOXX PDF freshness question (§15) before relying on it — either confirm it does
   refresh on a real cadence, or find/build a cross-check.
2. Accept that the STOXX PDF alone supplies company name + country + sector + weight, **not**
   ticker/ISIN/MIC — the `Country → curated MIC table → OpenFIGI name+micCode search` bridge
   (§9) is required and was proven to work for 2 new real candidates, but is a real additional
   implementation step, not a trivial one.
3. Either abandon the iShares-ETF-holdings idea for Generation 1, or invest in properly
   reverse-engineering the current API (likely requiring browser automation/network-tab
   inspection, out of scope for this research pass) if enrichment beyond what STOXX+OpenFIGI
   already provides is later found necessary.
4. Legal/terms-of-use review of the STOXX PDF's actual usage rights was not performed and should
   happen before any production automation, not assumed permissible from technical
   accessibility alone.

## 22. Proposed architecture — evaluated

The brief's proposed diagram (STOXX → candidate → iShares EXSA holdings → GLEIF → OpenFIGI →
filings.xbrl.org → ADMITTED) is **too optimistic about the iShares step specifically** — that
box does not currently work (§2/§3). The corrected, live-tested Generation-1 shape is:

```
STOXX CurrentComponents PDF (company name, country, sector, weight)
        │
        ▼
country → curated MIC table  (small, static, one entry per supported market)
        │
        ▼
OpenFIGI /v3/search (name + micCode filter) -> exactly one Common Stock instrument (ticker, FIGI)
        │
        ▼
OpenFIGI /v3/mapping (ID_ISIN reverse lookup, or GLEIF's own LEI->ISIN set once LEI is found)
        │
        ▼
GLEIF ISIN -> LEI
        │
        ▼
filings.xbrl.org entity + filing check
        │
        ▼
ADMITTED / NOT_INGESTIBLE / REJECTED
```
Not "too complex" — every step above was individually live-tested this session (this document
and the main research doc together). The one missing/unverified piece is the freshness
guarantee on the leftmost box, not a missing or wrong step in the chain itself.

---

## 23. Final source matrix (as requested)

| Function | Source | Free? | Automatable? | Identifier(s) | Authority | Result |
|---|---|---|---|---|---|---|
| Universe | STOXX `CurrentComponents` PDF | Yes (technically confirmed) | Yes (plain HTTP GET) | Company name, country, sector, weight | STOXX (the index provider itself) | **RESEARCH_ONLY** — freshness unconfirmed, terms not reviewed |
| Universe (alternative) | STOXX `selection-lists` portal | No | No | ISIN, SEDOL, RIC, name | STOXX | **AUTOMATION_RESTRICTED** |
| Universe (alternative) | iShares EUE/EXSA ETF holdings | Unknown | **No** (confirmed blocked) | Presumably ticker/ISIN/weight | Indirect (fund issuer) | **UNAVAILABLE** |
| Security enrichment | OpenFIGI | Yes | Yes | Ticker, FIGI, exchCode, MIC (via filter), securityType | Bloomberg (industry reference, not legal authority) | **ACTIVE** |
| LEI | GLEIF | Yes | Yes | LEI, ISIN | GLEIF (the official LEI federation) | **ACTIVE** |
| Instrument validation | OpenFIGI | Yes | Yes | (same as above) | (same as above) | **ACTIVE** |
| Fundamentals | `filings.xbrl.org` | Yes | Yes | LEI-keyed entities, filings | XBRL International (aggregator) | **ACTIVE** |

## 24. Final pilot table (as requested — plus 2 new candidates for a genuine generalization test)

| Company | STOXX 600? | ETF? | Ticker | Exchange (Bloomberg) | MIC | ISIN | GLEIF LEI | ESEF | Result |
|---|---|---|---|---|---|---|---|---|---|
| FCC | No | N/A (blocked) | FCC | SQ | XMAD | ES0122060314 | 95980020140005178328 | Yes (5 filings) | ADMITTED (Phase 5.1) |
| Alstom | **Yes** (0.08% weight) | N/A | ALO | FP | XPAR | FR0010220475 | 96950032TUYMW11FB530 | Yes (4 filings) | ADMITTED (Phase 5.1) |
| New Amsterdam Invest | No (expected, §6) | N/A | NAI | NA | XAMS | NL0015000CG2 | 724500JXEXUGEATP5L52 | Yes (4 filings) | ADMITTED (Phase 5.1) |
| Fincantieri | No | N/A | FCT | IM | MTAA | IT0005599938 | 8156005BDF49128B6239 | Yes (5 filings) | ADMITTED (Phase 5.1) |
| **Iberdrola** *(new)* | **Yes** | N/A | IBE | SQ | XMAD | not independently derived this pass | not queried this pass | not queried this pass | Identity-resolution steps 1 (STOXX) + 2 (OpenFIGI) proven; not carried further |
| **Saint Gobain** *(new)* | **Yes** | N/A | SGO | FP | XPAR | FR0000125007 | **NFONVGN05Z0FMN5PEC35** | **Yes — 4 real filings, 0 errors** | **Full chain independently proven, end to end, for a candidate never seeded from prior knowledge** |

## 25. Negative test case (as requested)

Documented in full in §9: `IBERDROLA` searched on OpenFIGI **without** a MIC filter returns 15+
ambiguous results across currency/venue variants with no deterministic single answer; the exact
same query **with** `micCode=XMAD` (derived from the STOXX PDF's own `Country=Spain` field)
returns exactly one, correctly-typed result. This is a real, live-executed instance of "ticker/
name alone → ambiguous" vs. "name + MIC → resolved" — not a repeat of the `FCC`/`FCT` global-
ticker-collision cases already documented in the main research doc, but a second, independent
confirmation of the same underlying principle using the search-by-name path specifically.

## 26. What remained open after the first pass (superseded by §27 below)

- ~~STOXX `CurrentComponents` PDF freshness~~ — **resolved in §27: confirmed stale, not merely
  suspected.**
- STOXX's actual terms of use for this specific PDF path — still not read; moot for production
  use now that the data itself is disqualified on freshness grounds regardless of licensing.
- The iShares JS-rendered holdings data — not reached; would need browser automation or further
  reverse-engineering effort genuinely out of scope for a research-only pass.
- Whether the `country → MIC` curated table needs entries beyond the four already-known pilot
  countries to cover STOXX 600's full country list (§19) — only Spain/France were exercised
  against real new candidates this pass. Still open — orthogonal to the universe-source verdict.
- Iberdrola's full chain (ISIN/LEI/ESEF) was not completed to the same depth as Saint Gobain's —
  the two identity-resolution steps that were run (STOXX presence, OpenFIGI name+MIC) succeeded,
  but this document does not claim the full chain was finished for it. Still open, and now lower
  priority given §27's verdict on the universe source itself.

---

## 27. Final freshness investigation (requested follow-up — decisive)

**Objective**: determine whether the free STOXX `CurrentComponents` PDF is actually current, by
testing it against real, dated, independently-verifiable 2026 STOXX Europe 600 index-review
events — not by re-examining PDF metadata alone.

### 27.1 Real, dated composition-change events found

**RESEARCH FINDING**, cross-referenced from STOXX's own press releases: STOXX Europe 600 is
reviewed quarterly. Two real, dated 2026 reviews were found and fetched directly:

- **March 23, 2026** ("first regular quarterly review 2026"): 12 additions
  (CSG A, Pan African Resources, ING Bank Slaski BSK, Aixtron, Technoprobe SPA, Hochschild
  Mining, Tauron, Valiant, Benefit Systems, Pirelli & C. S.p.A., Zabka Group, Amrize), 12
  deletions (Tecan, Alten, Grafton Grp, Eurazeo, Hexpol 'B', Greggs, Amplifon, Softcat, Azelis
  Group, Kinnevik B, and 2 more the source page truncated before listing).
- **June 22, 2026** ("second regular quarterly review 2026"): 11 additions (Soitec, AT&S Austria
  Tech.&Systemtech, Computacenter, SES, Comet Holdings 'R', Inficon, Acerinox, BAM Groep Kon.,
  Bank Millennium, Kety, TGS ASA), 11 deletions (Camurus, Christian Dior, Vidrala, Bavarian
  Nordic, B&M European Value Retail, Big Yellow Group, Wendel, Wallenstam B, INWIT, easyJet,
  Ambu 'B').

### 27.2 The decisive test

**LIVE TEST RESULT**: re-fetched `stoxx.com/document/Bookmarks/CurrentComponents/SXXGR.pdf`
fresh (same `ModDate: 2023-07-12` as the original fetch — the cached document has not changed
between the two fetches, itself a small additional data point), parsed all 9 pages, and searched
the full text for every one of the 23 real 2026 additions and 21 real 2026 deletions above:

| Test set | Expected if current | Actual result |
|---|---|---|
| June 2026 additions (11) | All present | **3 of 11 present** (Soitec, Computacenter, SES — the rest absent) |
| June 2026 deletions (11) | All absent | **8 of 11 still present** (Christian Dior, Vidrala, Bavarian Nordic, B&M European, Big Yellow, Wendel, Wallenstam, INWIT) |
| March 2026 additions (12) | All present | **1 of 12 present** (Aixtron) |
| March 2026 deletions (10 checked) | All absent | **9 of 10 still present** (all except Azelis) |

**Overall: 4 of 23 real 2026 additions found; 17 of 21 real 2026 deletions still present.** This
is not an ambiguous or borderline result — it is a decisive failure against both independently-
dated review events. (Caveat, stated plainly: this is a plain-text substring search, so a short
name like "SES" could in principle false-positive-match inside unrelated text; the pattern
across 44 total checks is overwhelming enough that a handful of possible false positives among
the 4 "present" additions does not change the conclusion.)

**VERIFIED FACT**: the PDF is not a stale-metadata-but-fresh-data situation (item 2 of the
requested investigation) — the actual constituent list itself fails to reflect two real,
confirmed composition changes from earlier in 2026.

### 27.3 A second, independently-stale STOXX document

**LIVE TEST RESULT**: a different STOXX "Bookmarks" path was found and tested —
`stoxx.com/document/Bookmarks/CurrentFactsheets/SXXGR.pdf` (a statistics factsheet, not a
constituent list — market cap, returns, risk figures). HTTP 200, real PDF, but its own metadata
shows `ModDate: 2023-09-13` — a *different* stale date than the Components PDF, but still over
three years old. **INFERENCE**: this is not a one-off caching accident on a single URL — the
entire `stoxx.com/document/Bookmarks/Current*` family of free documents appears to serve
long-unrefreshed snapshots, a structural pattern rather than an isolated glitch.

### 27.4 Alternative official STOXX endpoints — none found working

**LIVE TEST RESULT**: `stoxx.com/index-updates` — an announcement archive; every download link
in its table renders with an inactive/grayed-out icon (`pdf-inactive.png`), no active files.
`stoxx.com/periodic-review-information` — a review-results page with an explicitly empty table
("No Files Exists"). Neither offers a working, current, downloadable constituent file. **No
versioned or dated variant of the `CurrentComponents` URL was found or guessed successfully**
(no alternative path pattern was identified to test).

### 27.5 Alternative free sources — not found, not force-fit

**RESEARCH FINDING**: a search for community-maintained (e.g. GitHub) current STOXX Europe 600
datasets did not surface a specific, verifiable free dataset — only third-party aggregator
websites (MarketScreener, ChartMill) referencing the data, neither independently fetched or
accessibility-checked this pass (their own terms/scraping-friendliness were not evaluated — this
is an explicit gap, not a silent "assume it's fine"). **No alternative free, current,
automatable European universe source was identified and verified this pass.**

### 27.6 Classification

**C — NOT SOLVED**, for the universe-source question specifically. Per the exact framework
requested:

```
European identity resolution      READY   (GLEIF + OpenFIGI + filings.xbrl.org,
                                            proven for 4 pilots + 2 new candidates)
European ESEF ingestion           READY   (Phase 5.1, real production data)
European canonical mapping        READY   (Phase 5.1)
European universe discovery       NOT SOLVED  (this section)
```

This is reported as a real, informative result, not a failure to force past. The free STOXX PDF
route, tested as rigorously as the identity-resolution chain was, does not meet the bar this
project already holds every other source to. Forcing it into production merely to have "600
free tickers" would introduce exactly the kind of undefendable data-quality debt ADR-0010/0011's
own governing principle (comparability/defensibility over raw availability) exists to prevent.

**RECOMMENDATION**: do not select a universe source in this phase. The real, available choices
for a future, separate decision are: (a) a manually curated/reviewed candidate list (slow,
small, but fully defensible — arguably what Phase 5.1's four pilots already are, just not
labeled as a deliberate strategy), (b) a commercial data license (STOXX's own paid
selection-lists, or an equivalent), (c) further engineering investment in the iShares-holdings
route (browser automation to reverse-engineer the current API) if that's judged worth the
effort, or (d) revisiting this question later in case STOXX's free resources are refreshed or a
new free source appears. None of these is selected or recommended over the others here — that
remains a decision for the repo owner, informed by this research, not decided by it.

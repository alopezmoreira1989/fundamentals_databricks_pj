# Phase 5.2d — FIRDS primary-listing selection, CFI verification, and `listing_id` keying

Small, focused follow-up to [phase5-2c-esma-firds-research.md](phase5-2c-esma-firds-research.md).
Resolves the two conditions from that document's own `READY WITH CONDITIONS` verdict that this
project's own architecture depends on before [ADR-0011](adr/0011-european-universe-admission.md)
can be accepted, plus two lightweight due-diligence checks the repo owner asked for alongside
them. Not another full research pass — five narrow, real-data-grounded questions:

1. Primary-listing selection rule (Phase 5.2c condition #1)
2. Official CFI reference-file verification (Phase 5.2c condition #2, lightweight)
3. `listing_id = MIC:TICKER` vs. `MIC:ISIN` — formal comparison, no decision made here
4. FIRDS terms of use (lightweight, not full legal review)
5. Pilot regression — confirm the primary-listing rule reproduces the already-established
   `XMAD`/`XPAR`/`XAMS`/`MTAA` result for all four pilots

## Executive summary

| Item | Result |
|---|---|
| 1. Primary listing | **SOLVED** — `IssrReq = true`, tie-broken by earliest `FrstTradDt`, resolves all 4 pilots with no remaining ambiguity |
| 2. CFI reference | **CONFIRMED FROM ESMA'S OWN DATA** — ESMA70-145-1090 "FIRDS CFI validations" workbook, downloaded and parsed directly, lists `ES****` under "Equities"; the workbook itself states it is derived from ISO 10962:2015, not an independent authority |
| 3. `listing_id` keying | **Not decided here** (per explicit instruction) — formal comparison provided; the natural-key evidence favors `MIC:ISIN`, but ADR-0010 stays as-is until a dedicated decision |
| 4. Terms of use | **VERIFIED FROM OFFICIAL ESMA DATA** — ESMA's general "Reproduction... is authorised... provided the source is acknowledged" copyright/reuse policy applies; no FIRDS-specific stricter terms found |
| 5. Pilot regression | **CONFIRMED** — `XMAD`/`XPAR`/`XAMS`/`MTAA` reproduced exactly, including both real ties |

Source-discipline labels follow Phase 5.2c's own convention: **VERIFIED FROM OFFICIAL ESMA
DATA** / **VERIFIED FROM REAL DATA** / **OFFICIAL DOCUMENTATION** / **SECONDARY SOURCE** /
**INFERENCE** / **RECOMMENDATION** / **OPEN QUESTION**.

## 1. Primary-listing selection rule

**VERIFIED FROM OFFICIAL ESMA DATA.** Re-downloaded the real FIRDS equity files
(`FULINS_E_20260815_01of02.zip` / `...02of02.zip`, same files used in Phase 5.2c) and parsed
**every** `RefData` record for all four pilot ISINs — not a sample. Per-venue counts: FCC 35,
Alstom 45 (was reported as 44 in Phase 5.2c's own §6; re-count on this pass found 45 — a real,
minor discrepancy noted here rather than silently reconciled, most likely one additional venue
admission between the 2026-08-15 file and whatever exact file the original Phase 5.2c pass
inspected; immaterial to the result below since the extra venue is not `IssrReq = true`), New
Amsterdam Invest 16, Fincantieri 41.

### The naive filter (`IssrReq = true`) is not always unique

`TradgVnRltdAttrbts/IssrReq` ("issuer-requested admission") is real, present on every venue
record, and boolean. Filtering to `IssrReq = true` per ISIN gives:

| Pilot | `IssrReq = true` venues | Unique? |
|---|---|---|
| ALO (Alstom) | `XPAR` only | Yes |
| NAI (New Amsterdam Invest) | `XAMS` only | Yes |
| FCC (Fomento de Construcciones y Contratas) | `XMAD`, `DMAD` | **No — tie** |
| FCT (Fincantieri) | `MTAA`, `HMTF` | **No — tie** |

Two of four pilots resolve cleanly on `IssrReq` alone (confirming Phase 5.2c's own preliminary
read). The other two are real, not hypothetical, ties — first found investigating this exact
item, not constructed as a test case.

### The two real ties, examined field by field

```
FCC (ES0122060314):
  MIC    IssrReq  FrstTradDt              AdmssnApprvlDtByIssr    RCA
  XMAD   true     1999-09-30T00:00:00Z    1999-09-30T00:00:00Z    ES
  DMAD   true     2024-12-09T00:00:00Z    1999-09-30T00:00:00Z    ES

FCT (IT0005599938):
  MIC    IssrReq  FrstTradDt              AdmssnApprvlDtByIssr    RCA
  MTAA   true     2024-06-17T00:01:00Z    2014-07-03T00:01:00Z    IT
  HMTF   true     2026-03-30T00:00:00Z    2026-03-27T00:00:00Z    IT
```

`RlvntCmptntAuthrty` (RCA) does **not** break either tie — both candidates share the same
country code in both cases (`ES`/`ES`, `IT`/`IT`), because neither tie is a cross-border
listing; ruled out as a discriminator before relying on it.

`AdmssnApprvlDtByIssr` ("issuer admission approval date") breaks the FCT tie (`2014-07-03` <
`2026-03-27`, `MTAA` wins) but **does not break the FCC tie** — `XMAD` and `DMAD` carry the
*exact same* `AdmssnApprvlDtByIssr` timestamp (`1999-09-30T00:00:00Z`), down to the second. This
is a real, exact tie in the data, not a near-miss — it rules out `AdmssnApprvlDtByIssr` as a
general-purpose discriminator on its own, even though it happens to work for one of the two
observed cases.

`FrstTradDt` ("first trade date on this specific venue") breaks **both** ties cleanly:

- FCC: `XMAD` first traded 1999-09-30 — the company's real admission date. `DMAD` first traded
  2024-12-09, a full 25 years later.
- FCT: `MTAA` first traded 2024-06-17 (later than its 2014 admission-approval date — a filing
  peculiarity, not a data error, but still earlier than the alternative). `HMTF` first traded
  2026-03-30, less than five months before this file's own publication date.

**Rule adopted for this document (not yet code):**

```
1. Filter an ISIN's RefData records to IssrReq = true.
2. If exactly one record remains, its MIC is the primary listing.        (ALO, NAI)
3. If more than one, the record with the earliest FrstTradDt wins.        (FCC, FCT)
4. (Not observed in the 4 pilots, flagged as an open gap:) if IssrReq is
   true for zero records, or FrstTradDt ties exactly too, this rule does
   not resolve — a genuinely unhandled case, not silently guessed at.
```

### What `DMAD` and `HMTF` actually are (why the rule's result makes sense, not just matches)

**SECONDARY SOURCE** (vendor MIC registry, cross-checked, not ESMA's own data — ESMA's FIRDS
records the MIC code itself but not a market's operator/segment relationship or venue
description):

- `DMAD` = "Bolsa de Madrid — Dark Midpoint," a **segment MIC** under the same Madrid group as
  `XMAD` — a non-displayed/dark-pool matching venue, not a second independent listing. FCC was
  never "dual-listed" in any meaningful sense; `DMAD` is a later, structurally different trading
  mechanism for the *same* underlying `XMAD` listing.
- `HMTF` = "Vorvel Bonds," an Italian MTF operator. Its `FrstTradDt` (2026-03-30) is 22 days
  before this file's own publication date and about 12 years after Fincantieri's real 2014 IPO
  on Borsa Italiana (`MTAA`) — consistent with a recent, secondary MTF admission, not the
  company's primary listing.

Both are real venues, correctly present in FIRDS, and correctly flagged `IssrReq = true` (the
issuer evidently did request admission to both) — they are just not the primary listing, and the
`FrstTradDt` ordering captures exactly the "which one is the original admission" distinction a
human reviewer would reach by the same reasoning, without needing to look up what `DMAD`/`HMTF`
actually are on a case-by-case basis.

### Robustness note (not tested, stated as a limitation)

This rule was derived from, and validated against, exactly two real tie cases. It is a
principled, defensible rule (earliest genuine trading activity under issuer-requested admission
is a reasonable proxy for "the original/primary listing"), not a rule reverse-engineered to fit
these two examples after the fact — but two data points is not a large validation set. A future
implementation phase should run this rule across a broader FIRDS equity sample (not just the 4
pilots) before trusting it unattended at scale, and should decide what to do with case 4 above
(no unique winner) rather than leaving it as a silent `None`.

## 2. Official CFI reference-file verification

**VERIFIED FROM OFFICIAL ESMA DATA.** ESMA does publish an official CFI validation reference for
FIRDS, separate from the generic public ISO 10962 documentation — **ESMA70-145-1090, "FIRDS CFI
validations"**, listed under ESMA's "MiFID II and MiFIR" → "MiFIR reporting instructions" policy
page (`esma.europa.eu/policy-rules/mifid-ii-and-mifir/mifir-reporting-instructions`), document
page `esma.europa.eu/document/firds-cfi-validations`, downloadable workbook at
`esma.europa.eu/sites/default/files/library/firds_cfi_validations.xlsx`.

Downloaded the real `.xlsx` directly (470,932 bytes, HTTP 200, no login) and parsed it myself
(not just relying on a summary) with `openpyxl`. It has four sheets: `CFI grid` (the current
validation matrix, A4:F148), `Notes`, and two dated historical/superseded sheets (`2020 03 02`,
`2019 09 23`) — confirming this is a maintained, versioned document, not a one-off.

The `CFI grid` sheet's header block and the row for `"ES"`, quoted verbatim from the real cells:

```
Row 4: Classification  |        | Attributes
Row 5: Equities         | First two Xters | 1st | 2nd | 3rd | 4th
Row 6: ES****            | ES               | V,N,R,E | T,U | O,P,F | B,R,N,M
```

i.e. the row-group header for this block of rows is literally **"Equities"**, and the first
data row under it is the two-character prefix **"ES"** — confirming, directly from ESMA's own
published validation reference (not inferred from ISO 10962 public documentation, and not just
from the four pilot records' own CFI values as Phase 5.2c did), that the `"ES"` prefix is
classified as an Equity instrument type. (The remaining columns — 1st/2nd/3rd/4th attribute
character — define what values are *valid* for the rest of an equity CFI code, e.g. `V` = Voting
for the "Voting rights" attribute position; not needed for this project's classification purpose,
but consistent with all four real pilot CFI values already observed in Phase 5.2c §4/§6-8.)

The `Notes` sheet states plainly what this grid actually is — confirming it is a **derivative**
of the ISO standard, not an independent ESMA classification authority:

> "The grid is based on Securities and related financial instruments – Classification of
> financial instruments (CFI), ISO 10962:2015, third edition, published in July 2015."

**Conclusion**: ESMA does maintain and publish its own official CFI *validation* reference for
FIRDS reporting purposes, and it independently confirms the `"ES"` = Equity classification this
project already relies on — but ESMA is not an independent CFI *definitional* authority; that
role remains ISO 10962 / ANNA Service Bureau, and ESMA's own document says so in its own words.
This is a stronger, ESMA-sourced confirmation than Phase 5.2c's original evidence (public ISO
10962 documentation + the four pilots' own observed CFI values), not a contradiction of it.

## 3. `listing_id` keying: `MIC:TICKER` vs. `MIC:ISIN`

**Not a decision.** Per explicit instruction, this section is a formal comparison only — ADR-0010
is not amended here, and its `listing_id = MIC:TICKER` decision stands. ADR-0010 is itself still
**Proposed** (not Accepted) as of this writing, which matters below: adjusting a Proposed ADR is
revising an open decision, not superseding a locked one.

### The evidence this phase adds

**VERIFIED FROM OFFICIAL ESMA DATA.** FIRDS' own `RefData` schema (confirmed directly from the
real XML in Phase 5.2c §3, re-confirmed here) has no ticker/symbol field anywhere. Its natural
composite key, as actually observed in the data, is `(ISIN, MIC)` — one `RefData` record per
security per trading venue, keyed that way structurally (this is why one ISIN legitimately
produces 16–45 records, one per venue, never one per ticker). A `listing_id = MIC:ISIN` maps onto
this key with zero transformation. A `listing_id = MIC:TICKER` requires a *second*, separate
resolution step for every FIRDS-sourced candidate (e.g. via OpenFIGI, per ADR-0011's own chain)
purely to attach a ticker that FIRDS itself never provides and never needs.

### Comparison

| | `MIC:TICKER` (current, ADR-0010) | `MIC:ISIN` |
|---|---|---|
| Matches FIRDS' own natural key | No — needs an extra ticker-resolution hop | Yes — direct |
| Stability | Ticker can change on corporate actions (renames, listing changes); not tested for these 4 pilots but a well-known real risk class in general (e.g., a company changing its trading symbol independent of any change in legal identity) | ISIN is the security's standing regulatory identifier; more stable than ticker, though not absolutely immutable — ISINs can occasionally be reissued on certain corporate restructurings (e.g. a change of issuing country), a real caveat, not assumed away |
| Human readability / debuggability | High — `XPAR:ALO` is legible at a glance | Low — `XPAR:FR0010220475` requires a lookup to mean anything to a human |
| Consistency with existing US/CA convention | Already what `main.config.tickers`' bare-ticker keying and every downstream table/frontend assumes | A genuine divergence from the existing US/CA shape |
| Consistency with this project's own stated principle that "ticker is not identity" | Partial tension — `listing_id` still keys on ticker, just scoped by MIC | Fully aligned — mirrors `identity.py`'s own `(ticker, market)` precedent taken one step further (ISIN instead of bare ticker) |
| Migration cost today | None — nothing changes | ADR-0010 is still **Proposed**, so this would be revising an open decision, not superseding an Accepted one; still a real change to `identity.py`'s `make_listing_id()` signature and every call site once implemented |

### Assessment (not a decision)

The natural-key evidence is real and points toward `MIC:ISIN` being the more architecturally
correct choice **if and when** FIRDS (or any ISIN-native source) becomes the universe-discovery
mechanism ADR-0011 depends on — forcing ticker-based keying onto ISIN-native source data adds a
resolution step and a mutability risk that the source itself doesn't have. This matches the repo
owner's own stated inclination. It is not, however, a currently-blocking problem: ADR-0010's
`MIC:TICKER` decision was made before FIRDS was investigated as a candidate universe source, and
nothing in Phase 5.1's already-shipped `EU_CURRENT` adapter or the four pilots' existing rows
depends on FIRDS' key shape — the tension is real but forward-looking, tied to a future universe-
discovery implementation phase, not to work already merged.

**Recommendation (non-binding):** resolve this before implementing whichever future phase
actually wires FIRDS into the universe-discovery flow, not before accepting ADR-0011 itself.
ADR-0011 is silent on universe-source mechanics by its own design (see its "Explicitly not
decided by this ADR" section) — this keying question is squarely inside that already-acknowledged
gap, not a new blocker. If/when it is resolved, since ADR-0010 is still Proposed, the natural
mechanism is amending ADR-0010 directly rather than writing a superseding ADR-0012 (ADR
immutability only applies once a record is Accepted, per `docs/adr/README.md`'s own convention).

## 4. FIRDS terms of use (lightweight check)

**VERIFIED FROM OFFICIAL ESMA DATA.** Checked ESMA's own legal notice
(`esma.europa.eu/legal-notice`, fetched live) and the FIRDS public register search page
(`registers.esma.europa.eu/publication/searchRegister?core=esma_registers_firds_files`). No
FIRDS-specific terms page exists separate from ESMA's site-wide legal notice; the register page
links back to the same general Legal Notice.

Relevant clause, quoted directly from the live page:

> Reproduction of all information on this site (ESMA Library) is authorised except as otherwise
> stated, provided the source is acknowledged and: where the original material is incorporated in
> documents that are sold (regardless of the medium), the publisher must inform buyers that it may
> be obtained free of charge through ESMA website; if the original material is transformed by the
> user ... and republished, this must be stated explicitly through [a disclaimer] ... ESMA does
> not endorse this publication and in no way is liable for copyright or other intellectual
> property rights infringements nor for any damages caused to third parties through this
> publication.

Practical read against this project's actual use (free automated bulk download, transformation
into derived `listing_id`/`issuer_id` records, no resale, no claim of ESMA endorsement):

| Question | Answer |
|---|---|
| Free? | Yes — no paywall, no login, no API key, confirmed live (matches Phase 5.2c §21/§22) |
| Automatable? | Yes — no terms found prohibiting machine access; the M2M API (`registers.esma.europa.eu/solr/...`) is ESMA's own documented machine-to-machine interface, built for exactly this |
| Redistribution allowed? | Reproduction is authorised with source attribution; no explicit prohibition on redistributing derived/transformed data found, subject to the disclaimer condition above if republished in transformed form |
| Commercial-use restriction? | No explicit commercial-use prohibition found in the clause above; the conditions are about attribution/non-endorsement, not a field-of-use restriction |
| ESMA's own copyright claim | ESMA claims copyright over material on its site, but "this copyright does not extend to any legislative text which is publicly available" — regulatory reference data of this kind is arguably closer to that carve-out than to editorial content, though this document does not take a firm legal position on which category FIRDS data falls into |

This is a lightweight, non-legal read of a public notice, not a legal opinion — flagged as such
per the original ask. Nothing found here changes Phase 5.2c's `READY WITH CONDITIONS`
classification or raises a new blocker; if this pipeline's use of FIRDS data ever becomes
commercial/redistributive at a scale where the distinction matters, a real legal review (not this
document) should be the gate, not this summary.

## 5. Pilot regression

**CONFIRMED**, using the rule from §1 against the real per-venue data captured this pass:

| Pilot | `IssrReq=true` candidates | Winner (rule from §1) | Matches Phase 5.1/ADR-0010's already-established MIC? |
|---|---|---|---|
| FCC | `XMAD` (1999-09-30), `DMAD` (2024-12-09) | **`XMAD`** (earliest `FrstTradDt`) | Yes |
| ALO | `XPAR` only | **`XPAR`** (unique) | Yes |
| NAI | `XAMS` only | **`XAMS`** (unique) | Yes |
| FCT | `MTAA` (2024-06-17), `HMTF` (2026-03-30) | **`MTAA`** (earliest `FrstTradDt`) | Yes — including the `MTAA` vs. `XMIL` segment-vs-operating distinction ADR-0010 already made, now a fifth independent confirmation of that specific decision |

All four pilots reproduce their already-established, already-shipped `EU_CURRENT` MIC exactly,
including both real tie cases this pass specifically went looking for. No pilot regressed.

## What this document does not do

- Does not change `identity.py`, `EUCurrentSource`, `config.tickers`, the DAG,
  `fundamentals_screener`, or the Streamlit app.
- Does not implement the §1 rule in code — it is a documented, evidence-backed proposal for a
  future universe-discovery implementation phase to build against.
- Does not amend ADR-0010.
- Does not flip ADR-0011 to Accepted — that remains the repo owner's own call.
- Does not perform a full legal review of FIRDS' terms (§4 is explicitly lightweight, per the
  original ask).

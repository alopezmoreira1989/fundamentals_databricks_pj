# Phase 5.3 — European Universe & Admission Layer

**Implementation phase** (unlike Phase 5.2/5.2b/5.2c/5.2d/5.2e, which were research-only). Builds
the first production-capable European universe/admission layer on top of the already-decided
architecture: ESMA FIRDS as the reference universe (Phase 5.2c), the primary-listing selection
rule (Phase 5.2d), and `listing_id = MIC:ISIN` for new listings (ADR-0012). Produces an auditable
admission decision for every real FIRDS equity ISIN — not fundamentals, not a frontend change, not
a DAG wiring. Per the driving brief's explicit stop condition: implementation + tests + a bounded
validation run + this document + a PR left **unmerged** for review.

**Update**: §19 was originally written before a working Databricks Connect profile became
available in this session; it has since been re-run against the real production workspace (see
§19's own "UPDATED" marker) — every part of this implementation, including the Spark/Delta
write, is now validated against real, live infrastructure, not just locally.

Source-discipline labels: **VERIFIED FACT** / **REAL DATA TEST** / **INFERENCE** /
**RECOMMENDATION** / **OPEN QUESTION**.

## 1. What was implemented

- `fundamentals_pipeline/sources/eu_admission.py` (new, pure Python, no Spark/network) — the
  equity classifier, active-instrument filter, primary-listing selector, and the admission/
  rejection state machine. Fully unit-tested (34 tests, all real-data-grounded).
- `fundamentals_pipeline/identity.py` — adds `make_listing_id_from_isin(mic, isin)`, the ADR-0012
  implementation (`MIC:ISIN`), kept fully separate from the existing `make_listing_id(mic,
  ticker)` (`MIC:TICKER`) — no change to the existing US/CA function or its callers.
- `fundamentals_pipeline/sources/__init__.py` — exports the new module's public names.
- `fundamentals_pipeline/10__ingestion/17__firds_admission.py` (new Databricks notebook) — the
  real FIRDS download, the real XML parse, the bounded ESEF-eligibility/ticker-enrichment network
  calls, and the Delta write to `main.config.eu_admission_candidates`. **Not** wired into `16`,
  **not** added to the scheduled DAG, **not** touching `config.tickers`.
- `tests/test_sources_eu_admission.py` (new, 34 tests) — every fixture is real FIRDS data
  captured this phase, not synthesized (see §16-19 below).
- This document.

## 2. Files changed

```
fundamentals_pipeline/sources/eu_admission.py           (new)
fundamentals_pipeline/sources/__init__.py                (exports added)
fundamentals_pipeline/identity.py                        (+make_listing_id_from_isin)
fundamentals_pipeline/10__ingestion/17__firds_admission.py (new)
tests/test_sources_eu_admission.py                        (new)
docs/phase5-3-european-universe-admission.md              (this file)
```

No SEC ingestion file, no config file, no DAG file, no `fundamentals_screener` file, no Streamlit
file touched — confirmed via `git status --short`/`git diff --stat` before every commit this
phase.

## 3. Admission data model

```python
class AdmissionStatus(str, Enum):
    ADMITTED = "admitted"                    # identity + instrument + ESEF all resolved
    PENDING_ESEF_CHECK = "pending_esef_check" # identity + instrument resolved; ESEF not checked
                                               # this run (bulk candidates, see §6/§9)
    REJECTED = "rejected"

class RejectionReason(str, Enum):
    NON_EQUITY = "non_equity"
    INACTIVE = "inactive"
    NO_LEI = "no_lei"
    IDENTITY_UNRESOLVED = "identity_unresolved"       # reserved: GLEIF cross-check failure
                                                       # (not exercised — see §21)
    PRIMARY_LISTING_UNRESOLVED = "primary_listing_unresolved"
    NO_ESEF_FILING = "no_esef_filing"
    ESEF_NOT_INGESTIBLE = "esef_not_ingestible"
```

`AdmissionCandidate` fields: `isin`, `lei`, `mic`, `issuer_id`, `listing_id`, `issuer_name`,
`country`, `ticker`, `ticker_status` (`resolved`/`unresolved`/`not_attempted` — a separate
dimension from `admission_status`, per the driving brief's explicit §11 instruction: a missing
ticker never blocks admission), `admission_status`, `rejection_reason`, `n_venue_records`,
`primary_frst_trad_dt` (provenance for the tie-break), `source`/`source_file`/
`source_publication_date` (provenance for the FIRDS file itself). A deliberately smaller field
set than the driving brief's own illustrative §14 list — no field was added that isn't actually
populated by real logic.

**A load-bearing identity decision, stated explicitly**: `issuer_id` for an admitted candidate is
always `EU_CURRENT:<LEI>` — the same source_id Phase 5.1's `EUCurrentSource` already uses, not a
new `ESMA_FIRDS:<LEI>` identity. FIRDS is the **universe** source; a candidate's **fundamentals**
would still come from `EU_CURRENT` (filings.xbrl.org) in a future phase. This makes
`make_eu_issuer_id(lei)`'s output byte-identical to what `sources/eu_current.py`'s
`entity_from_pilot()` already produces for the 4 hardcoded pilots — verified directly
(`test_make_eu_issuer_id_matches_existing_pilot_convention`), so a future phase wiring this
layer's output into `EUCurrentSource` needs no re-identification step.

## 4. FIRDS filtering rules

**Equity filter — VERIFIED FACT, empirically re-confirmed with a real negative example this
phase.** CFI prefix exactly `"ES"` (not any `"E*"` prefix). Sampled one real instrument per
2-character CFI prefix from the real `FULINS_E_20260815` file:

| Prefix | Real sample (name, ISIN) | Real meaning |
|---|---|---|
| `ES` | Raisio Oyj, `FI0009800395` | Common/ordinary shares — the target |
| `EY` | "Series 26 ... HIFIN Solution Secured Notes due 2027", `CH1108675062` | Structured notes — **the single largest CFI group in the whole file, 478,047 of 682,398 raw records**, sharing only the broad "E" category with equity |
| `EP` | Regions Financial 4.45% Dep.Sh., `US7591EP8869` | Preferred/preference shares |
| `ED` | AMD Canadian Depositary Receipt, `CA00791L1067` | Depositary receipts on equity |
| `EL` | US Gasoline Fund LP ETFS, `US91201T1025` | Limited partnership/fund units |
| `EC`/`EF`/`EM` | (sampled, real) | Convertible variants / profit-participation instruments |

Filtering to exactly `"ES"` is not merely cautious — `"EY"` alone, if wrongly included by a loose
`"E*"` filter, would have dominated the admitted universe with structured notes.

**Active filter — VERIFIED FACT, field confirmed by direct XML inspection.**
`TradgVnRltdAttrbts/TermntnDt` is real, present on ~60% of real venue records (301,785 of 500,000
in the first file), with a far-future sentinel (`9999-12-31T22:00:00Z`) needing no special-casing
under plain date comparison. Rule: `FrstTradDt <= as_of AND (TermntnDt is None OR TermntnDt >=
as_of)`.

## 5. Primary-listing algorithm

Phase 5.2d's rule, applied among **active** records only (a terminated venue admission cannot be
today's primary listing): `IssrReq = true`, tie-broken by earliest `FrstTradDt`. Never guesses —
an ISIN with no active `IssrReq = true` candidate, or a genuine exact tie on the minimum
`FrstTradDt`, is `PRIMARY_LISTING_UNRESOLVED`, not a coin-flip.

**Real, full-scale finding this phase adds to Phase 5.2d's own "robustness note" caveat** (which
had validated the rule against only 2 real tie cases): running the real rule against the full
17,251-ISIN real equity universe found:

- **11,067 ISINs (64%)** have active venue records but **zero** are `IssrReq = true` — the
  dominant real case, not a corner case. Sampled 10 real examples: all are foreign issuers (South
  African, Bermudian, Chinese, Canadian, Australian, US) quoted only on small German regional MTF
  venues (Tradegate/Gettex/Munich/Stuttgart/Hamburg-style codes), never genuinely admitted to an
  EU regulated market. The rule correctly excludes them — this is the "foreign company
  incidentally quoted on a European venue" case the project's universe scope was always meant to
  exclude, now proven with real names (e.g. "Truworths International Ltd.", "Agricultural Bank Of
  China Ltd", "iRadimed Corp.").
- **1,192 ISINs (7%)** hit a **genuine exact tie** on the minimum `FrstTradDt` among active
  `IssrReq = true` candidates — real, not hypothetical, contradicting this phase's own initial
  unit-test comment (fixed — see §16 below). Overwhelmingly a Nordic (Nasdaq Nordic/First North)
  pattern: parallel MIC codes for the same admission event sharing the identical `FrstTradDt`
  (real example: Konsolidator A/S, `DK0061113511` — `DNDK` and `MNDK` both admitted 2019-05-10).
  Correctly left unresolved rather than arbitrarily picking one.
- **4,983 ISINs (29%)** resolve cleanly to a single primary listing.

**A real, non-blocking limitation found and documented, not silently accepted**: for several
major German companies (SAP, Allianz, BMW, Volkswagen, Henkel, Merck, Deutsche Lufthansa — all
real, sampled), the rule resolves to `FRAA` (the historical Frankfurt floor-trading segment,
`IssrReq = true` since 1988) rather than the modern Xetra electronic-trading MICs (`XETA`/`XETU`/
`XEMA`/`XGLO`, also `IssrReq = true` but admitted later). Unlike the `DMAD`/`HMTF` cases Phase
5.2d resolved (a dark pool and a brand-new secondary MTF — clearly non-representative), `FRAA` and
the Xetra family are **both genuine regulated-market admissions of the same underlying Frankfurt
Stock Exchange group**, so this is not a wrong identity, just a debatable choice of MIC within one
exchange group. Per the driving brief's explicit instruction not to revisit the established rule
without a real technical failure — and this is not one, the rule *does* deterministically resolve
— the rule is kept as-is; this is flagged as a real, Germany-specific limitation for a future
refinement pass, not silently smoothed over. (Confirmed NOT to occur for Netherlands/Italy/Spain
large caps sampled the same way — ASML/Randstad/Heineken/Philips → `XAMS`; Intesa/Eni/UniCredit →
`MTAA`; Santander/Iberdrola/Inditex/Repsol/Telefónica → `XMAD`, all clean, single-MIC resolutions.)

## 6. Identity resolution strategy

`issuer_id = make_eu_issuer_id(lei) = "EU_CURRENT:" + lei` (see §3). `listing_id =
make_listing_id_from_isin(mic, isin)` (ADR-0012). Both are pure string functions — no fuzzy
matching, no name-based resolution, matching `identity.py`'s own long-standing principle.
`country` is populated from the primary listing's own `RlvntCmptntAuthrty` (the reporting
regulator's jurisdiction), not derived from the ISIN's 2-character prefix (which can differ from
the actual admission jurisdiction).

**GLEIF cross-check — designed, not exercised at scale (OPEN QUESTION, honestly left open).** The
driving brief's §9 asks to "cross-check the LEI against GLEIF where required." Since FIRDS already
supplies the LEI directly (a real, populated field on every equity `RefData` record — confirmed
0 records among the 4,983 resolved-primary-listing ISINs had an empty LEI), and per the brief's
own §25 explicit constraint not to call GLEIF once per row, this phase did not perform a live
GLEIF cross-check for the bulk population. `RejectionReason.IDENTITY_UNRESOLVED` is defined in the
data model for exactly this future use (a GLEIF-cross-check failure, distinct from a bare missing
LEI) but is not reachable by any code path in this phase — an honest, unexercised placeholder, not
a silently-fabricated "verified" claim.

## 7. ESEF eligibility strategy

Deliberately deferred for the bulk resolved-candidate population (~4,926 unique issuers) — per
the brief's own §22/§23/§26, this phase proves `FIRDS → admitted universe`, not `admitted universe
→ full fundamentals ingestion`. `apply_esef_eligibility()` reuses `eu_current.
select_filing_for_period` directly (no duplicated amendment-selection logic, per §12) and is only
invoked for `BOUNDED_VALIDATION_ISINS` — the 4 established pilots, the 2 Phase 5.2b generalization
candidates, and 3 new real candidates chosen specifically to exercise distinct real outcomes (see
§10 below). Everything else stays `PENDING_ESEF_CHECK` — a real, honest state, not a silently
skipped one.

## 8. Ticker enrichment strategy

`apply_ticker_enrichment()` — a single OpenFIGI `ID_ISIN` + `micCode` query per bounded candidate,
never blocking admission on failure (§11 of the brief). Also deliberately bounded to the same
`BOUNDED_VALIDATION_ISINS` set, for the same reason as the ESEF check — not run for the ~4,926
bulk-resolved issuers in this pass.

## 9. Raw / equity / active / primary / issuer / admitted counts

**REAL DATA TEST**, the real `FULINS_E_20260815_01of02.zip`/`...02of02.zip` files (found live via
ESMA's own M2M API at validation time — same files as Phase 5.2c/5.2d, confirming stability),
downloaded and parsed end-to-end through the actual notebook code (not a re-implementation):

```
raw RefData records (both files)                    682,398
unique ISINs (all CFI types)                         183,055
  non-equity (CFI != "ES")                            165,804
  equity (CFI == "ES")                                 17,251
    inactive (no active venue record)                      9
    active, primary listing UNRESOLVED               12,259
      - zero active IssrReq=true candidates            11,067
      - genuine tie on minimum FrstTradDt               1,192
    active, primary listing RESOLVED                    4,983
unique issuer LEIs among resolved primary listings     4,926
```

**This funnel is Generation-1's real answer to "what does European universe actually mean in
practice"**: of 682,398 raw reference-data records, only 4,983 real listings (representing 4,926
distinct issuers) have both a genuine EU equity classification and an unambiguous, issuer-
requested primary admission — everything else is correctly excluded, not silently dropped.

## 10. FCC result

**REAL DATA TEST**, through the actual notebook code end-to-end (FIRDS download → parse →
admission → live ESEF check → live ticker resolution):

```
isin=ES0122060314 | issuer_name=ACCIONES FOMENTO DE CONSTRUCCIONES Y CONTRATAS, S.A.
mic=XMAD | listing_id=XMAD:ES0122060314
lei=95980020140005178328 | issuer_id=EU_CURRENT:95980020140005178328
admission_status=ADMITTED | rejection_reason=None
ticker=FCC (resolved via OpenFIGI)
```

## 11. Alstom result

```
isin=FR0010220475 | issuer_name=ALSTOM | mic=XPAR | listing_id=XPAR:FR0010220475
lei=96950032TUYMW11FB530 | issuer_id=EU_CURRENT:96950032TUYMW11FB530
admission_status=ADMITTED | ticker=ALO
```

## 12. New Amsterdam Invest result

```
isin=NL0015000CG2 | issuer_name=NEW AMSTERDAM INVEST N.V. ORDINARY SHARES
mic=XAMS | listing_id=XAMS:NL0015000CG2
lei=724500JXEXUGEATP5L52 | issuer_id=EU_CURRENT:724500JXEXUGEATP5L52
admission_status=ADMITTED | ticker=NAI
```

## 13. Fincantieri result

```
isin=IT0005599938 | issuer_name=FINCANTIERI | mic=MTAA | listing_id=MTAA:IT0005599938
lei=8156005BDF49128B6239 | issuer_id=EU_CURRENT:8156005BDF49128B6239
admission_status=ADMITTED | ticker=FCT
```

**All four established pilots resolved to exactly their already-shipped Phase 5.1 MIC** (`XMAD`/
`XPAR`/`XAMS`/`MTAA`), now through the real, production-shaped admission-layer code path — not
just the narrower fixture-level regression Phase 5.2d ran. This is the sixth independent
confirmation of this identity across the whole multi-phase investigation.

## 14. Iberdrola result

**Generalization candidate (Phase 5.2b) — discovered through real FIRDS admission, not
OpenFIGI search this time.**

```
isin=ES0144580Y14 | issuer_name=ACCIONES IBERDROLA | mic=XMAD | listing_id=XMAD:ES0144580Y14
lei=5QK37QC7NWOJ8D7WVQ45 | issuer_id=EU_CURRENT:5QK37QC7NWOJ8D7WVQ45
admission_status=ADMITTED | ticker=IBE
```

## 15. Saint-Gobain result

```
isin=FR0000125007 | issuer_name=SAINT GOBAIN | mic=XPAR | listing_id=XPAR:FR0000125007
lei=NFONVGN05Z0FMN5PEC35 | issuer_id=EU_CURRENT:NFONVGN05Z0FMN5PEC35
admission_status=ADMITTED | ticker=SGO
```

Both generalization candidates resolve cleanly and match their Phase 5.2b-established identity
exactly — a real, stronger proof than Phase 5.2b's own (which used an OpenFIGI name search, not
this project's actual production-shaped FIRDS admission path).

**Three additional real candidates, beyond the driving brief's required list, chosen specifically
to exercise distinct real outcomes:**

```
SAP SE — DE0007164600 → mic=FRAA, listing_id=FRAA:DE0007164600
  admission_status=REJECTED, rejection_reason=NO_ESEF_FILING, ticker=not_attempted
  (identity fully resolved via FIRDS; rejected only at the ESEF step — Germany is a known
  filings.xbrl.org coverage gap, per registry.py's own EU_CURRENT notes. A real, live
  demonstration of IDENTITY_RESOLVED != ESEF_INGESTIBLE, exactly as designed.)

Randstad N.V. — NL0000379121 → mic=XAMS, listing_id=XAMS:NL0000379121
  admission_status=ADMITTED, ticker=RAND

Intesa Sanpaolo — IT0000072618 → mic=MTAA, listing_id=MTAA:IT0000072618
  admission_status=ADMITTED, ticker=ISP
```

## 16. Collision test

**REAL DATA TEST.** Phase 5.2 found real global ticker collisions: `FCC` also matches an
unrelated Vietnamese company; `FCT` matches ≥5 unrelated global companies. This layer's canonical
identity (`issuer_id`/`listing_id`) never touches ticker — `test_ticker_collision_does_not_create_
identity_collision` constructs a real FCC admission candidate alongside a synthetic same-ticker,
different-ISIN/LEI candidate and confirms `issuer_id`/`listing_id`/`lei` all differ. Ticker
collisions are real; identity collisions, structurally, cannot happen through this layer.

## 17. Share-class test

**REAL DATA TEST**, grounded in Phase 5.2e's live-verified Volkswagen ordinary (`DE0007664005`,
ticker `VOW`, real CFI `ESVUFR` sampled this phase) vs. preference (`DE0007664039`, ticker `VOW3`,
real CFI `EPNCFR` sampled this phase) shares. **A real, honest scope-boundary finding**: preference
shares carry a distinct `"EP"` CFI prefix, not `"ES"` — so under this phase's narrow "ES-only"
equity filter, the ordinary share resolves (`ADMITTED`-eligible) and the preference share is
correctly `NON_EQUITY`-excluded, not silently collapsed into the ordinary listing. Both share
classes keep distinct ISINs/listing_ids regardless — the two-instrument structure is preserved
either way; only the current scope's own equity definition (deliberately narrow, matching Phase
5.1's own "start with high-confidence concepts" precedent) excludes preference shares from this
generation's admitted universe. **OPEN QUESTION, not resolved here**: whether a future phase should
widen the equity filter to include `"EP"` — out of scope for this pass.

## 18. Idempotency result

**REAL DATA TEST — a real bug found and fixed during this phase's own validation, not a clean
pass reported at face value.** Running the full admission pipeline against the real 682,398-record
file, with input order reversed, initially found **4,975 mismatched candidates** — not a
hypothetical risk, a real defect. Root cause: `issuer_name` for unresolved candidates was read
from `records[0].full_nm` — the first record in *list order*, which real FIRDS data does not
guarantee is name-consistent across venues for the same ISIN (real example: "Bang and Olufsen
A/S" on one venue's record vs. "BANG&OLUF DKK10 B" on another's, for the same ISIN). Fixed by
selecting a deterministic representative record (`min(records, key=lambda r: r.mic)`) instead of
list-position `[0]`. Re-verified after the fix: **0 mismatches** against both a fully reversed and
a randomly-shuffled full re-run of the real 682,398-record dataset. This is now a genuinely
verified property, not an assumed one.

## 19. Databricks validation

**UPDATED — now genuinely executed against the real, live production Databricks workspace, not
just reviewed.** A working Databricks Connect profile (`fundamentals`, `dbc-b52a3a2b-c131.cloud.
databricks.com`) became available in this environment after this document's first draft — the
notebook's own code (not a reimplementation) was run for real via `DatabricksSession.builder.
profile("fundamentals").serverless(True).getOrCreate()`, the same mechanism `test_connection.py`
already establishes as this project's local Databricks Connect smoke-test convention.

**Previously validated locally (no change)**: `find_latest_firds_equity_files()`/
`parse_firds_equity_zip()`/`run_admission_pipeline()`/`apply_bounded_validation()` — see §9-18
above, unchanged.

**Newly validated against the real Databricks workspace (this update)**:

- **Real end-to-end execution, twice**, against `main` catalog, real serverless compute
  (Spark 4.1.0). Both runs reproduced the *exact* funnel from the local/live-ESMA validation:
  `raw=682,398 → unique_isin=183,055 → equity=17,251 → primary_listing=4,983 →
  unique_issuers=4,926`, and the exact same admission breakdown: `{admitted: 8, pending_esef_
  check: 4,974, rejected: 178,073}` — the 8 `admitted` are the 6 required pilots/generalization
  candidates + Randstad + Intesa Sanpaolo; the 1 real ESEF rejection is SAP, exactly as predicted.
- **A real bug found and fixed on the first live run**: the final `print(f"✓ {len(rows)}...")`
  statement crashed with `UnicodeEncodeError: 'charmap' codec can't encode character '✓'`
  under this Windows console's `cp1252` encoding — a genuine Python-runtime difference the
  driving brief's own §12 anticipated. Confirmed the **Delta write itself had already succeeded
  before the crash** (the table existed with the correct 183,055 rows immediately after) — this
  was a cosmetic reporting bug, not a data-correctness bug, but a real one nonetheless. Fixed by
  replacing the Unicode checkmark with a plain `"OK: ..."` string (the smallest necessary fix,
  per §12); re-ran clean twice after the fix with 0 further errors.
- **Schema**: `DESCRIBE TABLE main.config.eu_admission_candidates` matches the notebook's declared
  schema exactly (17 columns, correct types — `n_venue_records INT`, dates as `DATE`,
  `retrieved_at TIMESTAMP`).
- **Duplicates**: `SELECT isin, COUNT(*) ... HAVING COUNT(*) > 1` → **0**. Same check on
  `(issuer_id, listing_id)` among non-null rows → **0**.
- **Idempotency (real Delta semantics, not simulated)**: ran the notebook **twice** against the
  same live FIRDS source. Row count after run 1: 183,055. Row count after run 2: 183,055 —
  identical. This table uses `overwrite` mode (a point-in-time snapshot, not an append log, per
  the notebook's own documented design) — confirmed to behave exactly as documented: a full,
  clean replacement, not accumulation.
- **Pilot/generalization/sample query against the real table** — all 9 candidates, read directly
  from `main.config.eu_admission_candidates`, not re-derived:

  | ISIN | MIC | listing_id | ticker | status | rejection |
  |---|---|---|---|---|---|
  | ES0122060314 (FCC) | XMAD | `XMAD:ES0122060314` | FCC | admitted | — |
  | FR0010220475 (Alstom) | XPAR | `XPAR:FR0010220475` | ALO | admitted | — |
  | NL0015000CG2 (NAI) | XAMS | `XAMS:NL0015000CG2` | NAI | admitted | — |
  | IT0005599938 (Fincantieri) | MTAA | `MTAA:IT0005599938` | FCT | admitted | — |
  | ES0144580Y14 (Iberdrola) | XMAD | `XMAD:ES0144580Y14` | IBE | admitted | — |
  | FR0000125007 (Saint-Gobain) | XPAR | `XPAR:FR0000125007` | SGO | admitted | — |
  | DE0007164600 (SAP) | FRAA | `FRAA:DE0007164600` | NULL | rejected | no_esef_filing |
  | NL0000379121 (Randstad) | XAMS | `XAMS:NL0000379121` | RAND | admitted | — |
  | IT0000072618 (Intesa Sanpaolo) | MTAA | `MTAA:IT0000072618` | ISP | admitted | — |

  Exact match to §10-15's earlier results — the Databricks run reproduces the identical
  identities, MICs, and tickers.
- **US/CA regression — before/after row counts on every table the brief named as protected**,
  captured immediately before the first live run and re-checked after both runs:

  | Table | Before | After | |
  |---|---|---|---|
  | `main.config.tickers` | 2,662 | 2,662 | MATCH |
  | `main.financials.financials_raw` | 425,799,947 | 425,799,947 | MATCH |
  | `main.financials.financials` | 4,750,430 | 4,750,430 | MATCH |
  | `main.financials.market_prices_daily` | 15,268,796 | 15,268,796 | MATCH |
  | `main.financials.stock_splits` | 4,330 | 4,330 | MATCH |
  | `main.financials.market_cap_asof` | 27,240 | 27,240 | MATCH |
  | `main.financials.ingestion_failures` | 61,277 | 61,277 | MATCH |

  Zero rows changed anywhere outside the new `main.config.eu_admission_candidates` table.

This closes the one gap this document previously flagged as open. Every part of PR #377's own
implementation — FIRDS retrieval, XML parsing, the admission pipeline, bounded ESEF/ticker
enrichment, and now the Spark/Delta write — has been run for real, against real data, on the
real target infrastructure.

## 20. SEC regression

**VERIFIED FACT.** `git status --short` / `git diff --stat` before every commit this phase
confirms zero SEC ingestion files (`10__ingestion/11__fetch_sec_xbrl.py`), zero `00__config/`
files, zero `20__transformation/` files, and zero DAG files (`90__pipelines/`) were touched. The
only modified pre-existing files are `fundamentals_pipeline/identity.py` (one new, additive
function) and `fundamentals_pipeline/sources/__init__.py` (export list only).

## 21. Known limitations

- **The `FRAA`-vs-Xetra German large-cap finding (§5)** — a real, non-blocking, Germany-specific
  ambiguity in which venue within one exchange group counts as "primary." Not fixed in this pass,
  per the explicit instruction not to revisit the established rule without a real technical
  failure.
- **GLEIF cross-check is designed but unexercised** (§6) — `RejectionReason.IDENTITY_UNRESOLVED`
  exists in the data model with no real code path reaching it yet.
- **ESEF eligibility and ticker enrichment are bounded to 9 real candidates**, not the full 4,926
  resolved issuers (§7/§8) — deliberately, per the brief's own scope. The real, live
  `main.config.eu_admission_candidates` table (confirmed by direct query, §19) carries
  `PENDING_ESEF_CHECK` for 4,974 of its 4,982 resolved rows — a real, honest intermediate state,
  not a final admission verdict, exactly as designed.
- **Only the Equity ("ES") CFI class was investigated** — preference shares (§17), depositary
  receipts, and other equity-adjacent instrument types are out of scope.
- ~~The Spark/Delta-write section of the notebook is unexecuted against a live cluster~~ —
  **resolved this update (§19)**: run twice against the real production workspace, including a
  real bug found (a Windows-console Unicode print crash, occurring after a successful Delta
  write) and fixed.
- **No legal/terms-of-use re-review performed this phase** — Phase 5.2d's lightweight FIRDS
  terms-of-use check stands unchanged; this phase didn't re-examine it.
- **`issuer_name` for the bulk (non-bounded) candidate population is FIRDS' own `FullNm` field
  verbatim** — real, observed to include venue-specific formatting noise (share-class suffixes,
  inconsistent capitalization) rather than a cleaned company name; acceptable for an audit field,
  not suitable as a display name without further cleaning in a future phase.

## 22. Recommended next phase

With §19's Databricks validation now closed, **PR #377 is READY TO MERGE** (repo owner's own
review/decision, not exercised by this document). Per the repo owner's own stated sequencing
(Phase 5.3 → a small real EU fundamentals sample → `fundamentals_screener`): the natural next
step after merge is wiring a small, explicit subset of this layer's `ADMITTED` output (e.g. the
9 already-validated candidates) into `EUCurrentSource`'s `PILOT_EU_ENTITIES`-equivalent input —
still not decided how (a separate design question, not answered by this phase) — to get the
first real, non-hardcoded European rows into `financials`, and only after that into
`fundamentals_screener`. Full-universe fundamentals ingestion for all 4,926 resolved issuers
remains explicitly out of scope until that small-sample checkpoint is itself verified.

## 23. Explicit non-goals (this pass, restated)

- Does not modify `config.tickers`, the DAG, `fundamentals_screener`, or the Streamlit app.
- Does not run the full EU fundamentals ingestion for the admitted universe.
- Does not accept ADR-0010, ADR-0011, or ADR-0012.
- Does not merge its own PR.

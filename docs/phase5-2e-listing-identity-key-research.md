# Phase 5.2e — Listing identity key: `MIC:TICKER` vs. `MIC:ISIN`

Architectural research phase, no implementation. Decides (via ADR-0012, status **Proposed**)
what `listing_id`'s canonical representation should be, before the European Universe &
Admission Layer (Phase 5.3) is built on top of a frozen listing identity. Follows
[phase5-2d-firds-primary-listing-and-identity-model.md](phase5-2d-firds-primary-listing-and-identity-model.md),
which first surfaced this question as real, load-bearing evidence rather than speculation.

Source-discipline labels (per the established convention):
**VERIFIED FACT** / **REAL DATA TEST** / **SECONDARY SOURCE** / **INFERENCE** /
**RECOMMENDATION** / **OPEN QUESTION**.

## Executive summary

`MIC:ISIN` is the architecturally better-fitting representation for the reasons Phase 5.2d
already surfaced (FIRDS' own natural key, ticker mutability) — this pass adds a real,
independently-verified ticker-change example, a real share-class example, and a concrete
per-subsystem migration-impact assessment. It does **not** recommend re-keying anything today.
The practical recommendation (§13/§22) is a **field-level, not a full-rekey**, change: keep
`listing_id = MIC:TICKER` as the pragmatic Generation-1 representation the existing US/CA
architecture already assumes everywhere, but formally define **`MIC:ISIN` as the stable
underlying identity** that should back any *new* (European/FIRDS-sourced) listing, with ticker
demoted to a mutable, resolved-on-demand display attribute for those listings specifically. A
full project-wide re-key is assessed as real, but deferred, work — see §9/§19.

## 1. Current system — what `listing_id`/`issuer_id` actually are today

**VERIFIED FACT.** `fundamentals_pipeline/identity.py` (Phase 5.0/ADR-0010, ADR status still
Proposed):

```python
def make_issuer_id(source_id: str, source_entity_id: str) -> str: ...   # "SEC_XBRL:0000320193"
def make_listing_id(mic: str, ticker: str) -> str: ...                   # "XPAR:ALO"
```

`issuer_id`/`mic`/`listing_id` are additive, nullable columns on `main.config.tickers`
(Phase 5.0's own explicit scope boundary) — **no** core financial/price Delta table is keyed
on either today. `main.config.tickers`' own real primary key is `ticker` alone (`NOT NULL`,
everything else nullable), and the table is itself physically `.partitionBy("ticker")`
(`00__config/02__tickers_master.py:1063`) — the identity columns Phase 5.0 added sit alongside
that existing physical key, not in place of it.

**Correction to an assumption carried over from Phase 5.2c/5.2d**: the four EU pilots
(FCC/ALO/NAI/FCT) do **not** have `issuer_id`/`mic`/`listing_id` populated on any
`config.tickers` row — they are never written to `config.tickers` at all.
`16__fetch_eu_xbrl.py` explicitly states it "Does NOT modify `main.config.tickers`"; their MIC/
LEI/ticker values exist only as hardcoded literals in that notebook's own `PILOT_EU_ENTITIES`
tuple and in `tests/test_issuer_listing_identity.py`'s fixtures. `mic`/`listing_id` are in fact
written as `None` on every `config.tickers` run today and never backfilled anywhere in the
codebase (`make_listing_id` is called only from `identity.py` itself and that one test file) —
only `issuer_id` gets backfilled, via `11__fetch_sec_xbrl.py`'s own
`MERGE INTO ... ON t.ticker = s.ticker`. This means the `issuer_id`/`listing_id` primitives are,
today, populated for **zero** rows of the real `config.tickers` table in either direction (not
for the pre-existing US/CA universe, per ADR-0010's own documented gap, and not for the EU
pilots either, since they were never admitted to that table). This is a materially different,
more thoroughly-verified starting point than earlier phases' passing references to this table —
corrected here via direct grep against the real notebook, not re-asserted from memory.

## 2. How deep does ticker-as-identity actually run? (real code evidence)

**VERIFIED FACT**, direct grep/read against the real files, not inferred:

| Table / system | Real evidence | Ticker's role |
|---|---|---|
| `financials` | `20__transformation/21__clean_and_merge.py:73`: `PARTITIONED BY (ticker, stmt)`. `21__clean_and_merge.py:284-288`: `MERGE INTO ... ON target.ticker = source.ticker AND target.stmt = source.stmt AND ...` | **Physical partition column AND merge key** — the deepest, most load-bearing usage in the system |
| `market_prices_daily` | `10__ingestion/12__fetch_market_data.py:118`: liquid-clustered on `(ticker, date)` (deliberately not `PARTITIONED BY` — a documented small-file-problem fix, not a de-prioritization of ticker). `:378-380`: `MERGE INTO ... ON t.ticker = s.ticker AND t.date = s.date` | Clustering key + merge key |
| `stock_splits` | `12__fetch_market_data.py:507-509`: `MERGE INTO ... ON t.ticker = s.ticker AND t.split_date = s.split_date` | Merge key |
| `fx_rates_daily` | `12__fetch_market_data.py:700-702`: `MERGE INTO ... ON t.base = s.base AND t.quote = s.quote AND t.date = s.date` | **Not ticker-keyed at all** — keyed on currency pair, a real, existing precedent for a non-ticker key elsewhere in the same file |
| **yfinance itself (the upstream source)** | `12__fetch_market_data.py:89-92`: "the bare ticker alone either 404s or can resolve to an unrelated Yahoo-side symbol... this mapping is used ONLY at the `yf.download()` call boundary." `yf.download()`'s own public API takes ticker/symbol strings only — no ISIN parameter exists anywhere in yfinance | **Structural constraint independent of this project's choices** — even a `MIC:ISIN`-keyed system must resolve a Yahoo-compatible ticker symbol before it can fetch a single price, because the upstream data source itself has no ISIN-based lookup. Ticker cannot be fully eliminated from the market-data path, only demoted from "identity" to "required external lookup key" |
| `financials_metrics`/`financials_intrinsic_value` | `22__derived_metrics.py:1414-1418`: `MERGE INTO ... ON target.ticker=source.ticker AND target.fiscal_year=... AND target.metric=...`, ticker the leading key in every `Window.partitionBy("ticker", ...)` (10+ call sites). `23__intrinsic_value.py:1086-1092`/`1255-1259`: two more `MERGE ... ON target.ticker=source.ticker AND ...` | Merge/window key, same depth as `financials` |
| `financials` dedup notebooks | `21d__dedup_clean_table.py:52`: `KEY=["ticker","stmt","concept","fiscal_year","period_type"]`; `21f__dedup_balance_sheet.py:47`: `BS_KEY=["ticker","stmt","concept","period_end"]`; `21g__dedup_flow_orphans.py:54`: `FLOW_KEY=[...]` | Ticker is the mandatory leading component of every dedup key across the whole `21*` chain, not just the main merge |
| `fundamentals_screener` | `urls.py:20-22`: `path("<str:ticker>/", views.company_detail, ...)` + 2 sibling routes; `views.py:771`: `company_detail(request, ticker)`; DuckDB repositories parameterize by ticker throughout (`repositories/companies.py`, `WHERE ticker = ?`). **~781 real `ticker` occurrences across 23 files, 0 genuine `issuer_id`/`listing_id`/`mic`/`isin` hits** (confirmed by direct grep — the only near-matches were `isinstance`/pandas `.isin()` false positives) | Public URL/routing key — and per CLAUDE.md's own "External consumers" section, route shapes are **a versioned public API contract** the external consumer (`alopezm_my_website`) couples against; changing it is an explicit breaking change requiring a version bump |
| Streamlit app | ~336 real `ticker` occurrences across 16 files (`views/company.py`, `views/overview.py`, `lib/data.py`, `lib/render.py`, ...), 0 genuine identity-model hits | Same sole-identity-key pattern as `fundamentals_screener`, not a versioned external contract though |
| `identity.py` | `make_listing_id(mic: str, ticker: str) -> str` | Ticker is a required positional argument of the function itself today |
| `tests/` | `tests/test_issuer_listing_identity.py` is the only test with real `issuer_id`/`listing_id`/`mic` fixtures — and even there, **no fixture anywhere carries an ISIN** (its 4 EU pilot fixtures have `mic` + `lei` only) | A real, current gap: nothing in the test suite exercises ISIN as an identity field at all yet |

**Honest characterization**: ticker-as-identity is not superficial. It is the real, physical
MERGE/window/partition key in every core table — `financials`, `financials_metrics`,
`financials_intrinsic_value`, `market_prices_daily`, `stock_splits`, `market_cap_asof`,
`config.tickers` — and the sole company key in both frontends (~1,100+ combined real
occurrences across `fundamentals_screener` and Streamlit), against **zero** genuine
`issuer_id`/`listing_id`/MIC/ISIN penetration anywhere outside `identity.py`, one unused
`config.tickers` schema stub, and one dedicated unit test file. This is genuinely **HIGH**
migration cost territory for a full re-key of anything that exists today (§9/§19) — not a
cosmetic rename — even though, per §1's correction, the identity columns themselves are
currently dead weight (populated for no production row), which is precisely why redefining
their *target* representation before anything starts consuming them is cheap and low-risk right
now, and would stop being cheap the moment a real consumer (e.g. Phase 5.3's admission layer)
starts writing to them under the wrong definition.

## 3. Define the three concepts precisely

- **Issuer**: the legal/reporting entity. Already correctly modeled as `issuer_id =
  SOURCE:SOURCE_ENTITY_ID` (ADR-0010) — financials are issuer-level data (one filing per
  reporting entity, regardless of how many markets it lists on).
- **Listing / instrument**: one specific security admitted to trading on one specific venue.
  FIRDS' own `RefData` grain (one record per `(ISIN, MIC)`) *is* this concept, directly.
- **Ticker**: a mutable, venue-assigned, human-facing trading symbol. Not an identity — a
  display/lookup convenience, already the project's own stated principle (ADR-0010's Magna/
  Mistras `MG` collision; `identity.py`'s `(ticker, market)` composite for `config.tickers`).

## 4. Option A — `MIC:TICKER`

**Advantages** (real, not hypothetical): human-readable (`XPAR:ALO` vs. `XPAR:FR0010220475`);
zero migration cost today (§2's table is exactly what already exists); matches every existing
US/CA table, `fundamentals_screener`'s public URL contract, and the Streamlit app unchanged.

**Risks** (real):
- **Ticker changes while the security doesn't** — §6 below is a real, verified example, not
  hypothetical.
- **FIRDS provides no ticker at all** (re-confirmed §1, Phase 5.2c §10) — every FIRDS-sourced
  candidate needs an extra resolution hop (OpenFIGI) purely to populate this field.
- **Ticker collisions across markets** — already the founding motivation for `listing_id`
  itself (ADR-0010's `MG` Magna/Mistras case).
- Share-class ambiguity is a real, open risk **if** ticker is the only field distinguishing
  classes — §8 below shows two share classes actually have two *different* tickers today
  (`VOW`/`VOW3`), so this specific risk did not materialize in the one real example checked,
  but is not proven absent in general (an issuer using the *same* ticker root with only a
  suffix/class-code convention elsewhere could still collide under a naive scheme — not tested
  here, flagged as **OPEN QUESTION**).

## 5. Option B — `MIC:ISIN`

**Advantages** (real): matches FIRDS' own native `(ISIN, MIC)` key with zero transformation
(§1/§7); ISIN is the security's standing regulatory identifier, more stable than a trading
symbol; naturally, structurally distinguishes every real multi-venue case already found (§7)
and every real share-class case (§8) without any extra logic.

**Risks** (real): human unreadability (§12); still requires a ticker-resolution step for
**display and for yfinance ingestion** regardless of what the identity key is (§2's yfinance
row) — `MIC:ISIN` does not eliminate the need for a ticker anywhere in the system, it only
moves ticker from "identity" to "attribute"; ISINs are not perfectly immutable either — they
can occasionally be reissued on certain corporate restructurings (e.g., re-domiciliation) — a
real caveat, not assumed away, though no such case was found or tested in this pass.

## 6. Real ticker-change test

**REAL DATA TEST + SECONDARY SOURCE (combined).** France Télécom S.A. rebranded to Orange S.A.
on 1 July 2013 — the **same legal entity**, not a merger or re-listing. Multiple independent
public financial-data sources confirm the security's ISIN, **`FR0000133308`**, is unchanged
across the rename — one source's own listing is literally titled *"ACTION ORANGE (EX FRANCE
TELECOM) (FR0000133308 - ORA)"*, i.e. the exact same ISIN under both the old and new name. The
Euronext Paris ticker changed from **`FTE`** (France Télécom) to **`ORA`** (Orange) at the same
time.

Live re-verification this session (**VERIFIED FACT**, OpenFIGI queried directly,
`ID_ISIN=FR0000133308`, `micCode=XPAR`): today's record resolves to `name: "ORANGE"`,
`ticker: "ORA"` — confirming the ISIN is still live and currently mapped to the post-rename
ticker. OpenFIGI does not expose historical ticker snapshots, so the pre-2013 `FTE` ticker
claim itself rests on the secondary financial-data sources above, not on a live regulatory
API's own historical record — flagged honestly rather than overstated as independently
regulator-verified.

**Result of the test, as specified**: under `MIC:TICKER`, this listing's identity would have
changed from `XPAR:FTE` to `XPAR:ORA` in 2013 — a real identity break for the same underlying
security. Under `MIC:ISIN`, it would have remained `XPAR:FR0000133308` throughout. This is
exactly the failure mode §13's stability requirement is meant to catch.

## 7. Real multi-venue (multi-MIC-per-ISIN) test

**VERIFIED FACT**, reusing the exhaustive per-ISIN FIRDS parse already done in Phase 5.2d (all
`RefData` records for all four pilots, not a sample): every single pilot ISIN legitimately
spans many MICs — FCC 35, Alstom 45, New Amsterdam Invest 16, Fincantieri 41. `MIC:ISIN`
distinguishes every one of these by construction (it *is* FIRDS' own per-venue grain); no
special-case logic is needed. `MIC:TICKER` would, in principle, distinguish them too (the
ticker doesn't change per venue for a given security in the data observed), **but** FIRDS
itself supplies no ticker for any of these records — so under `MIC:TICKER`, all 35–45 venue
records per pilot would need the *same* externally-resolved ticker attached before any of them
could be keyed at all, whereas `MIC:ISIN` requires nothing extra.

## 8. Real share-class test

**REAL DATA TEST**, OpenFIGI queried live this session for both Volkswagen share classes on
Xetra (`micCode=XETR`):

| Class | ISIN | Ticker | `securityType` (OpenFIGI) |
|---|---|---|---|
| Ordinary | `DE0007664005` | `VOW` | Common Stock |
| Preference | `DE0007664039` | `VOW3` | Preference |

Same issuer (Volkswagen AG), same venue group, two distinct ISINs, two distinct tickers.
**Both `MIC:TICKER` and `MIC:ISIN` correctly distinguish these two real classes** in this
specific case — the tickers happen to differ too (`VOW` vs. `VOW3`), so this example does not
by itself prove `MIC:TICKER` unsafe for share classes. It does confirm `MIC:ISIN` works
correctly with zero special-casing, and leaves open (flagged **OPEN QUESTION**, not tested)
whether some other issuer's share classes share a ticker root closely enough to risk collision
under a naive ticker-based scheme.

## 9. Market-data impact assessment

**RECOMMENDATION-supporting analysis**, per subsystem, LOW/MEDIUM/HIGH:

| Subsystem | Impact of moving to `MIC:ISIN` | Why |
|---|---|---|
| `financials`/`financials_metrics`/`financials_intrinsic_value` | **HIGH** | Physical `PARTITIONED BY (ticker, stmt)` + `MERGE ON ticker` (§2) — re-keying means rewriting the table's physical layout, not just adding a column |
| `market_prices_daily` | **HIGH** | Liquid-clustered on `(ticker, date)`, `MERGE ON ticker+date`, and yfinance's own ingestion boundary is structurally ticker-bound regardless (§2) — a re-key here doesn't remove the ticker dependency, it adds a second key alongside it |
| `stock_splits` | **MEDIUM** | Same `MERGE ON ticker` pattern as prices, but a much smaller/sparser table |
| `market_cap_asof` | **MEDIUM** | Derived from `market_prices_daily` + `financials`, inherits both tables' ticker-keyed grain |
| `fx_rates_daily` | **N/A** | Already not ticker-keyed (`base`/`quote`/`date`) — a real, existing precedent that non-ticker keys already work in this codebase |
| `config.tickers` | **LOW** | Already has nullable `issuer_id`/`mic`/`listing_id` columns (ADR-0010); adding an `isin` column is equally additive. The table's own physical key is `.partitionBy("ticker")` (`02__tickers_master.py:1063`), which stays unchanged — `listing_id`'s definition and the table's own physical partition column are two different things, and this recommendation only touches the former |
| `fundamentals_screener` | **HIGH for routing, LOW if ticker stays the URL key** | URL routes are a versioned public API contract (`<str:ticker>/`); as long as ticker remains the *routing* key (even if `MIC:ISIN` becomes the internal join key upstream), no frontend contract breaks |
| Streamlit app | **MEDIUM** | Ticker-keyed throughout `lib/`/`views/` (not exhaustively re-verified line-by-line in this pass — flagged for the codebase-inventory follow-up), but not a versioned external contract like `fundamentals_screener`, so lower stakes to touch later |
| Tests | **LOW** | Fixture-based, would need new ISIN/MIC fixture fields but no structural rewrite |

## 10. Fundamentals impact

**VERIFIED FACT**, already an explicit ADR-0010 decision, not new to this pass: "Financials are
modeled as issuer-level data ... market prices/market cap are modeled as listing-level data."
This pass's finding doesn't change that split — it only affects how the *listing* half of it is
keyed. Fundamentals should (and per ADR-0010, already conceptually do) key off `issuer_id`, not
`listing_id`, regardless of how this ADR resolves; the `MIC:TICKER`-vs-`MIC:ISIN` question is
scoped entirely to the listing/market-data side.

## 11. Ticker's role if `MIC:ISIN` is adopted for new (FIRDS-sourced) listings

**RECOMMENDATION.** Ticker becomes a mutable, resolved attribute of a listing, not part of its
identity: populated at admission time (via OpenFIGI, per ADR-0011's own chain) and re-resolved
on a defined cadence or on corporate-action detection, rather than baked into the identity
string. A ticker change (like Orange's, §6) would update the attribute in place without
changing `listing_id` — exactly the property §13 asks for. Missing/stale ticker becomes a
data-quality gap on an attribute, not an identity crisis.

## 12. Human readability

**Acknowledged, not a rejection reason** (per the explicit instruction). `XPAR:FR0010220475`
is materially less legible than `XPAR:ALO`. The recommended resolution (§22) keeps this
readability where it already exists (US/CA, `fundamentals_screener` routing) by not touching
those systems, while adopting the more correct identity for the part of the system (European,
FIRDS-sourced listings) where readability was never free in the first place — a FIRDS-sourced
listing needs an OpenFIGI ticker-resolution hop either way, so there's no readability regression
being introduced by this that Phase 5.1/ADR-0011's chain didn't already require.

## 13. Identity stability requirement

**RECOMMENDATION**, stated as the central argument: *"A listing identifier should remain
stable when the market symbol changes, provided the underlying instrument and venue remain the
same."* §6's real Orange test shows `MIC:TICKER` fails this requirement (`XPAR:FTE` →
`XPAR:ORA`, a real historical break) and `MIC:ISIN` satisfies it (`XPAR:FR0000133308`,
unchanged). This is the one piece of direct evidence in this document that most cleanly
resolves the underlying question — everything else is architecture-fit or migration-cost
reasoning; this is an observed historical fact matching the failure mode being evaluated.

## 14. Historical data

**INFERENCE**, consistent with §13: under `MIC:TICKER`, a 2012 France Télécom filing and a 2026
Orange filing would carry two different `listing_id`s for what is, in reality, one continuously
existing listing — a historical join/lookup would silently miss one side unless a ticker-alias
table were separately maintained. Under `MIC:ISIN`, the same `listing_id` covers both periods
correctly with no alias table needed. Not tested against this project's own historical data
(the four EU pilots have no observed ticker change in their own history), so this is reasoned
from the Orange evidence, not from an in-repo regression case — labeled as inference, not a
verified fact about this project's own data.

## 15. Multiple listings per issuer

**VERIFIED FACT**, consistent with §7 and ADR-0010's existing model: `issuer_id` (one per
issuer) naturally fans out to many `listing_id`s (one per real venue admission), and ticker
attaches to the listing as an attribute, not the issuer — matches the already-adopted
`issuer_id`/`listing_id` split; this pass doesn't change that shape, only the listing key's
internal representation.

## 16. Interaction with the Phase 5.2d primary-listing rule

**VERIFIED FACT.** The primary-listing rule from Phase 5.2d (`IssrReq = true`, tie-broken by
earliest `FrstTradDt`) operates entirely on FIRDS' native `(ISIN, MIC, IssrReq, FrstTradDt)`
fields — it never references ticker at all. Under `MIC:ISIN`, the rule's own output (a winning
`(ISIN, MIC)` pair) *is* the `listing_id`, with zero extra step. Under `MIC:TICKER`, the same
rule still runs unchanged, but its output must then pass through an extra ticker-resolution hop
(OpenFIGI) before a `listing_id` can be constructed at all — the primary-listing rule itself is
unaffected either way, but `MIC:ISIN` removes one dependency from the pipeline that produces it.

## 17. Data-source identity alignment

**VERIFIED FACT**, synthesizing what each source's own schema natively provides (re-confirmed
this session for FIRDS/OpenFIGI, cited from Phase 5.2/5.2c for GLEIF/filings.xbrl.org):

| Source | ISIN | MIC | LEI | Ticker | Natural identity |
|---|---|---|---|---|---|
| ESMA FIRDS | Native (`FinInstrmGnlAttrbts/Id`) | Native (`TradgVnRltdAttrbts/Id`) | Native (`Issr`) | **Absent** | `(ISIN, MIC)` |
| GLEIF | Via `LEI→ISIN`/`ISIN→LEI` lookups | Absent | Native, primary key | Absent | `LEI` |
| filings.xbrl.org | Absent | Absent | Native (`/entities/{LEI}`) | Absent | `LEI` |
| OpenFIGI | Accepted as query input, not returned directly | Accepted as query input (`micCode`) | Absent | Returned as an output attribute | `FIGI` (its own minted identifier) — ticker/ISIN/MIC are all just query inputs or output attributes, never OpenFIGI's own primary key |
| yfinance (market-price source) | **Not supported at all** — no ISIN parameter exists in the API | Not supported | Not supported | **The only supported query key** | Ticker (Yahoo-specific symbol) |

**The preferred canonical identity should minimize translation between sources** (the prompt's
own stated criterion): four of five sources here are ISIN/LEI-native; exactly one
(yfinance — the market-price source) is ticker-only and cannot be changed (§2). This is the
strongest structural argument in this document for `MIC:ISIN` as the *identity*, with ticker
kept as a required, permanent, but non-identity attribute specifically for the yfinance
boundary.

## 18. Performance / storage

**Qualitative assessment, as requested — no benchmarking performed.** An ISIN string
(12 characters, fixed-format `CCNNNNNNNNNC`) is not meaningfully larger or slower to index than
a variable-length ticker (typically 1–5 characters, occasionally longer). No meaningful
storage/indexing disadvantage is expected; not worth over-engineering or benchmarking further
per the prompt's own instruction.

## 19. Migration impact — is changing `listing_id` now cheap?

**RECOMMENDATION.** Changing the *definition* used for **new, not-yet-existing** European
listings is cheap — cheaper, in fact, than earlier phases assumed: §1's corrected inventory
shows `mic`/`listing_id` are populated for **zero** rows in production today, on either side of
the US/CA vs. EU split (not the 4-EU-pilots-populated picture this document originally started
from). Nothing reads either column for a real decision anywhere in the codebase yet. Re-keying
the **existing** US/CA universe or the four core financial/price tables (§9) is genuinely not
cheap — real physical partition/cluster/merge keys, a real public URL contract — and is
explicitly **not** proposed by this document. The two are separable: defining `MIC:ISIN` as the
identity for new FIRDS-sourced listings today does not require touching a single existing row,
and there is no live consumer whose behavior would even change.

## 20. Decision criteria — weighted qualitative assessment

| Criterion | `MIC:TICKER` | `MIC:ISIN` |
|---|---|---|
| 1. Identity stability | Fails (§6, §13) | Passes |
| 2. Collision resistance | Weaker (ADR-0010's own `MG` case) | Stronger |
| 3. Alignment with FIRDS | Requires extra hop (§1, §7) | Native (§7) |
| 4. Alignment with GLEIF | Neutral (GLEIF doesn't natively carry MIC either way) | Neutral |
| 5. Alignment with ESEF/filings.xbrl.org | Neutral (LEI-keyed, not listing-keyed at all) | Neutral |
| 6. Ticker-change resilience | Fails (§6) | Passes |
| 7. Multi-listing support | Works, needs ticker resolved per venue | Works natively (§7) |
| 8. Share-class support | Works in the one case tested (§8), untested generally | Works natively (§8) |
| 9. Market-data (yfinance) compatibility | Native | Still requires ticker as an attribute regardless (§2, §17) — neither option removes this |
| 10. Frontend usability | Native today | Requires keeping ticker as a separate routing/display field (§12) |
| 11. Historical correctness | Fails on rename (§14) | Passes |
| 12. Migration complexity (existing data) | None | Real if applied retroactively; none if scoped to new listings only (§19) |
| 13. Human readability | Better | Worse, mitigated by keeping ticker as a display attribute |

No numerical score manufactured, per the prompt's own instruction — the qualitative pattern is
clear: `MIC:ISIN` wins on identity-correctness criteria (1, 2, 3, 6, 7, 8, 11), `MIC:TICKER`
wins only on criteria that are really about *ticker as a convenience attribute* (9, 10, 13),
which §22's recommendation preserves either way by keeping ticker as a field regardless of
which one is the identity key.

## 21. Possible outcomes

Per the prompt's own instruction not to assume one of the first two must win: a third model was
considered — `listing_id` as a synthetic internal ID with `(MIC, ISIN)` as its defining
attributes, mirroring ADR-0010's own already-rejected "synthetic global `issuer_id`" alternative.
**Rejected for the same reason ADR-0010 rejected it for `issuer_id`**: adds a new ID-minting
system with its own persistence/uniqueness concerns for no benefit over the legible, directly
composable `MIC:ISIN` string — a string key that already carries no ambiguity doesn't need a
synthetic surrogate on top of it.

## 22. Recommended Generation-1 model

**RECOMMENDATION**, not a decision (ADR-0012 stays Proposed):

```
issuer_id  = SOURCE:SOURCE_ENTITY_ID          (unchanged, ADR-0010)
listing_id = MIC:ISIN                          (new listings, e.g. any FIRDS-sourced admission)
ticker     = mutable attribute of a listing,   (required for yfinance ingestion + frontend
             resolved via OpenFIGI at           routing/display; re-resolved on a defined
             admission time                     cadence or on corporate-action detection)
```

**Scoped explicitly to new (European/FIRDS-sourced) listings.** The existing ~2,662 US/CA
`config.tickers` rows and the four core financial/price tables are **not** touched or re-keyed
by this recommendation — `MIC:TICKER` remains their real, physical, unchanged key (§9/§19). This
avoids manufacturing a migration this document explicitly wasn't asked to perform, while still
giving Phase 5.3 a correctly-keyed foundation to build the admission layer on, rather than
inheriting a known-fragile key from the start.

## 23. Open questions (not resolved by this document)

- Whether a same-ticker-root share-class collision risk (§4, §8) exists anywhere in the real
  European universe FIRDS would surface — not tested, only the Volkswagen case (which has
  distinct tickers) was checked.
- Whether the eventual US/CA re-key (already deferred by ADR-0010 itself) should also target
  `MIC:ISIN` once undertaken, for consistency — not decided here; ADR-0010's own re-key design
  doc (`docs/phase5-identity-listing-model.md`) would need revisiting if so.
- The exact mechanics of ticker re-resolution/staleness detection for `MIC:ISIN`-keyed listings
  (§11) — designed only at the level of "an attribute, resolved via OpenFIGI," not specified
  further.
- Whether ISIN reissuance on corporate restructuring (§5's caveat) has ever actually affected
  any of the four pilots or any plausible near-term European candidate — not investigated.

## What this document does not do

- Does not change `identity.py`, any Delta table schema, `config.tickers`, the DAG,
  `fundamentals_screener`, or the Streamlit app.
- Does not implement `MIC:ISIN` anywhere.
- Does not perform any production re-key migration.
- Does not accept ADR-0012 — that remains the repo owner's own call.
- Does not begin Phase 5.3.

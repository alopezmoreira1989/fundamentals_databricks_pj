# Phase 5.1 — European (filings.xbrl.org) fundamentals adapter: pilot results

Companion to [ADR-0009](adr/0009-multi-market-fundamentals-ingestion-framework.md) and
[ADR-0010](adr/0010-issuer-listing-identity-model.md). This document records what Phase 5.1
actually shipped and the real, live evidence behind every design decision — the first pass that
moves real, non-US fundamentals data through the pipeline end to end.

## 1. Architecture

**Files changed/added:**
- `fundamentals_pipeline/sources/eu_current.py` (new) — pure, network-free adapter logic:
  `select_filing_for_period` (amendment rule), `is_consolidated_fact` (segment discrimination),
  `is_current_period_fact` (comparative-year discrimination), `extract_source_facts`,
  `EU_CANONICAL_MAPPING`, `map_source_fact_to_canonical`, `entity_from_pilot`.
- `fundamentals_pipeline/sources/__init__.py` — re-exports the above.
- `fundamentals_pipeline/10__ingestion/16__fetch_eu_xbrl.py` (new) — `EUCurrentSource` (the real
  `FundamentalsSource` implementation, actually called — unlike `SECXBRLSource`, still a proof)
  and `process_eu_entity`, the real ingestion entry point.
- `fundamentals_pipeline/20__transformation/21__clean_and_merge.py` — added `"ESEF"` to the
  three existing SEC-form-type allowlists (the one necessary change for EU rows to reach
  `financials`, not just `financials_raw`).
- `fundamentals_pipeline/sources/registry.py` — `EU_CURRENT.access_status`: `RESEARCH_ONLY` →
  `ACTIVE`, now that a real adapter has shipped and been validated.
- `tests/test_sources_eu_current.py` (new, 20 tests) — fixture-only, using real captured values.
- `tests/test_sources_registry.py` — updated the `EU_CURRENT` status expectation.

**Adapter design**: `filings.xbrl.org → EUCurrentSource → SourceEntity/SourceFiling/SourceFact →
existing mapping → financials_raw → financials` — exactly the shape ADR-0009's Phase 3 abstraction
was designed for, with no `EuropeanFinancialsProcessor`/parallel pipeline. `sources/` stays
network-free (the constraint `base.py`'s own docstring already states); `EUCurrentSource`'s real
HTTP calls live in the ingestion notebook, mirroring exactly where `SECXBRLSource`'s SEC-specific
network code lives.

**Registry/mapping changes**: no new source_id invented per country — Spain/France/Netherlands/
Italy all route through the single `EU_CURRENT` source_id, per ADR-0009's original design (country
lives on entity/listing/filing metadata, not source identity). `EU_CANONICAL_MAPPING` adds five
`MappingDecision`s (all `ACCEPTED`/`DIRECT`) — no changes to `mapping.py`'s model itself.

**DAG**: NOT wired into `91a__pipeline_pre22.py`'s scheduled `%run` chain in this pass —
`16__fetch_eu_xbrl.py` was run standalone (personal Databricks Repo clone + `databricks jobs
submit`, the same mechanism used for Phase 3/5.0's real verification) for the smoke test. The
minimal change a future phase would need to schedule it: one `%run "../10__ingestion/
16__fetch_eu_xbrl"` line added to `91a__pipeline_pre22.py`, most naturally right after `11`'s own
`%run` (no other DAG restructuring required — `21`/`21b`/`21f` already handle `ESEF` rows
correctly, confirmed by this pass's real smoke test).

## 2. Source behavior (real, live-verified)

- **Endpoints**: `GET https://filings.xbrl.org/api/entities/{LEI}/filings` (JSON:API, returns
  `data[].attributes.{fxo_id, period_end, date_added, processed, error_count, warning_count,
  sha256, json_url, report_url, viewer_url, package_url, inconsistency_count, country}`); filing
  facts at `GET https://filings.xbrl.org{json_url}` (OIM xBRL-JSON,
  `{"documentInfo": {...}, "facts": {"fact-N": {"value", "decimals", "dimensions": {"concept",
  "entity", "period", "unit", ...}}}}`).
- **Filing selection rule** (`select_filing_for_period`): group by `period_end`; among filings
  with `error_count == 0` AND a truthy `json_url`, the latest `processed` timestamp wins; every
  other ingestible candidate is `"superseded"` (not a failure); a non-ingestible filing (missing
  `json_url` or `error_count > 0`) is always reported, never silently dropped.
- **Amendment rule**: confirmed against TWO independent real cases — New Amsterdam Invest
  (`...-NL-0` processed 2026-05-03 → superseded by `...-NL-1` processed 2026-06-11) and, newly
  discovered this pass, **Fincantieri's own FY2024 amendment** (`...-IT-0` processed 2024-04-02 →
  superseded by `...-IT-1` processed 2025-04-15). Both correctly resolved live.
- **Entity resolution**: no fuzzy matching — the four pilot (ticker, LEI, MIC) triples are
  hardcoded, already-verified identity (ADR-0010), matching `/api/entities`'s own confirmed lack
  of a ticker/exchange field.
- **xBRL-JSON structure**: confirmed live against FCC's real FY2024 filing (512 facts) —
  `documentInfo.namespaces` lists `ifrs-full`/`iso4217`/`scheme` (the LEI URI scheme); each fact's
  `dimensions.entity` is `"scheme:<LEI>"`; `dimensions.period` is either a single ISO datetime
  (instant) or a `"start/end"` interval (duration); `dimensions.unit` is `"iso4217:<CCY>"`.
- **Quality/error handling**: `error_count`/`json_url` gate ingestibility (see above);
  `warning_count` does NOT block ingestion (both real amendment filings and NAI's whole filing
  history carry non-zero warnings — 14 for NAI's 2025 pair — and were correctly ingested).
  Fincantieri's real FY2025 filing (`error_count=0`, `json_url=null`) produced an explicit,
  itemized `ingestion_failures` row (`error_type="missing_json_url"`, `step="discover_filings"`)
  rather than silently vanishing — confirmed live in the real run.

## 3. Pilot results (real Databricks smoke test, 2026-08)

| Issuer | Country | Ticker | MIC | LEI | Fiscal years ingested | Currency | Accounting standard |
|---|---|---|---|---|---|---|---|
| Fomento de Construcciones y Contratas, S.A. | Spain | FCC | XMAD | 95980020140005178328 | 2020–2024 (5) | EUR | ifrs-full (verified: `ifrs-full` present in `documentInfo.namespaces`) |
| Alstom | France | ALO | XPAR | 96950032TUYMW11FB530 | 2023–2026 (4, FY ends March 31) | EUR | ifrs-full |
| New Amsterdam Invest N.V. | Netherlands | NAI | XAMS | 724500JXEXUGEATP5L52 | 2023–2025 (3) | EUR | ifrs-full |
| Fincantieri S.p.A. | Italy | FCT | MTAA | 8156005BDF49128B6239 | 2021, 2022, 2024 (3; FY2025 failed — see below) | EUR | ifrs-full |

**A correction to earlier (Phase 5.0) research**: that pass's live-research summary described
Fincantieri as having "5 fiscal years, 2021–2025." The real, itemized filing list actually has
only **4 distinct period_ends** (2021, 2022, 2024 [×2, an amendment], 2025 [`json_url=null`]) —
Fincantieri has **no FY2023 filing on filings.xbrl.org at all**. The earlier "five fiscal years"
phrasing was an AI-summarization imprecision (conflating filing count with distinct-year count),
caught and corrected here by the real per-fiscal-year smoke test output, not asserted from the
earlier research pass.

**Selected filings, with amendment/comparative evidence**:
- NAI FY2025: `724500JXEXUGEATP5L52-2025-12-31-ESEF-NL-1` (the amended version) — confirmed by
  running the pilot twice; both runs consistently selected `-NL-1`.
- Fincantieri FY2024: `8156005BDF49128B6239-2024-12-31-ESEF-IT-1` (the amended version).
- Alstom: all four fiscal years' `period_end` land on `-03-31` in `financials` — the non-calendar
  fiscal year is preserved exactly, not normalized to a calendar year (`fy` = the ending
  calendar year, e.g. `period_end=2025-03-31` → `fy=2025`, matching the pipeline's existing
  convention for non-December SEC filers like AAPL).

**Consolidated/standalone**: every mapped fact passed the `is_consolidated_fact` check (`{concept,
entity, period, unit}` exactly, no extra axis) — confirmed live that a dimensional fact (NAI's
`ifrs-full:ComponentsOfEquityAxis` equity breakdown) is correctly excluded.

**Canonical metrics successfully mapped** (real values, EUR):

| Ticker | FY | Revenue | Net Income | Total Assets | Cash & Equivalents |
|---|---|---|---|---|---|
| FCC | 2024 | 9,071,416,000 | 429,865,000 | 14,235,959,000 | 1,849,617,000 |
| ALO | 2025 | *(not mapped — see §4)* | 179,000,000 | 34,586,000,000 | 2,274,000,000 |
| NAI | 2025 | *(not mapped)* | 3,892,000 | 136,253,000 | 13,485,000 |
| FCT | 2024 | *(not mapped)* | 27,377,000 | 9,562,408,000 | 684,458,000 |

## 4. Mapping report (honest, not overclaimed)

Five `MappingDecision`s defined, all `ACCEPTED`/`DIRECT`: `Revenue` (`ifrs-full:Revenue`),
`Net Income` (`ifrs-full:ProfitLossAttributableToOwnersOfParent`), `Net Income (incl NCI)`
(`ifrs-full:ProfitLoss`), `Total Assets` (`ifrs-full:Assets`), `Cash & Equivalents`
(`ifrs-full:CashAndCashEquivalents`).

**A real, unplanned finding from the smoke test: `ifrs-full:Revenue` only matched for FCC.**
Alstom, New Amsterdam Invest, and Fincantieri's real filings do not tag their top-line revenue
figure with the generic `ifrs-full:Revenue` concept — each likely uses either a more specific
IFRS revenue-recognition tag or a company-specific taxonomy extension (NAI, a real-estate
investment vehicle, visibly uses `ifrs-full:RentalIncomeFromInvestmentProperty` for its primary
income line, confirmed during research — a concept deliberately NOT added to
`EU_CANONICAL_MAPPING`, since mapping a company-specific line item to the generic "Revenue"
canonical concept without individually verifying it for each company would violate "don't map
concepts merely because their names look similar"). Result: **Revenue is `NULL` for 3 of the 4
pilots** — this is the "NULL > questionable value" principle working exactly as designed, not a
bug to silently patch. `Net Income`, `Total Assets`, and `Cash & Equivalents` mapped cleanly for
all four issuers.

`Net Income (incl NCI)` (`ifrs-full:ProfitLoss`) mapped for all four pilots in `financials_raw`,
but is collapsed into the canonical `"Net Income"` label by `21`'s existing `CONCEPT_SYNONYMS`/
`CONCEPT_PRIORITY` mechanism before reaching `financials` — the same mechanism already used for
SEC filers, unmodified.

**A raw-layer near-duplication, confirmed harmless, worth fixing in a future pass**: several
concepts (most visibly Alstom's `ifrs-full:ProfitLoss`) appear as 2–4 separate xBRL-JSON facts
per fiscal year with byte-identical `(concept, period, unit)` dimensions and identical values —
confirmed live (all duplicate values matched exactly, e.g. Alstom FY2023 Net Income (incl NCI)
appeared 4 times, always `-108,000,000`). This is a genuine ESEF/Inline-XBRL characteristic (the
same fact value legitimately tagged in more than one place in the human-readable report — e.g.
the primary statement and an MD&A/notes repetition — sharing identical OIM dimensions since xBRL-
JSON's `dimensions` has no "where in the document" field). `extract_source_facts` does not dedupe
identical-dimension fact IDs, so `financials_raw` carries the redundant rows; `21`'s pre-existing
per-`(ticker, stmt, concept, fiscal_year)` dedup Window safely collapses them to one correct row
before `financials` (confirmed: `financials` shows exactly one row per ticker/concept/year, correct
values). Not a correctness bug, but a real, reportable inefficiency worth a future
`extract_source_facts` refinement (dedupe by exact `(concept, period, unit, value)` before
emitting).

## 5. Validation

- **Local**: `pytest -q` — 20 new fixture tests + the updated registry test pass; full existing
  suite stays green (`ruff check` clean on every touched/new file).
- **Live API checks**: real filing lists fetched for all four pilots via `requests` (not
  WebFetch-summarized, for the pieces where precision mattered); real xBRL-JSON structure
  confirmed against FCC's and New Amsterdam Invest's actual files before writing any parsing code.
- **Real Databricks smoke test** (personal Repo clone + `databricks jobs submit`, deleted after
  use): ran `16__fetch_eu_xbrl.py` for real — 105 records, 1 explicit failure (Fincantieri's
  missing-`json_url` FY2025), all 4 pilots represented — then ran `21__clean_and_merge.py` for
  real (safe: its `latest_scrape` filter meant this run touched ONLY the newly-ingested EU rows,
  confirmed by inspecting the orphan-delete step's own scoping before running it), landing exactly
  50 canonical rows in `financials` with correct `source_id="EU_CURRENT"`/`tag_namespace=
  "ifrs-full"` on every row.
- **Idempotency check**: ran the full ingest → merge cycle a SECOND time. `financials_raw` grew
  to 210 rows (append-only by design, matching SEC's own behavior exactly); `financials` stayed at
  exactly 50 rows (0 duplication — the existing MERGE dedup logic works correctly for `EU_CURRENT`
  rows, unmodified); `ingestion_failures` grew to 2 rows (one per run, also append-only by design).
- **`ingestion_failures` check**: the real Fincantieri FY2025 `missing_json_url` case is present,
  itemized, `step="discover_filings"`.
- **SEC regression check**: before/after row counts for `financials_raw`/`financials` by
  `source_id` confirmed `SEC_XBRL`'s 8,049/`NULL` rows completely unchanged; a byte-identical
  diff of every AAPL row in `financials` (value + period_end) against a pre-run snapshot returned
  zero differences, both after the first AND the second `21` run.

## 6. Remaining limitations (stated plainly)

- **Rate limits**: `filings.xbrl.org`'s real rate-limit policy is unknown/undocumented — this
  pilot's request volume (4 entities × ~5 filings × 1 fact-file fetch each ≈ 25 requests) was
  small enough not to surface one; a future universe-scale ingestion would need this researched.
- **Historical coverage**: only what each pilot's real filing list actually contains — FCC 5y,
  Alstom 4y, NAI 3y, Fincantieri 3y-of-5-attempted (one genuine gap year, one genuine retrieval
  failure) — not assumed uniform, not backfilled or estimated.
- **Unresolved source behavior**: the raw-layer near-duplicate fact emission (§4) is understood
  and confirmed harmless but not yet fixed at the source.
- **Unsupported concepts**: only 5 of roughly 500 facts per filing are mapped (deliberately narrow
  per "start with high-confidence concepts"); Revenue specifically has real, non-hypothetical gaps
  for 3 of 4 pilots (§4).
- **Identity limitations**: unchanged from Phase 5.0 — `mic`/`listing_id` are not backfilled for
  the existing US/CA universe, and `config.tickers` still has zero European rows (this pilot
  deliberately never writes there).
- **Currency**: `SourceFact.source_currency` is populated and validated per-filing, but
  `financials_raw`/`financials` still have no currency column to persist it in — an explicit,
  temporary Delta-write-time information-loss boundary (Phase 5.0/5.1's joint decision), not
  fixed in this pass.
- **Amendment limitations**: the selection rule is proven against two real cases in one source
  (`filings.xbrl.org`'s own `processed` timestamp semantics) — not yet tested against a source
  with different amendment semantics (e.g. a future ESAP adapter).
- **Not scheduled**: `16__fetch_eu_xbrl.py` is not wired into the daily `91a` DAG run in this
  pass (§1) — the pilot data landed via a one-time smoke-test run, not a recurring refresh.

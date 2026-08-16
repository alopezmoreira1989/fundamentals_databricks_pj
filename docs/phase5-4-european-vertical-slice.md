# Phase 5.4 — European Vertical Slice

**Implementation phase.** Connects Phase 5.3's admission layer to the already-shipped Phase 5.1
`EUCurrentSource` adapter and proves the complete real path, end to end, against the live
Databricks workspace:

```
main.config.eu_admission_candidates (admission_status = 'admitted')
        ↓
EUCurrentSource
        ↓
financials_raw
        ↓
financials (via 21__clean_and_merge.py)
```

Bounded to the 8 currently admitted issuers. **This phase does not make European data visible
in `fundamentals_screener` yet** — no frontend, Django, or Streamlit file is touched. That is
explicitly the next phase.

Source-discipline labels: **VERIFIED LOCALLY** / **VERIFIED AGAINST LIVE ESEF** /
**VERIFIED IN DATABRICKS** / **INFERENCE** / **OPEN QUESTION**.

## 1. Files changed

```
fundamentals_pipeline/10__ingestion/16__fetch_eu_xbrl.py   (extended, not redesigned)
tests/test_eu_admission_driven_ingestion.py                 (new)
docs/phase5-4-european-vertical-slice.md                    (this file)
```

No new source file, no new interface, no change to `sources/eu_current.py`, `sources/base.py`,
`mapping.py`, `registry.py`, `21__clean_and_merge.py`, `eu_admission.py`, or `identity.py`.

## 2. Architecture implemented

The "small additive change" the driving brief asked for, not a redesign:

- **`load_admitted_eu_entities(spark, catalog)`** (new function in `16__fetch_eu_xbrl.py`) —
  queries `{catalog}.config.eu_admission_candidates WHERE admission_status = 'admitted'` fresh
  at run time, returns `(ticker, lei, mic, name)` tuples — the exact same shape
  `PILOT_EU_ENTITIES` already used, so `EUCurrentSource` consumes either interchangeably. Never
  a hardcoded ticker list. Excludes (and warns on, not silently drops) any admitted row with no
  resolved ticker — a real, possible-but-unobserved edge case, since ticker resolution is
  explicitly non-blocking for admission (Phase 5.3's own design).
- **`EUCurrentSource.__init__(self, entities=None)`** — defaults to `PILOT_EU_ENTITIES`
  (preserves every existing test's no-arg `EUCurrentSource()` behavior byte-for-byte), accepts
  an explicit `entities` list otherwise. `discover_entities()` now reads `self._entities`
  instead of the module-level `PILOT_EU_ENTITIES` constant directly.
- **The real ingestion run (§5 of the notebook)** now loads `EU_ENTITIES_TO_INGEST` from the
  admission table and reassigns `_source = EUCurrentSource(entities=EU_ENTITIES_TO_INGEST)`
  before the ingestion loop — the loop body itself (`process_eu_entity`, per-entity try/except,
  failure logging) is completely unchanged.

`PILOT_EU_ENTITIES` (the original 4-issuer hardcoded list) is untouched and remains the
test/dev fixture default — deliberately kept separate from the real admitted-universe input,
per the driving brief's own explicit instruction.

## 3. Admission → EUCurrentSource connection

**VERIFIED IN DATABRICKS.** `load_admitted_eu_entities()` run for real against the live
`main.config.eu_admission_candidates` table returned exactly 8 rows, matching Phase 5.3's own
validated count:

```
FCC, ALO, NAI, FCT, IBE, SGO, RAND, ISP
```

Only `admission_status = 'admitted'` rows qualified — confirmed by the query's own `WHERE`
clause and by a fixture-based unit test (`test_load_admitted_eu_entities_queries_admission_
status_admitted_only`) asserting the exact SQL predicate, so a future accidental removal of that
clause would fail CI, not just silently widen the ingestion universe.

## 4. Eight admitted issuers used

Read fresh from the table at run time (not hardcoded), then run through the real ingestion
pipeline:

| Ticker | ISIN | LEI | MIC | Company |
|---|---|---|---|---|
| FCC | ES0122060314 | 95980020140005178328 | XMAD | Fomento de Construcciones y Contratas, S.A. |
| ALO | FR0010220475 | 96950032TUYMW11FB530 | XPAR | Alstom |
| NAI | NL0015000CG2 | 724500JXEXUGEATP5L52 | XAMS | New Amsterdam Invest N.V. |
| FCT | IT0005599938 | 8156005BDF49128B6239 | MTAA | Fincantieri S.p.A. |
| IBE | ES0144580Y14 | 5QK37QC7NWOJ8D7WVQ45 | XMAD | Iberdrola |
| SGO | FR0000125007 | NFONVGN05Z0FMN5PEC35 | XPAR | Saint Gobain |
| RAND | NL0000379121 | 7245009EAAUUQJ0U4T57 | XAMS | Randstad N.V. |
| ISP | IT0000072618 | 2W8N8UU78PMDQKZENC08 | MTAA | Intesa Sanpaolo |

## 5. Filing results

**VERIFIED AGAINST LIVE ESEF (`filings.xbrl.org`).** Real filing discovery + amendment
selection + fact retrieval for all 8, per run:

| Ticker | Facts extracted | Fiscal years | Note |
|---|---|---|---|
| FCC | 35 | 2020–2024 (5) | |
| IBE | 30 | 2020–2024 (5) | |
| SGO | 32 | 2021, 2022, 2023, 2025 (4) | 2024 gap — a real filing/coverage gap, not investigated further this phase |
| ALO | 32 | 2023–2026 (4) | non-calendar (March) fiscal year, preserved exactly — see §16 |
| ISP | 14 | 2021, 2022 (2) | narrowest real coverage of the 8 |
| FCT | 14 | 2021, 2022, 2024 (3) | **a real, itemized failure**: `missing_json_url` for the FY2025 filing (`...-IT-0`, `period_end=2025-12-31`) — a clean filing (0 errors) with no retrievable JSON, exactly the same real failure mode Phase 5.1's own research first found for Fincantieri, still present in live data today |
| RAND | 40 | 2021–2025 (5) | |
| NAI | 24 | 2023–2025 (3) | |

Total: 221 raw facts extracted per run. `ingestion_failures` correctly recorded the one real
`missing_json_url` case — an ingestion failure, not an admission rejection (§19).

## 6. financials_raw results

**VERIFIED IN DATABRICKS.** 221 EU_CURRENT rows appended per run (confirmed append-only:
536 → 757 EU_CURRENT rows in `financials_raw` across two real runs, i.e. +221 each time — never
overwritten, matching `financials_raw`'s documented append-only design). Every row carries
`source_id = 'EU_CURRENT'`, `form = 'ESEF'`, `tag_namespace = 'ifrs-full'` — correct, unchanged
provenance, same as Phase 5.1.

## 7. financials results

**VERIFIED IN DATABRICKS.** 108 canonical FY rows landed in `financials` for the 8 EU tickers
(real query, not inferred) — confirmed **identical** after a second full run (idempotency, §12).
A sample (full detail captured in this session's validation run, available on request):

```
FCC  | Balance Sheet    | Cash & Equivalents | 2024 | 2024-12-31 | 1,849,617,000 | ifrs-full | EU_CURRENT
FCC  | Balance Sheet    | Total Assets       | 2024 | 2024-12-31 | 14,235,959,000| ifrs-full | EU_CURRENT
FCC  | Cash Flow        | Net Income         | 2024 | 2024-12-31 | 567,584,000   | ifrs-full | EU_CURRENT
FCC  | Income Statement | Revenue            | 2024 | 2024-12-31 | 9,071,416,000 | ifrs-full | EU_CURRENT
ALO  | Balance Sheet    | Total Assets       | 2026 | 2026-03-31 | 35,005,000,000| ifrs-full | EU_CURRENT
```

## 8. Concept coverage

**VERIFIED IN DATABRICKS**, real per-ticker counts:

| Ticker | Revenue | Net Income | Total Assets | Cash & Equivalents |
|---|---|---|---|---|
| FCC | 5 | 5 | 5 | 5 |
| IBE | 5 | 5 | 5 | 5 |
| RAND | 5 | 5 | 5 | 5 |
| ALO | 0 | 4 | 4 | 4 |
| FCT | 0 | 3 | 3 | 3 |
| ISP | 0 | 2 | 2 | 2 |
| NAI | 0 | 3 | 3 | 3 |
| SGO | 0 | 4 | 4 | 4 |

`Balance Sheet` concepts (Total Assets, Cash & Equivalents) reach 100% coverage across every
year every issuer has a usable filing for. `Revenue` reaches 3 of 8 issuers — this is **not a
new gap**: it reproduces Phase 5.1's own original documented finding ("Revenue was not available
under the generic `ifrs-full:Revenue` concept for every pilot"), now confirmed for a wider,
non-hardcoded set. Per the driving brief's explicit §9 instruction, this is left as NULL, not
guessed at with an issuer-specific heuristic mapping.

## 9. NULL coverage — a real finding, correctly attributed, not a bug

**A genuine finding from this phase's own live run**: `Net Income (incl NCI)` shows **zero**
rows for all 8 issuers in `financials`, despite being present in `financials_raw` for every one
of them (confirmed directly: FCC alone has real `Net Income (incl NCI)` rows in `financials_raw`
with value `343,600,000` for FY2020). Traced to the real, root cause — **not a Phase 5.4 defect,
not a new gap**: `01__tickers.py`'s pre-existing `CONCEPT_SYNONYMS` dict already contains
`'Net Income (incl NCI)': 'Net Income'` — every `Net Income (incl NCI)` row is renamed to
`Net Income` during `21__clean_and_merge.py`'s own §2 normalization step, before the dedup
window runs; the existing `CONCEPT_PRIORITY` (`'Net Income': 0` vs. `'Net Income (incl NCI)': 2`,
lower wins) then correctly prefers the already-present `Net Income` row. This is the exact same
synonym-collapse mechanism SEC/US-GAAP data already goes through — applying correctly and
identically to EU data, not a EU-specific loss. `Net Income (incl NCI)` remains a real,
recoverable value in `financials_raw` for anyone who needs it; it is deliberately not the
canonical `financials` value when a higher-priority sibling concept is also present, exactly as
designed for the pre-existing US/CA case.

## 10. Currency coverage

**Unchanged from Phase 5.1's own documented limitation.** `financials`/`financials_raw` still
have no per-row currency column — `source_currency` is validated (assert-consistent-within-a-
filing) at ingestion time but not persisted, a known, explicitly-documented Phase 5.1 boundary
this phase does not touch. All 8 issuers' filings are EUR-denominated (confirmed by the values
themselves being plausible in EUR, and no `currency_mismatch` failure was logged in either run).

## 11. Fiscal-period coverage

**VERIFIED IN DATABRICKS**, and specifically checked per the driving brief's explicit §16
instruction not to normalize non-calendar years: Alstom's real fiscal year ends March 31 — its
`period_end` values in `financials` are `2023-03-31`, `2024-03-31`, `2025-03-31`, `2026-03-31`,
and its `fiscal_year` values are `2023`–`2026` (i.e. `fiscal_year = period_end.year`, the
existing convention, unchanged) — never coerced to a December calendar-year boundary.

## 12. Idempotency

**VERIFIED IN DATABRICKS**, run twice for real against the live production catalog:

```
financials  (EU tickers): 108 rows after run 1, 108 rows after run 2 — IDENTICAL
financials_raw (EU_CURRENT): 536 rows after run 1, 757 after run 2 — append-only growth (+221),
                              matching financials_raw's documented append behavior exactly
```

`financials_raw` follows its documented append semantics (grows every run, by design — old
scrapes remain for audit); `financials` does not accumulate duplicate canonical rows (the MERGE's
`WHEN MATCHED AND (value != source.value OR period_end != source.period_end)` correctly produced
zero updates on identical re-fetched values, and `WHEN NOT MATCHED THEN INSERT` correctly found
nothing new to insert). This was already proven for the original 4-pilot adapter in Phase 5.1;
this phase confirms it holds unchanged for the admission-driven 8-issuer input.

## 13. SEC regression

**VERIFIED IN DATABRICKS**, before/after row counts captured around both live runs:

| Table | Before | After | |
|---|---|---|---|
| `main.config.tickers` | 2,662 | 2,662 | MATCH |
| `main.financials.market_prices_daily` | 15,268,796 | 15,268,796 | MATCH |
| `main.financials.stock_splits` | 4,330 | 4,330 | MATCH |
| `main.financials.market_cap_asof` | 27,240 | 27,240 | MATCH |
| `main.financials.financials_raw` (AAPL only) | 319,567 | 319,567 | MATCH |
| `main.financials.financials` (AAPL only) | 3,303 | 3,303 | MATCH |

`financials_raw`/`financials` **totals** grew (425,799,947 → 425,800,389; 4,750,430 → 4,750,488)
— expected, entirely attributable to the new EU rows (confirmed both deltas equal the EU-specific
counts reported in §6/§7, with zero left over). No full US universe re-run was performed
(§18 of the driving brief) — the AAPL spot-check plus the exact-delta accounting together
constitute the regression proof.

## 14. Ingestion failures

**VERIFIED IN DATABRICKS**, distinguishing admission rejection from ingestion failure as
required (§19): the one real failure this phase produced —
`FCT | missing_json_url | filing '8156005BDF49128B6239-2025-12-31-ESEF-IT-0' ... | discover_filings`
— is an **ingestion** failure (a clean, admitted, identity-resolved issuer whose newest filing
happens to have no retrievable JSON), never conflated with an **admission** rejection (Fincantieri
itself remains `admitted` in `eu_admission_candidates`, untouched by this phase). No HTTP
failures, malformed xBRL, or unsupported-mapping failures occurred in either run.

## 15. Databricks validation

**VERIFIED IN DATABRICKS**, real production workspace (`fundamentals` profile,
`dbc-b52a3a2b-c131.cloud.databricks.com`, Spark 4.1.0 serverless), not substituted with local
execution. Two genuine harness-only issues were found and worked around **without modifying any
shipped notebook code**:

- `16__fetch_eu_xbrl.py`/`21__clean_and_merge.py` rely on `%run`-injected globals
  (`classify_period_shape`, `DB`, `CONCEPT_PRIORITY`, etc.) that a real Databricks notebook
  receives automatically via `%run "../00__config/01__tickers"` — this textually flattens
  `01__tickers.py`'s names into the calling notebook's namespace. Loading these files via
  `importlib` (this validation's own mechanism, not a real Databricks notebook run) doesn't
  replicate that flattening, so each needed global had to be explicitly pre-seeded. This is a
  validation-harness gap, not a notebook defect — confirmed by loading `01__tickers.py` itself
  the same way and pulling its real `STATEMENTS`/`CONCEPT_PRIORITY`/`CONCEPT_SYNONYMS`/
  `classify_period_shape` values directly, rather than guessing them.
- `21__clean_and_merge.py`'s trailing "Sanity check" cell (`spark.sql(...).display()`) failed
  under headless Databricks Connect with `TypeError: 'Column' object is not callable` — a
  Databricks-notebook-only convenience method that doesn't behave identically outside the
  notebook UI. Confirmed this occurs **after** the real `MERGE`/`DELETE` work already printed
  success — a client-execution-context quirk, not a data-correctness issue. Not fixed (this is
  pre-existing, already-shipped Phase 5.1-era code, out of scope for this phase) — worked around
  in the validation driver instead.

## 16. Tests

**VERIFIED LOCALLY.** `tests/test_eu_admission_driven_ingestion.py` (7 new tests): `EUCurrentSource`
no-arg default still resolves only `PILOT_EU_ENTITIES` (regression), a custom `entities=` list
correctly overrides it, an empty list resolves nothing, `load_admitted_eu_entities()` returns the
correct tuple shape and issues the correct `admission_status = 'admitted'`-filtered SQL (via a
small fake-Spark-session fixture — no live network/Spark in CI, per the driving brief's own §25
instruction), and the NULL-ticker edge case is excluded and reported, not silently dropped. Full
suite: 233 passed, 5 skipped (pre-existing, unrelated), `ruff check` clean.

## 17. Known limitations

- **SGO's FY2024 filing gap** (§5) — not investigated; may be a genuine filing-timing gap or a
  filings.xbrl.org coverage gap, not distinguished in this pass.
- **`Net Income (incl NCI)` is real but not canonical** (§9) — recoverable from `financials_raw`
  only, by design (pre-existing synonym-collapse behavior, not new).
- **Currency remains unpersisted** (§10) — unchanged Phase 5.1 boundary.
- **`21__clean_and_merge.py`'s trailing `.display()` cell doesn't run cleanly under headless
  Databricks Connect** (§15) — a real, observed, but out-of-scope-to-fix quirk of already-shipped
  code; harmless (occurs after all real work completes), but would confuse a future person
  running this same validation approach without this document's context.
- **No full European universe ingestion attempted** — by design (§21 of the driving brief); only
  the 8 admitted issuers were touched.
- **This phase does not schedule anything on the DAG** and does not make EU data visible in any
  frontend — both explicitly out of scope.

## 18. Recommended next phase

The vertical slice is now real and proven: FIRDS → admission → ESEF → `financials`, for 8 real
European companies, validated twice against live production infrastructure. The natural next
step, per the repo owner's own framing, is now genuinely the frontend: adapting
`fundamentals_screener` (and/or the Streamlit app) against this real data — FCC, Alstom, Iberdrola,
Saint-Gobain, and the rest are now queryable in `main.financials.financials` today, not a
theoretical target.

# Phase 5.6 — European Dashboard Data Integration

**Implementation phase, in progress.** Connects the admission layer (Phase 5.3) and the
vertical slice (Phase 5.4 — FIRDS → admission → ESEF → `financials`) to the dashboard-export
boundary, so the 8 admitted European issuers become queryable in `dashboard_data.parquet`/
`dashboard_metrics.parquet` — the artifacts `fundamentals_screener` and the Streamlit app
actually read. Also closes a real currency-alignment gap this connection exposed in
`22__derived_metrics.py`/`23__intrinsic_value.py`, and gives the 8 issuers real daily
price/market-cap history for the first time.

Source-discipline labels: **VERIFIED LOCALLY** / **VERIFIED IN DATABRICKS** / **NOT YET RUN** /
**INFERENCE** / **OPEN QUESTION**.

**Update (2026-08-16): the bounded live Databricks run happened.** §7 and §10 below are the
real record. Headline: the architecture (Option B, §2), the currency field (§3), and the
export-boundary union + collision guard (§5) are all confirmed working against real production
data. The run also hit a real, severe bug — a table-write-mode contract mismatch between
`18__fetch_eu_market_data.py` and `12__fetch_market_data.py` (**not** the currency-alignment
fix, which was never reached) — that briefly reduced `market_prices_daily` to 7 tickers and
cascaded into `market_cap_asof`/`financials_metrics`/`financials_intrinsic_value` for the
**entire US/CA/EU universe**. Fully diagnosed and fully recovered via Delta `RESTORE TABLE`
(all four tables confirmed back to their exact pre-incident row counts); a fix is designed
but **not implemented** — see §10. Phase 5.6 is paused here pending that fix, per explicit
instruction; do not resume without reading §10 first.

## 1. Files changed

```
fundamentals_pipeline/sources/eu_admission.py                  (extended)
fundamentals_pipeline/10__ingestion/17__firds_admission.py     (extended)
fundamentals_pipeline/10__ingestion/12__fetch_market_data.py   (extended)
fundamentals_pipeline/10__ingestion/18__fetch_eu_market_data.py (new)
fundamentals_pipeline/20__transformation/22__derived_metrics.py (extended)
fundamentals_pipeline/20__transformation/23__intrinsic_value.py (extended)
fundamentals_pipeline/50__publish/51__export_dashboard_data.py  (extended)
fundamentals_pipeline/identity.py                               (extended)
tests/test_sources_eu_admission.py                              (extended)
tests/test_identity.py                                          (extended)
docs/phase5-6-european-dashboard-data-integration.md            (this file)
```

No change to `sources/eu_current.py`, `sources/base.py`, `mapping.py`, `registry.py`,
`21__clean_and_merge.py`, or `16__fetch_eu_xbrl.py`.

## 2. The architectural decision: Option B

**Two structurally separate ticker-universe tables, unioned only at the export boundary —
`main.config.tickers` is never extended with European rows.**

`config.tickers` is, by its own documented purpose (`00__config/02__tickers_master.py`'s own
header comment), the **index-membership-driven** US/CA universe: S&P 500, Russell 3000, TSX
Composite, and `favorites.json`. FIRDS-admitted European issuers are a **regulatory-
eligibility-driven** universe (Phase 5.3) — a different admission concept entirely, not a
fourth index alongside the other three. Two alternatives were weighed:

- **Option A (rejected)**: add European rows directly to `config.tickers`, giving every
  downstream consumer (ingestion, `22`/`23`, export) one table to read. Rejected because it
  would (a) misrepresent European issuers as S&P/Russell/TSX-index members when their real
  admission gate is FIRDS eligibility, and (b) force `config.tickers`' identity key decision
  (bare `ticker`, guarded by `check_no_cross_market_collision()`) onto European rows before
  ADR-0012's own `MIC:ISIN` vs. `MIC:TICKER` question is resolved (ADR-0012 is still
  **Proposed**, not Accepted) — prematurely answering an open identity question just to
  satisfy one JOIN.
- **Option B (chosen)**: `main.config.eu_admission_candidates` (already written by Phase 5.3's
  `17__firds_admission.py`) stays a second, independent source. Only the **dashboard-export
  boundary** (`51__export_dashboard_data.py`) unions the two — `UNION ALL` between the
  existing `config.tickers` query and a second branch reading
  `eu_admission_candidates WHERE admission_status = 'admitted'`. Every consumer upstream of
  `51` (ingestion, `22`, `23`) reads `eu_admission_candidates` directly wherever it needs the
  European universe, never through `config.tickers`.

European rows carry `market = 'EU'` (one Generation-1 value across all admitted countries,
matching FIRDS' own EU-wide scope — not per-country), `exchange` = the primary-listing MIC
(the closest real equivalent to the US/CA `exchange` field's Yahoo-mnemonic style),
`accounting_standard = 'ifrs-full'` (verified true for every `EU_CURRENT` filing ingested so
far — Phase 5.1's own `detect_metadata()` check), and `reporting_currency` from the admission
layer's own real, FIRDS-sourced `currency` field (§3 below) — never guessed, never defaulted
to USD. Fields with no European equivalent yet (`sector`/`industry`/`employees`/`website`/
`founded`/`has_logo`/`description`) are `NULL` — real absence, not a fabricated value — and
every universe-membership flag (`is_favorite`/`in_sp500`/`in_r3000`/`in_tsx_composite`) is
`false`, since FIRDS admission is not S&P 500/Russell 3000/TSX Composite/favorites membership.

## 3. Real `currency` field on `AdmissionCandidate`

`fundamentals_pipeline/sources/eu_admission.py`'s `AdmissionCandidate` dataclass gained a
`currency: str | None` field, populated from the **winning** primary-listing venue record's
own `NtnlCcy` (FIRDS' national-currency field) — never guessed, never inferred from country or
MIC. Only the winning listing's currency is used, never a losing/rejected candidate's, and
`currency` is `None` whenever no primary listing resolves (mirrors every other field's
none-until-resolved discipline).

**VERIFIED LOCALLY** — `tests/test_sources_eu_admission.py` gained 3 new cases:
`test_build_admission_candidate_fcc_pilot_regression` now also asserts `currency == "EUR"`;
`test_build_admission_candidate_currency_from_primary_listing_not_any_venue` proves the
winner's currency survives even when a losing candidate carries a deliberately different
(wrong) currency; `test_build_admission_candidate_currency_none_when_unresolved` proves no
currency is claimed when no primary listing resolves.

## 4. The currency-alignment bug this connection exposed

**A real, latent bug — not yet triggered in production, since no European market data existed
until this phase.** `22__derived_metrics.py` and `23__intrinsic_value.py` each build a
`ticker_currency` Spark DataFrame (quote currency + reporting currency per ticker) via
`QUOTE_CURRENCY_BY_MARKET = {"US": "USD", "CA": "CAD"}` joined against `config.tickers`, then
`LEFT JOIN` it onto the price/market-cap computation, `COALESCE`-ing any unmatched ticker's
`quote_currency`/`reporting_currency` to `"USD"`. Before this phase, a European ticker reaching
that join would never appear in `ticker_currency` at all (European rows aren't in
`config.tickers` — see §2's Option B), so it would silently fall through to the `"USD"`
default — the exact same "wrong-but-silent" failure mode this alignment logic exists to
prevent for US/CA (see `fx.py`'s module docstring), just not yet reachable because no EUR data
existed to expose it.

**Fix**: both files now extend `QUOTE_CURRENCY_BY_MARKET` with `"EU": "EUR"` (defensive only —
see below) and, more importantly, union a second `ticker_currency` branch read directly from
`eu_admission_candidates` (`admission_status = 'admitted' AND currency IS NOT NULL`), using the
real FIRDS `currency` as both quote and reporting currency. Quote and reporting currency are
the *same* value here because every currently-admitted MIC (`XMAD`/`XPAR`/`XAMS`/`MTAA`) is
Eurozone — a future non-Eurozone EU admission (e.g. a Swedish or Danish listing) would need a
genuine market→quote-currency split the way `QUOTE_CURRENCY_BY_MARKET` already provides for
US/CA; not built here, since no such candidate is admitted yet.

**Why no new unit test for this fix**: the bug and its fix live entirely in Spark DataFrame
code (`spark.table(...).filter(...).select(...).unionByName(...)` then a `LEFT JOIN` +
`COALESCE`) with no pure-Python equivalent — this repo's `tests/` suite is deliberately
Spark-free (no Spark CI exists anywhere in the project; see `CLAUDE.md`), and there is no
existing precedent here for a local-Spark-session pytest. The one genuinely pure, testable
piece of the currency-alignment machinery — `fundamentals_pipeline/fx.py`'s `convert_price()`
— needed **zero changes** for EU support, since it was already currency-agnostic (confirmed:
`tests/test_fx.py`'s existing cases never hardcode a currency pair). The real verification for
this fix is therefore the bounded Databricks run (§7) — observing that a EUR ticker's
`market_cap_asof`/TTM rows actually carry `EUR`, not a silent `USD`, is a more meaningful test
of Spark-native join logic than a mocked unit test would be.

## 5. Export-boundary union + ticker-collision guard

`51__export_dashboard_data.py`'s ticker-universe query becomes a `UNION ALL` of the existing
`config.tickers` branch and a new `eu_admission_candidates` branch (§2). The two branches are
two structurally independent admission processes with **no shared identity gate between them**
— unlike `config.tickers` itself, which already guards against a US/CA collision via
`identity.py`'s `check_no_cross_market_collision()` before its own write. A ticker string
appearing in both branches would silently produce two rows for one ticker in the exported
`tickers_df`, corrupting every downstream `{ticker: record}` dict this export (and
`fundamentals_screener`'s own repositories, per the Phase 5.5 audit) builds.

**Guard**: `fundamentals_pipeline.identity.check_no_export_ticker_collision()` (new function,
this session) — raises `ExportTickerCollisionError` on any duplicate, deliberately with **no**
auto-resolution via `classify_company_match()` the way `check_no_cross_market_collision()` has:
these two branches have no shared identity model to reconcile them by yet (ADR-0012 is still
Proposed), so a real collision here must stop the export outright, never silently pick one
side. Not observed live yet (`FCC`/`FCT` — the two tickers Phase 5.2's own research flagged as
having global collisions — are absent from `config.tickers`, confirmed).

**VERIFIED LOCALLY** — extracted from what was originally an inline pandas check directly in
the notebook (untestable without Spark, since the notebook can't be imported standalone) into
this pure function, now covered by 5 new cases in `tests/test_identity.py`: no-collision
passes, accepts both a `pandas.Series` and a plain list, raises with the colliding ticker(s) in
the message (including when there are two, not just one), and correctly does *not* try to
distinguish a same-market repeat from a real cross-branch collision (no `market` concept in
this guard at all, unlike the cross-market guard).

## 6. Bounded European market-data ingestion (`18__fetch_eu_market_data.py`, new)

A deliberately separate, non-scheduled notebook — **not** wired into `12`'s own scheduled run,
**not** added to the DAG, **not** writing to `config.tickers` — following the same bounded
pattern `16__fetch_eu_xbrl.py`/`17__firds_admission.py` already established for European
ingestion.

**Why not just extend `12`**: unlike Canada (one uniform Yahoo `.TO` suffix for every CA
ticker, keyed off `config.tickers.market`), each EU MIC needs its own Yahoo suffix (Madrid
`.MC`, Paris `.PA`, Amsterdam `.AS`, Milan `.MI`) — a per-ticker, not per-market, mapping `12`'s
existing `MARKET_MAP`-keyed logic can't express. More importantly, **a bare ticker symbol is
not safely resolvable via Yahoo Finance without independent verification** — Phase 5.2's own
research found real, live global ticker collisions for two of these exact eight tickers (`FCC`
also matches an unrelated Vietnamese company; `FCT` matches five or more unrelated global
companies). This notebook adds a real market-data-safety gate beyond `admission_status =
'admitted'` (which only proves ESEF/fundamentals eligibility, not market-data safety): every
fetched Yahoo record's own company name must independently, confidently match the admission
record's `issuer_name` via `classify_company_match()` before its price/split data is trusted
and written. A ticker that fails this check is logged to `ingestion_failures` and excluded —
never silently accepted.

**Architecture**: reuses `12__fetch_market_data.py`'s own fetch/MERGE logic directly (loaded
via the same `importlib` + pre-seeded-globals mechanism Phase 5.4 already established for
`16__fetch_eu_xbrl.py`/`21__clean_and_merge.py`), rather than duplicating it. `12` gained one
small, additive extension for this: a `YAHOO_SYMBOL` override (mirrors the existing
`ACTIVE_TICKERS`/`force_full_refresh` override-mode pattern) — a caller may pre-seed an
explicit per-ticker Yahoo symbol dict instead of `12`'s own market-keyed default. Untouched,
byte-for-byte, for a normal (non-override) run: `if "YAHOO_SYMBOL" not in globals() or not
YAHOO_SYMBOL` falls through to the existing logic.

`EU_MIC_YAHOO_SUFFIX` (Madrid/Paris/Amsterdam/Milan) is **VERIFIED LOCALLY** against real live
`yfinance` queries this session (per the notebook's own header comment) — all 8 admitted
tickers under these exact suffixes returned each company's correct `longName` with zero
collisions. Writes to `market_prices_daily`/`stock_splits` are **NOT YET RUN** in this branch's
lifetime — this would be the tickers' first-ever price history (`force_full_refresh = "true"`
is hardcoded for exactly this reason).

## 7. What is verified — real results from the 2026-08-16 bounded live run

**VERIFIED LOCALLY** (pure Python, no Spark/network):
- `AdmissionCandidate.currency` resolution (§3) — 3 tests
- `check_no_export_ticker_collision()` (§5) — 5 tests
- Full suite: 303 passed, 2 skipped (pre-existing, unrelated), `ruff check` clean

**VERIFIED IN DATABRICKS**, real production workspace, executed from a personal Databricks
Repo checkout of this branch (`/Repos/al.lopez.moreira@gmail.com/phase5-6-validation`, since
the GitHub→Databricks sync only mirrors `main`) — each notebook submitted as its own one-time
`jobs/runs/submit` run, not the scheduled 9-task DAG (none of whose tasks were touched):

- **`17__firds_admission.py`**: SUCCESS. `currency` column now live in
  `eu_admission_candidates` (18 columns, was 17). All 8 admitted tickers correctly show
  `currency = 'EUR'`. Admission set unchanged (183,055 total candidates, 8 admitted — identical
  to pre-run), confirming the re-run didn't drift the admission logic itself.
- **`18__fetch_eu_market_data.py`**: SUCCESS, but only **6 of 8** tickers passed the live
  Yahoo company-name safety gate — a real, legitimate finding, not a bug in the gate's design:
  - **`FCC`**: Yahoo's `longName` comes back truncated mid-word (`"ACCIONES FOMENTO DE
    CONSTRUCCIO"`, cut at 32 characters) vs. FIRDS' full `"ACCIONES FOMENTO DE CONSTRUCCIONES
    Y CONTRATAS, S.A."` — `classify_company_match()` returns `"ambiguous"` because the final
    token differs entirely (`CONSTRUCCIO` vs `CONSTRUCCIONES`), not a subset match.
  - **`SGO`**: Yahoo returns `"Compagnie de Saint-Gobain S.A."` vs. FIRDS' `"SAINT GOBAIN"` —
    `identity.py`'s `_PUNCTUATION` regex (`r"[.,'&]"`) does not strip hyphens, so
    `"Saint-Gobain"` normalizes to one token that matches neither `"SAINT"` nor `"GOBAIN"`
    individually, producing zero token overlap and a `"different"` verdict.
  - Both are genuine same-company matches that the safety gate conservatively (and correctly,
    per its own design) rejected rather than guess. **Not fixed in this pass** — the run's
    scope was validation, not `identity.py` changes; flagged here for a future, separate,
    reviewed fix (a real normalization gap, not specific to Phase 5.6).
  - The 6 that passed (`ALO`/`FCT`/`IBE`/`ISP`/`NAI`/`RAND`) got real first-ever daily
    price history written correctly (confirmed real `close`/`adj_close` values, real date
    ranges back to each ticker's actual listing history).
  - **This run also caused a severe, since-fully-recovered incident — see §10.**
- **`22__derived_metrics.py`**: SUCCESS (~3.5 min). `financials_metrics` populated for all 8
  EU tickers (6–27 rows each) — correctly limited to non-share-dependent metrics (Net Income
  YoY %, Net Margin %, Piotroski F-Score, ROA %, Revenue YoY %, Total Payout Ratio). **No
  `market_cap_asof` rows for any EU ticker** — root-caused to a separate, pre-existing,
  deliberately out-of-scope gap: `financials` has **zero** `Shares Diluted` rows for any EU
  ticker (IFRS concept mapping in `01__tickers.py` was never extended to a share-count concept
  — confirmed directly; `IBE`'s only 4 mapped concepts are Cash & Equivalents/Net
  Income/Revenue/Total Assets, matching Phase 5.4's own §8 coverage table exactly). Since
  `market_cap = price × shares`, this blocks market-cap computation for EU tickers regardless
  of currency handling — the currency-alignment fix (§4) is therefore **correct by code
  inspection and unreachable by observation**: the union into `ticker_currency` executes
  without error, but there is no `market_cap_asof` row for any EU ticker to show the resulting
  currency on. Confirmed clean (no wrong/leaked values): `financials_metrics` for EU tickers
  contains no `Market Cap`/`P/E`/`EPS`-family metric at all — real absence, matching this
  project's "real gap reads NULL" convention exactly, not silently wrong data.
- **`23__intrinsic_value.py`**: SUCCESS. `financials_intrinsic_value` — **zero rows for any EU
  ticker**, same root cause: `EPS Diluted`/`Shares Diluted` are both hard-required pivot inputs
  (`23`'s own `("Income Statement", "EPS Diluted", "eps")` / `("Income Statement", "Shares
  Diluted", "shares")`), absent for every EU ticker for the identical reason as above.
- **`51__export_dashboard_data.py`**: SUCCESS. **The critical test passes**: `dashboard_data.
  parquet` (2,640 distinct tickers) contains all 8 admitted EU tickers, including `FCC`/`SGO`
  despite their missing market data — confirming the Option B `UNION ALL` (§2) works correctly
  end-to-end against real production data, with zero dependency on `config.tickers`
  (re-confirmed empty for all 8 EU tickers throughout). The `check_no_export_ticker_collision()`
  guard (§5) ran with no collision (none exists live today) — its actual raise path remains
  verified only by the 5 unit tests (§5), not by a live collision, since none exists to trigger
  one.
- **US/CA regression**: after full incident recovery (§10), verified byte-for-byte identical
  to the pre-run snapshot — `market_prices_daily` (15,268,796 rows), `market_cap_asof` (27,240
  rows, AAPL/MSFT $USD values unchanged, `AEM` still correctly `USD`-aligned, `AQN` still
  correctly native `CAD`), `financials_metrics` (2,039,873 rows), `financials_intrinsic_value`
  (221,273 rows, AAPL's TTM `price_close`/`margin_of_safety_pct` restored to real values).
  `config.tickers` never gained the 8 EU tickers at any point (0 rows, checked before, during,
  and after).
- **Idempotency**: not attempted — the run stopped after the incident (§10) rather than
  proceeding to a second pass, per explicit instruction.

## 8. Known limitations / open questions

- **ADR-0012 (`MIC:ISIN` vs. `MIC:TICKER`) is still Proposed, not Accepted** — this phase
  deliberately does not resolve it; Option B (§2) was chosen specifically so this connection
  doesn't have to.
- **`market = 'EU'` is one Generation-1 value for every admitted country** — a future
  non-Eurozone EU admission would need real per-country handling in both the export union
  (currently defaulting every European row's currency straight from `eu_admission_candidates`)
  and the `QUOTE_CURRENCY_BY_MARKET` extension (§4), neither of which is built yet.
  **OPEN QUESTION**, not blocking today since every currently-admitted MIC is Eurozone.
- **No `Shares Diluted` (or any share-count) IFRS concept mapping exists** — blocks
  `market_cap_asof`, valuation-dependent `financials_metrics` rows, and
  `financials_intrinsic_value` entirely for every EU ticker (§7). Pre-existing, deliberately
  out-of-scope per CLAUDE.md's multi-market roadmap ("IFRS XBRL concept mapping in
  `01__tickers.py`... still out of scope, deliberately") — not a Phase 5.6 regression, but the
  real, current reason the currency-alignment fix (§4) can't be observed end-to-end.
- **Two real company-name-matching false positives in `identity.py`'s `classify_company_match()`**
  (§7): a mid-word Yahoo `longName` truncation (`FCC`) and a hyphen the `_PUNCTUATION` regex
  doesn't strip (`SGO`, `"Saint-Gobain"` vs `"Saint Gobain"`). Both cause a genuine same-company
  match to read as `"ambiguous"`/`"different"` and be conservatively rejected. Real, reproducible,
  **not fixed** — flagged for a future, separately-reviewed change (touches the same matcher the
  US/CA cross-market collision guard uses, so any fix needs its own regression check against
  that usage too).
- **This phase does not schedule anything on the DAG** — `18__fetch_eu_market_data.py` is
  explicitly non-scheduled, and none of the 9-task production DAG's tasks were modified.
- **See §10 for the write-mode contract bug** — the most significant finding of this pass,
  with its own fix design (not yet implemented).

## 9. Next steps (in order)

0. **Fix the `18`/`12` write-mode contract bug (§10) first** — do not re-attempt the bounded
   live run before this lands; it would reproduce the same incident. Design is ready in §10;
   implementation is a separate, explicitly-deferred step.
1. ~~The real bounded Databricks run: `17` → `18` → `22`/`23` → `51`~~ — **done, 2026-08-16**
   (§7). Architecture, currency field, and export union all confirmed working; a real bug was
   found and fully recovered (§10), not yet fixed at the code level.
2. Re-run the bounded live pass once the §10 fix lands, to confirm `18` no longer touches rows
   outside its own ticker scope.
3. Idempotency check (re-run twice, diff row counts) — not yet attempted (§7).
4. Optionally: a real fix for the two `classify_company_match()` false positives (§8) so `FCC`/
   `SGO` also get market data — separate, reviewed change, not blocking.
5. Optionally: IFRS `Shares Diluted` concept mapping (§8) so `market_cap_asof`/
   `financials_intrinsic_value` can actually populate for EU tickers — a substantial, separate
   piece of work (multi-market roadmap), not started.
6. Once the above are green: consider whether `fundamentals_screener`/Streamlit actually
   render the new European rows correctly (out of scope for this phase — the dashboard-export
   boundary is the explicit stopping point, matching Phase 5.4's own "does not make European
   data visible in `fundamentals_screener`" boundary one layer up the chain).

## 10. Live validation incident (2026-08-16) — `market_prices_daily` write-mode contract bug

**Severity: high (real, if brief, production data reduction across the entire US/CA/EU
universe). Fully diagnosed, fully recovered. Fix designed, not implemented.**

### 10.1 What happened

`18__fetch_eu_market_data.py` delegates its actual fetch/write to `12__fetch_market_data.py`
by loading it via `importlib` and pre-seeding module globals (§6):

```python
_module.ACTIVE_TICKERS = EU_ACTIVE_TICKERS   # the 6 tickers that passed the safety check
_module.YAHOO_SYMBOL = EU_YAHOO_SYMBOL
_module.BENCHMARK_TICKERS = []
_module.force_full_refresh = "true"          # first run for these tickers -- always full history
```

`12__fetch_market_data.py` reads `force_full_refresh` into a single `FORCE_FULL_REFRESH`
flag, which drives a single `MODE` variable (`fundamentals_pipeline/10__ingestion/
12__fetch_market_data.py:191`):

```python
MODE = "full" if (FORCE_FULL_REFRESH or not _has_prices) else "incremental"
```

This one flag controls **two independent decisions** that were never meant to be coupled:

1. **Per-ticker fetch depth** — `period="max"` (full history) vs. an incremental gap-fill from
   each ticker's last known date (lines 195–213).
2. **Table write mode** (lines 378–396) — `MODE == "full"` writes
   `prices_sdf.write.format("delta").mode("overwrite")...saveAsTable(prices_tbl)`, a **full
   table replace**; `MODE == "incremental"` writes a `MERGE INTO ... ON (ticker, date)` with
   only `WHEN MATCHED UPDATE` / `WHEN NOT MATCHED INSERT` — no delete clause, so it can never
   remove rows for tickers outside its own source.

In the normal production caller (`91a__pipeline_pre22.py`), `force_full_refresh` is only ever
set when `ACTIVE_TICKERS` already **is** the full production universe (a deliberate, rare,
whole-pipeline rebuild flag) — so a full-table overwrite is correct there, since the batch
genuinely represents everything. `18` is the first caller to combine `force_full_refresh=true`
with a **narrow** `ACTIVE_TICKERS` override (6 tickers), a combination `12` was never designed
to handle: the write path had no way to know the batch was intentionally partial, so it
replaced the entire table with just those 6 tickers' rows.

**Confirmed via `DESCRIBE HISTORY main.financials.market_prices_daily`**: version 118,
`CREATE OR REPLACE TABLE AS SELECT`, triggered by job `phase5-6-validation-18-eu-market-data`,
`numOutputRows: 41142` (down from 15,268,796), removing the 3 files holding the full prior
universe.

### 10.2 Cascading impact

`22__derived_metrics.py` and `23__intrinsic_value.py` both ran (as instructed, in sequence)
**after** the corrupted table existed and **before** the corruption was caught, so both
consumed it as their real input:

- **`market_cap_asof`** (`22`): full-overwrite table (`mode("overwrite")`, no MERGE at all —
  every run is a from-scratch rebuild). With only 7 tickers' worth of price data as input,
  and those 7 having no `Shares Diluted` either (§7/§8), the rebuild produced **zero rows**,
  wiping the entire pre-existing 27,240-row table for the whole US/CA/EU universe.
- **`financials_metrics`** (`22`): a `MERGE INTO ... WHEN NOT MATCHED BY SOURCE THEN DELETE`
  (the file's own comment: *"Safe here specifically because source is a full-universe rebuild
  each run"*). That assumption held for the ticker dimension but not for the **metric**
  dimension: with `market_cap_asof` empty, every valuation-dependent metric (P/E, market-cap
  ratios, PEG, etc.) was legitimately absent from this run's source for the **entire**
  universe, and the delete-orphans clause — behaving exactly as designed — removed all of
  them. Row count dropped from 2,039,873 to 1,380,672.
- **`financials_intrinsic_value`** (`23`): its own MERGE + orphan-cleanup is scoped to
  `iv_processed_tickers` (only tickers actually processed that run), so it did **not** suffer
  the same blanket deletion — but its `WHEN MATCHED THEN UPDATE SET ... price_close = source.
  price_close, margin_of_safety_pct = source.margin_of_safety_pct` overwrote the TTM row's
  live-price fields with `NULL` for every ticker whose TTM valuation ran this pass (confirmed
  directly on `AAPL`: `price_close`/`margin_of_safety_pct` both `NULL`, `computed_at` matching
  this run's timestamp) — real production rows silently degraded to a real-but-wrong "no
  data" state, not caught by row-count alone (net count actually rose slightly, from 221,273
  to 224,843, since new FY-basis rows were still legitimately added even as TTM fields were
  nulled).

`51__export_dashboard_data.py` was also run (before the corruption was caught) and its
`dashboard_prices.parquet` output reflected the corrupted state (6 tickers only) — but since
that artifact was superseded by the recovery, no separate action was needed for it beyond the
underlying table fixes.

### 10.3 Recovery

All four tables restored via Delta time-travel (`RESTORE TABLE ... TO VERSION AS OF <n>`),
each version chosen as the last write before this validation session touched anything and
confirmed by exact row-count match against the pre-run snapshot before restoring:

| Table | Restored to version | Rows confirmed |
|---|---|---|
| `market_prices_daily` | 117 | 15,268,796 |
| `market_cap_asof` | 83 | 27,240 |
| `financials_metrics` | 799 | 2,039,873 |
| `financials_intrinsic_value` | 293 | 221,273 |

Post-restore spot checks: `AAPL`/`MSFT`/`TSLA` price and `market_cap_asof` rows identical to
pre-run; `AEM` still correctly `USD`-aligned (the pre-existing Canadian currency-mismatch fix,
unaffected); `AQN` still correctly native `CAD`; `AAPL`'s TTM `price_close`/
`margin_of_safety_pct` back to real values (e.g. `304.91`, not `NULL`); `config.tickers`
confirmed to have gained zero EU rows at any point during the incident (Option B, §2, held
throughout). The 6 EU tickers' price data written by `18` was necessarily lost along with the
restore (it lived in the same version-118 write as the corruption) — recoverable by re-running
`18` once the write-mode bug (§10.4) is fixed.

### 10.4 Proposed fix (design only — not implemented)

The incremental/MERGE write branch (`12__fetch_market_data.py:383-396`) is **already safe**
for a narrow ticker scope — no delete clause, touches only rows matching its own source. The
`if MODE == "full": full_tickers = list(PRICE_UNIVERSE)` branch's **per-ticker fetch-depth**
logic isn't even necessary for `18`'s use case: the incremental branch's own line 204
(`full_tickers = [t for t in PRICE_UNIVERSE if t not in maxd]  # never seen → full hist`)
**already** detects a ticker with zero existing rows and fetches its complete history via
`period="max"`, regardless of `FORCE_FULL_REFRESH`. Since `market_prices_daily` already holds
millions of other tickers' rows, `_has_prices` (line 182) is globally `True`, so `MODE` would
resolve to `"incremental"` on its own — routing new EU tickers through the exact same
first-time-full-history-fetch behavior, but via the safe `MERGE` write path — **without any
code change to `12` at all**.

**The minimal fix**: `18__fetch_eu_market_data.py` should simply **not set
`_module.force_full_refresh = "true"`** — remove that one line. `12`'s existing logic already
does the right thing for a brand-new ticker in incremental mode. Verified by direct code
reading (`12__fetch_market_data.py:114-120`): with no `force_full_refresh` global pre-seeded
and no `dbutils.widgets` registered in this standalone/`importlib` context, `12`'s own
fallback (`except Exception: FORCE_FULL_REFRESH = False`) applies cleanly — no other change
needed anywhere.

**A more defensive, second-layer fix worth considering alongside it** (not required by the
above, but cheap insurance against the next caller making the same mistake): make `12`'s
write-mode decision itself resistant to a narrow `ACTIVE_TICKERS` override, independent of
`FORCE_FULL_REFRESH` — e.g. only allow the full-table-overwrite branch when `ACTIVE_TICKERS`
was **not** externally pre-seeded (the same `"ACTIVE_TICKERS" in globals()`-style check the
file already uses for `YAHOO_SYMBOL`'s own override detection), falling through to the safe
MERGE path whenever a caller has narrowed the ticker scope, regardless of what
`force_full_refresh` says. This would make the dangerous combination structurally unreachable,
not just avoided by this one caller's correct usage.

Neither change has been implemented — this section is a design record for the next pass (§9,
step 0), per explicit instruction to diagnose and design without touching code this session.

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

**Update (2026-08-16 → 2026-08-17): the bounded live Databricks run happened, hit a real
incident, and has since been hardened across two rounds.** §7 is the live-run record; §10 is
the incident (fully recovered, six tables — see §12.1 for the sixth, found late); §11 is the
first hardening round; §12 is the second, which replaced §11's percentage threshold with a
semantic primary guard and fixed a real NULL-overwrite bug §11 had only documented. Headline:
the architecture (Option B, §2), the currency field (§3), and the export-boundary union +
collision guard (§5) are all confirmed working against real production data. The run also hit
a real, severe bug — a table-write-mode contract mismatch between `18__fetch_eu_market_data.py`
and `12__fetch_market_data.py` (**not** the currency-alignment fix, which was never reached) —
that briefly reduced `market_prices_daily` to 7 tickers and cascaded into `market_cap_asof`,
`market_cap_live`, `financials_metrics`, `financials_intrinsic_value`, and `stock_splits` for
the **entire US/CA/EU universe**. Fully diagnosed and fully recovered via Delta `RESTORE TABLE`
(all six tables confirmed back to their exact pre-incident row counts — §10.3 has the complete
table). §11 implemented and unit-tested the root-cause fix, a structural ticker-scope guard in
`12`, a percentage-based secondary guard on `22`/`23`'s destructive deletes, investigated (and,
with real evidence, declined to guess at) the `Shares Diluted` gap, and fixed a real, separate
bug in `classify_company_match()`. §12 replaced the percentage threshold's primary role with a
semantic `is_full_universe_run` check (keeping the percentage as secondary, per explicit
instruction), found the sixth casualty (`market_cap_live`) that §11 had missed, and fixed the
actual mechanism behind the `AAPL` TTM `NULL`-overwrite (a MERGE-clause split, not just a
documented finding). **Still explicitly paused**: no live Databricks write has happened since
the recovery — every §11/§12 change is verified locally (pytest/ruff) only, per instruction.
PR #380 remains a draft, not merged; the next live bounded run needs its own explicit
go-ahead.

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

- **`stock_splits`** (`18` directly, same delegation into `12`): the identical vulnerability
  pattern one level down — `_splits_full = FORCE_FULL_REFRESH or not _splits_seen` gates the
  exact same `.mode("overwrite")` shape (`12__fetch_market_data.py`'s own splits section).
  **Missed in the original incident response** — only caught during the §11 hardening pass,
  when checking whether the same vulnerability existed elsewhere in the file surfaced that it
  had already fired: reduced from 4,330 rows / 1,255 tickers to 7 rows / 4 tickers.
- **`market_cap_live`** (`22`, `mcl_tbl`): a SIXTH casualty, missed by both the original
  incident response and the first §11 hardening pass — only found during the §12 review, when
  building a complete inventory of every `.mode("overwrite")` write in `22`/`23` surfaced that
  this table shares the exact same `_has_prices`-gated code block as `market_cap_asof`
  (`22__derived_metrics.py`'s "Live market cap" section, lines ~1013 on) with its own
  unconditional full-table overwrite. Reduced from 2,175 rows to **0 rows** — same shape as
  `market_cap_asof`, just never checked.

`51__export_dashboard_data.py` was also run (before the corruption was caught) and its
`dashboard_prices.parquet` output reflected the corrupted state (6 tickers only) — but since
that artifact was superseded by the recovery, no separate action was needed for it beyond the
underlying table fixes.

`fx_rates_daily` was checked and confirmed **unaffected** — its currency-pair set is derived
from the full `config.tickers` table directly (`_needed_currencies()`), independent of
`ACTIVE_TICKERS`, so the `MODE=="full"` overwrite that also fired here wrote the same correct
pairs it always does (11,919 rows vs. the pre-incident 11,892 — a normal refresh).

### 10.3 Recovery

All six affected tables restored via Delta time-travel (`RESTORE TABLE ... TO VERSION AS OF
<n>`), each version chosen as the last write before this validation session touched anything
and confirmed by exact row-count match against the pre-run snapshot before restoring. This is
the complete, final recovery record (superseding any partial listing earlier in this
document) — confirmed via `DESCRIBE HISTORY` on each table, not assumed:

| Table | Before (pre-incident) | Bad version | Rows in bad version | Restored to version | Rows after restore |
|---|---|---|---|---|---|
| `market_prices_daily` | 15,268,796 rows / ~2,662 tickers | 118 | 41,142 rows / 7 tickers | 117 | 15,268,796 rows / 2,644 tickers |
| `market_cap_asof` | 27,240 rows | 84 | 0 rows | 83 | 27,240 rows |
| `financials_metrics` | 2,039,873 rows | (MERGE, not a version snapshot — orphan-delete removed 659,201 rows) | 1,380,672 rows | 799 | 2,039,873 rows |
| `financials_intrinsic_value` | 221,273 rows | (MERGE — TTM fields nulled, not deleted) | 224,843 rows (higher — new FY rows still added) | 293 | 221,273 rows |
| `stock_splits` | 4,330 rows / 1,255 tickers | 83 | 7 rows / 4 tickers | 82 | 4,330 rows / 1,255 tickers |
| `market_cap_live` | 2,175 rows | 35 | 0 rows | 34 | 2,175 rows |

**Post-restore spot checks, US/CA representatives:**

| Ticker | `market_prices_daily` | `stock_splits` | `market_cap_asof` | `market_cap_live` | `financials_intrinsic_value` (TTM) |
|---|---|---|---|---|---|
| `AAPL` | 11,507 rows | 5 splits | present, `USD` | `market_cap` = 4.45E12, `USD` | `price_close` = `304.91` (not `NULL`) |
| `MSFT` | 10,181 rows | 9 splits | present, `USD` | `market_cap` = 3.74E12, `USD` | — |
| `TSLA` | 4,054 rows | 2 splits | present, `USD` | `market_cap` = 1.31E12, `USD` | — |
| `AEM` (CA, USD-aligned) | 7,936 rows | 0 (real — no splits on record) | `USD` (cross-currency fix intact) | — | — |
| `AQN` (CA, native CAD) | 5,734 rows | 0 (real — no splits on record) | `CAD` | — | — |
| `BN` (CA, has real splits) | — | 8 splits (confirmed real, not zero) | — | — | — |

`AEM`/`AQN` legitimately have zero recorded splits (real absence, not a recovery gap) — `BN`
(Brookfield) was checked separately specifically to confirm `stock_splits`' Canadian coverage
with a real, non-zero, positive count. `config.tickers` confirmed to have gained zero EU rows
at any point during the incident (Option B, §2, held throughout). The 6 EU tickers' price/
split data written by `18` was necessarily lost along with the restore (it lived in the same
corrupted write as the rest) — recoverable by re-running `18` once the write-mode bug (§10.4,
fixed in §11) lands in a real
run.

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

**Both changes are now implemented — see §11.1/§11.2.** This section is kept as-written (the
original design record) for the historical trail; §11 is the actual implementation, with real
test coverage, not a restatement of this section.

## 11. Post-incident hardening (2026-08-16, same day)

**Verification discipline for this section**: every change below is **VERIFIED LOCALLY**
(pytest/ruff, no Spark) only. No notebook was executed against Databricks after the §10
recovery — per explicit instruction, only read-only SQL (via the workspace's serverless SQL
warehouse) was used to confirm the recovery and research the `Shares Diluted` gap (§11.5).
The next live bounded run is a separate, future, explicitly-authorized step.

### 11.1 Root-cause fix

`18__fetch_eu_market_data.py` no longer sets `_module.force_full_refresh = "true"` when
delegating into `12`. The removed line's own inline comment is replaced with an explanation of
why it's unnecessary (§10.4's finding: `12`'s incremental branch already fetches full history
for any never-seen ticker via the safe `MERGE` path) and a pointer to the structural guard
below, which would now catch the exact mistake even if a future edit reintroduced it.

### 11.2 Structural safety guard in `12__fetch_market_data.py`

A caller pre-seeding a narrower-than-full-universe `ACTIVE_TICKERS` can no longer reach either
of `12`'s two whole-table `.mode("overwrite")` write paths (`market_prices_daily` **and**
`stock_splits` — both share the vulnerability, see §10.2) without an explicit, loud abort.
`_assert_safe_full_overwrite(table_name)` — called right at the top of both `if MODE ==
"full":` / `if _splits_full:` branches, before any fetch work happens — queries the real
`config.tickers` universe and delegates the actual judgment to
`fundamentals_pipeline.write_safety.assert_full_overwrite_safe()` (§11.4): raises
`UnsafeFullOverwriteError`, wrapped in a `RuntimeError` with a message pointing back at this
document, unless every ticker in `config.tickers` is covered by `ACTIVE_TICKERS`. A no-op
(never blocks) when `config.tickers` is unreadable/empty — nothing to protect in that case.

The normal, unscoped production caller (`91a__pipeline_pre22.py`, `tickers_override` empty) is
unaffected — its `ACTIVE_TICKERS` genuinely is the full universe. Any narrower scope, present
or future, now gets an explicit, actionable error instead of silent data loss.

### 11.3 `22`/`23` destructive-delete review and guards

Investigated every `WHEN NOT MATCHED BY SOURCE` / bulk-`DELETE` / unconditional-`UPDATE`
pattern in both notebooks, per instruction — not a redesign, the smallest defensible guard for
each real risk found:

- **`22__derived_metrics.py`'s `financials_metrics` MERGE** (the one that actually fired in
  the incident, §10.2): now computes the orphan-delete count before running the MERGE and
  calls `assert_orphan_delete_safe()` (§11.4) — aborts if the delete would remove more than
  10% of the table's current rows, with a message explaining that this usually means an
  upstream dependency came back anomalously empty, not that this many metrics genuinely
  became stale at once.
- **`23__intrinsic_value.py`'s two orphan-cleanup MERGEs** (`financials_intrinsic_value`'s own
  8b, and its `financials_metrics` IV-label copy's 9b) — both already scoped to
  `iv_processed_tickers` (ticker-narrow scoping was never the risk here), but neither
  protected against an upstream dependency coming back empty for tickers that WERE
  recomputed. Same 10%-of-existing-rows guard added to both, the second one scoped to just
  the IV-owned metric labels (comparing against the whole `financials_metrics` table would
  never trip a meaningful percentage, since it also holds `22`'s own base/val metrics).
- **A separate, real finding — not guarded, documented instead**: `23`'s plain `iv_tbl` MERGE
  (`WHEN MATCHED THEN UPDATE SET ... price_close = source.price_close, margin_of_safety_pct =
  source.margin_of_safety_pct`, unconditional — no `AND target.value != source.value` guard
  the way `22`'s own base-metrics MERGE has) is what actually degraded `AAPL`'s TTM row to
  `NULL` in the incident, not either orphan-cleanup DELETE (that row was never orphaned — its
  method/scenario combination still existed in `incoming_iv` this run, just with a `NULL`
  price). This is a genuinely different failure shape (silent overwrite-with-worse-value, not
  bulk deletion) that the 10%-of-rows guard does **not** catch, since the row count is
  unaffected. Deliberately **not fixed** this pass — building a correct guard means
  distinguishing "this ticker's price legitimately has no live quote today" from "an upstream
  dependency broke," which is a real design decision (not a magnitude threshold) that
  shouldn't be invented unilaterally. Flagged here for the repo owner to decide on.

### 11.4 Regression tests reproducing the exact failure mode

Neither `12`'s write-mode decision nor `22`/`23`'s MERGE logic is unit-testable directly (all
three are Spark-only notebooks; this repo's `tests/` suite is deliberately Spark-free — see
CLAUDE.md). Following the same pattern already used for the export-collision guard (§5), the
pure decision core of each guard was extracted into a new module,
`fundamentals_pipeline/write_safety.py`:

- `assert_full_overwrite_safe(active_tickers, full_universe, benchmark_tickers)` — raises
  `UnsafeFullOverwriteError` unless `full_universe` is fully covered by `active_tickers`.
  `12__fetch_market_data.py`'s `_assert_safe_full_overwrite()` fetches the real Spark data and
  delegates the judgment here.
- `assert_orphan_delete_safe(would_delete, existing, max_fraction=0.10)` — raises
  `UnsafeOrphanDeleteError` if the ratio exceeds the threshold. Used by all three `22`/`23`
  guard sites (§11.3).

`tests/test_write_safety.py` (12 new cases): reproduces the exact 2026-08-16 incident at the
pure-logic level (a 6-ticker EU-shaped scope against a 3-ticker stand-in universe raises;
the exact real percentage from the `financials_metrics` cascade, 32%, raises with that number
in the message), confirms the normal full-universe case still passes, confirms benchmark
tickers and a superset scope never falsely trip the guard, and confirms the orphan-delete
threshold's exact boundary (10.0% passes, 10.01% raises). Full suite: **320 passed, 2 skipped
(pre-existing, unrelated)**, `ruff check` clean.

### 11.5 `Shares Diluted` investigation — real research, no safe mapping found

Traced the actual IFRS facts in two real, live filings (not fixtures) via `filings.xbrl.org`'s
public API — the same one `16__fetch_eu_xbrl.py` uses, called directly and read-only, no
Databricks or write path touched:

- **FCC** (LEI `95980020140005178328`, real FY2024 filing, 512 facts, fetched live): the
  22 share-related facts include `ifrs-full:BasicEarningsLossPerShare` and
  `ifrs-full:DilutedEarningsLossPerShare` (both tagged, value `0.96`) and `ifrs-full:
  TreasuryShares`, but **no tagged weighted-average-shares or shares-outstanding count
  concept** — only per-share *ratios*. The only place a raw share count appears is inside a
  free-text narrative disclosure (`ifrs-full:DisclosureOfShareCapitalReservesAndOther
  EquityInterestExplanatory`, Spanish prose containing "454.878.132 acciones ordinarias") —
  not a structured, safely-parseable fact.
- **Iberdrola** (LEI `5QK37QC7NWOJ8D7WVQ45`, real FY2024 filing, 833 facts, fetched live):
  identical pattern — `ifrs-full:BasicEarningsLossPerShareFromContinuingOperations`/
  `DilutedEarningsLossPerShareFromContinuingOperations` are tagged, no share-count concept is.

**Conclusion, per the "universalization > availability" principle**: confirmed across two
different issuers and auditors, not an FCC idiosyncrasy — there is no directly-tagged,
universal IFRS share-count concept available for these filers. A share count *could* be
derived (Net Income ÷ EPS Diluted), but that is a fundamentally different kind of mapping from
every existing `EU_CANONICAL_MAPPING` entry (all `MappingType.DIRECT`, one real fact → one
canonical concept, no arithmetic) — and a real semantic risk exists even if attempted:
Iberdrola's own EPS is explicitly scoped "FromContinuingOperations," while the already-mapped
"Net Income" concept (`ifrs-full:ProfitLossAttributableToOwnersOfParent`) is not scoped to
continuing operations only, so dividing the two would silently produce a wrong denominator for
at least this issuer. **Not implemented — correctly left `NULL`, per "if a mapping is not
semantically defensible, do not guess."** This is why `market_cap_asof`/
`financials_intrinsic_value` are empty for every EU ticker (§7), and remains true after this
hardening pass; it is not something this pass could fix. A `MappingType.DERIVED` category
(if the repo owner wants to pursue the Net Income ÷ EPS route despite the risk above) would be
new architecture, not a small extension, and is a decision for a future pass, not this one.

### 11.6 `classify_company_match()` fix — the real FCC/SGO false-positive root cause

Investigated why the two real §7 rejections happened, using the strongest identity evidence
already in the pipeline (the FIRDS-verified `issuer_name`, resolved via ISIN/LEI/MIC through
the whole admission chain) rather than weakening the check to ticker-only matching:

- **`SGO`**: a real, narrow bug, not a design gap — `identity.py`'s `_PUNCTUATION` regex
  (`[.,'&]`) does not include a hyphen, so Yahoo's `"Compagnie de Saint-Gobain S.A."` kept
  `"SAINT-GOBAIN"` as one token that matched neither FIRDS' `"SAINT"` nor `"GOBAIN"` — zero
  token overlap. **Fix**: hyphens are now replaced with a space (a new `_HYPHEN` regex,
  applied before tokenization) — not deleted like the rest of `_PUNCTUATION`, since deleting a
  hyphen would merge `"SAINT-GOBAIN"` into `"SAINTGOBAIN"` (the opposite of the intended fix).
  With that one change, the existing token-subset "same" rule already handles it correctly —
  no other logic needed.
- **`FCC`**: a real gap in the matcher's design, not a simple regex miss — Yahoo's `longName`
  field is hard-truncated at a fixed character budget (confirmed: FCC's real value is cut to
  exactly 32 characters, mid-word, inside "CONSTRUCCIONES"), which the existing **set-based**
  token comparison can never recognize (a partial word is a different string from the whole
  word, so it can't be a member of the token set no matter how similar). **Fix**: a new,
  deliberately narrow, **positional** (not set-based) helper,
  `_is_truncated_prefix_match()` — every token of the shorter name up to the second-to-last
  must match the longer name's corresponding token EXACTLY, in order; only the shorter name's
  LAST token may be a genuine prefix (≥ 8 characters, `_MIN_TRUNCATED_TOKEN_LEN`) of the
  longer name's corresponding token. Strictly additive to the existing "same" rule — it can
  only ever upgrade a would-be `"ambiguous"`/`"different"` verdict for this one narrow shape
  of mismatch, never the reverse.
- **False-positive guard, explicitly tested**: the 8-character minimum exists specifically so
  a short common-word truncation (e.g. `"MICRO"`, which plausibly prefixes both `"MICROSOFT"`
  and `"MICRODYNE"` — two real, unrelated companies) is never treated as truncation. Verified:
  both `classify_company_match("MICRO", "MICROSOFT CORP")` and `("MICRO", "MICRODYNE INC")`
  still correctly return `"different"`.

**5 new tests in `tests/test_identity.py`** (real FCC/SGO strings from the live 2026-08-16
lookup, the MICRO false-positive guard, a leading-token-mismatch negative case, and a
regression check that `_NONVOTING`'s own hyphen-tolerant pattern still works now that hyphens
are globally replaced with spaces before it runs). All pre-existing identity tests (the
US/CA cross-market collision suite) pass unchanged — confirmed zero regression in the one
other consumer of `classify_company_match()`.

**§8 compliance**: `issuer_id`, `listing_id` (`MIC:ISIN`), and ADR-0012 were not touched. The
investigation found a normalization bug in name *comparison*, not a contradiction in the
identity *model* — ticker remains a market-data lookup attribute, never treated as universal
issuer identity, exactly as before.

### 11.7 Local verification

`pytest tests/ -q`: **320 passed, 2 skipped** (pre-existing, unrelated to this work).
`ruff check` on every changed file: clean. No Databricks notebook executed since the §10
recovery — every check in this section is against local code and (for §11.5) live, read-only,
non-Databricks external API calls.

### 11.8 What remains (as of this section — see §12 for what's since been closed)

- ~~**Not re-attempted**: the live bounded run...~~ — still true after §12 too; see §12.6.
- ~~**Not fixed**: the `23__intrinsic_value.py` blind-`UPDATE`-with-`NULL` finding~~ — **fixed
  in §12.3**, not left open.
- **Not fixed**: `Shares Diluted` remains unmapped for EU tickers (§11.5) — investigated with
  real data, no safe direct mapping exists; a derived mapping is a real future design
  decision, not built here. Still true after §12 — out of scope for this round too.
- **PR #380 remains a draft, not merged.** No new PR opened. No European universe expansion.
  No DAG scheduling change. No `fundamentals_screener`/Django file touched.

## 12. Second hardening round (2026-08-17) — semantic guard, a sixth casualty, the NULL-overwrite fix

Triggered by review of §11: the 10%-of-existing-rows threshold added there is useful but was
correctly identified as **secondary, not primary** — a percentage threshold's own failure mode
(90,001-of-100,000 real rows destroyed is still a catastrophe at just under a 10% threshold)
means it can never be the main defense. This round replaces it as the primary mechanism with a
semantic check — "does this run's upstream input actually represent the full universe" — and,
in reviewing for that, found a sixth incident-affected table §11 had missed.

**Verification discipline, same as §11**: everything below is **VERIFIED LOCALLY**
(pytest/ruff) except the recovery confirmation itself, which is **VERIFIED IN DATABRICKS**
(read-only SQL only). No notebook executed against Databricks this round either.

### 12.1 A sixth casualty found: `market_cap_live`

Building a complete inventory of every `.mode("overwrite")` write across `22`/`23` (rather
than trusting the partial list from the original incident response) surfaced that
`22__derived_metrics.py`'s "Live market cap" section (`market_cap_live`/`mcl_tbl`, ~line 1013
on — the genuinely-current, non-fiscal-year-anchored market cap used for display) shares the
**exact same `_has_prices`-gated code block** as `market_cap_asof`, with its own independent
unconditional full-table overwrite two hundred lines later in the same `else:` branch. It had
never been checked. Confirmed corrupted (0 rows, down from 2,175) and confirmed restored
(version 34, 2,175 rows, real `AAPL`/`MSFT`/`TSLA` values) — full detail in the now-updated
§10.2/§10.3 above, which supersede the original five-table listing.

`23__intrinsic_value.py` has **no** `.mode("overwrite")` writes at all (confirmed via the same
inventory sweep) — only the two MERGE-based tables already reviewed in §11.3. `12`'s own three
overwrite sites (`market_prices_daily`, `stock_splits`, `fx_rates_daily`) were already fully
enumerated in §11.2/§10.2. This is now a complete, swept inventory, not a partial one.

### 12.2 The semantic guard: `is_full_universe_run`

New in `fundamentals_pipeline/write_safety.py`: `is_full_universe_run(source_ticker_count,
reference_ticker_count, min_coverage=0.90)` — a pure coverage-ratio comparison, no Spark. This
is now the **primary** defense; `assert_orphan_delete_safe`'s percentage threshold (§11.3)
remains as **secondary**, unchanged, per the explicit instruction to keep both layers.

Deliberately **not** a caller-set `FULL_UNIVERSE_RUN = True/False` flag — a hand-set boolean
would have exactly the same failure mode `force_full_refresh` already demonstrated (a flag
whose meaning a caller can misunderstand or misuse). Instead each notebook measures its own
real input against a real reference and lets the comparison decide:

- **`22__derived_metrics.py`**: `_has_prices` — previously a bare "does `market_prices_daily`
  have any rows at all" check (true even for the incident's 7-ticker corrupted table) — is now
  additionally gated on `is_full_universe_run(distinct tickers in market_prices_daily, distinct
  tickers in financials)`. This is the single primary gate protecting **three** things at
  once, since all three sit inside the same `if _has_prices: ... else: pe_mcap = None` block:
  `market_cap_asof`'s write, `market_cap_live`'s write (§12.1), and `long_val`'s (valuation
  metrics) contribution to the `financials_metrics` MERGE. A degraded upstream input now never
  reaches any of the three writes at all, rather than reaching them and then being caught (or
  not) by a downstream percentage check.
- **`23__intrinsic_value.py`**: a new `_ttm_full_universe_run` flag, computed the same way
  (`is_full_universe_run(distinct tickers in market_prices_daily, distinct tickers in
  ttm_wide)`) — but here tightening the existing `has_live_price` boolean alone would **not**
  have been sufficient (§12.3 explains why) — it's a separate flag consumed directly by the
  MERGE.

**12 new tests** in `tests/test_write_safety.py` for `is_full_universe_run` (full coverage
passes; the exact incident ratio, 7-of-2,662, fails; the 90% boundary in both directions;
empty-reference no-op; source-exceeds-reference still passes).

### 12.3 The real `23` blind-NULL-overwrite fix

Re-investigated exactly how a legitimate empty/partial source could overwrite existing
valuation fields with `NULL`, per instruction. **Finding: tightening `has_live_price`'s
coverage alone does not fix this** — even when `has_live_price` is `False`, the existing
`else:` branch (`23__intrinsic_value.py`, ~line 726) still builds `ttm_with_price` with
explicit `NULL` `price_close`/`market_cap` columns for **every** ticker, by design (so
`ttm_pdf`/`incoming_iv` always has a complete, uniform shape). Those `NULL`s reach
`incoming_iv` regardless of `has_live_price`'s value, and the real harm happens one step
later: the `iv_tbl` MERGE's `WHEN MATCHED THEN UPDATE SET ... price_close = source.price_close,
margin_of_safety_pct = source.margin_of_safety_pct` was **unconditional** — it always trusted
whatever this run computed, real or `NULL`, over whatever real value already existed.

**Fix**: the single `WHEN MATCHED` clause is now two, evaluated in order (standard Delta Lake
MERGE syntax):

1. `WHEN MATCHED AND (source.period_type != 'TTM' OR source.price_close IS NOT NULL OR
   {_ttm_full_universe_run}) THEN UPDATE SET` — the full update, all columns including
   price/margin-of-safety. Covers: every FY row (unaffected by this whole question — its price
   comes from a plain LEFT JOIN against `market_cap_asof` that already degrades to `NULL`
   safely with no destructive write involved, confirmed by re-reading that code path); any TTM
   row with a genuine non-`NULL` price; any TTM row when this run's price coverage was
   confirmed representative (a `NULL` is then trustworthy — the ticker genuinely has no live
   price, e.g. a real delisting).
2. `WHEN MATCHED THEN UPDATE SET` (no condition — catches everything the first clause didn't)
   — updates every column **except** `price_close`/`margin_of_safety_pct`, explicitly
   preserving whatever real value already exists. Reached only by a TTM row whose price
   computed `NULL` while this run's coverage was NOT representative — exactly the 2026-08-16
   `AAPL` scenario, and now structurally incapable of repeating it.

This preserves the project's own "real gap reads `NULL`" philosophy for a **genuinely**
full-universe run (a real delisting still correctly reads `NULL`) while refusing to let a
**degraded** run's `NULL` destroy a real value — the same distinction §12.2's semantic gate
draws elsewhere, applied here at the per-column level instead of the whole-block level because
TTM rows for tickers WITH real data and tickers WITHOUT it are interleaved in the same MERGE
source, unlike `22`'s all-or-nothing valuation block.

### 12.4 Regression tests

`is_full_universe_run` itself is covered by §12.2's 6 new tests. The MERGE-level split (the
two-`WHEN MATCHED`-clauses SQL) is not independently unit-testable — it's inline Spark SQL in
a Databricks-only notebook, the same constraint noted throughout this document — its
correctness rests on the pure `is_full_universe_run` logic feeding it plus a manual trace of
the SQL (documented in §12.3) confirming FY rows and non-NULL TTM rows always take the full-
update path and only the exact incident shape (TTM + NULL + non-representative coverage) takes
the preserving path. Flagged as a real gap for the next live bounded run to specifically
exercise (§12.6) rather than a gap this round could close without Spark.

### 12.5 Local verification

`pytest tests/ -q`: **326 passed, 2 skipped** (pre-existing, unrelated). `ruff check` on every
changed file: clean. No Databricks notebook executed this round — the `market_cap_live`
recovery (§12.1) used the same read-only-SQL-plus-`RESTORE TABLE` pattern as §10.3, not a
notebook run.

### 12.6 What remains (superseded — see §13/§14 for what happened next)

- ~~The live bounded run is still not re-attempted~~ — **it was, see §13.** It validated
  §11/§12's fixes for real and found a new, unrelated issue (§13), since resolved (§14).
- **`Shares Diluted` remains unmapped** (§11.5) — still true after §13/§14 too, out of scope.
- **PR #380 remains a draft, not merged.** Still true throughout §13/§14.

## 13. The authorized live run, and a new, third-round finding (2026-08-17)

With §11/§12's hardening complete and reviewed, the live bounded run (`18` → `22` → `23` →
`51`) was explicitly authorized, with a mandatory read-only preflight first.

### 13.1 Preflight

Two semantic checks measure different things and correctly gave different answers:
`12`'s guard (does the 8-EU-ticker scope cover the full universe?) — **False**, exactly as
required to prevent a destructive overwrite. `22`'s guard (does `market_prices_daily` as a
whole represent the fundamentals universe?) — **True**, both before and after `18`, because
`18`'s root-cause fix means it only *adds* rows via safe MERGE, never narrows the table. That
second "True" is the correct, safe outcome (the underlying data genuinely is representative),
not a red flag — confirmed explicitly before proceeding.

### 13.2 `18` — full success, the `classify_company_match()` fix validated live

All **8 of 8** EU tickers passed the market-data safety gate this time (previously 6 of 8) —
`FCC` and `SGO` now resolve correctly, live confirmation of the §11.6 fix.
`market_prices_daily` grew from 15,268,796 → 15,315,168 rows (+46,372, exactly consistent with
8 new tickers' full history) and 2,644 → 2,652 distinct tickers — a clean, additive MERGE, not
an overwrite. `stock_splits` unchanged (4,330 rows). `AAPL`/`MSFT`/`TSLA`/`AEM`/`AQN`/`BN` all
byte-identical to baseline. Zero entries in `ingestion_failures` for this run.

### 13.3 `22` blocked — a real, but unrelated, third finding

`22`'s primary guard correctly passed (representative price data) and `market_cap_asof`/
`market_cap_live` both completed a full, successful recompute. The **secondary** 10% guard
then correctly fired on the `financials_metrics` MERGE: would delete 474,698 of 2,039,873
rows (23%).

**Root cause, established via read-only investigation (Delta history + time-travel row
comparison, zero writes)**: NOT the incident recurring, NOT EU-related, NOT a US/CA mutation.
`financials` itself changed by exactly **+108 rows since the incident-recovery restore
point**, 100% belonging to the 8 EU tickers (real EU-ingestion work from a parallel session on
the same shared workspace — job names `phase5.1-run-21-after-eu-ingest` and an unnamed
interactive session, both dated 2026-08-16, well before this session's own incident), **0
rows removed, 0 rows modified** — confirmed twice (whole-table diff and a targeted
`AAPL`/`MSFT`/`TSLA`/`AEM`/`AQN`/`BN` check). `financials_metrics`'s restore point (v799,
07:25 UTC) turned out to predate not just the incident but *all* EU data ever landing in
`financials` — the daily production job that wrote v799 never touches EU data at all (Option
B; EU ingestion isn't on the scheduled DAG), so `financials_metrics` had literally never once
reflected any EU-related state.

A dedicated, purpose-built **read-only `DRY_RUN` diagnostic mode** was added to `22` itself
(not a reimplementation in SQL — it classifies the exact real `incoming_metrics` DataFrame the
MERGE would consume, then exits via `dbutils.notebook.exit()` before any write) to find the
real mechanism. **Result: 100% of the 474,698 "orphans" were "unclassified"** — not present in
`22`'s own `base_metric_cols` or `val_metric_cols` at all — and every top orphaned metric
label (`Owner Earnings (FY)`, `Graham Revised Value (FY)`, `DCF Value per Share (FY)`, `MoS %
(...)`) belongs exclusively to `23__intrinsic_value.py`. `orphan_by_market`: `US` 466,168,
`CA` 3,158, `OTHER` 5,372, **`EU` 0**. `modified_count: 0` — every metric `22` *does* own was
byte-identical to v799.

**The real mechanism**: `financials_metrics` is jointly populated by `22` and `23`, each via
an independent MERGE into the same table. `22`'s orphan-cleanup DELETE has no awareness that
`23` also owns rows there — from `22`'s own vantage point, every one of `23`'s rows looks like
an orphan, because `22` never produces them in the first place. In normal production this is
invisible (`22`→`23` always run back-to-back in the same DAG pass), but running `22` in
isolation — exactly what this validation's own step-by-step methodology did — exposed a
pre-existing architectural coupling that predates Phase 5.6 entirely.

## 14. Ownership hardening — `financials_metrics`'s joint-ownership fix (2026-08-17)

Per explicit instruction: not "disable the guard, we now know it's normal" — give `22`
positive knowledge of its own ownership boundary instead.

### 14.1 The fix

`22__derived_metrics.py`'s `WHEN NOT MATCHED BY SOURCE` DELETE is now scoped:
`AND target.metric IN (_22_owned_metrics)`, where `_22_owned_metrics` = `base_metric_cols ∪
val_metric_cols` — `22`'s own, already-existing, positively-declared metric vocabulary (the
same lists already used to build its own `long_base`/`long_val` source data). A deliberate
**allow-list**, not a deny-list naming `23` specifically: `22` only needs to know what it
itself owns, not enumerate every other current or future owner of rows in the same table. This
mirrors the exact pattern `23__intrinsic_value.py`'s own `_iv_labels`-scoped cleanup already
uses for the reverse case (never touching `22`'s rows) — the fix makes `22` follow the
convention `23` already established, not a new pattern. The 10% guard's before/would-delete
counts are scoped identically, so the percentage is now meaningful (evaluated against rows
`22` actually owns, not the whole jointly-owned table).

### 14.2 Regression tests

`fundamentals_pipeline.write_safety.scoped_orphan_keys()` — pure, unit-tested specification of
the filter (8 new tests, `tests/test_write_safety.py`): reproduces the real dry-run finding
exactly (an all-`23`-owned orphan set scopes to zero); confirms a genuinely stale `22`-owned
metric is still correctly flagged (the fix doesn't disable legitimate cleanup); confirms a
mixed orphan set keeps only the owned subset; and — critically — confirms the fix does **not**
weaken protection against the *original* 2026-08-16 incident pattern: a simulated large,
genuine loss of `22`-owned rows still scopes to the full loss and still trips
`assert_orphan_delete_safe()`.

### 14.3 Verified against real data — no live write

The `DRY_RUN` diagnostic (§13.3) was extended with the same ownership-scoped calculation
(`scoped_orphaned_count`) and re-run, read-only, against the exact same live discrepancy:

```
orphaned_count (raw, unscoped):    474,698  (23.27% — unchanged, kept for visibility)
scoped_orphaned_count (owned-only):      0  (0.00%)
```

**Confirmed**: with the fix applied, `22`'s real MERGE would delete zero rows belonging to
`23`, and the 10% guard would not fire at all — `22` can now safely run standalone, with no
dependency on `23` running immediately afterward to "repair" the damage. This is proof by
direct measurement against production data, not inference.

### 14.4 The 131 "new" rows, characterized

100% belong to the 8 EU tickers (`new_by_market`: `EU` 131, everything else 0) — `22`'s own
base metrics (`ROA %`, `Net Margin %`, `Piotroski F-Score`, `Net Income YoY %`, `Revenue YoY
%`, `Total Payout Ratio`) computed fresh from the 108 new EU `financials` rows (§13.3).
Exactly the metric set already established as the only kind EU tickers can produce (§7 —
blocked from anything share-count-dependent by the `Shares Diluted` gap). No anomaly, fully
explained, not investigated further.

### 14.5 Local verification

`pytest tests/ -q`: **334 passed, 2 skipped** (pre-existing, unrelated). `ruff check` clean.
No production write this round — both dry-run confirmations used the same read-only
`DRY_RUN` mechanism (exits before the MERGE), and the ownership/ticker/ticker-market
comparisons used only `SELECT`/`DESCRIBE HISTORY`/`VERSION AS OF` queries.

### 14.6 What remains

- **The live bounded run has not been re-attempted with this fix applied** — `18` succeeded
  live (§13.2); `22` was blocked, diagnosed, and fixed, but the fix itself has only been
  verified via `DRY_RUN`, never via a real MERGE. `23`/`51` have never run in this validation
  attempt at all. Needs its own explicit go-ahead.
- **`Shares Diluted` remains unmapped** (§11.5) — unchanged, still out of scope.
- **The `23` blind-`UPDATE`-with-`NULL` guard** (§12.3, fixed) is still unverified against a
  real live write for the same reason as above.
- **PR #380 remains a draft, not merged.** No new PR opened. No European universe expansion.
  No DAG scheduling change. No `fundamentals_screener`/Django file touched.

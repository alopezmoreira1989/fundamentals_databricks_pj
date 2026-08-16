# Phase 5.6 — European Dashboard Data Integration

**Implementation phase, in progress.** Connects the admission layer (Phase 5.3) and the
vertical slice (Phase 5.4 — FIRDS → admission → ESEF → `financials`) to the dashboard-export
boundary, so the 8 admitted European issuers become queryable in `dashboard_data.parquet`/
`dashboard_metrics.parquet` — the artifacts `fundamentals_screener` and the Streamlit app
actually read. Also closes a real currency-alignment gap this connection exposed in
`22__derived_metrics.py`/`23__intrinsic_value.py`, and gives the 8 issuers real daily
price/market-cap history for the first time.

Source-discipline labels: **VERIFIED LOCALLY** / **VERIFIED IN DATABRICKS** / **NOT YET RUN** /
**INFERENCE** / **OPEN QUESTION**. Unlike Phase 5.4's doc, most of this phase's own live
production behavior is still **NOT YET RUN** as of this writing — this phase was paused
mid-work for a machine handoff (see the git history around branch
`phase5-6-european-dashboard-data-integration`, draft PR #380) before the real bounded
Databricks run could happen. This document captures the architecture and code as they stand,
not results that don't exist yet; sections describing unrun behavior say so explicitly rather
than presenting inference as verification.

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

## 7. What is verified vs. not yet run

**VERIFIED LOCALLY** (pure Python, no Spark/network):
- `AdmissionCandidate.currency` resolution (§3) — 3 tests
- `check_no_export_ticker_collision()` (§5) — 5 tests
- Full suite: 303 passed, 2 skipped (pre-existing, unrelated), `ruff check` clean

**NOT YET RUN** (the real bounded Databricks run, deliberately deferred — see the branch's own
machine-handoff note):
- `17__firds_admission.py` re-run with the new `currency` column populated in
  `eu_admission_candidates` (currently live in production without it)
- `18__fetch_eu_market_data.py` — first-ever `market_prices_daily`/`stock_splits` rows for the
  8 admitted EU tickers, including the live Yahoo company-name safety check actually gating
  real writes (only individually spot-checked live so far, not run through the full notebook)
- `22__derived_metrics.py`/`23__intrinsic_value.py` — the currency-alignment fix (§4) actually
  producing `EUR`-denominated `market_cap_asof`/TTM rows for the 8 tickers, not a silent `USD`
  default
- `51__export_dashboard_data.py` — the `UNION ALL` (§2) and collision guard (§5) against real
  data, confirming the 8 EU tickers appear correctly in the exported artifacts and no collision
  fires
- **Idempotency**: re-running the chain twice and confirming no duplicate/drifted rows (the
  same check Phase 5.4's own §12 performed for the ingestion layer — not yet repeated at the
  export/derived-metrics layer this phase touches)
- **US/CA/Canada regression**: before/after row counts on `financials_metrics`,
  `financials_intrinsic_value`, and the exported artifacts for the existing US/CA universe,
  confirming the `UNION ALL` and the new `ticker_currency` branch changed nothing for
  non-European tickers (the same style of proof Phase 5.4's own §13 gave for the ingestion
  layer)

## 8. Known limitations / open questions

- **ADR-0012 (`MIC:ISIN` vs. `MIC:TICKER`) is still Proposed, not Accepted** — this phase
  deliberately does not resolve it; Option B (§2) was chosen specifically so this connection
  doesn't have to.
- **`market = 'EU'` is one Generation-1 value for every admitted country** — a future
  non-Eurozone EU admission would need real per-country handling in both the export union
  (currently defaulting every European row's currency straight from `eu_admission_candidates`)
  and the `QUOTE_CURRENCY_BY_MARKET` extension (§4), neither of which is built yet.
  **OPEN QUESTION**, not blocking today since every currently-admitted MIC is Eurozone.
- **No unit test exists (or can reasonably exist, in this repo's testing philosophy) for the
  Spark-native currency-alignment join fix itself** (§4) — the real bounded Databricks run is
  the verification for that piece, not a substitute test.
- **This phase does not schedule anything on the DAG** — `18__fetch_eu_market_data.py` is
  explicitly non-scheduled, and none of the 9-task production DAG's tasks were modified.

## 9. Next steps (in order)

1. The real bounded Databricks run (§7's "NOT YET RUN" list): `17` → `18` → `22`/`23` → `51`.
2. Idempotency check (re-run twice, diff row counts).
3. US/CA/Canada regression (before/after row counts on the existing universe).
4. Once all three are green: consider whether `fundamentals_screener`/Streamlit actually
   render the new European rows correctly (out of scope for this phase — the dashboard-export
   boundary is the explicit stopping point, matching Phase 5.4's own "does not make European
   data visible in `fundamentals_screener`" boundary one layer up the chain).

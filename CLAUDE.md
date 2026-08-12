# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

Databricks analytical pipeline that ingests SEC EDGAR XBRL filings (10-K/10-Q) for ~3,000 US tickers, joins Yahoo Finance prices, derives financial metrics + intrinsic values, runs an investment-archetype **backtester**, and serves it all via Delta tables to a Databricks dashboard **and a public Streamlit app** (fed by GitHub Release parquet artifacts). Entry point: `90__pipelines/91__full_pipeline.py` (Databricks notebook source format, run as a Databricks Job). A pure-Python public API inside the installable `fundamentals_pipeline` package (`schemas.py`, `valuation.py`, `periods.py`, `backtest.py`, `splits.py`, `fx.py`, `identity.py`, `tickers_universe.py`; no Spark/Databricks dependency) holds the reference formula/contract/backtest logic and is unit-tested by `tests/` (pytest). **All new work is produced in English** (docs, code, identifiers, comments, commit messages); some legacy content (prose, metric labels, JSON hierarchy values) is still in Spanish and is treated as data — see Conventions.

## Conventions that must be preserved

- **File naming `NN__name.py` is mandatory.** Files use `<stage><order>__<purpose>` so the pipeline order is visible from filenames alone (e.g., `21__clean_and_merge.py`, `21b__derive_quarterly.py`). New files must follow the pattern.
- **Naming convention: `NN__name`, double underscore after the numeric prefix.** Every folder or file that is a sequential stage of the pipeline — or a parallel consumer of a published stage's output (e.g. the frontends under `60__frontends/`) — is named `NN__descriptive_name`: two underscores between the number and the name, single underscores only to separate words *within* the name (e.g. `21b__derive_quarterly.py`, `60__frontends/61__streamlit/`). This is non-negotiable going forward — any new pipeline step, audit script, or frontend added from here on follows `NN__name`, never `NN_name` or an unprefixed name.
  - **Exception — the importable library modules and tooling are never numbered.** The pure-Python public modules at the top of the `fundamentals_pipeline` package (`schemas.py`, `valuation.py`, `periods.py`, `backtest.py`, `splits.py`) are the project's single source of truth for financial logic + the data contract; they are library modules, not pipeline stages, so they keep plain names (no `NN__` prefix) and sit alongside the numbered stage dirs. `tests/` (root-level, standard Python convention, exercising those modules and the Streamlit `lib/` transversally) is likewise not a stage and stays unprefixed. The package is installable (`pyproject.toml` at the repo root) and is imported the same way by every consumer — pipeline notebooks, scripts, tests, and `fundamentals_screener` — via `pip install -e .`, never via `sys.path`. A future addition that is genuinely transversal infrastructure rather than a step in the sequence follows this exception, not the `NN__` rule — flag it to the repo owner if which one applies is ambiguous.
- **English for all new work; don't bulk-translate existing Spanish data labels.** All new documentation, code, identifiers, variable names, comments, and commit messages must be in English. (The owner may phrase requests in Spanish — that is never a cue to switch the output language.) **Caveat:** the existing Spanish concept/metric labels in `00__config/*.json` (`concept_hierarchy.json`, `metrics_hierarchy.json`) are used as Delta join keys and dashboard display values — renaming them is a breaking data change. Leave existing identifiers as-is unless explicitly asked to migrate them, and only with the data/dashboard impact handled.
- **`Q4 = FY − YTD_Q3` is intentional, not a bug.** `20__transformation/21b__derive_quarterly.py` derives Q4 by subtracting the YTD-Q3 figure from the annual FY to capture year-end audit adjustments. Do not "fix" this to `Q1+Q2+Q3+Q4`. Q1–Q3 use standalone SEC reports or YTD deltas based on each concept's `kind` (flow_additive / flow_nonadditive / stock).
- **Balance Sheet dedup**: SEC re-reports prior snapshots in later 10-Qs. Dedup by `(ticker, concept, period_end)` keeping the latest `filed` — preserve this when touching ingestion/merge.
- **Pricing is `period_end`-aligned, sourced from `market_prices_daily`.** `10__ingestion/12__fetch_market_data.py` writes a **daily** price store `main.financials.market_prices_daily` (one row per `(ticker, date)`; raw `close` for market cap, `adj_close` for returns). `22__derived_metrics.py` computes each FY's `market_cap` as the raw `close` on the latest trading day ≤ that FY's `period_end` × `Shares Diluted` reported as-of `period_end` — so non-December filers (AAPL/Sep, MSFT/Jun, WMT/Jan) are priced at their real fiscal close, not Dec 31. Use raw `close` (not `adj_close`) for market cap — `adj_close` would fold future splits into the historical cap.
- **`market_cap_asof` is the period_end-aligned price/market-cap table.** `22__derived_metrics.py` persists `main.financials.market_cap_asof` (`ticker`, `fiscal_year`, `period_end`, `price_close`, `market_cap`, `currency`) — the as-of-fiscal-close price and cap, keyed like the old `market_data` but on the **fiscal** (not calendar) basis. `23__intrinsic_value.py` (Margin of Safety / TTM price) and `50__publish/51__export_dashboard_data.py` (the exported `Market Cap` row) read it. New price/cap consumers should read `market_cap_asof`, not `market_data`. `currency` (added alongside the currency-alignment fix below) is each row's real native reporting currency, STORED not inferred — read it instead of assuming USD.
- **Currency alignment: `price_close`/`market_cap` are converted into each ticker's `reporting_currency` before storage — a same-ticker unit-mismatch fix, not cross-market USD normalization.** Some filers report fundamentals in a different currency than their primary listing quotes in (a USD-reporting, CAD-quoted Canadian gold miner is the concrete case — Barrick, Agnico Eagle). `market_cap = price × shares` is only arithmetically correct when both operands share a currency, so this is a correctness bug independent of any cross-market comparability choice. The repo owner's explicit decision remains: **native reporting currency only, no dual-currency columns, no blanket-USD conversion** — normalizing everything to one display currency is a future FRONTEND concern (a "view in USD" toggle), not a pipeline one. `22__derived_metrics.py` detects a quote-currency (`QUOTE_CURRENCY_BY_MARKET = {"US": "USD", "CA": "CAD"}`, keyed off `market`) vs `reporting_currency` mismatch and converts via `fundamentals_pipeline/fx.py`'s `convert_price()`; `23__intrinsic_value.py` applies the same fix independently to its TTM live-price path (which bypasses `market_cap_asof` entirely — reads `market_prices_daily` directly, so it needed its own copy of the alignment logic). **Non-negotiable date-anchoring rule, no exceptions:** every conversion uses the FX rate dated at the figure's own observation date — a fiscal `period_end` for FY, a live price's own trade date for TTM — never the SEC `filed` timestamp, never "today's"/run-time spot rate (the currency-domain analogue of the `market_data`→`market_cap_asof` fiscal-close fix above; converting a 2022 figure with today's rate would reintroduce that exact bug class in a new dimension). A missing required FX rate is never silent and never a stale-rate fallback — it's logged to `ingestion_failures` (`step="currency_alignment"` / `"currency_alignment_ttm"`) with the ticker/date/pair, and the affected `(ticker, fiscal_year)` rows are excluded from `market_cap_asof`/TTM `market_cap` for that run (same "real gap reads NULL/absent" convention as every other data guard in `22`) — deliberately scoped to just those rows rather than raising and aborting the whole run, so one Canadian ticker's transient FX gap doesn't block that run's refresh for the other ~2,600+ tickers too. `fundamentals_pipeline.fx.MissingFxRateError` is the pure reference/contract (raised by `convert_price()` and locked down by `tests/test_fx.py`) that this Spark-native logging-and-exclusion path mirrors — it is not itself raised inside `22`/`23`. `main.financials.fx_rates_daily` (`base`, `quote`, `pair`, `date`, `rate`) is fetched by `12__fetch_market_data.py` using the same batched yfinance machinery as prices/splits, for currency pairs derived DYNAMICALLY from whatever currencies are actually in play (not a hardcoded list) — both directions of every pair (e.g. `CADUSD=X` and `USDCAD=X`) are fetched so `fx.py` never needs to invert a rate. `51__export_dashboard_data.py` publishes the table's **full daily history** (not just latest) as the `dashboard_fx` artifact (schema v11) so a future USD-lens toggle can convert a historical figure using that figure's own period_end rate. **This is a prerequisite gate for Canadian ingestion**: don't flip `02__tickers_master.py`'s `INGEST_TSX_COMPOSITE` on in production before this landed (it did, 2026-07) — ingesting Canadian filers without it would have silently produced wrong market-cap-derived multiples for any USD-reporting/CAD-quoted name.
- **`main.config.tickers` identity key is `(ticker, market)`, not bare `ticker`.** A bare
  ticker symbol is not a safe identity across markets — e.g. Magna International trades as
  `MG` on the TSX, Mistras Group as `MG` on the NYSE (confirmed still live in SEC's own
  `company_tickers.json` as of 2026-07: a stale entry from before Mistras went private in
  2023). `02__tickers_master.py` sets `market` (`"US"` for S&P 500/Russell 3000/favorites,
  `"CA"` for admitted S&P/TSX Composite tickers), dedups on `(ticker, market)`, and calls
  `fundamentals_pipeline/identity.py`'s `check_no_cross_market_collision()` before the Delta
  write; `11__fetch_sec_xbrl.py` calls the same guard before CIK resolution. It raises
  `CrossMarketCollisionError` instead of silently overwriting one company's row with
  another's, but also tells apart a genuine collision from the SAME company dual-listed on
  both markets (`BAM`, `SHOP`, `GFL`, ...) via `classify_company_match()` — normalized-name
  token comparison, three-way (same/different/ambiguous) and deliberately conservative: it
  only ever auto-merges a dual-listing when confident, and raises (never silently guesses)
  for anything else, including a merely-plausible partial match. `market` is deliberately a
  **new** column, distinct from the pre-existing `exchange` (Yahoo per-venue mnemonic —
  NYQ/NMS/NGM/...) and `country` (incorporation jurisdiction) columns, which already carry
  live data displayed on both frontends and must not be repurposed or overwritten.
- **Canadian ticker-universe onboarding (multi-market roadmap Phase 1) — ticker-identity
  layer implemented, IFRS/valuation work still pending.** `02__tickers_master.py`'s
  `fetch_tsx_composite()` pulls S&P/TSX Composite membership via XIC (iShares Core S&P/TSX
  Capped Composite Index ETF) — a **plain CSV** at a `blackrock.com/ca/investors/...` path,
  NOT the varnish-api `fundDownload` SpreadsheetML endpoint `fetch_russell3000()` uses (that
  endpoint 400s for this Canadian fund regardless of params, confirmed 2026-07). Every XIC
  candidate is gated through SEC's `company_tickers.json` (Phase 1 = Canadian **MJDS/40-F
  filers** only, i.e. companies that also register with the SEC — not the whole TSX) AND
  `classify_company_match()` — a ticker resolving in SEC's map is necessary but not
  sufficient, since that map is bare-ticker-keyed across its entire ~10,000-company universe
  and plenty of XIC tickers coincidentally resolve to an unrelated US filer (confirmed real
  cases: `TVE`→the Tennessee Valley Authority, `IAU`→the iShares Gold Trust ETF, `SAP`→SAP SE,
  among others). Excluded tickers are logged with a reason, not silently dropped.
  `accounting_standard`/`reporting_currency` are left `NULL` for admitted Canadian rows —
  real MJDS/40-F filers are a genuine mix (confirmed 2026-07-09 against live production
  data from `main.config.tickers`: most banks/energy file `ifrs-full` in CAD, and Imperial
  Oil (`IMO`) also files `us-gaap` in **CAD** despite being a US-style filer, but
  Shopify/BlackBerry file `us-gaap` in USD, and Nutrien/Gildan file `ifrs-full` in
  **USD** — currency is not a function of namespace alone) —
  `11__fetch_sec_xbrl.py` derives both from the real per-ticker `companyfacts` response it
  already fetches for ingestion and writes them back via `MERGE INTO config.tickers`, scoped
  to `market="CA"` rows only. `12__fetch_market_data.py` translates a Canadian ticker to its
  Yahoo symbol (`f"{ticker}.TO"`) only at the `yf.download()` call boundary — the bare ticker
  stays the identity everywhere else (`market_prices_daily`, `stock_splits`, `config.tickers`,
  `financials`). **Known, un-fixed limitation:** `02`'s `has_logo`/`industry`/`description`/
  `employees`/`website`/`founded` probes call `yf.Ticker(ticker)` with the bare ticker —
  `country`/`exchange` are explicitly protected (re-seeded to `"Canada"`/`"TSX"` after the
  probe) but the other fields are not, so they may come back `NULL` (or, rarely, data for an
  unrelated Yahoo-side symbol) for Canadian tickers until that's addressed as follow-up work.
  **Still out of scope, deliberately:** IFRS XBRL concept mapping in `01__tickers.py`, dual-
  currency (native + USD) storage anywhere in the pipeline, and re-keying
  `financials_raw`/`financials`/`market_prices_daily`/`market_cap_asof`/both frontends off
  bare `ticker` — those are separate, not-yet-started roadmap items. (The same-ticker
  price/fundamentals currency-*mismatch* bug this note originally flagged as blocking
  Canadian ingestion — see the currency-alignment entry above — has since been fixed; that
  was a correctness gate, not a "dual-currency" feature, and is not reopened by this note.)
- **`market_data` is frozen legacy** — no longer rebuilt. It was the calendar-year-aligned (last raw `close` per **calendar** year × FY `Shares Diluted`) price/cap table; its 0–11mo fiscal offset distorted multiples for non-December filers, so `12` no longer writes it and all consumers were migrated to `market_cap_asof`. The table is left in place for ad-hoc back-compat queries but receives no new data. Don't add new dependencies on it.
- **`EPS CAGR (5Y) %` and `PEG` (2026-07) — the first real published metrics computed from a
  multi-year EPS window, not a single-year snapshot.** Previously, a working trailing EPS CAGR
  calculation existed only *inside* `23__intrinsic_value.py`, as a disposable internal input to
  the Graham Revised valuation model (split-adjusted, point-in-time, 5y with a [3,5]y fallback
  window) — computed, used once, then discarded; nothing published it. `22__derived_metrics.py`
  now publishes it for real:
  - **`EPS CAGR (5Y) %`** (base block) reuses the split-factor machinery `22` already computes
    for `_shares_adj` (the cross-year share-count adjustment used by Net Buyback Yield %/
    Piotroski's no-dilution signal) rather than re-deriving a second copy the way `23` does —
    `_eps_adj = EPS Diluted / _split_factor`, the inverse of `_shares_adj`'s multiplication (EPS
    = NI/shares, so if shares are scaled up by `factor` to match today's post-split basis, EPS
    must be scaled *down* by the same factor). The CAGR window itself (base_year in
    `[fiscal_year-5, fiscal_year-3]`, picking the smallest/longest-span base_year, both endpoints
    required `> 0`) is ported from `23`'s own proven logic almost verbatim, but stored `× 100`
    (a percent, matching every sibling growth metric — `23`'s own copy is a raw fraction since it
    feeds a formula, not a display column; a deliberate, not overlooked, divergence).
  - **`PEG`** (val block) = `P/E ÷ EPS CAGR (5Y) %`, gated on both being positive. `val_wide` is
    normally a **separate lineage** from the base block's `metrics_wide` (re-derived from `raw`,
    not reused — see the file's own comment on `_tbv_val` for the established precedent). PEG is
    a deliberate, narrow exception: rather than re-deriving the whole CAGR self-join a second
    time inside `val_wide`'s lineage (duplicating a non-trivial, failure-prone computation), a
    single already-computed column is joined in from `metrics_wide` (`how="left"`, preserves
    `val_wide`'s own market-cap-gated row grain).
  - Both are registered in `00__config/metrics_hierarchy.json` (`Growth > Multi-Year`,
    `Valuation > Price Multiples`) — required for `dashboard_metrics`'s LEFT JOIN against that
    table to give them a category/sort_order at export time, same as any other metric.
  - **Consumers**: `fundamentals_screener`'s Lynch Investor Presets criteria — `EPS CAGR (5Y) %`
    display-only, `PEG < threshold` the actual filter (see the Investor Presets entry below) —
    and Graham's own Strict-level EPS-growth criterion, which uses a *different*, literal-to-the-
    book 3-year-rolling-average test (`eps_growth_avg_tickers`, screener-side, over the raw
    `EPS Diluted` concept directly — not this published metric, which is a single-endpoint 5y
    CAGR, a different definition serving a different consumer).
- **Screener "modes" (`fundamentals_screener`) share one route + one nav pattern — established by
  the Net-Net Finder (milestone "Net-Net Finder", issues #257–262), the first of a planned family.
  Investor Presets (below) is the second and, as of 2026-07, the family is General Screener /
  Net-Net Finder / Investor Presets. Follow this same shape for the next mode rather than
  inventing a new one:**
  - **One URL, one `?mode=` query param** on the existing `screen` view (`?mode=general` is the
    default/omittable; a new mode gets its own value, e.g. `?mode=presets`) — never a separate
    URL route per mode. `views.screen()` branches on `mode` right at the top and delegates to a
    private `_<mode>_screen(request)` function; the general screener's own logic is untouched
    below that branch.
  - **A shared `_mode_nav.html` partial** (two-or-more-tab pill bar, `<a href="?mode=...">`,
    active state via a `mode` context key every mode's view must set) — included at the top of
    every mode's template, carrying forward only the descriptive filters that mode genuinely
    shares with the others (a precomputed `shared_qs` context key, built the same way in every
    mode's view — see `views.screen()`'s own `shared_qs` construction — never assembled ad hoc in
    a template with nested `{% if %}` chains).
  - **AJAX mode-switching (superseded the original full-page-reload decision).** The mode nav,
    and any mode-specific filter/level control, was originally full-page-reload-only — confirmed
    deliberately when the Net-Net Finder was built, since at the time each mode's within-mode
    interactions (filters, sort, pagination) weren't AJAX either, so a mode-switch partial-swap
    wouldn't have saved much. Once every mode's own within-mode interactions became AJAX
    (auto-apply filters, sort, pagination, level/preset pills), switching modes via full navigation
    became the one slow step left, confirmed by real production latency measurements — so the
    mode-nav itself was converted to AJAX too. Mechanism: `screener.js` (one shared static file,
    loaded unconditionally by `base_screener.html`) delegates a `click` listener on `#main` for
    `.nav-tabs a`; on click it `fetch()`es with an `X-Mode-Switch: 1` header, swaps `#main`'s
    innerHTML, reads `X-Page-Title` (RFC-2047-decoded via `decodeURIComponent`/`quote()` — Django
    MIME-encodes non-ASCII header values, e.g. an em dash, which `fetch()`'s `Headers.get()`
    doesn't decode on its own) into `document.title`, `history.pushState`s, then re-invokes
    `initModeContent()` to rewire the freshly-swapped mode's own within-mode listeners (`#main`
    itself is never replaced, so its own delegated listener survives the swap). Server side:
    `views._render_mode()` is a 3-tier responder per mode view — full page (`full_template`) /
    `X-Mode-Switch` main-fragment (`main_template`, exactly the `{% block content %}` body,
    included both by the full template and returned raw here — one source of truth) /
    `X-Requested-With` narrow within-mode fragment (`fragment_template`, unchanged from before).
    Browser back/forward (`popstate`) stays a full reload — deliberately out of scope, matches
    what was actually asked for.
  - **Each mode gets its own dedicated template(s) and — if its own results list can plausibly
    exceed ~50 rows — Python-side pagination**, mirroring `views.PAGE_SIZE`/the general
    screener's own pagination-control markup. Confirmed the hard way on the Net-Net Finder: an
    assumption that "results are always a small handful" turned out false for its loosest level
    (positive-NCAV alone matched roughly a third of the universe against real data), and shipping
    that unpaginated produced a multi-megabyte response — verify the actual row-count order of
    magnitude against real data before assuming a new mode's results list is naturally small
    enough to skip pagination.
  - **A repository method built for a bulk/filtered LIST (e.g. `net_net_screen`) is not
    automatically reusable for a single-ticker DETAIL view** (e.g. surfacing the same signal on
    the company Valuation page) — the list version's eligibility gate usually doesn't apply
    (a detail page shows a company's own numbers regardless), so a companion single-ticker method
    (e.g. `net_net_snapshot`) is typically warranted rather than filtering the bulk list down to
    one row.
  - **CGI hosting caps how much AJAX mode-switching can help.** The `fundamentals_screener`
    reference deployment (Dinahosting plain CGI, see External consumers below) has no persistent
    process between requests, so every request — AJAX or not — still pays a full Python/Django
    cold-start cost; confirmed via real production `curl` timing (~1.5–2.0s per request
    uniformly, ~0.18s for a static asset) that this, not the mode-nav being full-reload, was the
    real latency floor. A follow-up (splitting `fundamentals_pipeline/schemas.py`'s pandas-free
    constants into `artifacts.py`, see Layout below) shaved cold-import time but confirmed DuckDB's
    own Python binding imports pandas on any parameterized query regardless — a real, structural
    ceiling on further gains within this stack, not a bug to keep chasing. Moving off CGI (e.g. a
    persistent-process host) was investigated and explicitly deferred by the repo owner in the
    short term — don't re-propose it without a fresh instruction to look at hosting again.

- **Investor Presets (`?mode=presets`) — three schools (Graham/Buffett/Lynch), each with its own
  criteria set, all fully live and evaluated at a user-chosen conservatism level.**
  `services.py`'s `_PRESET_DEFINITIONS` holds each school's static copy (portrait, tagline,
  headline); `repositories/company_listing.py`'s `_PRESET_WHERE` (a per-preset SQL-predicate
  function over the latest-FY metric pivot) and `_preset_multi_year_checks` (per-preset,
  per-level multi-year check specs) are what `preset_screen()` actually filters on.
  - **Shared multi-year repository methods**, reusable across any preset's deferred/live
    multi-year criteria rather than each one reimplementing its own window logic:
    `sustained_metric_tickers` (a `dashboard_metrics` metric ≥ a threshold in every one of the
    last N years, exact count — used by Buffett's sustained ROE), `sustained_concept_tickers`
    (same shape, but over a raw `dashboard_data` *concept* like "Net Income" rather than a
    derived `dashboard_metrics` metric — needed because a metric and a raw statement line item
    are two different published tables with different column shapes),
    `uninterrupted_dividend_tickers` (a *tolerant* variant — dividend rows are absent, not
    zeroed, for a non-payer/cut year, so a plain "latest N non-null rows" window can silently
    reach past a real gap into a
    company's paying years from before it stopped; this one anchors to each ticker's own most
    recent reporting year and requires a *contiguous* run of at least a floor, not the full
    window, tolerating a newer payer's shorter-but-real record), `eps_growth_tickers` (single-
    year endpoint EPS growth over N years — latest FY vs. FY−N, both required positive) and
    `eps_growth_avg_tickers` (Graham's own literal EPS-growth test — 3-year averages at each end
    of an N-year span, not single-year endpoints; used only at Graham's Strict level). All five
    live in `CompanyListingRepository`; a preset's check-spec tuple is dispatched to the right one
    via `_qualifying_tickers`.
  - **Strict / Moderate / Relaxed conservatism levels**, one pill per school (mirrors the
    Net-Net Finder's own level pill exactly, including CSS reuse — `level-btn`,
    `level-dot--strict`, `level-dot--moderate`, `level-dot--relaxed`, zero new styles) but
    rendered *inside* the school's own
    card below the criteria list, not next to the school pill — the two pills answer different
    questions ("which school" vs. "how conservative") and shouldn't compete for the same
    "choose something" moment. **Pill display order is `("relaxed", "moderate", "strict")` in
    both features** (`company_listing.PRESET_LEVELS` / `views._NET_NET_LEVELS`) — increasing
    conservatism left to right, a deliberate cross-feature consistency choice; the two features'
    *defaults* still differ (Net-Net defaults to Relaxed, Investor Presets defaults to Strict —
    the repo owner's explicit choice, since Presets' criteria are otherwise this app's closest
    analogue to a book's literal numbers, and the toughest reading should be the resting state)
    and are hardcoded at each fallback site, not derived from the tuple's first element.
    Per-preset, per-level threshold values (`_GRAHAM_THRESHOLDS`/`_BUFFETT_THRESHOLDS`/
    `_LYNCH_THRESHOLDS` in `company_listing.py`) are the single source of truth both the SQL
    predicates and the criteria-list label copy (`services.py`'s `_graham_criteria`/
    `_buffett_criteria`/`_lynch_criteria`, invoked by `get_preset_definition(preset, level)`)
    read from — the label text is never a second, hand-duplicated copy of the numbers. "Strict"
    is Graham's own literal numbers where one genuinely exists (*The Intelligent Investor*'s
    exact rules), *except* the dividend criterion's window — Graham's literal 20-year bar is
    confirmed infeasible against real published data (dividend payers rarely have 20y of history
    on record at all) and is capped at 10y, the export's own retention limit, instead. Buffett
    and Lynch have no single quotable rule set (their criteria here are this project's own
    synthesis of each investor's philosophy, not book quotes), so their "Moderate" tier is
    exactly today's pre-levels live values, with Strict/Relaxed tightening/loosening from there.
  - **`EPS CAGR (5Y) %` and `PEG`** (Lynch's remaining two criteria) are real published
    `22__derived_metrics.py` metrics, not screener-side computations — see the "EPS CAGR (5Y) %
    and PEG" bullet above. `EPS CAGR (5Y) %` is display-only at every level (no threshold of its
    own); `PEG < threshold` is the actual filter.
  - **The Django-side query/criteria work and the pipeline metric it depends on are two
    separately-deployed changes** — merging the Django side does nothing until the *next*
    scheduled Databricks pipeline run actually publishes the new metric into `dashboard_metrics`
    (the GitHub Release is a snapshot, not live-queried). Confirmed as a real, temporary gap in
    practice (Lynch's PEG filter returned 0 matches at every level, including Relaxed, for
    several hours after the Django-side PR shipped, until the next pipeline run backfilled the
    metric) — not a bug to chase if you see it, but worth flagging to the repo owner if a new
    preset criterion depends on a metric that was *just* added to the pipeline.

- **Forecasting (2026-08) — 10-year cross-sectional ML scenario forecasts, published as
  `dashboard_forecast`, surfaced in `fundamentals_screener`'s Forecasting page/tab.**
  `fundamentals_pipeline/forecasting.py` + the pipeline stage `20__transformation/
  24__forecasting.py` train **LightGBM quantile regression** (5 quantile levels — p10/p25/p50/
  p75/p90, i.e. Bear/Low Bear/Crab/Low Bull/Bull) cross-sectionally across the full ticker
  universe for years 1-5 of Revenue/Net Income/Free Cash Flow, using LightGBM's **native
  `Dataset`/`train` API, not the `sklearn` wrapper** — deliberate: issue #329's Phase 0 audit
  only verified `lightgbm`/`catboost` install on Databricks Free Edition serverless, never
  `scikit-learn`, and a real local run confirmed `LGBMRegressor(...)` hard-fails there
  (`LightGBMError: scikit-learn is required`). A friend's suggestion mid-build to switch to
  Random Forest + `GridSearchCV` was assessed and declined for the same reason (RF has no
  native quantile-regression support without an unverified dependency; `GridSearchCV` needs
  full scikit-learn) — the legitimate part of that feedback (no regularization/early-stopping/
  held-out evaluation discipline) was addressed instead: `_REGULARIZATION_DEFAULTS`, an
  optional `validation_panel` kwarg enabling `lgb.early_stopping`, a pure-numpy
  `roc_auc_score` (deliberately not `sklearn.metrics`, same install-risk reasoning), and
  `cross_validate_loss_classifier` (walk-forward/expanding-window CV, never a random split —
  this project's own no-look-ahead discipline).
  - **Two-part (hurdle) target design**: a continuous `log_growth` regressor (positive-to-
    positive only) plus an independent binary `is_loss` classifier per horizon, combined via
    `reconstruct_forecast_value` — `value_t = value_from * exp(predicted_growth)`
    structurally cannot represent a company going into losses, so growth alone isn't enough.
    Combination rule: a quantile level `q` is "the value below which fraction `q` of the
    distribution lies," so `P(loss) >= q` (or `value_from` itself already non-positive) floors
    that quantile's forecast to `0.0` — no separate "loss magnitude" model.
  - **Chernozhukov et al. (2010) quantile rearrangement** (`rearrange_quantiles`) sorts the 5
    independently-predicted quantiles into monotonic order per row before use — 5 separately
    trained models have no structural guarantee p10 ≤ p25 ≤ ... ≤ p90 without it.
  - **Years 6-10 blend toward each scenario's own DCF terminal-growth rate** —
    `terminal_growth_for_quantile` maps a quantile level to a terminal growth rate via linear
    interpolation across `valuation_assumptions.json`'s existing bear/mid/bull `dcf.
    growth_terminal` profiles (anchored at q=0.10/0.50/0.90; q=0.25/0.75 are a principled
    interpolation, not an arbitrary halfway split) — resolving the "5 quantile scenarios vs. 3
    DCF profiles" mismatch by reusing the DCF model's own scenario assumptions rather than
    inventing new ones. `blend_terminal_years`/`blend_terminal_years_from_values` produce the
    full years-6-10 value path (not one lump Gordon terminal value, since the fan chart needs
    a point at every FY), mirroring `valuation.py`'s own two-stage DCF spirit. The exact same
    3-anchor interpolation function is reused a second time for **WACC** (not just growth) to
    compute PV-discounted forward multiples — `terminal_growth_for_quantile`'s parameter names
    are cosmetic, it's generic linear interpolation across bear/mid/bull, not growth-specific.
  - **PV-discounted forward P/E / FCF Yield** (`valuation.py`'s `pv_discount`/`forward_pe`/
    `forward_fcf_yield`, issue #334) were added as pure functions before anything called
    them — issue #336's tab UI was deliberately scoped to only *consume* published data, so
    nothing in the original milestone breakdown ever wired them in. Fixed by extending
    `24__forecasting.py` itself: each `(ticker, quantile_level, horizon)` net_income/
    free_cash_flow forecast is discounted at that quantile's interpolated WACC against the
    ticker's own latest market cap, appended as `forward_pe`/`forward_fcf_yield` **`metric`**
    rows in the **same** `financials_forecast` table/`dashboard_forecast` artifact — no new
    artifact or schema version needed, since `metric` is already a free-text column. Flag this
    kind of gap early in future milestones: a "pure function only" issue followed by a
    "UI only, consume what's published" issue can silently skip the wiring step in between.
  - **Consumer-side (`fundamentals_screener`)**: `ForecastRepository` reads `dashboard_forecast`
    directly (this package's own DuckDB view naming — the artifact name itself, not a short
    alias), explicitly filtering `metric` to the 3 raw target metrics for the fan chart vs.
    `forward_pe`/`forward_fcf_yield` (mid/"Crab" scenario only) for the multiples table, since
    both live in the same table. `services.get_forecast_chart` composes it with
    `CompanyRepository`'s existing statement/metric-history reads for historical context, and
    **pre-pends each scenario's own FY0 historical value server-side** before the JS chart ever
    computes its shared min/max scale — this is what makes "every line (historical + all 5
    scenarios) shares one y-scale/domain, visually originating from the same FY0 point" (issue
    #336's explicit required test) trivial on the JS side rather than something the chart code
    has to engineer. The Forecasting page/tab was originally built against `web/` (issue #335,
    PR #346) and had to be rebuilt from scratch in `fundamentals_screener` once `web/` was
    retired mid-milestone — see ADR-0008.
  - **A real production 500 (2026-08-05, fixed same-day)**: `services.get_forecast_chart`
    used `zip(a, b, strict=True)` — the exact Python 3.10-only pitfall this file's own "Python
    floor" note (under External consumers) already documents from a prior 2026-07-20 incident
    — written fresh into new code despite that documented history. `ruff.toml`'s repo-wide
    `target-version = "py310"` does not catch this for `fundamentals_screener/` specifically;
    only hitting a real per-ticker URL after deploy did (`curl -s -o /dev/null -w "%{http_code}"
    https://alopezm.xyz/apps/screener/AAPL/`) — the consumer's own deploy health check only
    pings the general screener landing page, which exercises no per-ticker view code at all.
    **When writing new code in `fundamentals_screener/`, actively check for `zip(...,
    strict=...)` and module-level PEP 604 unions — don't rely on remembering the floor, and
    don't trust a clean `ruff check` alone as proof.**
  - **The daily GitHub Release publish can silently regress a working release to a `draft`**
    (encountered twice now: 2026-05-31, 2026-07-30/2026-08-05) **well after
    `52__publish_to_github.py`'s own retry-and-verify loop reported success** — a draft has no
    public `releases/download/<tag>/…` URL, so every consumer 404s despite the Databricks job
    itself showing `SUCCESS`. Diagnostic: `gh api repos/.../releases --paginate -q '.[] |
    select(.tag_name=="latest") | {id, draft}'`; fix is a one-line `gh api -X PATCH
    repos/.../releases/<id> -f draft=false` (exactly what the script's own `publish_release()`
    does) — no pipeline re-run needed. If this recurs often, it may be worth a periodic
    post-hoc re-verification rather than trusting the publish-time check alone.
  - **Terminal growth rate floored at 2% inflation (2026-08-12).** `24__forecasting.py`'s
    `INFLATION_FLOOR = 0.02` wraps the terminal-growth call at the years-11-20 blend step:
    `max(fc.terminal_growth_for_quantile(...), INFLATION_FLOOR)`. Today's
    `valuation_assumptions.json` `growth_terminal` values (bear 2%/mid 2.5%/bull 3%) already
    clear this, but only coincidentally — the floor makes it an enforced invariant a future
    config edit or per-ticker override can't silently violate. Deliberately scoped to the
    terminal rate only: NOT applied to `terminal_growth_for_quantile`'s second reuse for WACC
    (section 5b — "inflation floor" isn't meaningful for a discount rate), and NOT applied to
    the explicit years-1-10 ML forecasts, which must stay free to show a genuinely declining
    company as declining.
  - **Standalone Forecasting/Valuation pages removed (2026-08-12) — both are Company Detail
    tabs only now.** `fundamentals_screener/urls.py` no longer has `forecasting`/
    `forecasting_data`/`valuation`/`valuation_data` routes; `forecasting.html`/`valuation.html`
    and their view functions are deleted (`services.get_margin_of_safety`/
    `ValuationRepository.margin_of_safety`, which only those views used, went with them —
    `company_detail`'s own Valuation tab already used `get_margin_of_safety_scenarios`
    independently). Both standalone pages had become pure duplicates of their own Company
    Detail tab (the Valuation tab is actually a superset — it also has the Multiples & yields
    card the old standalone page never had) — the "Full forecasting/valuation page →" links
    were the confusing, purposeless artifact. `valuation.html`'s own "Forecasting →" cross-link
    is gone with the file; the Valuation page's other inbound link (Company Detail's own
    Forecasting tab) now points at `{% url 'company_detail' ticker %}#pane-valuation`, using
    the existing hash-based tab-restore JS (`DOMContentLoaded` handler in
    `company_detail.html`) rather than a page navigation.
  - **Fan chart migrated from hand-rolled inline-`<svg>` to Chart.js (2026-08-12)** — same
    chart-library migration already done for Price/Income Statement/Balance Sheet/Cash
    Flow/Quarterly (see the "CHART LIBRARY INTEGRATION" note under Layout). `forecasting.js`
    now builds one Chart.js `line` dataset per scenario on a **linear numeric x-axis** (years
    relative to FY0), not category labels — a ticker with less than 10 years of history just
    has a shorter historical dataset, the numeric-scale equivalent of the old SVG's variable-
    length `histX(index, len)`. The terminal (FY+11 and later) segment's lighter/dashed
    treatment is Chart.js's per-segment `segment: {borderColor, borderDash}` styling keyed off
    `ctx.p0.parsed.x >= 10`, not a second dataset — legend-chip toggling and tooltips are
    simpler with one dataset per scenario. The shaded terminal-zone band + FY0 reference line
    are drawn by a small custom Chart.js plugin (`beforeDatasetsDraw`, canvas `fillRect`/
    `stroke` using `chart.scales.x.getPixelForValue`) since Chart.js has no built-in region-
    shading primitive. **Script load order matters**: `forecasting.js` must load AFTER
    `chart.umd.js` (moved to the end of `company_detail.html`'s `extra_scripts` block) — it was
    originally ordered before, which would have left `Chart` undefined when it ran; the
    `chart.umd.js` load condition also had to gain `or forecast_chart.metrics` (it previously
    only fired for `statement_chart_data`/`quarterly_chart_data`/`bs_compositions_data`, a real
    gap for any ticker that somehow has forecast data without those).

## Operational gotchas

- **SEC User-Agent must be set before running ingestion.** `00__config/01__tickers.py` ships with placeholder `"MyCompany myemail@example.com"`. SEC blocks requests without a real org/email. Flag this if you see it unchanged when working near ingestion.
- **Unity Catalog schemas are pre-provisioned.** Code reads/writes `main.financials` and `main.config`; it does NOT create catalog or schema. Don't add `CREATE CATALOG` / `CREATE SCHEMA` statements — assume they exist.
- **`%run` and `dbutils` only work inside Databricks.** Notebooks pull config via `%run "/Workspace/.../01__tickers"`. Flag any change that introduces these in a `.py` that's expected to run locally via Databricks Connect. The notebooks that import the `fundamentals_pipeline` package (`51`, `71`) rely on it being pip-installed in the session — done once in `91__full_pipeline`'s session-dependencies `%pip` cell — and do NOT manipulate `sys.path`.
- **Tests + lint exist (don't repeat the old "none" claim).** A pytest suite at repo root (`tests/`) covers the pure importable modules of the `fundamentals_pipeline` package and the Streamlit `lib/` helpers — no Spark/network needed: `pip install -r requirements-dev.txt && pytest -q` (dev deps only — `requirements-dev.txt` installs the `fundamentals_pipeline` package via `-e .`; `requirements.txt` also installs it via `-e .` so Streamlit Cloud can import it). Fixture-backed tests skip if `60__frontends/61__streamlit/fixtures/*` are absent (gitignored). Lint is `ruff.toml` (line-length 120, py310). There is **no Spark CI** — notebooks are still validated ad-hoc / via `30__analysis` checks.
- **Still no catalog/schema CREATE.** Code reads/writes `main.financials` / `main.config` only.

## Workflow

- **Plan before editing notebooks or pipeline `.py` files.** They are stateful and side-effecting (writes to Delta tables, calls SEC/yfinance APIs). Outline the change first, then implement.
- **Flag Databricks-only assumptions** explicitly when proposing changes (uses `dbutils`, `%run`, `spark`, Unity Catalog three-part names, etc.) so the user knows what will break locally.
- **Run from `91__full_pipeline.py`** as a Databricks Job; it accepts `tickers_override`, `run_optimization` (gates `93__delta_maintenance`), `rebuild_config`, and `force_full_refresh`. Local smoke test for Databricks Connect credentials is `test_connection.py` (gitignored).
- **Branch discipline: `main` is the single source of truth.** GitHub `main` is the production source and feeds the read-only Databricks Repo mirror (see *Sync GitHub → Databricks Repo* below). Do feature work on `dev_alm`, validate, then merge to `main` via the normal PR flow. **Never force-push `main`** — it triggers the sync and is the production source.

## Parallel worktree discipline

- Run parallel Claude Code sessions with `claude -w <name>`, launched from a
  checkout on `dev_alm` — temp branches (`worktree-*`) fork from `dev_alm`.
- Temp branch → `dev_alm`: local `--no-ff` merge, no PR. If `dev_alm` moved
  while the worktree was open, merge `dev_alm` into the temp branch and
  resolve conflicts there before merging back. `dev_alm` is the single
  serialized integration point — integrate one temp branch, let it settle,
  then the next.
- `dev_alm` → `main`: unchanged — normal reviewed PR flow, never force-push
  `main` (it is the production source and triggers the Databricks sync).
- Cleanup: `git worktree remove .claude/worktrees/<name>` then
  `git branch -d worktree-<name>` (never `-D`) — `-d` refuses unmerged
  branches, so it cannot drop work that has not landed in `dev_alm`.
- Never run two sessions against the same working directory; that is the
  file-level collision worktrees exist to prevent.

## Layout

- `00__config/` — tickers list, XBRL concept map, metric hierarchies, master-table builders, `valuation_assumptions.json`, `backtest_archetypes.json`
- `fundamentals_pipeline/` — the installable package (`pyproject.toml` at repo root) and the project's single source of truth. Its **importable** public modules (pure Python, no Spark/Streamlit/Django dep, unit-tested) sit at the top of the package alongside the numbered stage dirs: `artifacts.py` (pandas-free: `ARTIFACT_NAMES`, `SchemaError`, `META_REQUIRED_KEYS`/`validate_meta`/`assert_meta` and the raw column-spec dicts), `schemas.py` (re-exports everything in `artifacts.py` for backward compat, plus the pandas-dependent dtype-level checks — `dtype_family`/`validate_artifact`/`assert_artifact`; split from `artifacts.py` 2026-07 because a caller needing only the artifact-name constants — `fundamentals_screener`'s request-path DuckDB connection setup — was paying pandas's ~400ms cold-import cost on every request under its CGI deployment just to read a plain string tuple; see the AJAX-mode-switching entry above for why that mattered), `valuation.py` (scalar Graham/DCF/Owner-Earnings/EPS-CAGR refs), `periods.py` (Q4 arithmetic), `backtest.py` (as-of/no-look-ahead, predicate eval, CAGR/drawdown/vol/Sharpe), `splits.py` (cumulative split factor), `identity.py` (cross-market ticker-collision guard + company-name matching), `tickers_universe.py` (pure CSV parsing for non-US ticker-universe sources), `fx.py` (currency-conversion helper — see the currency-alignment convention above). The `NN__` subdirectories (below) are Databricks notebook stages — not importable. The `fundamentals_screener` Django app imports these modules as a public API and never reimplements them. Exempt from the `NN__` filename rule (they're library modules, not stages) — see the naming-convention exception under Conventions.
- `10__ingestion/` — parallel SEC (8-worker, rate-limited) and yfinance fetch. `12` also prices `BENCHMARK_TICKERS` (SPY) into `market_prices_daily` for the backtester (not in `config.tickers` — no fundamentals).
- `20__transformation/` — annual merge, quarterly derivation, pruning, derived metrics, intrinsic value
- `30__analysis/` — ad-hoc validation queries; `36__run_log_report.py` reads the run-log
- `40__dashboards/` — dashboard SQL and `.lvdash.json`
- `50__publish/` — `51` exports parquet artifacts (data/metrics/prices/backtest + meta, schema-asserted against `fundamentals_pipeline/schemas.py`); `52` uploads them to the GitHub Release `latest`
- `60__frontends/` — frontend consumers of the published Release artifacts (no Databricks dependency)
  - `61__streamlit/` — public Streamlit Cloud app (Screener / Company / Backtest pages)
  - (The former `62__web/` Next.js frontend was removed early on. A Django app, `web/`, was
    subsequently built as its Django-based successor — see ADR-0002 — but was retired in turn
    (ADR-0008, 2026-08): it was never actually deployed, and `fundamentals_screener` below had
    become the repo's one real, live Django presentation layer. Don't resurrect `web/`'s
    layering conventions (strict `views → services → repositories → infrastructure`, UUID pks,
    a repository tier for analytical storage, etc.) for `fundamentals_screener` without a fresh
    decision — see `docs/adr/0008-retire-web-consolidate-on-fundamentals-screener.md` for why
    they don't automatically carry over.)
- `fundamentals_screener/` — a standalone installable Django app (own `pyproject.toml` at `fundamentals_screener/`, package code at `fundamentals_screener/fundamentals_screener/`), originally extracted from the now-deleted `web/`'s `apps/companies` + `apps/screener` + `apps/valuation` for reuse by an external Django project (alopezm.xyz's `/apps/screener/`), and since `web/`'s retirement (ADR-0008) the repo's **only** Django presentation layer. Not a pipeline stage (exempt from `NN__`) and not part of any host project's own `INSTALLED_APPS` in this repo — a fully separate distribution with its own README. See **External consumers** below for the versioning contract and its own architecture (no auth, no PostgreSQL, CGI-safe synchronous data layer, Python 3.9 floor).
- `70__backtest/71__run_backtest.py` — applies `backtest_archetypes.json` screens to history (no look-ahead) → `backtest_results` + `backtest_summary`
- `90__pipelines/` — `91__full_pipeline.py` orchestration entry point; `93__delta_maintenance.py` (OPTIMIZE/VACUUM, gated on `run_optimization`)
- `tests/` — pytest suite (repo root) for the `fundamentals_pipeline` importable modules + Streamlit `lib/`; transversal tooling, intentionally unprefixed (not an `NN__` stage — see the naming-convention exception under Conventions)

### Delta tables (Unity Catalog)
- `main.financials`: `financials_raw` (append-only audit), `financials` (clean facts), `financials_metrics`, `financials_intrinsic_value`, `market_prices_daily` (daily prices, liquid-clustered), `market_cap_asof` (period_end-aligned price + market cap + `currency`, written by `22`), `fx_rates_daily` (`base`/`quote`/`pair`/`date`/`rate`, full daily history, written by `12`), `market_data` (frozen legacy — no longer rebuilt), `backtest_results` + `backtest_summary` (backtester), `ingestion_failures`, `pipeline_run_timings` (legacy).
- `main.config`: `tickers`, `concept_hierarchy`, `metrics_hierarchy`, `pipeline_runs` (per-step run-log: `run_id`/`step`/`minutes`/`status`), `pipeline_run_coverage` (per-run coverage/freshness snapshot).

## Sync GitHub → Databricks Repo

GitHub `main` is the single source of truth. The Databricks Repo (synced by
`.github/workflows/sync-databricks.yml`) is a **read-only mirror**, located at
`/Workspace/Shared/fundamentals_databricks_pj` (Actions variable
`DATABRICKS_REPO_PATH`).

- **The Repo lives under `/Workspace/Shared/`, not under `/Repos/<your-email>`.** The sync
  is done by the service principal `gh-actions-repo-sync`, which cannot recreate a Repo in a
  human's `/Repos/<email>` namespace (that is owner-managed). That's why the
  auto-repair (delete+recreate) requires a folder the SP actually controls, such as
  `/Workspace/Shared`. The `REPO_ID` is resolved from the git URL, not from a fixed id.
- **Don't edit or run the notebooks directly from the synced Repo.**
  Opening a `.py` notebook in the workspace editor rewrites it (cell metadata/reformatting)
  and creates local changes that break the pull with `GIT_CONFLICT`.
- To iterate interactively, clone the repo into a separate Databricks Repo under
  your user folder and work there; push the changes via GitHub.
- If you need to run a notebook from the synced Repo, launch it as a **Job** against
  its path (don't open it in the editor): running does not rewrite the source, editing does.
- The workflow is self-repairing: on `GIT_CONFLICT` it deletes and recreates the Repo from
  `main`, discarding the local state. If you had unsaved work in the synced
  Repo, it will be lost — that's why you must not work there.

## External consumers

`fundamentals_screener/` (see Layout above) is a separately-versioned installable Django app
consumed by an external repo (the owner's personal site, not in this workspace) as
`pip install git+https://github.com/alopezmoreira1989/fundamentals_databricks_pj.git#subdirectory=fundamentals_screener`.
It reads this repo's published GitHub Release `latest` artifacts (the `dashboard_*.parquet`
files — one per `fundamentals_pipeline.artifacts.ARTIFACT_NAMES` entry — + `dashboard_meta.json`,
the same set the Streamlit app reads) and depends on `fundamentals_pipeline` (for
`schemas`/`statement_layout`/`fx`) the normal way. It was originally extracted from the
now-deleted `web/` Django app (ADR-0002/ADR-0008), but is a fully independent codebase — no
import from anything that used to be `web/`, and no code anywhere in this repo still depends
on it existing.

- **Release/deploy pipeline is automatic end-to-end, except for one manual approval gate —
  check for it every time a `fundamentals_screener` version is cut.** Merging a
  `fundamentals_screener/pyproject.toml` version bump into this repo's `main` fires
  `.github/workflows/release-screener.yml`, which tags `fundamentals-screener-vX.Y.Z` and
  fires a `repository_dispatch` (`screener-released`, via the `SCREENER_NOTIFY_PAT` secret) at
  `alopezmoreira1989/alopezm_my_website`. That repo's own
  `.github/workflows/bump-fundamentals-screener.yml` catches it and opens a PR there
  (`chore: bump fundamentals-screener to fundamentals-screener-vX.Y.Z`) bumping its
  `requirements.txt` pin. **That PR's CI/CD run routinely lands as `action_required` and just
  sits there — it is waiting on a manual workflow-run approval, not failing.** Nothing deploys
  until a human (or Claude, if asked) does, in order: (1) `gh run list -R
  alopezmoreira1989/alopezm_my_website --workflow=ci.yml` to find the stuck run for that PR's
  branch, (2) `gh api -X POST
  repos/alopezmoreira1989/alopezm_my_website/actions/runs/<id>/approve` to let its test/lint
  jobs actually execute, (3) once green, `gh pr merge <n> -R alopezmoreira1989/alopezm_my_website
  --merge --delete-branch`. Merging to that repo's `main` triggers its CI/CD's real `deploy` job
  (SSH to Dinahosting + health check) — that is the step that actually ships the new version
  live. **Whenever you cut a new `fundamentals_screener` release, always check `gh pr list -R
  alopezmoreira1989/alopezm_my_website --state open` for a stuck bump PR** — the notify/tag side
  on this repo can succeed while the consumer-side deploy never happens if that approval step is
  missed (this has already happened once, 2026-07, across a laptop switch — the tag existed but
  the live site was still on the prior version until the bump PR was manually approved+merged).
- **This is a public API contract.** `fundamentals_screener/fundamentals_screener/urls.py`'s
  route names, the template filenames under
  `fundamentals_screener/fundamentals_screener/templates/fundamentals_screener/`, and the
  shape of `fundamentals_screener/fundamentals_screener/dtos.py` are what the consuming
  project's own code/templates couple against. **Changing any of them is a breaking change**:
  bump `fundamentals_screener/pyproject.toml`'s version and tag the commit *before* the
  consumer updates its pinned `git+https://...@vX.Y.Z` install — never let the consumer track
  a moving ref.
- **Design decisions specific to this package — grounded in its actual deployment target, not
  arbitrary, don't "simplify" these away:**
  - **Data layer**: no network on the request path, ever. `fundamentals_screener` ships
    `manage.py sync_fundamentals_data` (a cron-driven command) + a `data_source.py`/
    `repository.py` pair that only ever reads what's already on disk. This is deliberate for
    the reference deployment (plain CGI, no persistent process between requests — a
    background-thread refresh-on-stale pattern would never survive to complete) — see the
    package README's "Keeping data fresh" section. This split (`data_source.py` = fetch/
    cache/validate, `repository.py` = bare `connection()` with views registered) mirrors what
    the consuming project had already independently built and proven (2,627 tickers, 1.57M
    rows) before this package existed; it was built to match that proven interface exactly,
    specifically so it can be a drop-in replacement for those three files rather than a
    second, parallel implementation of the same job.
  - **DuckDB view names**: `dashboard_data`/`dashboard_metrics`/`dashboard_prices`/
    `dashboard_backtest`/`dashboard_forecast`/`dashboard_fx`/`dashboard_filings` (the literal
    artifact names, sourced dynamically from `fundamentals_pipeline.artifacts.ARTIFACT_NAMES`)
    — not short aliases. Any SQL written against this repository must use these names.
  - **Settings**: a single `FUNDAMENTALS_DATA_PATH` (a local directory) is the one required
    setting — see the package README for the full list (optional `LOGO_DEV_KEY`, recommended
    `CACHES`).
  - **Python floor**: `>=3.9` — the real production constraint on the target host (Python
    3.9.2, no compiler), so `duckdb`/`pandas` are pinned to exact versions
    (`duckdb==1.4.5`/`pandas==2.3.3`) verified to still support 3.9; their later releases
    dropped it. Do not introduce Python ≥3.10-only syntax (`dataclass(slots=True)`,
    `match`/`case`, etc.) anywhere in `fundamentals_screener/`. Two
    non-obvious 3.10-only patterns already caused real import-time/runtime failures on a real
    Python 3.9.2 retest (both originally fixed 2026-07-20) — watch for them in review, since a
    plain `grep` for `match`/`case`/`slots=True` won't catch either: (1) a **module-level
    assignment** of a PEP 604 union, e.g. `Number = float | int | None` — `from __future__
    import annotations` only defers *annotation* evaluation, not a normal assignment's
    right-hand side, so this still raises `TypeError` at import time pre-3.10; use
    `typing.Union[...]` instead. (2) `zip(..., strict=True)` (or `strict=False`) — the
    `strict=` keyword itself doesn't exist before 3.10, regardless of its value; do a manual
    `len(a) != len(b)` check and call plain `zip(a, b)` instead. **Pattern (2) recurred a third
    time, 2026-08-05, and this time reached production** (a real 500 on the live site) — see
    the Forecasting entry above — despite being written by the same session that had *just*
    read this exact paragraph, underscoring that knowing about the pitfall and catching it
    while writing new code are not the same thing; `ruff check` passing is not proof, since
    `ruff.toml`'s repo-wide `target-version = "py310"` doesn't know this one package has a
    lower floor. `ruff.toml`'s `target-version = "py310"` makes ruff actively suggest
    reintroducing both (`UP007`, `B905`) — the affected files
    (`fundamentals_pipeline/{backtest,periods,valuation}.py`,
    `fundamentals_screener/fundamentals_screener/{repositories/base,charts,services}.py`) have
    targeted per-file-ignores for exactly those two rules, nowhere else. The root
    `fundamentals_pipeline`
    package's own `pyproject.toml` `requires-python` is **also**
    `>=3.9` for the same reason — `pip` enforces that metadata before even looking at the
    code, so `fundamentals_screener`'s git dependency on it would hard-fail to install on
    Python 3.9.2 if that floor were ever raised back to `>=3.10`, regardless of whether the
    handful of modules `fundamentals_screener` actually imports (`schemas`/
    `statement_layout`/`fx`, plus `backtest`/`periods`/`valuation` transitively via
    `fundamentals_pipeline/__init__.py`'s eager imports) stayed 3.9-compatible. Keep the two
    floors in lockstep; don't raise the root one without re-checking this.
  - **No personalization, no i18n**: favorites/watchlists/history are not implemented — they'd
    depend on login-scoped apps this package doesn't assume the host has (see
    ADR-0008/`docs/adr/0008-retire-web-consolidate-on-fundamentals-screener.md` — build them
    here, from scratch, if actually needed; don't assume the deleted `web/` app's
    implementation is a template to resurrect, its architecture was deliberately different).
    The async Yahoo Finance news widget IS built (`/<ticker>/news/`,
    cached via Django's generic `django.core.cache` API) — the host must configure a
    persistent `CACHES` backend (e.g. `FileBasedCache`) for it to actually cache under CGI;
    unconfigured it still works, just re-fetches Yahoo on every request (see the package
    README). Every template is English-only with no `{% load i18n %}` anywhere, regardless of
    the host project's own locale configuration (the target consumer is bilingual ES/EN; this
    app's pages stay English by design).
  - **Own base template**: ships `base_screener.html` (own `<head>`/CSS/JS, navy/cyan/orange
    palette under `static/fundamentals_screener/css/app.css`) rather than extending a host
    `base.html` — the opposite of the "inherit the host's layout" instruction the consumer
    project states for its other `/apps/<name>/` sections, a deliberate, confirmed exception
    for this one.
- See `fundamentals_screener/README.md` for the full install/settings/scope writeup — that
  file is the one a consumer's maintainer actually reads, so keep it, not just this section,
  current when any of the above changes.

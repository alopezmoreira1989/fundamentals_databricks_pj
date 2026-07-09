# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

Databricks analytical pipeline that ingests SEC EDGAR XBRL filings (10-K/10-Q) for ~3,000 US tickers, joins Yahoo Finance prices, derives financial metrics + intrinsic values, runs an investment-archetype **backtester**, and serves it all via Delta tables to a Databricks dashboard **and a public Streamlit app** (fed by GitHub Release parquet artifacts). Entry point: `90__pipelines/91__full_pipeline.py` (Databricks notebook source format, run as a Databricks Job). A pure-Python public API inside the installable `fundamentals_pipeline` package (`schemas.py`, `valuation.py`, `periods.py`, `backtest.py`, `splits.py`, `fx.py`, `identity.py`, `tickers_universe.py`; no Spark/Databricks dependency) holds the reference formula/contract/backtest logic and is unit-tested by `tests/` (pytest). **All new work is produced in English** (docs, code, identifiers, comments, commit messages); some legacy content (prose, metric labels, JSON hierarchy values) is still in Spanish and is treated as data — see Conventions.

## Conventions that must be preserved

- **File naming `NN__name.py` is mandatory.** Files use `<stage><order>__<purpose>` so the pipeline order is visible from filenames alone (e.g., `21__clean_and_merge.py`, `21b__derive_quarterly.py`). New files must follow the pattern.
- **Naming convention: `NN__name`, double underscore after the numeric prefix.** Every folder or file that is a sequential stage of the pipeline — or a parallel consumer of a published stage's output (e.g. the frontends under `60__frontends/`) — is named `NN__descriptive_name`: two underscores between the number and the name, single underscores only to separate words *within* the name (e.g. `21b__derive_quarterly.py`, `60__frontends/61__streamlit/`). This is non-negotiable going forward — any new pipeline step, audit script, or frontend added from here on follows `NN__name`, never `NN_name` or an unprefixed name.
  - **Exception — the importable library modules and tooling are never numbered.** The pure-Python public modules at the top of the `fundamentals_pipeline` package (`schemas.py`, `valuation.py`, `periods.py`, `backtest.py`, `splits.py`) are the project's single source of truth for financial logic + the data contract; they are library modules, not pipeline stages, so they keep plain names (no `NN__` prefix) and sit alongside the numbered stage dirs. `tests/` (root-level, standard Python convention, exercising those modules and the Streamlit `lib/` transversally) is likewise not a stage and stays unprefixed. The package is installable (`pyproject.toml` at the repo root) and is imported the same way by every consumer — pipeline notebooks, scripts, tests, and the Django web app — via `pip install -e .`, never via `sys.path`. A future addition that is genuinely transversal infrastructure rather than a step in the sequence follows this exception, not the `NN__` rule — flag it to the repo owner if which one applies is ambiguous.
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
- `fundamentals_pipeline/` — the installable package (`pyproject.toml` at repo root) and the project's single source of truth. Its **importable** public modules (pure Python, no Spark/Streamlit/Django dep, unit-tested) sit at the top of the package alongside the numbered stage dirs: `schemas.py` (export↔app artifact contract), `valuation.py` (scalar Graham/DCF/Owner-Earnings/EPS-CAGR refs), `periods.py` (Q4 arithmetic), `backtest.py` (as-of/no-look-ahead, predicate eval, CAGR/drawdown/vol/Sharpe), `splits.py` (cumulative split factor), `identity.py` (cross-market ticker-collision guard + company-name matching), `tickers_universe.py` (pure CSV parsing for non-US ticker-universe sources), `fx.py` (currency-conversion helper — see the currency-alignment convention above). The `NN__` subdirectories (below) are Databricks notebook stages — not importable. The Django `web/` app imports these modules as a public API and never reimplements them. Exempt from the `NN__` filename rule (they're library modules, not stages) — see the naming-convention exception under Conventions.
- `10__ingestion/` — parallel SEC (8-worker, rate-limited) and yfinance fetch. `12` also prices `BENCHMARK_TICKERS` (SPY) into `market_prices_daily` for the backtester (not in `config.tickers` — no fundamentals).
- `20__transformation/` — annual merge, quarterly derivation, pruning, derived metrics, intrinsic value
- `30__analysis/` — ad-hoc validation queries; `36__run_log_report.py` reads the run-log
- `40__dashboards/` — dashboard SQL and `.lvdash.json`
- `50__publish/` — `51` exports parquet artifacts (data/metrics/prices/backtest + meta, schema-asserted against `fundamentals_pipeline/schemas.py`); `52` uploads them to the GitHub Release `latest`
- `60__frontends/` — frontend consumers of the published Release artifacts (no Databricks dependency)
  - `61__streamlit/` — public Streamlit Cloud app (Screener / Company / Backtest pages)
  - (The former `62__web/` Next.js frontend was removed — the decoupled web frontend is now the Django `web/` app below, not a `60__frontends/` static build.)
- `web/` — Django presentation + application layer (auth, templates, REST API, user data), a **separate consumer** of the published Release artifacts, not a pipeline stage (so exempt from `NN__`). Imports `fundamentals_pipeline` as a normal dependency and never reimplements financial logic. **Strict, one-directional layering — enforce it on every change (see `docs/architecture.md`):** `views → services → repositories → infrastructure (DuckDB / PostgreSQL) → fundamentals_pipeline`. Every access to persistent data goes through the **repository** tier; views never execute SQL/read Parquet/hold business logic, services never execute raw SQL, repositories never implement financial/valuation/screening logic (that's `fundamentals_pipeline`). **Repositories are mandatory for analytical storage — DuckDB, Parquet artifacts, future Databricks SQL — always, no matter how simple the query.** But use judgment, not boilerplate: **trivial Django-ORM CRUD (e.g. CustomUser) may use the ORM directly from a service** rather than a pass-through repository; promote to a repository only when it adds real value (complex query, DTO mapping, aggregation, caching, multiple sources, storage/infra isolation). **Design goal (litmus test): swapping the storage engine (DuckDB → Databricks SQL / Postgres / Snowflake / BigQuery) must touch only `repositories/` + `infrastructure/` — never a view, service, or the pipeline.** Dirs: `apps/<name>/` (per-app views/urls/services, + models only when the app owns Postgres data), `services/` (cross-cutting use-case orchestration), `repositories/` (shared data gateway → DTOs), `infrastructure/` (`storage` = artifact fetch/cache, `duckdb` = query engine over the cached parquet). Reads artifacts via DuckDB; PostgreSQL holds only app data; no Databricks queries at request time. **Read-model rules:** repositories own ALL SQL (none elsewhere), return immutable DTOs/frozen dataclasses (never raw rows/dicts/DataFrames — views/services never touch SQL column names), parameterize every query, no `SELECT *` (list only needed columns), and push filter/aggregate/LIMIT into DuckDB (predicate+projection pushdown; never load a whole parquet to filter in Python). Services stay storage-agnostic. **New Django code must pass mypy + django-stubs** (`web/mypy.ini`, settings `config.settings.dev`); run from `web/`. **Every application model uses an explicit UUID primary key (`UUIDField(primary_key=True, default=uuid.uuid4, editable=False)`) — never Auto/BigAutoField** (`DEFAULT_AUTO_FIELD` stays BigAutoField only for framework tables). **Custom user model is live:** `apps/users` `User(AbstractUser)` with UUID pk + unique/required email + `AUTH_USER_MODEL="users.User"` (set before the first migration); users CRUD uses the ORM directly from the service layer, no repository. Web tests run on SQLite in-memory (`config.settings.test`), no Postgres needed.
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
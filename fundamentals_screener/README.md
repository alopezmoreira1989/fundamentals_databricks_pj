# fundamentals_screener

Installable Django app: a self-contained SEC EDGAR fundamentals screener, company detail, and
valuation pages, reading Parquet artifacts published by
[fundamentals_databricks_pj](https://github.com/alopezmoreira1989/fundamentals_databricks_pj)
via DuckDB.

Extracted from that repo's own `web/` Django app for reuse in an external Django project. It
is visually and functionally self-contained: it ships its own base template
(`base_screener.html`, not extending any host template), its own CSS/JS, and does not depend
on the host project having any particular apps installed (no auth, no user-data apps). The one
exception is the small `Update` model backing the public **Updates** development journal (see
below) — see [ADR-0013](../docs/adr/0013-updates-development-journal-model.md) for why.

## Install

```text
pip install "fundamentals-screener @ git+https://github.com/alopezmoreira1989/fundamentals_databricks_pj.git#subdirectory=fundamentals_screener"
```

(Or `pip install -e .` from this directory for local development against a sibling checkout.)

## Wire it into a host Django project

1. Add to `INSTALLED_APPS` — both entries are required (`django.contrib.humanize` is a Django
   built-in, used by `company_detail.html`'s `|intcomma` filter; it's not bundled by default
   unless the host project already has it):

   ```python
   INSTALLED_APPS = [
       ...,
       "django.contrib.humanize",
       "fundamentals_screener",
   ]
   ```

2. Mount the URLs wherever you like:

   ```python
   # urls.py
   path("apps/screener/", include("fundamentals_screener.urls")),
   ```

   Reverse with `{% url 'fundamentals_screener:screen' %}`,
   `{% url 'fundamentals_screener:company_detail' ticker %}`.
3. Run migrations — this package ships one small schema migration for the `Update` model (the
   public Updates/development-journal section, see below) plus a data migration that seeds
   ~10 historical entries:

   ```bash
   python manage.py migrate
   ```

   Nothing else in this package touches the database — every other view is still read-only
   against published Parquet artifacts.
4. Set the one required setting:

   ```python
   FUNDAMENTALS_DATA_PATH = env("FUNDAMENTALS_DATA_PATH", default=str(BASE_DIR / "data" / "fundamentals"))
   ```

   This is a local directory the app reads the `dashboard_*.parquet` files (one per
   `fundamentals_pipeline.artifacts.ARTIFACT_NAMES` entry — currently data/metrics/prices/
   backtest/filings/forecast/fx) + `dashboard_meta.json` from. **Nothing in this package
   downloads them on the request path** — see "Keeping data fresh" below.
5. Optional setting: `LOGO_DEV_KEY` (a [Logo.dev](https://logo.dev) publishable key) — enables
   real company logos instead of the monogram fallback. Unset ⇒ always monogram, no error.
6. Recommended setting: a persistent `CACHES` backend, for the "Latest news" widget on the
   company Overview tab (Yahoo Finance headlines, cached 30 min). Django's default
   `LocMemCache` is process-local — under CGI hosting (a fresh process per request, see
   "Keeping data fresh" below) it never actually persists between requests, so the widget
   still works but re-fetches Yahoo on every page view instead of caching. The simplest fix,
   with no new dependency, is Django's built-in file-based backend:

   ```python
   CACHES = {
       "default": {
           "BACKEND": "django.core.cache.backends.filebased.FileBasedCache",
           "LOCATION": str(BASE_DIR / "var" / "django_cache"),
       },
   }
   ```

   If your host does run a persistent process (a real WSGI server, not CGI), `LocMemCache`
   is fine as-is.

## Keeping data fresh

This package ships `manage.py sync_fundamentals_data` — run it from a cron job (daily matches
the upstream pipeline's own publish cadence):

```bash
python manage.py sync_fundamentals_data          # download only what's missing
python manage.py sync_fundamentals_data --force  # re-download everything
```

It downloads the 6 `dashboard_*.parquet` files + `dashboard_meta.json` from the upstream
repo's GitHub Release `latest` into `FUNDAMENTALS_DATA_PATH`, then validates them against
`fundamentals_pipeline.schemas`. The web views never touch the network — they only ever read
whatever is already on disk via `fundamentals_screener.repository.connection()`. The Filings
tab's data (`dashboard_filings.parquet` — real SEC 10-K/10-Q filings) is one of the 6, fetched
by the upstream pipeline itself (which already has its own SEC credentials), so this package
never needs any SEC-specific setting of its own.

This is deliberate, not a missing feature: the reference deployment for this package is plain
CGI hosting (`mod_cgi`, no persistent process between requests), where a lazy
fetch-on-request-with-background-refresh pattern simply wouldn't survive from one request to
the next (a background thread never gets to complete before the process exits). If your host
DOES run a persistent process, you can still just run the sync command via cron/deploy-hook —
there's no reason not to.

## What v1 covers (and what it deliberately doesn't)

Ported from `web/`'s `apps/companies`, `apps/screener`, `apps/valuation`:

- **Screener** (`/`): paginated, multi-metric, filterable (sector/industry/index/country/
  market) company table, every column sortable, state in the URL (bookmarkable). Three **modes**
  behind one route/`?mode=` param — **General Screener** (the table above, default),
  **Net-Net Finder** (`?mode=netnet`, below), and **Investor Presets** (`?mode=presets`, below)
  — sharing only the sector/country/market filters, switched via one shared mode-nav tab bar.
  The mode switch (and any mode-specific filter/level control) is AJAX — a `fetch()` swap of the
  page's `#main` region, not a full reload — layered on top of a design that degrades correctly
  with JS disabled (every control is still a plain `<a href="?...">` or `<form method="get">`).
  Net-Net Finder was the first of this family; follow its established pattern (one route/one
  `?mode=`, a shared mode-nav partial, Python-side pagination once a mode's own result set can
  plausibly exceed ~50 rows) for the next one rather than inventing a new shape.
- **Net-Net Finder** (`?mode=netnet`): Graham-style deep-value screen — companies trading below
  their net current asset value (NCAV), at three liquidation-conservatism levels (**Relaxed** /
  **Moderate** / **Strict** — Relaxed counts every current asset at full face value, the
  stricter levels apply a per-line-item haircut; see the root repo's `README.md` for the exact
  formulas). A hero card shows the level switch, headline stats (universe size, positive-NCAV
  count, count trading below value, count at Graham's classic ≤ 0.67× net-net threshold), and a
  "hide likely value traps" filter (Piotroski F-Score < 4). The results table shows Price,
  NCAV/Share, a discount bar+chip, an F-Score dot row, an Altman Z-Score zone pill (safe/grey/
  distress), and Market Cap. The same NCAV/Share-vs-price card also appears on **Company
  detail**'s Valuation tab and the standalone **Valuation** page (below) for any ticker with NCAV
  data at any level — including a negative one, shown deliberately (a company's own numbers,
  not a screened/filtered view).
- **Investor Presets** (`?mode=presets`): three classic investor "schools" — **Graham**
  (Defensive Investor), **Buffett** (Quality Compounder), **Lynch** (GARP) — each its own
  criteria set, portrait card, and company table. Every criterion is fully live (filters real
  data), including multi-year checks (Graham's positive-earnings/uninterrupted-dividend/EPS-
  growth history, Buffett's sustained ROE) and Lynch's `PEG`/`EPS CAGR (5Y)` (real published
  pipeline metrics, not screener-side math). A second pill inside each school's own card — below
  its criteria list, not next to the school pill — picks a conservatism level: **Strict**
  (default; the investor's own literal numbers where a book quote genuinely exists, e.g.
  Graham's exact rules from *The Intelligent Investor*) / **Moderate** (today's baseline
  thresholds for Buffett/Lynch, who have no single quotable rule set) / **Relaxed** (loosened,
  for more matches — since AND'ing several real criteria together can otherwise leave very few
  or zero companies passing, which is expected, not a bug, given how demanding some of these
  combinations are). The criteria list's own label text embeds the active level's actual numbers
  (e.g. "Current ratio ≥ 2" at Strict vs. "≥ 1.5" at Relaxed), not a generic description.
- **Company detail** (`/<ticker>/`): overview KPIs, financial statements (Income/Balance
  Sheet/Cash Flow, with row hierarchy — subtotals/grand-totals/headline rows indented and
  styled), quarterly Income Statement, derived metrics (5-year sparkline trend per metric,
  plus a user-switchable benchmark: **Industry** / **Sector** / **Compare to a company**, each
  pill showing its own peer count — Industry defaults to whichever basis the classic
  industry-if-≥3-peers-else-sector cascade resolves to, and a "Show peers" disclosure names the
  actual tickers when the peer set is small enough (Industry only — Sector peer sets are always
  too large to list and get a plain count+name sentence instead). Switching updates in place via
  a small AJAX partial-swap (`?bench=industry|sector|compare&compare=TICKER`, degrades to a full
  reload with JS disabled). A ▲/▼ delta chip rides next to each percent-unit metric's own Latest
  value, showing the signed gap vs. the active benchmark), valuation football field + Margin of
  Safety table + Net-Net card, price chart with SMA 20/50/200, a "Latest news" card (async Yahoo
  Finance headlines, `/<ticker>/news/`, cached — see the `CACHES` recommendation above), and a
  **Filings** tab listing the ticker's real SEC 10-K/10-Q filings (form, date, direct document
  link), read straight from the `dashboard_filings` artifact the upstream pipeline publishes —
  no SEC call, no SEC-specific setting, of this package's own, ever (see "Keeping data fresh").
- **Forecasting** — a Company Detail tab (not a standalone page): a 30-year cross-sectional ML
  scenario fan chart (Revenue / Net Income / Free Cash Flow, 5 quantile scenarios — Bear/Low
  Bear/Crab/Low Bull/Bull — years 1-10 from a LightGBM quantile-regression model, years 11-20
  front-loaded-decay toward each scenario's own DCF terminal-growth rate, floored at 2% so no
  scenario implicitly grows slower than inflation) plus a PV-discounted forward P/E / FCF Yield
  table (mid/"Crab" scenario), rendered with Chart.js. Not ported from `web/` — built directly
  in this package. Sourced from the `dashboard_forecast` artifact (written by the upstream
  pipeline's `24__forecasting.py`); a ticker with no published forecast yet simply omits the tab.
  Valuation (football field + Margin of Safety + Net-Net card) is likewise a Company Detail
  tab only — both used to also have their own standalone `/<ticker>/valuation/` and
  `/<ticker>/forecasting/` pages, removed as purely redundant with the tab.
- **Updates** (`/updates/`): a public development journal — what changed in this project, why,
  and how, newest first. Each entry (`/updates/<slug>/`) is Markdown, managed through Django
  admin, backed by the one Django model this package ships (`Update` — see
  [ADR-0013](../docs/adr/0013-updates-development-journal-model.md)). Only `is_published=True`
  entries are ever shown publicly. Ships an RSS feed at `/updates/feed/` (Django's built-in
  syndication framework, no new dependency). Ten historical entries, reconstructed from the
  project's real git history, are seeded automatically on `manage.py migrate`.
- JSON siblings of the remaining two (`/data/`, `/<ticker>/data/`).

**Not ported** — these existed in the source app but depend on things this package
deliberately doesn't assume the host project has:

- Favorites / watchlists / browsing-history personalization (`company_detail` in the source
  app called into three login-scoped apps to render "add to favorites"/"add to watchlist"
  widgets and record view history — none of that ships here).
- A "compare tickers" page — this was in the original Streamlit app this project also
  publishes, but was never built in the Django `web/` app this package was extracted from, so
  it's a gap, not a deliberate cut.

If you need any of the above, they're straightforward to add on top of this package's
`services.py`/`repositories/` — none of what's missing is load-bearing for what's here.

## No i18n

Every template is plain English with no `{% load i18n %}`/`{% trans %}`/`{% blocktrans %}`
anywhere. If your host project has `django.middleware.locale.LocaleMiddleware` and other
locales configured, this app's pages stay English regardless — by design, not an oversight.

## Architecture

`views.py → services.py → repositories/ → repository.py (DuckDB) → data_source.py (cache)`.
No financial/valuation logic lives in this package — every number is already computed
upstream by `fundamentals_pipeline` (installed as a dependency for its `schemas`,
`statement_layout`, and `fx` modules) and published as Parquet; this package only reads,
formats, and renders.

The one exception is the Updates section (`models.py` → `views.py` directly, no
repository/DuckDB involved) — a small, isolated Django model for maintainer-authored journal
entries, not financial data. See
[ADR-0013](../docs/adr/0013-updates-development-journal-model.md).

## Versioning — this is a public API contract

`urls.py`'s route names, the template filenames under `templates/fundamentals_screener/`, and
the shape of `dtos.py` are what any consuming project's own templates/overrides couple
against. Changing any of them is a breaking change: bump the version (tag this repo) before a
consumer updates its pinned `git+https://...@vX.Y.Z` install.

## License

[MIT](LICENSE).

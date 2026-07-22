# fundamentals_screener

Installable Django app: a self-contained SEC EDGAR fundamentals screener, company detail, and
valuation pages, reading Parquet artifacts published by
[fundamentals_databricks_pj](https://github.com/alopezmoreira1989/fundamentals_databricks_pj)
via DuckDB.

Extracted from that repo's own `web/` Django app for reuse in an external Django project. It
is visually and functionally self-contained: it ships its own base template
(`base_screener.html`, not extending any host template), its own CSS/JS, and does not depend
on the host project having any particular apps installed (no auth, no user-data apps).

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
   `{% url 'fundamentals_screener:company_detail' ticker %}`,
   `{% url 'fundamentals_screener:valuation' ticker %}`.
3. Set the one required setting:

   ```python
   FUNDAMENTALS_DATA_PATH = env("FUNDAMENTALS_DATA_PATH", default=str(BASE_DIR / "data" / "fundamentals"))
   ```

   This is a local directory the app reads 5 `dashboard_*.parquet` files + `dashboard_meta.json`
   from. **Nothing in this package downloads them on the request path** — see "Keeping data
   fresh" below.
4. Optional setting: `LOGO_DEV_KEY` (a [Logo.dev](https://logo.dev) publishable key) — enables
   real company logos instead of the monogram fallback. Unset ⇒ always monogram, no error.
5. Recommended setting: a persistent `CACHES` backend, for the "Latest news" widget on the
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

It downloads the 5 `dashboard_*.parquet` files + `dashboard_meta.json` from the upstream
repo's GitHub Release `latest` into `FUNDAMENTALS_DATA_PATH`, then validates them against
`fundamentals_pipeline.schemas`. The web views never touch the network — they only ever read
whatever is already on disk via `fundamentals_screener.repository.connection()`.

This is deliberate, not a missing feature: the reference deployment for this package is plain
CGI hosting (`mod_cgi`, no persistent process between requests), where a lazy
fetch-on-request-with-background-refresh pattern (which is what `fundamentals_databricks_pj`'s
own `web/` app does, since it runs under a real WSGI server) simply wouldn't survive from one
request to the next. If your host DOES run a persistent process, you can still just run the
sync command via cron/deploy-hook — there's no reason not to.

## What v1 covers (and what it deliberately doesn't)

Ported from `web/`'s `apps/companies`, `apps/screener`, `apps/valuation`:

- **Screener** (`/`): paginated, multi-metric, filterable (sector/industry/index/country/
  market) company table, every column sortable, state in the URL (bookmarkable).
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
  Safety table, price chart with SMA 20/50/200, a "Latest news" card (async Yahoo Finance
  headlines, `/<ticker>/news/`, cached — see the `CACHES` recommendation above).
- **Valuation** (`/<ticker>/valuation/`): a standalone version of the same football field +
  MoS table.
- JSON siblings of all three (`/data/`, `/<ticker>/data/`, `/<ticker>/valuation/data/`).

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

## Versioning — this is a public API contract

`urls.py`'s route names, the template filenames under `templates/fundamentals_screener/`, and
the shape of `dtos.py` are what any consuming project's own templates/overrides couple
against. Changing any of them is a breaking change: bump the version (tag this repo) before a
consumer updates its pinned `git+https://...@vX.Y.Z` install.

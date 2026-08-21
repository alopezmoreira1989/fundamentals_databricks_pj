"""Seed the initial development-journal entries.

These are retrospective: they document work already completed, reconstructed from this
project's real git history (commit dates, PR/issue numbers, merged branches) rather than
invented. `published_at` approximates when each phase of work actually landed; each entry's
own text says plainly that it's a retrospective record, not a same-day announcement.

A data migration (not a fixture/loaddata call) so a host project gets these automatically on
`manage.py migrate`, the same way it gets the schema — no separate manual step. Reversible: the
down-migration deletes exactly the slugs this migration created, so it doesn't touch any entry
a maintainer added by hand afterward.
"""

from __future__ import annotations

from django.db import migrations

_UPDATES = [
    {
        "title": "Project origins: an SEC EDGAR fundamentals pipeline on Delta Lake",
        "slug": "project-origins-sec-edgar-pipeline",
        "category": "pipeline",
        "published_at": "2026-05-11",
        "summary": (
            "How this project started: ingesting SEC XBRL filings into Delta tables on "
            "Databricks — the foundation everything since has been built on."
        ),
        "content": """\
*Retrospective entry — reconstructed from the project's git history to document work completed
in May 2026, at the start of the project.*

### What changed

The first working version of the pipeline: a scraper that pulled SEC EDGAR XBRL filings
(10-K/10-Q) for the S&P 500, wrote the parsed facts into Delta tables, and exposed them through
a handful of dashboard SQL queries.

### Why

Public equity fundamentals are scattered across thousands of individual filings, each in its
own XBRL dialect of tag names. The goal was a single, queryable table of clean financial facts
per company per period — the raw material every later feature (derived metrics, valuation,
screening, backtesting) would depend on.

### Implementation

SEC's `companyfacts` API was the source; XBRL concept tags (e.g. `us-gaap:Revenues`) were
mapped to a small set of canonical line items (Revenue, Net Income, Total Assets, ...) up
front, rather than exposing raw tag names downstream. Output landed in Delta tables on
Databricks from day one — not a later migration, the project's storage layer was Delta/Unity
Catalog from its first working commit.

### Result

A working, if narrow, pipeline covering the S&P 500's core financial statements — the shape
every subsequent expansion (more tickers, more concepts, more markets) has extended rather than
replaced.
""",
        "is_published": True,
    },
    {
        "title": "A public Streamlit dashboard, decoupled from Databricks",
        "slug": "public-streamlit-dashboard",
        "category": "frontend",
        "published_at": "2026-05-24",
        "summary": (
            "The first public-facing frontend: a Streamlit app that reads published parquet "
            "artifacts from a GitHub Release instead of querying Databricks directly."
        ),
        "content": """\
*Retrospective entry — reconstructed from the project's git history to document work completed
in May 2026.*

### What changed

A Streamlit app (Screener / Company / Backtest pages) was added as the project's first public
frontend, together with a publish step that exports the relevant Delta tables to parquet files
and attaches them to a GitHub Release.

### Why

Databricks isn't a public service — a frontend can't query Unity Catalog directly without
exposing credentials or standing up its own API layer. Publishing a small, versioned snapshot
of the data as static files lets a frontend be genuinely public with zero Databricks
dependency, and zero live database to keep online.

### Implementation

A `51__export_dashboard_data` stage reads the relevant Delta tables, writes them to parquet with
an explicit schema contract (checked at write time, so a broken export fails loudly instead of
shipping malformed data), and a follow-up stage uploads the files to a GitHub Release tagged
`latest`. The Streamlit app downloads and caches that release's artifacts on startup — no
Databricks Connect, no live credentials, nothing but public HTTP.

### Result

A working public dashboard over real S&P 500 fundamentals, deployed to Streamlit Cloud, with a
data pipeline (Databricks) that has no idea the frontend exists. That same publish-to-GitHub-
Release pattern is still how every frontend in the project gets its data today.
""",
        "is_published": True,
    },
    {
        "title": "An investment-philosophy backtester, and the first real test suite",
        "slug": "backtester-and-first-test-suite",
        "category": "testing",
        "published_at": "2026-06-16",
        "summary": (
            "A no-look-ahead backtester for archetype screens against SPY, and the pure-logic "
            "test suite that came with it."
        ),
        "content": """\
*Retrospective entry — reconstructed from the project's git history to document work completed
in June 2026.*

### What changed

A backtesting module that applies named investment-philosophy screens ("archetypes" — e.g.
Graham net-net, Buffett quality-compounder) to historical fundamentals, holds the resulting
portfolio forward, and compares it against an SPY benchmark. Alongside it: the project's first
real automated test suite, covering the pure-Python formula and backtest logic with no Spark or
network dependency.

### Why

A backtest is only meaningful if it can't see the future — a screen has to be evaluated using
only the data that would have actually been available on that historical date. That's an easy
invariant to violate accidentally and hard to verify by eye, which made it the first part of
the codebase that clearly needed real, repeatable tests rather than manual spot-checks.

### Implementation

The as-of/no-look-ahead logic, predicate evaluation, and performance math (CAGR, drawdown,
volatility, Sharpe) were pulled out into pure Python functions with no Spark dependency, so they
could be unit tested directly. That split — pure, testable logic in an importable package versus
Spark-only orchestration in numbered pipeline notebooks — became the pattern the rest of the
project's `fundamentals_pipeline` package now follows.

### Result

A working backtester with a benchmark comparison, and a `tests/` suite that could run on a
laptop in seconds with no Databricks connection — the starting point for what's now a much
larger pure-logic test suite covering valuation, period derivation, and multi-market identity
rules.
""",
        "is_published": True,
    },
    {
        "title": "CI on every push: pytest, mypy, ruff",
        "slug": "ci-pytest-mypy-ruff",
        "category": "testing",
        "published_at": "2026-07-02",
        "summary": (
            "Automated the checks that used to be manual: a GitHub Actions workflow running "
            "the test suite and linters on every push."
        ),
        "content": """\
*Retrospective entry — reconstructed from the project's git history to document work completed
in July 2026.*

### What changed

A GitHub Actions workflow that ran `pytest` against the pure-Python pipeline suite, `pytest`
plus `mypy` (with `django-stubs`) against the `web/` Django app, and `ruff` lint — on every push
and pull request, instead of relying on running checks locally before pushing.

### Why

The test suite from the backtester work was only as useful as remembering to run it. Wiring it
into CI turned "did anyone check this passes" into a required, automatic gate — visible on every
PR rather than dependent on memory.

### Implementation

Three jobs: `pipeline-tests` ran `pytest` against the Spark-free `fundamentals_pipeline` package
and frontend `lib/` helpers (Databricks notebooks need a real Spark session and aren't
unit-testable the same way, so they were never part of this gate); a separate `web` job ran
`pytest` plus `mypy .` against the `web/` Django app specifically, using `django-stubs` for
Django-aware type checking; and `lint` ran `ruff check` across both.

### Result

Every push and PR got an automatic pass/fail signal within a couple of minutes. The `web`/mypy
job was removed about a month later when `web/` itself was retired (see "A Django web app with
Postgres" below) — mypy isn't part of this project's CI today. The `pipeline-tests` and `lint`
jobs have grown alongside the test suite since (currently several hundred passing tests), and a
separate release workflow was added later for tagging and notifying downstream consumers when
`fundamentals_screener` itself is versioned.
""",
        "is_published": True,
    },
    {
        "title": "Onboarding Canada: cross-market ticker identity and currency alignment",
        "slug": "canadian-market-expansion",
        "category": "markets",
        "published_at": "2026-07-07",
        "summary": (
            "The first non-US market: S&P/TSX Composite filers, and the identity/currency bugs "
            "that come with assuming a ticker symbol is globally unique."
        ),
        "content": """\
*Retrospective entry — reconstructed from the project's git history to document work completed
in July 2026.*

### What changed

The ticker universe expanded beyond the S&P 500/Russell 3000 to include Canadian MJDS/40-F
filers on the S&P/TSX Composite, along with the identity and currency-handling changes that
expansion required.

### Why

A bare ticker symbol isn't a safe identity across markets — Magna International trades as `MG`
on the TSX, and Mistras Group traded as `MG` on the NYSE. Treating "ticker" as a global primary
key would have silently merged two unrelated companies' data. Separately, some Canadian filers
report fundamentals in USD while their shares quote in CAD (or vice versa) — `market_cap =
price × shares` is only correct when both sides share a currency, which made this a genuine
correctness bug, not just a display preference.

### Implementation

Ticker identity became a `(ticker, market)` pair instead of a bare ticker, with an explicit
collision guard that distinguishes a genuine collision from the same company legitimately
dual-listed on two markets. A separate `market_prices_daily`/`fx_rates_daily` pipeline stage
fetches daily FX rates and converts price/market-cap figures into each company's own reporting
currency, anchored to the specific date being priced — never today's spot rate applied
retroactively to a historical figure.

### Result

Canadian tickers now flow through the same pipeline as US ones, with a currency-alignment fix
that also caught and corrected the same class of bug for any future non-USD market. The
frontends gained a market filter and currency badges so a CAD-denominated figure is never
displayed as if it were USD.
""",
        "is_published": True,
    },
    {
        "title": "Extracting the screener into its own installable package",
        "slug": "extract-fundamentals-screener-package",
        "category": "architecture",
        "published_at": "2026-07-19",
        "summary": (
            "The Django presentation layer became a standalone, versioned package so an "
            "external site could depend on it directly, instead of vendoring the code."
        ),
        "content": """\
*Retrospective entry — reconstructed from the project's git history to document work completed
in July 2026.*

### What changed

The screener/company-detail/valuation views were extracted from the project's internal `web/`
Django app into `fundamentals_screener` — a separate, independently versioned, installable
Django app with its own `pyproject.toml`, meant to be pip-installed by an external Django
project rather than copy-pasted into it.

### Why

A second, external site (this project's owner's personal site) needed the same screener
functionality. Copying the code would have meant maintaining two divergent versions; extracting
it into a real installable package meant one source of truth, consumed the normal way — `pip
install git+...`.

### Implementation

The app is deliberately narrow in what it assumes about its host: no authentication, no
user-data apps, no network calls on the request path (a management command syncs published
data on a schedule instead). Its own URL route names, template filenames, and DTO shapes are
treated as a public API contract — a breaking change requires a version bump before the
consuming site updates its pinned dependency. A separate release workflow tags a new version and
notifies the consumer's repo automatically when the package's version changes.

### Result

The screener now ships as its own product with its own version history, consumed the same way
any third-party Django app would be. This is also the app this Updates section itself lives in.

### Next step

This same extraction is *why* Django models were, until now, deliberately absent from this
package (see "A Django web app with Postgres — and why it was retired a month later" below) —
and why adding the one for this feature needed its own documented decision rather than being
folded in quietly.
""",
        "is_published": True,
    },
    {
        "title": "A Django web app with Postgres — and why it was retired a month later",
        "slug": "django-web-app-postgres-retired",
        "category": "architecture",
        "published_at": "2026-08-05",
        "summary": (
            "Built a full Django presentation layer with Neon Postgres for user accounts and "
            "watchlists, then consolidated on a leaner, database-free package once the "
            "tradeoffs became clear."
        ),
        "content": """\
*Retrospective entry — reconstructed from the project's git history to document work completed
between July and August 2026.*

### What changed

Before `fundamentals_screener` existed as its own package, the project had a separate `web/`
Django application: a fuller build with a custom user model, watchlists and browsing-history
backed by real database tables, and PostgreSQL (Neon, serverless) in production with SQLite as
a local-dev fallback. It was retired about a month after being scaffolded, and its
presentation-layer responsibilities were consolidated onto `fundamentals_screener`.

### Why

`web/` was never actually deployed, and its architecture — a database-backed app with user
accounts, watchlist ownership, and a Postgres dependency — assumed a hosting environment the
real target didn't have. The reference deployment for the public-facing screener turned out to
be plain CGI hosting: no persistent process between requests, no straightforward path to a
managed Postgres connection pool, and a genuine cost to every extra runtime dependency. Building
toward a database-backed, authenticated app was solving for infrastructure the project didn't
have.

### Implementation

`fundamentals_screener`'s views/services/repositories were rebuilt read-only against published
parquet artifacts (DuckDB, no ORM, no database), matching what the CGI deployment could actually
support. `web/`'s models, migrations, and settings were deleted rather than kept around unused.
The decision — including that none of `web/`'s choices (custom user model, UUID primary keys,
Postgres) carry forward automatically — was written up as an ADR so a future feature wanting
authentication or persisted data would make that call deliberately, not by accident.

### Result

A simpler, CGI-deployable package with one real runtime dependency surface (parquet + DuckDB)
instead of two (that, plus Postgres/auth). It's also the reason this Updates feature — the
first thing since `web/`'s retirement to reintroduce a real Django model — got its own written
architectural decision rather than just being added.
""",
        "is_published": True,
    },
    {
        "title": "10-year ML forecasts with quantile regression",
        "slug": "ml-forecasting-quantile-regression",
        "category": "ml",
        "published_at": "2026-08-03",
        "summary": (
            "LightGBM quantile models forecasting Revenue, Net Income, and Free Cash Flow "
            "across five scenarios, blended into each company's own DCF terminal growth."
        ),
        "content": """\
*Retrospective entry — reconstructed from the project's git history to document work completed
in August 2026.*

### What changed

A forecasting pipeline stage that trains LightGBM quantile-regression models cross-sectionally
across the full ticker universe, producing five scenario forecasts (bear through bull) for
Revenue, Net Income, and Free Cash Flow, for years 1 through 10 out.

### Why

The project's valuation models (DCF, Graham Revised, Owner Earnings) all need a growth
assumption. Those assumptions previously came from fixed, hand-set profiles. A model that learns
plausible growth ranges from the actual cross-section of companies gives a data-driven range
instead of one static number per scenario.

### Implementation

Years 1–5 come from LightGBM's native quantile-regression API (not the scikit-learn wrapper —
the target Databricks serverless runtime doesn't have scikit-learn installed). Each forecast is
a two-part model: a continuous growth regressor plus an independent loss-probability classifier,
combined so a company genuinely at risk of a loss year isn't forced through a pure
exponential-growth curve. The five independently trained quantiles are then sorted into
monotonic order (Chernozhukov et al., 2010) since nothing guarantees p10 ≤ p50 ≤ p90 on their
own. Years 6–10 blend toward each scenario's own DCF terminal-growth rate, reusing the existing
valuation model's bear/mid/bull assumptions rather than inventing new ones.

### Result

A published `dashboard_forecast` artifact feeding a fan-chart forecast view per company, plus
forward P/E and FCF-yield multiples computed by discounting the forecast against each company's
current market cap.
""",
        "is_published": True,
    },
    {
        "title": "Splitting a 2.5-hour pipeline into a 9-task Databricks job",
        "slug": "multi-task-databricks-job-dag",
        "category": "pipeline",
        "published_at": "2026-08-13",
        "summary": (
            "A single transient failure used to cost a full pipeline re-run. Decomposing the "
            "pipeline into an orchestrated multi-task DAG made retries cheap instead."
        ),
        "content": """\
*Retrospective entry — reconstructed from the project's git history to document work completed
in August 2026.*

### What changed

The pipeline's single-notebook orchestrator was split into a 9-task Databricks Job DAG, with
explicit dependency edges: ingestion and cleanup, derived metrics, a parallel fan-out (intrinsic
value / backtest / forecasting), analysis checks, export, publish, and Delta maintenance.

### Why

A real incident forced the issue: a Spark checkpoint/executor-loss failure inside the derived-
metrics step meant re-running the *entire* pipeline — ingestion, cleanup, everything — to retry
just that one step, at a cost of 2.5+ hours. When one task in a long chain runs as a single
Databricks Job task, a transient failure anywhere is as expensive as a failure at the very end.

### Implementation

Each stage of the old single notebook became its own Job task with `depends_on` edges matching
its real data dependencies — three previously-sequential steps (intrinsic value, backtest,
forecasting) turned out to have zero cross-dependencies on each other, only on the derived-
metrics step before them, so they now run in parallel. A shared `run_id`, relayed forward
through each task's own direct dependency (Databricks' `taskValues` API doesn't reach arbitrary
upstream tasks, only direct ones), ties one run's per-task rows together in a run-log table. The
job's topology is also captured as reviewable, version-controlled IaC (`databricks.yml`)
alongside the notebooks themselves.

### Result

A full run now takes roughly 19 minutes instead of 2.5–3+ hours for the same scope, and a
transient failure in one task costs a retry of that task alone, via Databricks' native Repair
Run, not a restart of the whole pipeline.
""",
        "is_published": True,
    },
    {
        "title": "European filings: an ESEF/XBRL adapter for a second regulatory regime",
        "slug": "european-filings-adapter",
        "category": "markets",
        "published_at": "2026-08-16",
        "summary": (
            "SEC EDGAR and Canadian filers share one regulator's XBRL dialect. European "
            "issuers file under ESEF instead — a genuinely different source, mapped into the "
            "same tables."
        ),
        "content": """\
*Retrospective entry — reconstructed from the project's git history to document work completed
in August 2026, and still ongoing.*

### What changed

A pilot adapter for European issuers, sourced from `filings.xbrl.org` rather than SEC EDGAR,
covering a handful of verified companies across Spain, France, the Netherlands, and Italy — the
project's first fundamentals source outside the SEC's own filing system.

### Why

Canada's filers still submit through SEC EDGAR under US GAAP or IFRS, so the earlier Canadian
expansion could reuse the existing SEC ingestion path largely unchanged. European issuers file
under ESEF instead, in IFRS XBRL, through a completely different regulatory system — a real
second source, not a variant of the first one.

### Implementation

The pipeline's `FundamentalsSource` abstraction (designed ahead of time for exactly this) got its
first real implementation beyond SEC: a source that discovers filings via the `filings.xbrl.org`
API, resolves amendments to the correct filing per period, and maps a deliberately small,
high-confidence set of IFRS concepts into the same canonical statement lines every other source
already produces. Company identity for admitted European issuers is tracked separately from the
existing US/Canada ticker universe (a different admission process entirely), and only unioned
in at the point data is published, so the two never get conflated.

### Result

Real financial-statement data for the pilot issuers is now flowing through the same
`financials` → derived-metrics → dashboard pipeline as every US and Canadian company. Concept
coverage is intentionally still narrow and being expanded deliberately, one verified mapping at
a time, rather than mapped in bulk without checking each one against the real filings.

### Next step

Widening IFRS concept coverage beyond the current pilot set, and eventually admitting more
European issuers beyond the initial verified pilot group.
""",
        "is_published": True,
    },
]


def seed_updates(apps, schema_editor):
    Update = apps.get_model("fundamentals_screener", "Update")
    for entry in _UPDATES:
        Update.objects.update_or_create(slug=entry["slug"], defaults=entry)


def remove_seeded_updates(apps, schema_editor):
    Update = apps.get_model("fundamentals_screener", "Update")
    Update.objects.filter(slug__in=[entry["slug"] for entry in _UPDATES]).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("fundamentals_screener", "0001_initial"),
    ]

    operations = [
        migrations.RunPython(seed_updates, remove_seeded_updates),
    ]

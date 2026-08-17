# Databricks notebook source
# MAGIC %md
# MAGIC # 50__publish / 51__export_dashboard_data
# MAGIC
# MAGIC Exports the dashboard-ready slice of `main.financials.*` to local files on the driver
# MAGIC (`/tmp/` — required by pandas/pyarrow's parquet writers), then copies them to the
# MAGIC `main.financials._publish` Unity Catalog Volume, which is what `52__publish_to_github`
# MAGIC actually reads from (a real cross-task-readable location — `51`/`52` run as separate
# MAGIC Databricks Job Tasks, which don't share driver-local `/tmp`) to upload as GitHub Release
# MAGIC assets — together they let the public Streamlit app render without touching Databricks at
# MAGIC runtime.
# MAGIC
# MAGIC **Outputs** (written to `/tmp/`, then copied to `/Volumes/main/financials/_publish/`)
# MAGIC - `dashboard_data.parquet`    — long-format financials joined with concept_hierarchy
# MAGIC - `dashboard_metrics.parquet` — long-format derived metrics joined with metrics_hierarchy
# MAGIC - `dashboard_filings.parquet` — SEC 10-K/10-Q filing list (see 15__fetch_sec_filings.py)
# MAGIC - `dashboard_forecast.parquet` — 10-year ML scenario forecasts (see 24__forecasting.py)
# MAGIC - `dashboard_meta.json`       — build timestamp, ticker list, row counts, schema version
# MAGIC
# MAGIC **Universe:** all tickers that have data in `financials`.
# MAGIC
# MAGIC **Retention window:** last 10 fiscal years (FY) and last 12 quarters per ticker.
# MAGIC
# MAGIC Databricks-only: uses `spark`, `%run`, and writes to driver-local `/tmp/` before copying
# MAGIC (plain `shutil`, not `dbutils.fs` — see §5 below) to the `main.financials._publish` Volume.

# COMMAND ----------

# MAGIC %run "../00__config/01__tickers"

# COMMAND ----------

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# The schema contract ships in the installable `fundamentals_pipeline` package
# (fundamentals_pipeline/schemas.py). Every environment installs it the same way
# (`pip install -e .`); on Databricks that install normally happens once in
# 91__full_pipeline's session-dependencies cell, but this notebook has no guarantee of
# that shared session if it's ever run standalone or as its own Databricks Job task (a
# task gets a fresh Python env) — same reinstall-on-ImportError pattern already used by
# 11__fetch_sec_xbrl.py.
try:
    from fundamentals_pipeline import schemas as _schemas
    from fundamentals_pipeline.identity import check_no_export_ticker_collision
except ImportError:
    import subprocess
    import sys

    subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "-e", "../.."])
    from fundamentals_pipeline import schemas as _schemas
    from fundamentals_pipeline.identity import check_no_export_ticker_collision

SCHEMA_VERSION = 15  # +dashboard_forecast artifact (10-year cross-sectional ML scenario
                     # forecasts — see 24__forecasting.py; issue #333)
FY_YEARS       = 10
QUARTERS       = 12
PRICE_YEARS    = 10                              # daily-price retention window (calendar years)

OUT_DIR        = Path("/tmp")
DATA_PARQUET   = OUT_DIR / "dashboard_data.parquet"
METRIC_PARQUET = OUT_DIR / "dashboard_metrics.parquet"
META_JSON      = OUT_DIR / "dashboard_meta.json"
PRICE_PARQUET  = OUT_DIR / "dashboard_prices.parquet"
BACKTEST_PARQUET = OUT_DIR / "dashboard_backtest.parquet"
FX_PARQUET     = OUT_DIR / "dashboard_fx.parquet"
FILINGS_PARQUET = OUT_DIR / "dashboard_filings.parquet"
FORECAST_PARQUET = OUT_DIR / "dashboard_forecast.parquet"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Resolve ticker universe (all tickers with data in financials)

# COMMAND ----------

# Universe flags (is_favorite / in_sp500 / in_r3000 / in_tsx_composite) drive the
# Streamlit screener's universe filter. COALESCE to false so NULLs don't leak through.
#
# Phase 5.6: UNION ALL with a second, structurally-separate branch for FIRDS-admitted European
# issuers (main.config.eu_admission_candidates, admission_status = 'admitted' only) --
# deliberately NOT an extra row in `config.tickers` itself. `config.tickers` is, by its own
# documented purpose (00__config/02__tickers_master.py's own header), the INDEX-MEMBERSHIP-
# DRIVEN US/CA ticker universe (S&P 500 / Russell 3000 / TSX Composite / favorites.json) --  a
# semantically different admission concept from FIRDS' regulatory-eligibility-driven European
# universe. Conflating the two into one table just to satisfy this one JOIN would answer an
# unresolved identity question (ADR-0012's still-open MIC:ISIN vs. MIC:TICKER decision)
# prematurely, and would misrepresent European issuers as if they were part of a US/CA index.
# See docs/phase5-6-european-dashboard-data-integration.md for the full architectural decision.
#
# European rows carry `market = 'EU'` (a single Generation-1 value across all admitted
# countries, matching FIRDS' own EU-wide scope -- see fundamentals_pipeline/sources/
# eu_admission.py), `exchange` = the primary-listing MIC (the closest real equivalent to the
# US/CA `exchange` field's own Yahoo-mnemonic style), `accounting_standard = 'ifrs-full'`
# (verified true for every EU_CURRENT filing ingested so far -- Phase 5.1's own `detect_
# metadata()` check), and `reporting_currency` from the admission layer's own real, FIRDS-
# sourced `currency` field (never guessed, never defaulted to USD). Fields with no European
# equivalent yet (sector/industry/employees/website/founded/has_logo/description) are NULL --
# real absence, not a fabricated value -- and every universe-membership flag is `false` (a
# FIRDS-admitted issuer is not S&P 500/Russell 3000/TSX Composite/favorites membership, a
# different concept entirely).
tickers_df = spark.sql(f"""
    SELECT
      t.ticker, t.company, t.sector, t.industry, t.has_logo,
      t.description, t.exchange, t.country, t.employees, t.website, t.founded,
      t.accounting_standard, t.reporting_currency, t.market,
      COALESCE(t.is_favorite,       false) AS is_favorite,
      COALESCE(t.in_sp500,          false) AS in_sp500,
      COALESCE(t.in_r3000,          false) AS in_r3000,
      COALESCE(t.in_tsx_composite,  false) AS in_tsx_composite
    FROM {CATALOG}.config.tickers t
    JOIN (SELECT DISTINCT ticker FROM {CATALOG}.{SCHEMA}.financials) f
      ON f.ticker = t.ticker

    UNION ALL

    SELECT
      e.ticker, e.issuer_name AS company, CAST(NULL AS STRING) AS sector,
      CAST(NULL AS STRING) AS industry, CAST(NULL AS BOOLEAN) AS has_logo,
      CAST(NULL AS STRING) AS description, e.mic AS exchange, e.country,
      CAST(NULL AS BIGINT) AS employees, CAST(NULL AS STRING) AS website,
      CAST(NULL AS INT) AS founded,
      'ifrs-full' AS accounting_standard, e.currency AS reporting_currency, 'EU' AS market,
      false AS is_favorite, false AS in_sp500, false AS in_r3000, false AS in_tsx_composite
    FROM {CATALOG}.config.eu_admission_candidates e
    JOIN (SELECT DISTINCT ticker FROM {CATALOG}.{SCHEMA}.financials) f
      ON f.ticker = e.ticker
    WHERE e.admission_status = 'admitted'

    ORDER BY ticker
""").toPandas()

if tickers_df.empty:
    raise ValueError("No tickers with financial data found")

# Ticker-collision guard (Phase 5.6): the US/CA and European branches above are two
# structurally independent admission processes with no shared gate against each other -- unlike
# `config.tickers` itself, which already guards against a US/CA collision via
# `identity.py`'s `check_no_cross_market_collision()` before its own write. A ticker appearing
# in BOTH branches would silently produce two rows for one ticker string in `tickers_df`,
# corrupting every downstream `{ticker: record}` dict this export (and fundamentals_screener's
# own repositories, per the Phase 5.5 audit) builds. Not observed today (FCC/FCT are absent
# from config.tickers, confirmed live) -- but per this project's own "reject, never silently
# guess" principle, a real collision here must stop the run, not corrupt the export.
# Extracted into fundamentals_pipeline/identity.py's check_no_export_ticker_collision() so
# this exact logic is unit-tested (tests/test_identity.py) without needing a Spark session.
check_no_export_ticker_collision(tickers_df["ticker"])

tickers = tickers_df["ticker"].tolist()
# Build records with native Python bools — pandas/numpy bool_ would be stringified
# to "True"/"False" by json.dumps(default=str) and break boolean parsing in the app.
ticker_meta = [
    {
        "ticker":      r.ticker,
        "company":     r.company,
        "sector":      r.sector,   # NULL → app maps to "Unknown"
        "industry":    r.industry, # NULL → app maps to "Unknown"
        # has_logo: True hit / False miss / None (probe skipped or errored). Keep it a
        # native bool|None — pd.NA / numpy bool_ would be stringified by json.dumps(default=str).
        "has_logo":    None if pd.isna(r.has_logo) else bool(r.has_logo),
        # Company Overview tab (schema v9). description may be "" — that is fine; NULL → "".
        "description": r.description or "",
        "exchange":    r.exchange    or "",
        "country":     r.country     or "",
        "employees":   None if pd.isna(r.employees) else int(r.employees),
        "website":     r.website     or "",
        "founded":     None if pd.isna(r.founded)   else int(r.founded),
        "accounting_standard": r.accounting_standard or "",
        "reporting_currency":  r.reporting_currency  or "",
        # Listing market ("US"/"CA") — the quote currency a ticker's PRICE trades in, which
        # can differ from reporting_currency (e.g. a USD-reporting, CAD-quoted TSX filer).
        # See CLAUDE.md's currency-alignment convention / QUOTE_CURRENCY_BY_MARKET in
        # 22__derived_metrics.py, whose ground truth this mirrors.
        "market":              r.market              or "US",
        "is_favorite":       bool(r.is_favorite),
        "in_sp500":          bool(r.in_sp500),
        "in_r3000":          bool(r.in_r3000),
        # S&P/TSX Composite membership (multi-market roadmap) — drives the screener's
        # "S&P/TSX Composite" Universe option. Independent of `market`: a dual-listed
        # US-primary ticker (BAM, SHOP, GFL, …) can be in_tsx_composite=true with market="US".
        "in_tsx_composite":  bool(r.in_tsx_composite),
    }
    for r in tickers_df.itertuples()
]
print(f"✓ Exporting {len(tickers)} ticker(s)")

# Register the export universe as a temp view so the slice queries below semi-join it
# instead of embedding a ~2.5k-literal IN(...) list. The literal list made Catalyst
# planning ~5x slower (≈42s vs ≈13s per query) for an identical result set.
spark.createDataFrame([(t,) for t in tickers], "ticker string").createOrReplaceTempView("_export_universe")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Financials slice (long format + hierarchy)
# MAGIC
# MAGIC LEFT JOIN against `concept_hierarchy` so the Streamlit app gets
# MAGIC `section / group / display_name / sort_order` resolved at write time.
# MAGIC Rows whose `concept` isn't in the hierarchy still flow through with NULL
# MAGIC hierarchy columns — the renderer just won't show them.

# COMMAND ----------

# Precise retention done IN SQL so toPandas() pulls only the final slice (no driver-side
# loop over ~2.5k tickers). DENSE_RANK over DISTINCT period_end/fiscal_year — not ROW_NUMBER
# over rows — keeps ALL concept rows for the last N periods regardless of how many concepts a
# period has (the old ROW_NUMBER `* 30` cap was a guess that broke as concept counts grew, and
# forced an over-pull + pandas trim_recent loop that pushed 51 past its 600s timeout). FY and
# the quarterly bucket (Q1..Q4 combined) are ranked independently: last FY_YEARS distinct FY
# *fiscal years* + last QUARTERS distinct quarter period_ends per ticker.
#
# FY ranks by fiscal_year, NOT period_end (real bug, found + fixed during the "horizon
# extension" milestone, 2026-08-06): `Shares Outstanding (Cover Page)` legitimately carries the
# 10-K's cover-page "as-of" date (~3 weeks after fiscal close) rather than the fiscal year-end
# date every other concept in that same fiscal year uses -- e.g. AAPL FY2025's ~40 other
# concepts all have period_end=2025-09-27, but that one concept has period_end=2025-10-17. A
# period_end-based DENSE_RANK treats those as two separate rank tiers per fiscal year, so
# FY_YEARS=10 was silently only ever retaining 5 real fiscal years (confirmed against real
# production data: AAPL's exported Income Statement history was capped at 2021-2025 despite 17
# years, 2009-2025, existing in the source `financials` table). fiscal_year has no such
# multi-value-per-year quirk, so ranking on it directly is both correct and simpler. The
# quarterly bucket's own period_end-based ranking is untouched -- this fix is scoped to the FY
# bucket, where the problem was confirmed; quarterly retention wasn't shown to have the same
# issue and isn't in scope here.
financials = spark.sql(f"""
    WITH base AS (
      SELECT
        f.ticker, f.period_type, f.period_end, f.fiscal_year,
        f.stmt, f.concept, f.value,
        h.section, h.group, h.sort_order,
        COALESCE(h.display_name, f.concept) AS display_name,
        CASE WHEN f.period_type = 'FY' THEN 'FY' ELSE 'Q' END AS pt_bucket
      FROM {CATALOG}.{SCHEMA}.financials f
      LEFT JOIN {CATALOG}.config.concept_hierarchy h
        ON h.stmt = f.stmt AND h.concept = f.concept
      WHERE f.ticker IN (SELECT ticker FROM _export_universe)
        AND f.period_type IN ('FY', 'Q1', 'Q2', 'Q3', 'Q4')
    ),
    ranked AS (
      SELECT *,
        DENSE_RANK() OVER (
          PARTITION BY ticker, pt_bucket ORDER BY period_end DESC
        ) AS period_rank,
        DENSE_RANK() OVER (
          PARTITION BY ticker, pt_bucket ORDER BY fiscal_year DESC
        ) AS fy_rank
      FROM base
    )
    SELECT
      ticker, period_type, period_end, fiscal_year,
      stmt, section, `group`, concept, display_name, sort_order, value
    FROM ranked
    WHERE (pt_bucket = 'FY' AND fy_rank <= {FY_YEARS})
       OR (pt_bucket = 'Q'  AND period_rank <= {QUARTERS})
""").toPandas()

print(f"  financials rows: {len(financials):,}")
print(financials.groupby(["ticker", "period_type"]).size().unstack(fill_value=0))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Derived metrics slice (long format + hierarchy)

# COMMAND ----------

metrics = spark.sql(f"""
    WITH ranked AS (
      SELECT
        m.ticker, 'FY' AS period_type,
        MAKE_DATE(m.fiscal_year, 12, 31) AS period_end,
        m.fiscal_year,
        h.category, h.subcategory, m.metric,
        h.unit, h.sort_order, m.value,
        DENSE_RANK() OVER (PARTITION BY m.ticker ORDER BY m.fiscal_year DESC) AS yr_rank
      FROM {CATALOG}.{SCHEMA}.financials_metrics m
      LEFT JOIN {CATALOG}.config.metrics_hierarchy h
        ON h.metric = m.metric
      WHERE m.ticker IN (SELECT ticker FROM _export_universe)
        AND m.fiscal_year BETWEEN 1990 AND 2099
    )
    SELECT ticker, period_type, period_end, fiscal_year,
           category, subcategory, metric, unit, sort_order, value
    FROM ranked
    WHERE yr_rank <= {FY_YEARS}
""").toPandas()

# Market Cap lives in market_cap_asof (period_end-aligned, written by 22) — inject it as a
# `Market Cap` metric row so the Streamlit screener can pivot it like any other metric.
# category/subcategory are NULL on purpose: the detail page's metrics grid filters on
# category.dropna(), so these rows are invisible there but the screener still picks them up.
# `period_end` is the REAL fiscal close and the value is priced as-of it, so — unlike the old
# market_data source — it's on the same fiscal basis as every other FY metric (no 0–11mo offset).
#
# `unit`: market_cap_asof's `currency` column (added alongside the currency-alignment fix,
# schema v11) is each row's REAL native reporting currency (not always USD once Canadian
# tickers exist — see fundamentals_pipeline/fx.py) — read it directly rather than hardcoding
# 'usd'. Defensively falls back to the 'usd' literal for a table that predates that column.
MKT_COLUMNS = ["ticker", "period_type", "period_end", "fiscal_year",
               "category", "subcategory", "metric", "unit", "sort_order", "value"]
try:
    _mca_cols = {f.name for f in spark.table(f"{CATALOG}.{SCHEMA}.market_cap_asof").schema.fields}
    _unit_expr = "LOWER(md.currency)" if "currency" in _mca_cols else "'usd'"
    market_cap = spark.sql(f"""
        WITH ranked AS (
          SELECT
            md.ticker,
            'FY'                  AS period_type,
            md.period_end         AS period_end,
            md.fiscal_year,
            CAST(NULL AS STRING)  AS category,
            CAST(NULL AS STRING)  AS subcategory,
            'Market Cap'          AS metric,
            {_unit_expr}          AS unit,
            CAST(NULL AS DOUBLE)  AS sort_order,
            md.market_cap         AS value,
            DENSE_RANK() OVER (PARTITION BY md.ticker ORDER BY md.fiscal_year DESC) AS yr_rank
          FROM {CATALOG}.{SCHEMA}.market_cap_asof md
          WHERE md.ticker IN (SELECT ticker FROM _export_universe)
            AND md.market_cap IS NOT NULL
        )
        SELECT ticker, period_type, period_end, fiscal_year,
               category, subcategory, metric, unit, sort_order, value
        FROM ranked
        WHERE yr_rank <= {FY_YEARS}
    """).toPandas()
except Exception as e:
    print(f"⚠ market_cap_asof unavailable ({e}) — skipping Market Cap rows.")
    market_cap = pd.DataFrame(columns=MKT_COLUMNS)

# Market Cap (Live) — mirrors the Market Cap injection above, but reads market_cap_live
# (22__derived_metrics.py) instead: one row per ticker (not FY_YEARS-many), priced with
# TODAY's close × the most recent point-in-time "Shares Outstanding (Cover Page)" count,
# not a fiscal year's own weighted-average Shares Diluted. Same category/subcategory=NULL
# convention (invisible on the generic metrics grid, picked up by name like Market Cap).
try:
    _mcl_cols = {f.name for f in spark.table(f"{CATALOG}.{SCHEMA}.market_cap_live").schema.fields}
    _live_unit_expr = "LOWER(md.currency)" if "currency" in _mcl_cols else "'usd'"
    market_cap_live = spark.sql(f"""
        SELECT
            md.ticker,
            'FY'                       AS period_type,
            md.period_end              AS period_end,
            md.fiscal_year,
            CAST(NULL AS STRING)       AS category,
            CAST(NULL AS STRING)       AS subcategory,
            'Market Cap (Live)'        AS metric,
            {_live_unit_expr}          AS unit,
            CAST(NULL AS DOUBLE)       AS sort_order,
            md.market_cap              AS value
        FROM {CATALOG}.{SCHEMA}.market_cap_live md
        WHERE md.ticker IN (SELECT ticker FROM _export_universe)
          AND md.market_cap IS NOT NULL
    """).toPandas()
except Exception as e:
    print(f"⚠ market_cap_live unavailable ({e}) — skipping Market Cap (Live) rows.")
    market_cap_live = pd.DataFrame(columns=MKT_COLUMNS)

# NCAV / Share (Live) + NCAV Ratio (Live) — same market_cap_live table, relaxed/moderate/strict.
# Unpivoted via a plain UNION ALL of six single-column selects (one per source column), the
# exact same shape as the Market Cap (Live) block above — kept deliberately simple/uniform
# rather than a fancier LATERAL VIEW EXPLODE, since there's no local Spark session to test
# against here.
_NCAV_LIVE_COLUMNS = [
    ("ncav_share_live_relaxed",  "NCAV / Share (Live)",            "usd"),
    ("ncav_ratio_live_relaxed",  "NCAV Ratio (Live)",              "ratio"),
    ("ncav_share_live_moderate", "NCAV (Moderate) / Share (Live)", "usd"),
    ("ncav_ratio_live_moderate", "NCAV (Moderate) Ratio (Live)",   "ratio"),
    ("ncav_share_live_strict",   "NCAV (Strict) / Share (Live)",   "usd"),
    ("ncav_ratio_live_strict",   "NCAV (Strict) Ratio (Live)",     "ratio"),
]
try:
    _ncav_live_union = " UNION ALL ".join(
        f"""
        SELECT
            md.ticker, 'FY' AS period_type, md.period_end AS period_end, md.fiscal_year,
            CAST(NULL AS STRING) AS category, CAST(NULL AS STRING) AS subcategory,
            '{_metric}' AS metric, '{_unit}' AS unit, CAST(NULL AS DOUBLE) AS sort_order,
            md.{_col} AS value
        FROM {CATALOG}.{SCHEMA}.market_cap_live md
        WHERE md.ticker IN (SELECT ticker FROM _export_universe)
          AND md.{_col} IS NOT NULL
        """
        for _col, _metric, _unit in _NCAV_LIVE_COLUMNS
    )
    _ncav_live_metrics = spark.sql(_ncav_live_union).toPandas()
except Exception as e:
    print(f"⚠ market_cap_live NCAV columns unavailable ({e}) — skipping NCAV (Live) rows.")
    _ncav_live_metrics = pd.DataFrame(columns=MKT_COLUMNS)

# Retention now enforced in SQL (last FY_YEARS years per ticker, per source), so no
# driver-side trim needed — just stack the long-format metric frames.
metrics = pd.concat([metrics, market_cap, market_cap_live, _ncav_live_metrics], ignore_index=True)
print(f"  + Market Cap rows: {len(market_cap):,}")
print(f"  + Market Cap (Live) rows: {len(market_cap_live):,}")
print(f"  + NCAV (Live) rows: {len(_ncav_live_metrics):,}")
print(f"  metrics rows: {len(metrics):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3b. Daily price slice (market_prices_daily)
# MAGIC
# MAGIC Daily adjusted/raw close for the export universe, last `PRICE_YEARS` calendar
# MAGIC years. Daily grain only — the Streamlit Price tab resamples to weekly/monthly
# MAGIC client-side, so nothing is pre-aggregated here. `adj_close` drives the chart;
# MAGIC raw `close` is tooltip-only. `value` precision matters (BRK.A trades >$600k), so
# MAGIC both columns stay float64 — no downcast.

# COMMAND ----------

# Resilient read: market_prices_daily may not exist yet (12__fetch_market_data not run
# on full history) or may be empty for this universe. In either case fall back to an
# empty, correctly-typed frame so 52 still finds the file and the app degrades to
# "no data" instead of crashing.
PRICE_COLUMNS = {"ticker": "object", "date": "datetime64[ns]", "close": "float64", "adj_close": "float64"}
try:
    prices = spark.sql(f"""
        SELECT p.ticker, p.date, p.close, p.adj_close
        FROM {CATALOG}.{SCHEMA}.market_prices_daily p
        WHERE p.ticker IN (SELECT ticker FROM _export_universe)
          AND p.date >= ADD_MONTHS(CURRENT_DATE(), -12 * {PRICE_YEARS})
    """).toPandas()
    # volume is a one-line add here (and in PRICE_COLUMNS) if a volume subplot is ever wanted.
    if prices.empty:
        print("⚠️ market_prices_daily returned 0 rows for this universe — writing empty price slice")
except Exception as exc:  # noqa: BLE001 — table absent or unreadable: degrade, don't fail the export
    print(f"⚠️ Could not read market_prices_daily ({type(exc).__name__}: {exc}) — writing empty price slice")
    prices = pd.DataFrame({c: pd.Series(dtype=t) for c, t in PRICE_COLUMNS.items()})

# Keep close/adj_close as float64 (no downcast — large prices lose cents in float32).
prices["close"] = prices["close"].astype("float64")
prices["adj_close"] = prices["adj_close"].astype("float64")

if prices.empty:
    print("  prices rows: 0 (empty slice)")
else:
    print(f"  prices rows: {len(prices):,}")
    print(f"  distinct tickers: {prices['ticker'].nunique():,}")
    print(f"  date range: {prices['date'].min()} → {prices['date'].max()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3c. Backtest slice (backtest_results)
# MAGIC
# MAGIC Equity-curve series per archetype × fiscal_year from `70__backtest/71__run_backtest`.
# MAGIC Resilient: the table may not exist yet (backtester not run) — fall back to an
# MAGIC empty, correctly-typed frame so the app degrades to "no data" instead of crashing.

# COMMAND ----------

BACKTEST_COLUMNS = {
    "archetype": "object", "fiscal_year": "int64",
    "portfolio_return": "float64", "benchmark_return": "float64",
    "portfolio_value": "float64", "benchmark_value": "float64", "n_holdings": "int64",
}
try:
    backtest = spark.sql(f"""
        SELECT archetype, fiscal_year, portfolio_return, benchmark_return,
               portfolio_value, benchmark_value, n_holdings
        FROM {CATALOG}.{SCHEMA}.backtest_results
        ORDER BY archetype, fiscal_year
    """).toPandas()
    if backtest.empty:
        print("⚠️ backtest_results returned 0 rows — writing empty backtest slice")
except Exception as exc:  # noqa: BLE001 — table absent or unreadable: degrade, don't fail the export
    print(f"⚠️ Could not read backtest_results ({type(exc).__name__}: {exc}) — writing empty backtest slice")
    backtest = pd.DataFrame({c: pd.Series(dtype=t) for c, t in BACKTEST_COLUMNS.items()})

if backtest.empty:
    print("  backtest rows: 0 (empty slice)")
else:
    print(f"  backtest rows: {len(backtest):,} ({backtest['archetype'].nunique()} archetype(s))")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3d. FX rates slice (fx_rates_daily)
# MAGIC
# MAGIC **Full daily history** (no retention window, unlike prices) — a future frontend "view
# MAGIC in USD" toggle needs the rate from a HISTORICAL figure's own `period_end`, never today's
# MAGIC spot rate (see `fundamentals_pipeline/fx.py`'s date-anchoring rule), so truncating this
# MAGIC to a recent window would silently break that for older fiscal years. The table itself is
# MAGIC tiny (a handful of currency pairs × ~20 years of daily rates), so exporting it whole
# MAGIC costs nothing. Building the toggle itself is out of scope here — this just publishes the
# MAGIC data it will need.

# COMMAND ----------

FX_COLUMNS = {"base": "object", "quote": "object", "pair": "object",
              "date": "datetime64[ns]", "rate": "float64"}
try:
    fx_rates = spark.sql(f"""
        SELECT base, quote, pair, date, rate
        FROM {CATALOG}.{SCHEMA}.fx_rates_daily
        ORDER BY base, quote, date
    """).toPandas()
    if fx_rates.empty:
        print("⚠️ fx_rates_daily returned 0 rows — writing empty FX slice")
except Exception as exc:  # noqa: BLE001 — table absent or unreadable: degrade, don't fail the export
    print(f"⚠️ Could not read fx_rates_daily ({type(exc).__name__}: {exc}) — writing empty FX slice")
    fx_rates = pd.DataFrame({c: pd.Series(dtype=t) for c, t in FX_COLUMNS.items()})

if fx_rates.empty:
    print("  fx rows: 0 (empty slice)")
else:
    print(f"  fx rows: {len(fx_rates):,} ({fx_rates[['base', 'quote']].drop_duplicates().shape[0]} pair(s))")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3e. SEC filing list (sec_filings)
# MAGIC
# MAGIC Real 10-K/10-Q filings (form, filing date, report date, direct document link) written
# MAGIC by `15__fetch_sec_filings.py`. Both frontends' company-page Filings tab reads this
# MAGIC directly instead of making their own live SEC calls (see that stage's own docstring for
# MAGIC the incident that motivated moving this into the pipeline).

# COMMAND ----------

FILINGS_COLUMNS = {"ticker": "object", "form": "object", "filing_date": "object",
                    "report_date": "object", "description": "object", "url": "object"}
try:
    filings = spark.sql(f"""
        SELECT ticker, form, filing_date, report_date, description, url
        FROM {CATALOG}.{SCHEMA}.sec_filings
        ORDER BY ticker, filing_date DESC
    """).toPandas()
    if filings.empty:
        print("⚠️ sec_filings returned 0 rows — writing empty filings slice")
except Exception as exc:  # noqa: BLE001 — table absent or unreadable: degrade, don't fail the export
    print(f"⚠️ Could not read sec_filings ({type(exc).__name__}: {exc}) — writing empty filings slice")
    filings = pd.DataFrame({c: pd.Series(dtype=t) for c, t in FILINGS_COLUMNS.items()})

if filings.empty:
    print("  filings rows: 0 (empty slice)")
else:
    print(f"  filings rows: {len(filings):,} ({filings['ticker'].nunique():,} ticker(s))")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3f. Forecasting (financials_forecast)
# MAGIC
# MAGIC 10-year cross-sectional ML scenario forecasts (Revenue/Net Income/Free Cash Flow) written
# MAGIC by `24__forecasting.py` — years 1-5 from LightGBM quantile regression, years 6-10 blended
# MAGIC toward each scenario's DCF terminal-growth rate. `forecast_value` is already an absolute
# MAGIC dollar value (never a raw growth rate), so consumers just read it directly.

# COMMAND ----------

FORECAST_COLUMNS = {"ticker": "object", "fiscal_year": "int64", "horizon": "int64",
                     "metric": "object", "quantile_level": "float64", "forecast_value": "float64"}
try:
    forecast = spark.sql(f"""
        SELECT ticker, fiscal_year, horizon, metric, quantile_level, forecast_value
        FROM {CATALOG}.{SCHEMA}.financials_forecast
        ORDER BY ticker, metric, quantile_level, horizon
    """).toPandas()
    if forecast.empty:
        print("⚠️ financials_forecast returned 0 rows — writing empty forecast slice")
except Exception as exc:  # noqa: BLE001 — table absent or unreadable: degrade, don't fail the export
    print(f"⚠️ Could not read financials_forecast ({type(exc).__name__}: {exc}) — writing empty forecast slice")
    forecast = pd.DataFrame({c: pd.Series(dtype=t) for c, t in FORECAST_COLUMNS.items()})

if forecast.empty:
    print("  forecast rows: 0 (empty slice)")
else:
    print(f"  forecast rows: {len(forecast):,} ({forecast['ticker'].nunique():,} ticker(s))")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write parquet + meta

# COMMAND ----------

# Spark Connect's toPandas() stashes a non-JSON-serializable PlanMetrics object in
# df.attrs (query execution metrics). pandas' to_parquet (PyArrow) then tries
# json.dumps(df.attrs) as file metadata and crashes with
# "TypeError: Object of type PlanMetrics is not JSON serializable". Clear attrs first.
financials.attrs = {}
metrics.attrs = {}
prices.attrs = {}
backtest.attrs = {}
fx_rates.attrs = {}
filings.attrs = {}
forecast.attrs = {}

# Schema contract — fail the run LOUDLY rather than shipping an artifact the public app
# can't read. assert_artifact raises SchemaError naming the offending artifact/column.
_schemas.assert_artifact("dashboard_data", financials)
_schemas.assert_artifact("dashboard_metrics", metrics)
_schemas.assert_artifact("dashboard_prices", prices)
_schemas.assert_artifact("dashboard_backtest", backtest)
_schemas.assert_artifact("dashboard_fx", fx_rates)
_schemas.assert_artifact("dashboard_filings", filings)
_schemas.assert_artifact("dashboard_forecast", forecast)

financials.to_parquet(DATA_PARQUET, index=False)
metrics.to_parquet(METRIC_PARQUET, index=False)
prices.to_parquet(PRICE_PARQUET, index=False)
backtest.to_parquet(BACKTEST_PARQUET, index=False)
fx_rates.to_parquet(FX_PARQUET, index=False)
filings.to_parquet(FILINGS_PARQUET, index=False)
forecast.to_parquet(FORECAST_PARQUET, index=False)

# Per-ticker FY range — used by the Streamlit masthead.
fy_ranges = (
    financials[financials["period_type"] == "FY"]
    .groupby("ticker")["fiscal_year"]
    .agg(["min", "max"])
    .rename(columns={"min": "fy_min", "max": "fy_max"})
    .reset_index()
    .to_dict(orient="records")
)

meta = {
    "schema_version":   SCHEMA_VERSION,
    "build_timestamp":  datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "tickers":          ticker_meta,   # list of {"ticker", "company"} dicts
    "fy_ranges":        fy_ranges,
    "row_counts": {
        "financials":   int(len(financials)),
        "metrics":      int(len(metrics)),
        "prices":       int(len(prices)),
        "backtest":     int(len(backtest)),
        "fx":           int(len(fx_rates)),
        "filings":      int(len(filings)),
        "forecast":     int(len(forecast)),
    },
    "retention": {
        "fy_years":     FY_YEARS,
        "quarters":     QUARTERS,
        "price_years":  PRICE_YEARS,
    },
}
_schemas.assert_meta(meta)
META_JSON.write_text(json.dumps(meta, indent=2, default=str))

print("\n✓ Wrote:")
print(f"  {DATA_PARQUET}   ({DATA_PARQUET.stat().st_size / 1024:.1f} KB)")
print(f"  {METRIC_PARQUET} ({METRIC_PARQUET.stat().st_size / 1024:.1f} KB)")
print(f"  {PRICE_PARQUET}  ({PRICE_PARQUET.stat().st_size / 1024:.1f} KB)")
print(f"  {BACKTEST_PARQUET} ({BACKTEST_PARQUET.stat().st_size / 1024:.1f} KB)")
print(f"  {FX_PARQUET}     ({FX_PARQUET.stat().st_size / 1024:.1f} KB)")
print(f"  {FILINGS_PARQUET} ({FILINGS_PARQUET.stat().st_size / 1024:.1f} KB)")
print(f"  {FORECAST_PARQUET} ({FORECAST_PARQUET.stat().st_size / 1024:.1f} KB)")
print(f"  {META_JSON}      (schema_version={SCHEMA_VERSION})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Copy to the publish Volume — the real 51→52 handoff
# MAGIC
# MAGIC `52__publish_to_github` reads from `VOLUME_PATH` below, not from the driver-local `/tmp`
# MAGIC paths written above — the two steps run as SEPARATE Databricks Job Tasks (Phase 2 of the
# MAGIC "split 91 into a multi-task job" effort), which don't share a driver/`/tmp`. Writing to
# MAGIC `/tmp` first is still necessary (pandas'/pyarrow's parquet writers need a real local
# MAGIC filesystem path), this just also copies the result to a Unity Catalog Volume — a real
# MAGIC cross-task-readable location — right after.
# MAGIC
# MAGIC This copy was originally an opt-in `COPY_TO_VOLUME` flag for local fixture extraction
# MAGIC (`set True for local dev`); now unconditional (`main.financials._publish` — a managed
# MAGIC Volume, created 2026-08 specifically for this) since `52` depends on it every run, not
# MAGIC just when a developer happens to want it.
# MAGIC
# MAGIC **Plain `shutil.copy`, not `dbutils.fs.cp`.** A real run against this workspace's
# MAGIC serverless/shared compute confirmed `dbutils.fs.cp("file:/tmp/...", ...)` raises
# MAGIC `LocalFilesystemAccessDeniedException: Cannot access non /Workspace local filesystem
# MAGIC path` — this workspace's shared-UC enforcement blocks `dbutils.fs` specifically from
# MAGIC touching driver-local paths outside `/Workspace`. A Volume is a real mounted POSIX path
# MAGIC on the driver (unlike DBFS, which does need `dbutils.fs`), so plain `pathlib`/`shutil`
# MAGIC read it and write it with no `dbutils.fs` permission check involved at all.

# COMMAND ----------

import shutil

VOLUME_PATH = Path("/Volumes/main/financials/_publish")

VOLUME_PATH.mkdir(parents=True, exist_ok=True)
for f in [DATA_PARQUET, METRIC_PARQUET, PRICE_PARQUET, BACKTEST_PARQUET, FX_PARQUET,
          FILINGS_PARQUET, FORECAST_PARQUET, META_JSON]:
    dest = VOLUME_PATH / f.name
    shutil.copy(f, dest)
    print(f"  ✓ {f.name} → {dest}")

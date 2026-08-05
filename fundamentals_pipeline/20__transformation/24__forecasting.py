# Databricks notebook source
# MAGIC %md
# MAGIC # 20__transformation / 24__forecasting
# MAGIC
# MAGIC Orchestrates the LightGBM quantile-regression Forecasting models
# MAGIC (`fundamentals_pipeline/forecasting.py`, issues #330/#331) for the full ticker
# MAGIC universe, each pipeline run: assembles the cross-sectional training panel, trains 15
# MAGIC quantile regressors + up to 15 binary loss classifiers, predicts each ticker's own
# MAGIC latest fiscal year forward for years 1-5, then blends years 6-10 by converging each
# MAGIC scenario's own growth rate toward the DCF model's existing terminal-growth assumption
# MAGIC (`00__config/valuation_assumptions.json`) — the same two-stage spirit as `23__intrinsic_value`,
# MAGIC not a new concept.
# MAGIC
# MAGIC **Writes to:** `{catalog}.{schema}.financials_forecast` — full overwrite each run (this
# MAGIC table is always fully recomputed from scratch, same pattern as `15__fetch_sec_filings.py`'s
# MAGIC `sec_filings` and `23__intrinsic_value.py`'s own tables — no incremental MERGE, no
# MAGIC `force_full_refresh` branching).
# MAGIC
# MAGIC **Also writes (section 5b):** PV-discounted forward P/E / FCF Yield (issue #334's
# MAGIC `fundamentals_pipeline.valuation.forward_pe`/`forward_fcf_yield`, wired in here so
# MAGIC `fundamentals_screener` has something to read — issue #334 itself only added the pure
# MAGIC functions) — appended to the same `financials_forecast` table as `forward_pe`/
# MAGIC `forward_fcf_yield` `metric` rows, discounted at each quantile's own interpolated WACC.
# MAGIC
# MAGIC **Out of scope (per issue #332):** touching `23__intrinsic_value.py`'s existing scenario
# MAGIC math or `valuation_assumptions.json`'s existing bull/mid/bear DCF parameters (only *read*
# MAGIC here), publishing the `dashboard_forecast` artifact (issue #333), any
# MAGIC `fundamentals_screener`/UI change.

# COMMAND ----------

# MAGIC %run "../00__config/01__tickers"

# COMMAND ----------

# Inherit ACTIVE_TICKERS from the parent orchestrator when available (same idiom
# 15__fetch_sec_filings.py / 11__fetch_sec_xbrl.py already use) — else derive fresh for a
# standalone run of this notebook.
if "ACTIVE_TICKERS" not in globals() or not ACTIVE_TICKERS:
    ACTIVE_TICKERS = [row.ticker for row in spark.table(f"{CATALOG}.config.tickers").select("ticker").collect()]
    print(f"✓ Config loaded — {len(ACTIVE_TICKERS)} active tickers from {CATALOG}.config.tickers")
else:
    print(f"✓ Inherited {len(ACTIVE_TICKERS)} tickers from parent (override mode)")

# COMMAND ----------

# MAGIC %md ### ⚠️ Extra dependency
# MAGIC `lightgbm` isn't preinstalled on serverless (confirmed via issue #329's Phase 0 audit,
# MAGIC which verified it installs cleanly) — installed here, scoped to this stage, not in
# MAGIC `91__full_pipeline.py`'s shared session cell (same pattern as
# MAGIC `12__fetch_market_data.py`'s `%pip install yfinance` /
# MAGIC `14__fetch_oracle_statements.py`'s `%pip install lxml`).

# COMMAND ----------

# MAGIC %pip install lightgbm

# COMMAND ----------

import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from fundamentals_pipeline import forecasting as fc
from fundamentals_pipeline import valuation as fpv

# ── Paths & table names ──────────────────────────────────────────────────────
ASSUMPTIONS_JSON_PATH = "../00__config/valuation_assumptions.json"

financials_tbl = f"{CATALOG}.{SCHEMA}.financials"
financials_raw_tbl = f"{CATALOG}.{SCHEMA}.financials_raw"
metrics_tbl = f"{CATALOG}.{SCHEMA}.financials_metrics"
market_tbl = f"{CATALOG}.{SCHEMA}.market_cap_asof"
tickers_tbl = f"{CATALOG}.config.tickers"
forecast_tbl = f"{CATALOG}.{SCHEMA}.financials_forecast"
failures_tbl = f"{CATALOG}.{SCHEMA}.ingestion_failures"

EXPLICIT_HORIZONS = (1, 2, 3, 4, 5)
TERMINAL_YEARS = 5  # years 6-10

print(f"Financials source : {financials_tbl}")
print(f"Metrics source     : {metrics_tbl}")
print(f"Market cap source  : {market_tbl}")
print(f"Tickers source     : {tickers_tbl}")
print(f"Target             : {forecast_tbl}")

# COMMAND ----------

# MAGIC %md ## 1. Resolve growth_terminal per ticker (bull/mid/bear), mirroring 23__intrinsic_value.py
# MAGIC
# MAGIC Self-contained copy of `23`'s already-proven override-merge logic (notebook stages don't
# MAGIC cross-import — see `13__fetch_dimensional_10k.py`'s established pattern) — used here only
# MAGIC to resolve `growth_terminal`, never touching `23`'s own scenario math.

# COMMAND ----------

def _load_assumptions(path: str) -> dict:
    """JSON loader tolerant of // comment lines and discardable _xxx keys."""
    raw = Path(path).read_text(encoding="utf-8")
    lines = [line for line in raw.splitlines() if not line.strip().startswith("//")]
    data = json.loads("\n".join(lines))

    def _clean(obj):
        if isinstance(obj, dict):
            return {k: _clean(v) for k, v in obj.items() if not k.startswith("_")}
        if isinstance(obj, list):
            return [_clean(x) for x in obj]
        return obj

    return _clean(data)


ASSUMPTIONS = _load_assumptions(ASSUMPTIONS_JSON_PATH)
SCENARIOS = ASSUMPTIONS["scenarios"]
OVERRIDES = ASSUMPTIONS.get("overrides", {})


def _is_scenario_leaf(v) -> bool:
    return isinstance(v, dict) and len(v) > 0 and set(v.keys()) <= {"bull", "mid", "bear"}


def _merge_scenario_aware(base: dict, over: dict, scenario: str) -> dict:
    out = {k: (dict(v) if isinstance(v, dict) else v) for k, v in base.items()}
    for k, v in over.items():
        if _is_scenario_leaf(v):
            out[k] = v.get(scenario, out.get(k))
        elif isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _merge_scenario_aware(out[k], v, scenario)
        else:
            out[k] = v
    return out


def _growth_terminal_for(ticker: str, scenario: str) -> float:
    merged = _merge_scenario_aware(SCENARIOS[scenario], OVERRIDES.get(ticker, {}), scenario)
    return float(merged["dcf"]["growth_terminal"])


# Precomputed once per ticker (2,600+ tickers x 3 scenarios) rather than re-merging on every
# (ticker, metric, quantile) row in the terminal-blend step below.
TERMINAL_GROWTH_BY_TICKER: dict[str, dict[str, float]] = {
    ticker: {
        "bear": _growth_terminal_for(ticker, "bear"),
        "mid": _growth_terminal_for(ticker, "mid"),
        "bull": _growth_terminal_for(ticker, "bull"),
    }
    for ticker in ACTIVE_TICKERS
}
print(f"✓ Resolved growth_terminal (bull/mid/bear) for {len(TERMINAL_GROWTH_BY_TICKER):,} tickers")


def _wacc_for(ticker: str, scenario: str) -> float:
    merged = _merge_scenario_aware(SCENARIOS[scenario], OVERRIDES.get(ticker, {}), scenario)
    return float(merged["dcf"]["wacc"])


# Same precompute shape as TERMINAL_GROWTH_BY_TICKER above, for the forward P/E / FCF Yield
# step (section 5b) -- each scenario's own WACC, resolved once per ticker rather than
# re-merging per (ticker, metric, quantile) row.
WACC_BY_TICKER: dict[str, dict[str, float]] = {
    ticker: {
        "bear": _wacc_for(ticker, "bear"),
        "mid": _wacc_for(ticker, "mid"),
        "bull": _wacc_for(ticker, "bull"),
    }
    for ticker in ACTIVE_TICKERS
}
print(f"✓ Resolved WACC (bull/mid/bear) for {len(WACC_BY_TICKER):,} tickers")

# COMMAND ----------

# MAGIC %md ## 2. Read inputs, assemble the training panel

# COMMAND ----------

_tickers_scope = F.col("ticker").isin(ACTIVE_TICKERS)

# Revenue, Net Income (build_training_panel's `financials` param) and Depreciation &
# Amortization (a raw ingested concept per 01__tickers.py's concept map — NOT published in
# financials_metrics, confirmed via 22__derived_metrics.py's own base_metric_cols list, which
# only passes through CapEx/Operating Cash Flow/Free Cash Flow, not D&A).
_financials_pdf = (
    spark.table(financials_tbl)
    .filter(_tickers_scope)
    .filter(F.col("period_type") == "FY")
    .filter(F.col("concept").isin("Revenue", "Net Income"))
    .select("ticker", "fiscal_year", F.col("concept"), "value")
    .toPandas()
)

_dna_pdf = (
    spark.table(financials_tbl)
    .filter(_tickers_scope)
    .filter(F.col("period_type") == "FY")
    .filter(F.col("concept") == "Depreciation & Amortization")
    .select("ticker", "fiscal_year", F.col("concept").alias("metric"), "value")
    .toPandas()
)

# financials_metrics has no period_type column at all (it only ever stores FY-basis metrics —
# confirmed via its own CREATE TABLE schema: ticker, company, fiscal_year, metric, value).
_metrics_pdf = (
    spark.table(metrics_tbl)
    .filter(_tickers_scope)
    .filter(F.col("metric").isin(
        "Free Cash Flow", "Gross Margin %", "Operating Margin %", "Net Margin %", "ROE %",
        "Debt / Equity", "Debt / Assets", "Net Debt / EBITDA", "CapEx",
    ))
    .select("ticker", "fiscal_year", "metric", "value")
    .toPandas()
)
_metrics_pdf = pd.concat([_metrics_pdf, _dna_pdf], ignore_index=True)

_tickers_pdf = (
    spark.table(tickers_tbl).filter(_tickers_scope)
    .select("ticker", "sector", "industry").toPandas()
)

_market_cap_pdf = (
    spark.table(market_tbl).filter(_tickers_scope)
    .select("ticker", "fiscal_year", "market_cap").toPandas()
)

# main.financials.financials has no `filed` column (only period_end) -- filed lives in the
# append-only financials_raw audit table. MIN(filed), not MAX, for the conservative "earliest
# this figure was actually knowable" as-of date -- never a later restatement date (mirrors
# 21__clean_and_merge.py's own dedup-ordering use of `filed`).
_filed_pdf = (
    spark.table(financials_raw_tbl)
    .filter(_tickers_scope)
    .filter(F.col("fp") == "FY")
    .filter(F.col("concept").isin("Revenue", "Net Income"))
    .groupBy("ticker", F.col("fy").alias("fiscal_year"))
    .agg(F.min("filed").alias("filed"), F.max("period_end").alias("period_end"))
    .toPandas()
)

panel = fc.build_training_panel(
    financials=_financials_pdf, metrics=_metrics_pdf, tickers=_tickers_pdf,
    market_cap=_market_cap_pdf, filed_dates=_filed_pdf, horizons=(*EXPLICIT_HORIZONS,),
)
print(f"✓ Training panel assembled: {len(panel):,} rows ({panel['ticker'].nunique():,} tickers)")

# COMMAND ----------

# MAGIC %md ## 3. Train models (with a walk-forward validation split for early stopping)

# COMMAND ----------

# A held-out expanding-window slice for early stopping — never a random split (see
# forecasting.expanding_window_split's own no-look-ahead rationale). The cutoff is this run's
# panel's own as_of_date distribution's 85th percentile: the most recent ~15% of rows (by
# as-of-date) validate, everything earlier trains.
_as_of_dates = panel["as_of_date"].dropna().sort_values()
if len(_as_of_dates) >= 20:
    _cutoff = _as_of_dates.iloc[int(len(_as_of_dates) * 0.85)]
    train_panel, validation_panel = fc.expanding_window_split(panel, "as_of_date", _cutoff)
    print(f"✓ Validation split at {_cutoff} — {len(train_panel):,} train / {len(validation_panel):,} validation rows")
else:
    # Too little as-of-date spread this run (e.g. a tiny tickers_override smoke test) for a
    # meaningful held-out slice -- train on everything, no early stopping, same as omitting
    # validation_panel entirely.
    train_panel, validation_panel = panel, None
    print("⚠ Too few distinct as_of_date values for a validation split — training without early stopping")

quantile_models = fc.train_quantile_models(train_panel, validation_panel=validation_panel)
print(f"✓ Trained {len(quantile_models)} quantile regressors")

classifier_models = fc.train_loss_classifiers(train_panel, validation_panel=validation_panel)
print(f"✓ Trained {len(classifier_models)} loss classifiers (of up to "
      f"{len(fc.TARGET_METRICS) * len(EXPLICIT_HORIZONS)})")

# COMMAND ----------

# MAGIC %md ## 4. Predict years 1-5 for each ticker's own latest fiscal year

# COMMAND ----------

panel["_ticker_latest_fy"] = panel.groupby("ticker")["fiscal_year"].transform("max")
latest_panel = panel[panel["fiscal_year"] == panel["_ticker_latest_fy"]].drop(columns=["_ticker_latest_fy"])

explicit_forecasts = fc.predict_forecast(latest_panel, quantile_models, classifier_models)
print(f"✓ Years 1-5 forecast: {len(explicit_forecasts):,} rows")

# COMMAND ----------

# MAGIC %md ## 5. Blend years 6-10 toward each scenario's own terminal growth rate

# COMMAND ----------

# Each ticker's own base-year value per target metric (the row's own current value at its
# latest fiscal_year, identical across every horizon in that ticker's slice of `latest_panel`).
_base_values = (
    latest_panel.drop_duplicates("ticker")
    .set_index("ticker")[list(fc.TARGET_METRICS.values())]
)

_horizon5 = explicit_forecasts[explicit_forecasts["horizon"] == 5]

_terminal_rows: list[dict] = []
_failed_terminal: list[dict] = []
for row in _horizon5.itertuples(index=False):
    try:
        value_from = _base_values.loc[row.ticker, fc.TARGET_METRICS[row.metric]]
        terminal = TERMINAL_GROWTH_BY_TICKER.get(row.ticker)
        if terminal is None:
            raise KeyError(f"no growth_terminal resolved for {row.ticker!r}")
        terminal_growth = fc.terminal_growth_for_quantile(
            row.quantile_level, bear_terminal=terminal["bear"],
            mid_terminal=terminal["mid"], bull_terminal=terminal["bull"],
        )
        blended = fc.blend_terminal_years_from_values(
            value_from, row.forecast_value, terminal_growth, terminal_years=TERMINAL_YEARS,
        )
        if blended is None:
            continue  # no meaningful exit CAGR (non-positive base) -- skip, don't fabricate
        for step, value in enumerate(blended, start=1):
            _terminal_rows.append({
                "ticker": row.ticker, "fiscal_year": row.fiscal_year, "horizon": 5 + step,
                "metric": row.metric, "quantile_level": row.quantile_level,
                "forecast_value": value,
            })
    except Exception as e:  # noqa: BLE001 -- one ticker's terminal-blend failure must not abort the run
        _failed_terminal.append({
            "ticker": row.ticker,
            "error": {"error_type": "terminal_blend_failed", "error_message": str(e)[:500]},
        })

terminal_forecasts = pd.DataFrame(_terminal_rows) if _terminal_rows else pd.DataFrame(
    columns=["ticker", "fiscal_year", "horizon", "metric", "quantile_level", "forecast_value"]
)
print(f"✓ Years 6-10 forecast: {len(terminal_forecasts):,} rows"
      + (f" ({len(_failed_terminal)} ticker/metric/quantile combo(s) failed)" if _failed_terminal else ""))

# COMMAND ----------

# MAGIC %md ## 5b. Forward P/E / FCF Yield (issue #334's PV-discounted multiples, wired in)
# MAGIC
# MAGIC Reuses `fundamentals_pipeline.valuation.forward_pe`/`forward_fcf_yield` (issue #334) --
# MAGIC discounts each `(ticker, horizon, quantile_level)` Net Income / Free Cash Flow forecast
# MAGIC back to present value at that quantile's own interpolated WACC (via
# MAGIC `terminal_growth_for_quantile`'s generic 3-anchor linear interpolation across
# MAGIC bear/mid/bull -- the same function the terminal-growth blend above already uses; it
# MAGIC isn't specific to growth rates), against each ticker's own latest-FY market cap. Appends
# MAGIC `forward_pe`/`forward_fcf_yield` `metric` rows to the same long-format table -- no new
# MAGIC artifact/schema needed (`metric` is already a free-text column).

# COMMAND ----------

all_forecasts = pd.concat([explicit_forecasts, terminal_forecasts], ignore_index=True)

# Each ticker's own market cap as of its latest fiscal year (same year latest_panel/
# _base_values key off) -- a ticker with no market_cap_asof row for that year is skipped
# below, never fabricated.
_latest_fy_by_ticker = latest_panel.drop_duplicates("ticker")[["ticker", "fiscal_year"]]
_current_market_cap_pdf = _market_cap_pdf.merge(_latest_fy_by_ticker, on=["ticker", "fiscal_year"], how="inner")
MARKET_CAP_BY_TICKER: dict[str, float] = dict(
    zip(_current_market_cap_pdf["ticker"], _current_market_cap_pdf["market_cap"], strict=True)
)

_multiple_rows: list[dict] = []
_failed_multiples: list[dict] = []
for row in all_forecasts.itertuples(index=False):
    if row.metric not in ("net_income", "free_cash_flow"):
        continue
    try:
        market_cap = MARKET_CAP_BY_TICKER.get(row.ticker)
        wacc_by_scenario = WACC_BY_TICKER.get(row.ticker)
        if market_cap is None or wacc_by_scenario is None:
            continue  # no market cap / no resolved assumptions -- skip, don't fabricate
        wacc = fc.terminal_growth_for_quantile(
            row.quantile_level, bear_terminal=wacc_by_scenario["bear"],
            mid_terminal=wacc_by_scenario["mid"], bull_terminal=wacc_by_scenario["bull"],
        )
        if row.metric == "net_income":
            value = fpv.forward_pe(market_cap, row.forecast_value, wacc, row.horizon)
            out_metric = "forward_pe"
        else:
            value = fpv.forward_fcf_yield(market_cap, row.forecast_value, wacc, row.horizon)
            out_metric = "forward_fcf_yield"
        if value is None:
            continue  # ungated (e.g. non-positive PV earnings for forward_pe) -- skip
        _multiple_rows.append({
            "ticker": row.ticker, "fiscal_year": row.fiscal_year, "horizon": row.horizon,
            "metric": out_metric, "quantile_level": row.quantile_level, "forecast_value": value,
        })
    except Exception as e:  # noqa: BLE001 -- one row's forward-multiple failure must not abort the run
        _failed_multiples.append({
            "ticker": row.ticker,
            "error": {"error_type": "forward_multiple_failed", "error_message": str(e)[:500]},
        })

forward_multiples = pd.DataFrame(_multiple_rows) if _multiple_rows else pd.DataFrame(
    columns=["ticker", "fiscal_year", "horizon", "metric", "quantile_level", "forecast_value"]
)
all_forecasts = pd.concat([all_forecasts, forward_multiples], ignore_index=True)
print(f"✓ Forward P/E / FCF Yield: {len(forward_multiples):,} rows"
      + (f" ({len(_failed_multiples)} row(s) failed)" if _failed_multiples else ""))

# COMMAND ----------

# MAGIC %md ## 6. Write financials_forecast (full overwrite)

# COMMAND ----------

_forecast_schema = StructType([
    StructField("ticker", StringType(), False),
    StructField("fiscal_year", IntegerType(), False),
    StructField("horizon", IntegerType(), False),
    StructField("metric", StringType(), False),
    StructField("quantile_level", DoubleType(), False),
    StructField("forecast_value", DoubleType(), True),
])

if len(all_forecasts):
    (spark.createDataFrame(all_forecasts, schema=_forecast_schema)
     .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(forecast_tbl))
    print(f"✓ wrote {len(all_forecasts):,} rows → {forecast_tbl}")
else:
    print("⚠ No forecast rows produced this run — leaving any existing financials_forecast table untouched")

# COMMAND ----------

# MAGIC %md ## 7. Log failures

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {failures_tbl} (
        ticker         STRING    NOT NULL,
        error_type     STRING    NOT NULL,
        error_message  STRING,
        step           STRING    NOT NULL,
        scraped_at     TIMESTAMP NOT NULL
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

_all_failures = _failed_terminal + _failed_multiples
if _all_failures:
    _scraped_at = datetime.utcnow()
    _fail_schema = StructType([
        StructField("ticker", StringType(), False),
        StructField("error_type", StringType(), False),
        StructField("error_message", StringType(), True),
        StructField("step", StringType(), False),
        StructField("scraped_at", TimestampType(), False),
    ])
    _fail_records = [{
        "ticker": f["ticker"],
        "error_type": f["error"]["error_type"],
        "error_message": f["error"]["error_message"],
        "step": "forecasting",
        "scraped_at": _scraped_at,
    } for f in _all_failures]
    (spark.createDataFrame(_fail_records, schema=_fail_schema)
     .write.mode("append").saveAsTable(failures_tbl))
    print(f"✓ Logged {len(_fail_records):,} failure(s) to {failures_tbl}")
else:
    print("✓ No failures to log")

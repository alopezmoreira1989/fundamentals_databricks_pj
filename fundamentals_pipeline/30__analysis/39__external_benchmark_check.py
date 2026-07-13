# Databricks notebook source
# MAGIC %md
# MAGIC # 30__analysis / 39__external_benchmark_check  ⚠️ EXTERNAL benchmark validator
# MAGIC
# MAGIC A second **external** validator (the first is `35__reconcile_filings`, which checks
# MAGIC filing *fidelity* against the SEC linkbase oracle). This one checks *live-pricing
# MAGIC correctness*: it cross-references our published valuation metrics against **yfinance**
# MAGIC for a sample of tickers, to catch calculation bugs that only show up as a divergence
# MAGIC from an independent source — e.g. the MLI split-lag bug (#235), caught only because
# MAGIC someone happened to eyeball one ticker against Finviz.
# MAGIC
# MAGIC **The critical design constraint (see issue #236): separate valid methodological
# MAGIC divergence from a genuine error.** Our plain `"P/E"` / `"P/B"` / `"Market Cap"` metrics
# MAGIC are *deliberately* priced at each FY's own `period_end` close (`market_cap_asof`'s
# MAGIC fiscal-close design, documented in `web/templates/help.html`) — comparing these against
# MAGIC yfinance's live-price figure will "diverge" any time the stock has moved since that FY
# MAGIC closed, and that is NOT a bug. Our `"... (TTM, live)"` metrics (`23__intrinsic_value.py`
# MAGIC §5) are different: they are deliberately priced off TODAY's close, same as yfinance's
# MAGIC own fields, so a divergence there IS a real signal. `00__config/external_benchmark_config.json`
# MAGIC tags every checked metric with this `comparison_class`, and only `should_match_live`
# MAGIC metrics can ever be flagged `likely_bug` — `methodology_different` metrics are always
# MAGIC `info`, logged for trend visibility, never alarmed on.
# MAGIC
# MAGIC **Sample**: `is_favorite` tickers + `always_check` (JSON config, includes MLI as a
# MAGIC permanent regression case) + a stateless rotating slice of the rest, keyed off the ISO
# MAGIC week number (`hash(ticker) % NUM_BUCKETS == week % NUM_BUCKETS`) — cycles the full
# MAGIC universe over `NUM_BUCKETS` weeks with no persisted cursor.
# MAGIC
# MAGIC **Writes only `main.config.external_benchmark_findings`** (partitioned by `run_id`,
# MAGIC idempotent re-run of the same `run_id`). Read-only against `main.financials.*` /
# MAGIC `main.config.tickers`.
# MAGIC
# MAGIC > Databricks-only: uses `spark`, `%run`, network calls to yfinance.
# MAGIC > **Intentionally NOT wired into `91__full_pipeline.py`** — same precedent as
# MAGIC > `92__reconciliation_job.py`. Run ad-hoc, or wire into its own schedule later.

# COMMAND ----------

# MAGIC %run "../00__config/01__tickers"

# COMMAND ----------

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# %pip is unsupported when this notebook is pulled in via %run — same defensive subprocess
# install as 02__tickers_master.py's yfinance import. No-op where yfinance already ships.
try:
    import yfinance as yf  # noqa: F401
except ImportError:
    import subprocess
    import sys

    subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "yfinance"])
    import yfinance as yf  # noqa: F401

TICKERS_TBL  = TICKERS_TABLE
METRICS_TBL  = f"{CATALOG}.{SCHEMA}.financials_metrics"
FINDINGS     = f"{CATALOG}.config.external_benchmark_findings"
CONFIG_JSON  = "../00__config/external_benchmark_config.json"

# ── Sample sizing / tolerance ────────────────────────────────────────────────
NUM_BUCKETS         = 13    # weeks to cycle the full ~2,600-ticker universe (~1 quarter if run weekly)
PROBE_WORKERS       = 8     # matches 02__tickers_master.py's _INDUSTRY_PROBE_WORKERS — same flaky .info endpoint
LIVE_METRIC_TOL_PCT = 0.15  # should_match_live metrics: flag likely_bug beyond this rel-diff

checked_at     = datetime.now(timezone.utc)
checked_at_sql = checked_at.strftime("%Y-%m-%d %H:%M:%S")
run_id         = checked_at.strftime("%Y%m%d%H%M%S")
print(f"run_id = {run_id}  ({checked_at.isoformat(timespec='seconds')})")

# COMMAND ----------

# MAGIC %md ## 1. Load config + build the ticker sample

# COMMAND ----------

_config = json.loads(Path(CONFIG_JSON).read_text(encoding="utf-8"))
ALWAYS_CHECK = set(_config["always_check"])
METRIC_SPECS = [m for m in _config["metrics"]]  # [{our_metric, yfinance_field, comparison_class}, ...]
CHECKED_METRICS = [m["our_metric"] for m in METRIC_SPECS]
print(f"✓ Config loaded — {len(METRIC_SPECS)} metrics, {len(ALWAYS_CHECK)} always-check tickers")

tickers_pdf = spark.table(TICKERS_TBL).select("ticker", "is_favorite").toPandas()

_iso_week = checked_at.isocalendar()[1]
_current_bucket = _iso_week % NUM_BUCKETS


def _bucket(ticker: str) -> int:
    return int(hashlib.md5(ticker.encode()).hexdigest(), 16) % NUM_BUCKETS


tickers_pdf["_in_rotation"] = tickers_pdf["ticker"].apply(lambda t: _bucket(t) == _current_bucket)
sample_mask = (
    tickers_pdf["is_favorite"].fillna(False)
    | tickers_pdf["ticker"].isin(ALWAYS_CHECK)
    | tickers_pdf["_in_rotation"]
)
SAMPLE_TICKERS = sorted(tickers_pdf.loc[sample_mask, "ticker"].unique().tolist())
print(f"✓ Sample: {len(SAMPLE_TICKERS):,} tickers (bucket {_current_bucket}/{NUM_BUCKETS}, "
      f"ISO week {_iso_week}) — favorites + always_check + rotation")

# COMMAND ----------

# MAGIC %md ## 2. Our published metrics for the sample

# COMMAND ----------

_tickers_sql  = ", ".join(f"'{t}'" for t in SAMPLE_TICKERS)
_metrics_sql  = ", ".join(f"'{m}'" for m in CHECKED_METRICS)

our_pdf = spark.sql(f"""
    SELECT ticker, metric, fiscal_year, value
    FROM {METRICS_TBL}
    WHERE ticker IN ({_tickers_sql}) AND metric IN ({_metrics_sql}) AND value IS NOT NULL
""").toPandas()

# Latest fiscal_year per (ticker, metric) — mirrors the _latest_fy_metric "most recent row"
# pattern already used in views/overview.py. Most should_match_live metrics only ever have
# one row per ticker anyway (no multi-year TTM history), so this is a no-op for them.
our_pdf = (
    our_pdf.sort_values("fiscal_year", ascending=False)
    .drop_duplicates(subset=["ticker", "metric"], keep="first")
)
print(f"✓ Our metrics: {len(our_pdf):,} rows across {our_pdf['ticker'].nunique():,} tickers")

# COMMAND ----------

# MAGIC %md ## 3. yfinance fetch — threaded per-ticker `.info`

# COMMAND ----------

_YF_FIELDS = sorted({m["yfinance_field"] for m in METRIC_SPECS})


def _probe_yf(ticker: str) -> dict:
    """One ticker's yfinance fields from a SINGLE `.info` call. None on any error/missing field —
    same defensive shape as 02__tickers_master.py's _probe_company_info (the flakiest yfinance
    endpoint, hence threaded + graceful None rather than raising)."""
    blank = {f: None for f in _YF_FIELDS}
    try:
        info = yf.Ticker(ticker).info or {}
    except Exception:
        return blank
    return {f: info.get(f) for f in _YF_FIELDS}


with ThreadPoolExecutor(max_workers=PROBE_WORKERS) as ex:
    _yf_results = dict(ex.map(lambda t: (t, _probe_yf(t)), SAMPLE_TICKERS))

yf_pdf = pd.DataFrame.from_dict(_yf_results, orient="index").reset_index(names="ticker")
_yf_hit_cells = sum(yf_pdf[f].notna().sum() for f in _YF_FIELDS if f in yf_pdf.columns)
_yf_total_cells = len(SAMPLE_TICKERS) * len(_YF_FIELDS)
print(f"✓ yfinance probe: {len(SAMPLE_TICKERS):,} tickers queried, "
      f"{_yf_hit_cells:,}/{_yf_total_cells:,} fields resolved ({_yf_hit_cells / max(_yf_total_cells, 1):.1%})")

# COMMAND ----------

# MAGIC %md ## 4. Compare + classify
# MAGIC
# MAGIC `comparison_class` (from config) decides whether a divergence CAN be flagged at all —
# MAGIC see the module docstring. Missing data on either side is its own `data_gap` bucket,
# MAGIC never conflated with a value mismatch.

# COMMAND ----------

_metric_by_name = {m["our_metric"]: m for m in METRIC_SPECS}
_yf_by_ticker = yf_pdf.set_index("ticker").to_dict(orient="index")

findings = []
for _, row in our_pdf.iterrows():
    ticker, metric, fiscal_year, our_value = row["ticker"], row["metric"], row["fiscal_year"], row["value"]
    spec = _metric_by_name[metric]
    yf_field = spec["yfinance_field"]
    comparison_class = spec["comparison_class"]
    external_value = (_yf_by_ticker.get(ticker) or {}).get(yf_field)

    if our_value is None or external_value is None:
        severity = "data_gap"
        abs_diff = rel_diff = None
        notes = "yfinance field missing/None" if external_value is None else "our metric missing"
    else:
        abs_diff = abs(our_value - external_value)
        rel_diff = abs_diff / abs(external_value) if external_value != 0 else None
        if comparison_class == "methodology_different":
            severity = "info"
            notes = "expected divergence (different pricing basis) — not a bug signal"
        elif rel_diff is not None and rel_diff > LIVE_METRIC_TOL_PCT:
            severity = "likely_bug"
            notes = f"rel_diff {rel_diff:.1%} exceeds {LIVE_METRIC_TOL_PCT:.0%} tolerance"
        else:
            severity = "ok"
            notes = ""

    findings.append({
        "run_id": run_id, "checked_at": checked_at_sql, "ticker": ticker,
        "our_metric": metric, "yfinance_field": yf_field, "comparison_class": comparison_class,
        "our_value": our_value, "external_value": external_value,
        "abs_diff": abs_diff, "rel_diff": rel_diff, "tolerance_pct": LIVE_METRIC_TOL_PCT,
        "severity": severity, "notes": notes,
    })

findings_pdf = pd.DataFrame(findings)
print(f"✓ {len(findings_pdf):,} findings this run")
if len(findings_pdf):
    print(findings_pdf.groupby("severity").size().to_string())

# COMMAND ----------

# MAGIC %md ## 5. Write findings

# COMMAND ----------

_DESIRED = [
    "run_id", "checked_at", "ticker", "our_metric", "yfinance_field", "comparison_class",
    "our_value", "external_value", "abs_diff", "rel_diff", "tolerance_pct", "severity", "notes",
]


def _table_exists(fqn: str) -> bool:
    try:
        spark.sql(f"SELECT 1 FROM {fqn} LIMIT 1").collect()
        return True
    except Exception:
        return False


if _table_exists(FINDINGS):
    if [f.name for f in spark.table(FINDINGS).schema.fields] != _DESIRED:
        print(f"⚠ schema drift on {FINDINGS} — dropping and recreating.")
        spark.sql(f"DROP TABLE {FINDINGS}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {FINDINGS} (
        run_id           STRING,
        checked_at       TIMESTAMP,
        ticker           STRING,
        our_metric       STRING,
        yfinance_field   STRING,
        comparison_class STRING,
        our_value        DOUBLE,
        external_value   DOUBLE,
        abs_diff         DOUBLE,
        rel_diff         DOUBLE,
        tolerance_pct    DOUBLE,
        severity         STRING,
        notes            STRING
    )
    USING DELTA
    PARTITIONED BY (run_id)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

findings_sdf = spark.createDataFrame(findings_pdf[_DESIRED]) if len(findings_pdf) else spark.createDataFrame([], schema=spark.table(FINDINGS).schema)
findings_sdf.write.mode("overwrite").option("replaceWhere", f"run_id = '{run_id}'").saveAsTable(FINDINGS)
print(f"✓ Wrote {len(findings_pdf):,} findings → {FINDINGS} (run_id={run_id})")

# COMMAND ----------

# MAGIC %md ## 6. Report

# COMMAND ----------

_flagged = findings_pdf[findings_pdf["severity"].isin(["likely_bug", "data_gap"])] if len(findings_pdf) else findings_pdf
print(f"{len(_flagged):,} rows needing review (likely_bug / data_gap) out of {len(findings_pdf):,} total")
if len(_flagged):
    display(spark.createDataFrame(_flagged[_DESIRED]).orderBy("severity", "ticker"))  # noqa: F821

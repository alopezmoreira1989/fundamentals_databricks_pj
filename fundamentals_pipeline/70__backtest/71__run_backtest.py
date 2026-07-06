# Databricks notebook source
# MAGIC %md
# MAGIC # 70__backtest / 71__run_backtest
# MAGIC
# MAGIC Applies named investor **archetypes** (`00__config/backtest_archetypes.json`) to historical
# MAGIC fundamentals and reports forward returns, **with no look-ahead bias**.
# MAGIC
# MAGIC **Method (annual, filing-date-driven):** for each archetype and each fiscal year `Y`, a
# MAGIC name is selected if its FY-`Y` metrics pass the archetype's predicates. A name's metrics
# MAGIC are only usable from their **as-of date** — the SEC 10-K `filed` date (from
# MAGIC `financials_raw`), or `period_end + as_of_lag_days` when the filing date is unavailable.
# MAGIC The name is **entered at the price on its as-of date** and **exited at its FY-(`Y`+1)
# MAGIC as-of date** (≈ a 1-year forward hold beginning only after the 10-K is public). The cohort
# MAGIC return for year `Y` is the equal-weight mean of holding returns; these chain into an equity
# MAGIC curve. Benchmark = SPY over each holding's own window (NULL if SPY is absent from the price
# MAGIC store).
# MAGIC
# MAGIC **⚠️ Survivorship bias:** the universe is tickers alive *today*. A point-in-time universe
# MAGIC is out of scope — results are biased **upward** (delisted losers never enter). The export
# MAGIC and the Streamlit view print this caveat.
# MAGIC
# MAGIC **Reuses** `fundamentals_pipeline/backtest.py` (as-of eligibility, predicate eval,
# MAGIC CAGR / max-drawdown / vol / Sharpe) — the SAME pure helpers the Streamlit view uses.
# MAGIC
# MAGIC **Primary output:** `{catalog}.{schema}.backtest_results` (equity-curve series) +
# MAGIC `{catalog}.{schema}.backtest_summary` (per-archetype metrics). Idempotent MERGE.
# MAGIC
# MAGIC **Databricks-only:** `spark`, `%run`, Unity Catalog three-part names.

# COMMAND ----------

# MAGIC %md ### ⚠️ Path note
# MAGIC `%run` paths are relative to this notebook's workspace location. If `CATALOG`/`SCHEMA`
# MAGIC come back undefined, adjust the path to point at `00__config/01__tickers`.

# COMMAND ----------

# MAGIC %run "../00__config/01__tickers"

# COMMAND ----------

import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T

# The pure backtest helpers ship in the installable `fundamentals_pipeline` package
# (fundamentals_pipeline/backtest.py). Installed once in 91__full_pipeline's session-
# dependencies cell, so this %run-included notebook imports it directly — no sys.path
# manipulation (see 51 for the same pattern).
from fundamentals_pipeline import backtest as bt

# ── Paths & table names ──────────────────────────────────────────────────────────
ARCHETYPES_JSON_PATH = "../00__config/backtest_archetypes.json"

metrics_tbl = f"{CATALOG}.{SCHEMA}.financials_metrics"
full_tbl    = f"{CATALOG}.{SCHEMA}.{TABLE}"
raw_tbl     = f"{CATALOG}.{SCHEMA}.financials_raw"
prices_tbl  = f"{CATALOG}.{SCHEMA}.market_prices_daily"
results_tbl = f"{CATALOG}.{SCHEMA}.backtest_results"
summary_tbl = f"{CATALOG}.{SCHEMA}.backtest_summary"

print(f"Metrics : {metrics_tbl}")
print(f"Prices  : {prices_tbl}")
print(f"Results : {results_tbl}")
print(f"Summary : {summary_tbl}")

# COMMAND ----------

# MAGIC %md ## 1. Load archetypes config

# COMMAND ----------

def _load_json(path: str) -> dict:
    """JSON loader tolerant of // comments and discardable _xxx keys (same idiom as 23)."""
    raw = Path(path).read_text(encoding="utf-8")
    lines = [ln for ln in raw.splitlines() if not ln.strip().startswith("//")]
    data = json.loads("\n".join(lines))

    def _clean(obj):
        if isinstance(obj, dict):
            return {k: _clean(v) for k, v in obj.items() if not k.startswith("_")}
        if isinstance(obj, list):
            return [_clean(x) for x in obj]
        return obj

    return _clean(data)


_CFG       = _load_json(ARCHETYPES_JSON_PATH)
CONFIG     = _CFG["config"]
ARCHETYPES = _CFG["archetypes"]
LAG_DAYS   = int(CONFIG.get("as_of_lag_days", 90))
RISK_FREE  = float(CONFIG.get("risk_free_rate", 0.04))
BENCHMARK  = CONFIG.get("benchmark", "SPY")

# Metrics referenced by ANY archetype (predicates + rank_by) — only these are pivoted.
NEEDED_METRICS = set()
for _a in ARCHETYPES.values():
    for _m, _, _ in _a["predicates"]:
        NEEDED_METRICS.add(_m)
    if _a.get("rank_by"):
        NEEDED_METRICS.add(_a["rank_by"][0])
NEEDED_METRICS = sorted(NEEDED_METRICS)

print(f"✓ {len(ARCHETYPES)} archetype(s): {list(ARCHETYPES)}")
print(f"  lag={LAG_DAYS}d, risk_free={RISK_FREE}, benchmark={BENCHMARK}")
print(f"  metrics needed: {NEEDED_METRICS}")

# COMMAND ----------

# MAGIC %md ## 2. Wide fundamentals + as-of date + entry price (Spark)
# MAGIC
# MAGIC `financials_metrics` (long) pivoted to one row per `(ticker, fiscal_year)`. The **as-of
# MAGIC date** is the 10-K `filed` date (from `financials_raw`) or `period_end + lag`. The
# MAGIC **entry price** is the latest `market_prices_daily` close on/before the as-of date
# MAGIC (same as-of pricing pattern as `22__derived_metrics`).

# COMMAND ----------

# Pivot the needed metrics wide.
wide = (
    spark.table(metrics_tbl)
    .filter(F.col("metric").isin(NEEDED_METRICS))
    .groupBy("ticker", "fiscal_year")
    .pivot("metric", NEEDED_METRICS)
    .agg(F.first("value"))
)

# FY fiscal close (period_end) per (ticker, fiscal_year).
fy_pe = (
    spark.table(full_tbl)
    .filter(F.col("period_type") == "FY")
    .groupBy("ticker", "fiscal_year")
    .agg(F.max("period_end").alias("period_end"))
)

# 10-K filing date per (ticker, fiscal_year). financials_raw.form like '10-K%'; fy = fiscal_year.
filed_10k = (
    spark.table(raw_tbl)
    .filter(F.col("form").startswith("10-K"))
    .groupBy("ticker", F.col("fy").alias("fiscal_year"))
    .agg(F.max("filed").alias("filed_10k"))
)

asof = (
    fy_pe.join(filed_10k, on=["ticker", "fiscal_year"], how="left")
    .withColumn(
        "as_of_date",
        F.coalesce(F.col("filed_10k"), F.date_add(F.col("period_end"), LAG_DAYS)),
    )
    .filter(F.col("as_of_date").isNotNull())
    .select("ticker", "fiscal_year", "as_of_date")
)

# Entry price: latest close on/before as_of_date. `asof` is ~30k rows → broadcast it so the
# ~75M-row daily price store isn't shuffled on `ticker`. Pick the latest row per
# (ticker, fiscal_year) with a partial-aggregable max-of-struct instead of a global
# row_number window (which would sort the whole exploded join just to keep rn==1). `date` is
# unique per ticker in the daily store, so max(struct(date, …)) is an unambiguous "latest".
prices = spark.table(prices_tbl).select("ticker", "date", "close", "adj_close")
_targets = F.broadcast(asof).join(prices, on="ticker").filter(F.col("date") <= F.col("as_of_date"))
entry = (
    _targets.groupBy("ticker", "fiscal_year")
    .agg(F.max(F.struct(F.col("date"), F.col("adj_close"), F.col("close"))).alias("_e"))
    .select(
        "ticker", "fiscal_year",
        F.col("_e.date").alias("entry_date"),
        F.col("_e.adj_close").alias("entry_adj"),
        F.col("_e.close").alias("entry_close"),
    )
)

base = (
    wide.join(asof, on=["ticker", "fiscal_year"], how="inner")
    .join(entry, on=["ticker", "fiscal_year"], how="inner")
)
pdf = base.toPandas()
print(f"✓ {len(pdf):,} priced (ticker, fiscal_year) rows")

# COMMAND ----------

# MAGIC %md ## 3. Forward returns + SPY benchmark (pandas)
# MAGIC
# MAGIC Exit = the SAME ticker's FY-(`Y`+1) entry (price + date). Benchmark = SPY over each
# MAGIC holding's own entry→exit window (as-of in pandas via `searchsorted`).

# COMMAND ----------

# Exit = next fiscal year's entry for the same ticker.
_nxt = pdf[["ticker", "fiscal_year", "entry_adj", "entry_date"]].rename(
    columns={"entry_adj": "exit_adj", "entry_date": "exit_date"}
)
_nxt["fiscal_year"] = _nxt["fiscal_year"] - 1   # attach FY Y+1's entry to row FY Y
pdf = pdf.merge(_nxt[["ticker", "fiscal_year", "exit_adj", "exit_date"]],
                on=["ticker", "fiscal_year"], how="left")
pdf["holding_return"] = pdf["exit_adj"] / pdf["entry_adj"] - 1.0

# Chronological-anomaly guard (#163): "next fiscal year's entry" assumes filing order tracks
# fiscal-year order, which a late or restated 10-K can break — FY Y's own as-of date can then
# land AFTER FY (Y+1)'s, making the "exit" predate the "entry". A backwards-dated pair isn't a
# measurable forward holding, so null it out (same treatment as a genuinely missing exit) rather
# than let it corrupt that cohort-year's return. Rare (~0.05% of rows historically); surfaced in
# the coverage/sanity report below.
_bad_order = pdf["exit_date"].notna() & (pdf["exit_date"] <= pdf["entry_date"])
if _bad_order.any():
    print(f"⚠ {int(_bad_order.sum())} row(s) with exit_date <= entry_date (late/restated 10-K) "
          f"— excluded from forward-return calc.")
    pdf.loc[_bad_order, "holding_return"] = np.nan

# SPY benchmark — daily series collected to the driver (one ticker, tiny), as-of via searchsorted.
_spy = (
    spark.table(prices_tbl).filter(F.col("ticker") == BENCHMARK)
    .select("date", "adj_close").toPandas().dropna(subset=["adj_close"]).sort_values("date")
)
HAS_BENCHMARK = len(_spy) > 0
if HAS_BENCHMARK:
    _spy_dates = pd.to_datetime(_spy["date"]).to_numpy()
    _spy_vals = _spy["adj_close"].to_numpy(dtype="float64")

    def _spy_asof(dates) -> np.ndarray:
        d = pd.to_datetime(pd.Series(dates)).to_numpy()
        idx = np.searchsorted(_spy_dates, d, side="right") - 1
        out = np.where(idx >= 0, _spy_vals[np.clip(idx, 0, len(_spy_vals) - 1)], np.nan)
        # NaT entry dates → NaN (searchsorted places NaT last; guard explicitly).
        out = np.where(pd.isna(d), np.nan, out)
        return out

    pdf["spy_entry"] = _spy_asof(pdf["entry_date"])
    pdf["spy_exit"] = _spy_asof(pdf["exit_date"])
    pdf["benchmark_return"] = pdf["spy_exit"] / pdf["spy_entry"] - 1.0
else:
    print(f"⚠ {BENCHMARK} not in {prices_tbl} — benchmark columns will be NULL.")
    pdf["benchmark_return"] = np.nan

print(f"✓ {pdf['holding_return'].notna().sum():,} rows with a measurable forward return")

# COMMAND ----------

# MAGIC %md ## 4. Build cohorts → equity curve + summary (reuses `_core.backtest`)

# COMMAND ----------

computed_at = datetime.utcnow()
series_rows: list[dict] = []
summary_rows: list[dict] = []


def _metric_dict(row) -> dict:
    return {m: (None if pd.isna(row[m]) else float(row[m])) for m in NEEDED_METRICS}


# The metric dicts are archetype-independent — build them ONCE (aligned to pdf row order)
# instead of rebuilding inside every archetype's pdf.apply (was ~8× redundant). Only the
# predicate evaluation below varies per archetype.
_metric_recs = [_metric_dict(rec) for rec in pdf.to_dict("records")]
_has_return = pdf["holding_return"].notna().to_numpy()

for name, a in ARCHETYPES.items():
    preds = a["predicates"]
    cap = int(a.get("max_holdings", 9999))
    rank_by = a.get("rank_by")

    # Predicate filter (pure _core.backtest.passes_predicates) + measurable forward return.
    mask = np.fromiter(
        (bt.passes_predicates(mr, preds) for mr in _metric_recs),
        dtype=bool, count=len(_metric_recs),
    )
    sel = pdf[mask & _has_return].copy()

    per_year = []
    for fy, grp in sel.groupby("fiscal_year"):
        g = grp
        if len(g) > cap:
            if rank_by:
                g = g.sort_values(rank_by[0], ascending=(rank_by[1] == "asc")).head(cap)
            else:
                g = g.sort_values("ticker").head(cap)
        port_ret = float(g["holding_return"].mean())
        _br = g["benchmark_return"]
        bench_ret = float(_br.mean()) if _br.notna().any() else None
        per_year.append((int(fy), len(g), port_ret, bench_ret))

    per_year.sort()
    if not per_year:
        print(f"  ⊘ {name}: no qualifying cohorts")
        continue

    # Equity curves (base 100) + the series rows.
    pv, bv = 100.0, 100.0
    equity = [100.0]
    port_returns, bench_returns = [], []
    for fy, n, pr, br in per_year:
        pv *= (1.0 + pr)
        equity.append(pv)
        port_returns.append(pr)
        bvalue = None
        if br is not None:
            bv *= (1.0 + br)
            bvalue = bv
            bench_returns.append(br)
        series_rows.append({
            "archetype": name, "fiscal_year": fy, "n_holdings": int(n),
            "portfolio_return": pr, "benchmark_return": br,
            "portfolio_value": pv, "benchmark_value": bvalue,
            "computed_at": computed_at,
        })

    n_years = len(per_year)
    bench_cagr = bt.cagr(100.0, bv, len(bench_returns)) if bench_returns else None
    port_cagr = bt.cagr(100.0, pv, n_years)
    summary_rows.append({
        "archetype": name,
        "cagr": port_cagr,
        "max_drawdown": bt.max_drawdown(equity),
        "annual_vol": bt.annualized_vol(port_returns),
        "sharpe": bt.sharpe(port_returns, RISK_FREE),
        "benchmark_cagr": bench_cagr,
        "excess_cagr": (port_cagr - bench_cagr) if (port_cagr is not None and bench_cagr is not None) else None,
        "start_year": per_year[0][0],
        "end_year": per_year[-1][0],
        "n_years": n_years,
        "computed_at": computed_at,
    })
    print(f"  ✓ {name}: {n_years} yrs, CAGR={port_cagr}, vs {BENCHMARK} CAGR={bench_cagr}")

# COMMAND ----------

# MAGIC %md ## 5. Write to Delta (MERGE — idempotent)

# COMMAND ----------

_results_schema = T.StructType([
    T.StructField("archetype",        T.StringType(),    False),
    T.StructField("fiscal_year",      T.IntegerType(),   False),
    T.StructField("n_holdings",       T.IntegerType(),   True),
    T.StructField("portfolio_return", T.DoubleType(),    True),
    T.StructField("benchmark_return", T.DoubleType(),    True),
    T.StructField("portfolio_value",  T.DoubleType(),    True),
    T.StructField("benchmark_value",  T.DoubleType(),    True),
    T.StructField("computed_at",      T.TimestampType(), True),
])
_summary_schema = T.StructType([
    T.StructField("archetype",      T.StringType(),    False),
    T.StructField("cagr",           T.DoubleType(),    True),
    T.StructField("max_drawdown",   T.DoubleType(),    True),
    T.StructField("annual_vol",     T.DoubleType(),    True),
    T.StructField("sharpe",         T.DoubleType(),    True),
    T.StructField("benchmark_cagr", T.DoubleType(),    True),
    T.StructField("excess_cagr",    T.DoubleType(),    True),
    T.StructField("start_year",     T.IntegerType(),   True),
    T.StructField("end_year",       T.IntegerType(),   True),
    T.StructField("n_years",        T.IntegerType(),   True),
    T.StructField("computed_at",    T.TimestampType(), True),
])

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {results_tbl} (
        archetype        STRING NOT NULL,
        fiscal_year      INT    NOT NULL,
        n_holdings       INT,
        portfolio_return DOUBLE,
        benchmark_return DOUBLE,
        portfolio_value  DOUBLE,
        benchmark_value  DOUBLE,
        computed_at      TIMESTAMP
    ) USING DELTA
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true', 'delta.autoOptimize.autoCompact'='true')
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {summary_tbl} (
        archetype      STRING NOT NULL,
        cagr           DOUBLE,
        max_drawdown   DOUBLE,
        annual_vol     DOUBLE,
        sharpe         DOUBLE,
        benchmark_cagr DOUBLE,
        excess_cagr    DOUBLE,
        start_year     INT,
        end_year       INT,
        n_years        INT,
        computed_at    TIMESTAMP
    ) USING DELTA
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true', 'delta.autoOptimize.autoCompact'='true')
""")

if series_rows:
    spark.createDataFrame(series_rows, schema=_results_schema).createOrReplaceTempView("incoming_bt_results")
    spark.sql(f"""
        MERGE INTO {results_tbl} AS t
        USING incoming_bt_results AS s
        ON t.archetype = s.archetype AND t.fiscal_year = s.fiscal_year
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    # Orphan cleanup: drop prior rows for archetypes recomputed this run but no longer present
    # for a year (e.g. a predicate change shrank the cohort history).
    _live_archetypes = ", ".join("'" + n.replace("'", "''") + "'" for n in ARCHETYPES)
    spark.sql(f"""
        MERGE INTO {results_tbl} AS t
        USING (
            SELECT t.archetype, t.fiscal_year FROM {results_tbl} t
            WHERE t.archetype IN ({_live_archetypes})
              AND NOT EXISTS (SELECT 1 FROM incoming_bt_results s
                              WHERE s.archetype = t.archetype AND s.fiscal_year = t.fiscal_year)
        ) AS s
        ON t.archetype = s.archetype AND t.fiscal_year = s.fiscal_year
        WHEN MATCHED THEN DELETE
    """)
    print(f"✓ {len(series_rows):,} series rows → {results_tbl}")

if summary_rows:
    spark.createDataFrame(summary_rows, schema=_summary_schema).createOrReplaceTempView("incoming_bt_summary")
    spark.sql(f"""
        MERGE INTO {summary_tbl} AS t
        USING incoming_bt_summary AS s
        ON t.archetype = s.archetype
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"✓ {len(summary_rows):,} summary rows → {summary_tbl}")

# COMMAND ----------

# MAGIC %md ## 6. Preview

# COMMAND ----------

if summary_rows:
    spark.sql(f"""
        SELECT archetype, ROUND(cagr*100,1) AS cagr_pct, ROUND(benchmark_cagr*100,1) AS bench_pct,
               ROUND(excess_cagr*100,1) AS excess_pct, ROUND(max_drawdown*100,1) AS maxdd_pct,
               ROUND(sharpe,2) AS sharpe, n_years, start_year, end_year
        FROM {summary_tbl}
        ORDER BY cagr DESC
    """).display()
else:
    print("⊘ No backtest results produced (empty universe or no qualifying cohorts).")

# COMMAND ----------

# MAGIC %md ## 7. Coverage + sanity report (non-fatal, #163)
# MAGIC
# MAGIC Read-only diagnostics over this run's own computed data — printed only, never raises (a
# MAGIC flag here means "look at this", not "abort the run"; `backtest_results`/`backtest_summary`
# MAGIC are already committed by this point).
# MAGIC
# MAGIC - **No-look-ahead, asserted on the real data** — `fundamentals_pipeline/backtest.py`'s
# MAGIC   `as_of_date`/`as_of_eligible`/`latest_price_asof` are unit-tested in isolation, but this
# MAGIC   Spark-native pipeline re-derives the same as-of/entry-price logic natively rather than
# MAGIC   calling them, so nothing previously tied the two together. This checks the actual `pdf`
# MAGIC   this run produced: every row's `entry_date <= as_of_date`, and `exit_date > entry_date`
# MAGIC   wherever an exit exists.
# MAGIC - **Coverage** — per archetype, years covered and cohort-size distribution.
# MAGIC - **Benchmark alignment** — the benchmark's own price-date range vs. the backtest's
# MAGIC   entry/exit window, and the NULL-rate of `benchmark_return` where a holding return exists.
# MAGIC - **Sanity bounds** — flags an archetype summary with an implausible CAGR/Sharpe/drawdown.

# COMMAND ----------

print("=" * 20 + " Coverage + sanity report " + "=" * 20)

# ── No-look-ahead invariant, asserted on the real computed data ──────────────────────────
_bad_asof = pdf[pdf["entry_date"] > pdf["as_of_date"]]
if len(_bad_asof):
    print(f"🚩 NO-LOOK-AHEAD VIOLATION: {len(_bad_asof)} row(s) priced AFTER their as-of date:")
    print(_bad_asof[["ticker", "fiscal_year", "entry_date", "as_of_date"]].head(5).to_string(index=False))
else:
    print(f"✓ No-look-ahead: entry_date <= as_of_date holds for all {len(pdf):,} priced rows.")

_has_exit = pdf["exit_date"].notna()
_bad_exit = pdf[_has_exit & (pdf["exit_date"] <= pdf["entry_date"])]
if len(_bad_exit):
    print(f"🚩 EXIT-BEFORE-ENTRY: {len(_bad_exit)} row(s) with exit_date <= entry_date:")
    print(_bad_exit[["ticker", "fiscal_year", "entry_date", "exit_date"]].head(5).to_string(index=False))
else:
    print(f"✓ Exit-after-entry holds for all {int(_has_exit.sum()):,} rows with a measurable exit.")

# ── Coverage per archetype ────────────────────────────────────────────────────────────────
print("\n-- Coverage per archetype --")
_series_by_archetype: dict[str, list[dict]] = {}
for _r in series_rows:
    _series_by_archetype.setdefault(_r["archetype"], []).append(_r)

for _name in ARCHETYPES:
    _rows = _series_by_archetype.get(_name, [])
    if not _rows:
        print(f"  {_name}: no qualifying cohorts")
        continue
    _sizes = [_r["n_holdings"] for _r in _rows]
    print(f"  {_name}: {len(_rows)} yr(s) [{_rows[0]['fiscal_year']}-{_rows[-1]['fiscal_year']}], "
          f"n_holdings min={min(_sizes)} max={max(_sizes)} avg={sum(_sizes) / len(_sizes):.1f}")

# ── Benchmark alignment ────────────────────────────────────────────────────────────────────
print("\n-- Benchmark alignment --")
if not HAS_BENCHMARK:
    print(f"  ⊘ {BENCHMARK} absent from {prices_tbl} — all benchmark columns are NULL by design.")
else:
    _spy_min_d, _spy_max_d = _spy["date"].min(), _spy["date"].max()
    # exit_date is object-dtype with a NaT/NaN mix (rows with no next-FY exit yet) — dropna()
    # before max()/min(), since comparing a bare NaN float against date objects raises.
    _bt_min_d, _bt_max_d = pdf["entry_date"].min(), pdf["exit_date"].dropna().max()
    print(f"  {BENCHMARK} price coverage: {_spy_min_d} .. {_spy_max_d}")
    print(f"  Backtest date span:        {_bt_min_d} .. {_bt_max_d}")
    if pd.notna(_bt_min_d) and pd.Timestamp(_bt_min_d) < pd.Timestamp(_spy_min_d):
        print(f"  ⚠ Backtest entries start before {BENCHMARK}'s own price history — earliest "
              f"years' benchmark_return will be NULL.")
    if pd.notna(_bt_max_d) and pd.Timestamp(_bt_max_d) > pd.Timestamp(_spy_max_d):
        print(f"  ⚠ Backtest exits extend past {BENCHMARK}'s latest priced date — most recent "
              f"year's benchmark_return will be NULL.")
    _bench_null_rate = pdf.loc[pdf["holding_return"].notna(), "benchmark_return"].isna().mean()
    print(f"  benchmark_return NULL rate (of rows with a holding_return): {_bench_null_rate:.1%}")

# ── Summary sanity bounds (non-fatal) ──────────────────────────────────────────────────────
print("\n-- Summary sanity bounds --")
_CAGR_BOUND, _SHARPE_BOUND, _MDD_BOUND = 1.0, 5.0, -0.95   # |100%| CAGR, |5| Sharpe, -95% drawdown
_flagged = False
for _row in summary_rows:
    _issues = []
    if _row["cagr"] is not None and abs(_row["cagr"]) > _CAGR_BOUND:
        _issues.append(f"CAGR={_row['cagr']:.1%}")
    if _row["sharpe"] is not None and abs(_row["sharpe"]) > _SHARPE_BOUND:
        _issues.append(f"Sharpe={_row['sharpe']:.2f}")
    if _row["max_drawdown"] is not None and _row["max_drawdown"] < _MDD_BOUND:
        _issues.append(f"MaxDD={_row['max_drawdown']:.1%}")
    if _issues:
        _flagged = True
        print(f"  ⚠ {_row['archetype']}: implausible {', '.join(_issues)} — review predicates/data.")
if not _flagged:
    print("  ✓ All archetype summary metrics within plausible bounds.")

print("=" * 67)

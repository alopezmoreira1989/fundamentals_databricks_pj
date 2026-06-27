# Databricks notebook source
# MAGIC %md
# MAGIC # 30_analysis / 37__split_adjust_check  — split-adjust valuation validator (read-only)
# MAGIC
# MAGIC Post-run validation for the split-adjust fix (PR #90, `8b15348`). Confirms that the new
# MAGIC `main.financials.stock_splits` store landed and that the cross-year per-share computations
# MAGIC it feeds — **EPS-CAGR → Graham Revised**, **Net Buyback Yield %**, **Piotroski no-dilution** —
# MAGIC no longer read a stock split as a real collapse/dilution.
# MAGIC
# MAGIC The fix rescales ONLY cross-year inputs: `factor(period_end) = ∏ ratio for split_date >
# MAGIC period_end`; `EPS_adj = EPS/factor`, `Shares_adj = Shares×factor`. The most recent period
# MAGIC has `factor = 1`, so **current valuations are unchanged** — §5 asserts that invariant.
# MAGIC Per-row IVs / price comparisons / Margin of Safety stay on own-period basis; market cap /
# MAGIC P-E / Earnings Yield are untouched (raw close × raw as-of shares).
# MAGIC
# MAGIC **Read-only** against `main.financials.*`. No writes. **Databricks-only** (`spark`, `%run`).
# MAGIC
# MAGIC **Non-raising by design** — `91` runs this inline via `%run` (shared session, serverless-safe,
# MAGIC no child-notebook stall), so it cannot be wrapped in try/except. Instead of `assert`, each
# MAGIC check records into `_issues`; the module-level **`SPLIT_ADJUST_OK`** boolean is the verdict
# MAGIC `91` reads for its step status. Run standalone too — the summary prints PASS / FAIL.
# MAGIC
# MAGIC Symptom baseline (pre-fix, for eyeballing §2): NVDA Graham Revised FY2024 ≈ 319,
# MAGIC MoS% FY2025 ≈ −613%, Net Buyback Yield % spiking ≈ +900% on the 10:1.

# COMMAND ----------

# MAGIC %run "../00_config/01__tickers"

# COMMAND ----------

from pyspark.sql import functions as F

FIN      = f"{CATALOG}.{SCHEMA}.financials"
METRICS  = f"{CATALOG}.{SCHEMA}.financials_metrics"
IV       = f"{CATALOG}.{SCHEMA}.financials_intrinsic_value"
SPLITS   = f"{CATALOG}.{SCHEMA}.stock_splits"

# Known recent splits to spot-check (yfinance ratio convention: 4:1 → 4.0, 10:1 → 10.0).
KNOWN_SPLITS = [
    ("NVDA", "2021-07-20",  4.0),
    ("NVDA", "2024-06-10", 10.0),
    ("AAPL", "2020-08-31",  4.0),
    ("TSLA", "2022-08-25",  3.0),
    ("AMZN", "2022-06-06", 20.0),
    ("GOOGL", "2022-07-18", 20.0),
]

# Verdict state. _fail() records a hard problem (flips SPLIT_ADJUST_OK); _warn() is advisory only.
_issues = []
_warnings = []
def _fail(msg):
    _issues.append(msg)
    print(f"✗ {msg}")
def _warn(msg):
    _warnings.append(msg)
    print(f"⚠ {msg}")

# COMMAND ----------

# MAGIC %md ## 1. `stock_splits` landed and is sane

# COMMAND ----------

splits_present = spark.catalog.tableExists(SPLITS)
if not splits_present:
    _fail(f"{SPLITS} does not exist — the post-PR#90 pipeline run has not populated it "
          "(12 self-backfills it on the empty-table flag). Skipping remaining checks.")
    n_splits = n_tickers_with_splits = 0
else:
    splits = spark.table(SPLITS)
    n_splits = splits.count()
    n_tickers_with_splits = splits.select("ticker").distinct().count()
    print(f"stock_splits: {n_splits:,} rows across {n_tickers_with_splits:,} tickers")
    if n_splits == 0:
        _fail("stock_splits is EMPTY — backfill did not run; cross-year metrics fell back to raw.")

    # Hard sanity: ratios must be positive (reverse splits are fractional but > 0); no NULL keys.
    bad = splits.filter(
        F.col("ticker").isNull() | F.col("split_date").isNull()
        | F.col("ratio").isNull() | (F.col("ratio") <= 0)
    )
    n_bad = bad.count()
    print(f"rows with NULL key / non-positive ratio: {n_bad}")
    if n_bad:
        bad.show(20, truncate=False)
        _fail(f"stock_splits has {n_bad} malformed rows (NULL key or ratio ≤ 0).")

# COMMAND ----------

# MAGIC %md ## 1b. Known-split spot check
# MAGIC Each `(ticker, date, ratio)` above should be present (yfinance occasionally dates a split
# MAGIC ±1 trading day, so we match on a ±3-day window and the ratio within 1%). Advisory only —
# MAGIC a name may legitimately be outside the universe.

# COMMAND ----------

n_missing = 0
if splits_present and n_splits:
    known = spark.createDataFrame(KNOWN_SPLITS, "ticker string, ex_date string, ratio double") \
                 .withColumn("ex_date", F.to_date("ex_date"))
    matched = (
        known.join(splits, on="ticker", how="left")
        .withColumn("date_ok", F.abs(F.datediff("split_date", "ex_date")) <= 3)
        .withColumn("ratio_ok", F.abs(F.col("ratio") - known["ratio"]) / known["ratio"] <= 0.01)
        .groupBy(known["ticker"], known["ex_date"], known["ratio"].alias("expected_ratio"))
        .agg(F.max(F.when(F.col("date_ok") & F.col("ratio_ok"), 1).otherwise(0)).alias("found"))
        .orderBy("ticker", "ex_date")
    )
    matched.show(50, truncate=False)
    n_missing = matched.filter(F.col("found") == 0).count()
    if n_missing:
        _warn(f"{n_missing}/{len(KNOWN_SPLITS)} known splits absent — confirm those tickers are "
              "in the universe before alarm (advisory, not a failure).")
    else:
        print(f"all {len(KNOWN_SPLITS)} known splits found ✓")

# COMMAND ----------

# MAGIC %md ## 2. NVDA time series — the canonical regression case
# MAGIC Eyeball that the post-fix series is sane vs the pre-fix baseline in the header. Graham
# MAGIC Revised IV should no longer be ~319 and MoS% should be well above −613%; Net Buyback
# MAGIC Yield % should not spike to ±900% on a split year.

# COMMAND ----------

if splits_present:
    print("── NVDA Graham Revised (mid scenario, FY) ──")
    spark.table(IV).filter(
        (F.col("ticker") == "NVDA") & (F.col("method") == "graham_revised")
        & (F.col("scenario") == "mid") & (F.col("period_type") == "FY")
    ).select(
        "fiscal_year", "intrinsic_value_per_share", "price_close", "margin_of_safety_pct"
    ).orderBy("fiscal_year").show(30, truncate=False)

    print("── NVDA cross-year share metrics ──")
    spark.table(METRICS).filter(
        (F.col("ticker") == "NVDA")
        & F.col("metric").isin("Net Buyback Yield %", "Piotroski F-Score")
    ).groupBy("fiscal_year").pivot("metric").agg(F.first("value")) \
     .orderBy("fiscal_year").show(30, truncate=False)

# COMMAND ----------

# MAGIC %md ## 3. Net Buyback Yield % — implausible-magnitude scan
# MAGIC A real buyback/issuance almost never moves the diluted count more than ~50% YoY. Magnitudes
# MAGIC beyond ±150% are the split-artifact signature. After the fix these should be near-absent
# MAGIC among split tickers; whatever remains should be genuine micro-cap recaps, not the names in
# MAGIC `stock_splits`.

# COMMAND ----------

n_nby_split = 0
if splits_present and n_splits:
    split_names = splits.select("ticker").distinct()
    nby = spark.table(METRICS).filter(
        (F.col("metric") == "Net Buyback Yield %") & (F.abs("value") > 150)
    )
    nbY_split = nby.join(split_names, on="ticker", how="inner")
    n_nby_split = nbY_split.count()
    print(f"|Net Buyback Yield %| > 150  — total rows: {nby.count()}, of which split-ticker rows: {n_nby_split}")
    print("Worst offenders that ARE split tickers (should be ~empty post-fix):")
    nbY_split.select("ticker", "fiscal_year", "value").orderBy(F.abs("value").desc()).show(20, truncate=False)
    if n_nby_split:
        _warn(f"{n_nby_split} split-ticker rows still have |Net Buyback Yield %| > 150 — "
              "inspect for residual split artifacts vs genuine recaps.")

# COMMAND ----------

# MAGIC %md ## 4. Graham Revised — extreme-negative MoS% scan
# MAGIC A split that floored EPS-CAGR to 0 collapsed Graham Revised IV, driving MoS% deeply
# MAGIC negative (NVDA −613%). Count rows below −300% and check split tickers are no longer
# MAGIC over-represented.

# COMMAND ----------

n_grv_split = 0
if splits_present and n_splits:
    grv_bad = spark.table(IV).filter(
        (F.col("method") == "graham_revised") & (F.col("scenario") == "mid")
        & (F.col("margin_of_safety_pct") < -300)
    )
    grv_bad_split = grv_bad.join(split_names, on="ticker", how="inner")
    n_grv_split = grv_bad_split.count()
    print(f"Graham Revised MoS% < −300%  — total rows: {grv_bad.count()}, split-ticker rows: {n_grv_split}")
    grv_bad_split.select("ticker", "fiscal_year", "intrinsic_value_per_share",
                         "price_close", "margin_of_safety_pct") \
        .orderBy("margin_of_safety_pct").show(20, truncate=False)
    if n_grv_split:
        _warn(f"{n_grv_split} split-ticker rows still have Graham Revised MoS% < −300% — "
              "inspect for residual split artifacts.")

# COMMAND ----------

# MAGIC %md ## 5. Invariant — latest period is on factor = 1 (current valuations unchanged)
# MAGIC The cumulative factor at a period_end is `∏ ratio for split_date > period_end`. For each
# MAGIC ticker's MOST RECENT fiscal year there can be no future split, so the factor must be
# MAGIC exactly 1 — which is what guarantees the fix leaves today's headline numbers untouched.
# MAGIC This recomputes the factor in SQL (independent of `_core/splits.py`) and checks it.

# COMMAND ----------

n_viol = 0
if splits_present and n_splits:
    # period_end of each FY (one row per ticker/fiscal_year), from the clean facts.
    fy_pe = (
        spark.table(FIN)
        .filter(F.col("period_type") == "FY")
        .groupBy("ticker", "fiscal_year")
        .agg(F.max("period_end").alias("period_end"))
    )
    # Latest fiscal_year per ticker that actually has a split somewhere in its history.
    latest_fy = (
        fy_pe.join(split_names, on="ticker", how="inner")
        .groupBy("ticker").agg(F.max("fiscal_year").alias("fiscal_year"))
        .join(fy_pe, on=["ticker", "fiscal_year"], how="inner")
    )
    # factor = exp(Σ ln(ratio)) over splits strictly after the period_end. No future split → factor 1.
    factor = (
        latest_fy.join(splits, on="ticker", how="left")
        .withColumn("lr", F.when(F.col("split_date") > F.col("period_end"), F.log("ratio")))
        .groupBy("ticker", "fiscal_year", "period_end")
        .agg(F.exp(F.sum("lr")).alias("factor"))
        .withColumn("factor", F.coalesce("factor", F.lit(1.0)))
    )
    violations = factor.filter(F.abs(F.col("factor") - 1.0) > 1e-9)
    n_viol = violations.count()
    print(f"latest-FY rows with factor ≠ 1 (must be 0): {n_viol}")
    if n_viol:
        violations.orderBy("ticker").show(50, truncate=False)
        _fail(f"{n_viol} tickers carry a split factor ≠ 1 on their most-recent fiscal year — a "
              "split is dated AFTER the latest period_end, silently rescaling current valuations.")

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

SPLIT_ADJUST_OK = not _issues

print("=" * 55)
print("split-adjust validation:", "PASS ✓" if SPLIT_ADJUST_OK else "FAIL ✗")
print("=" * 55)
print(f"  • stock_splits         : {n_splits:,} rows / {n_tickers_with_splits:,} tickers")
print(f"  • known-split spot-miss: {n_missing} / {len(KNOWN_SPLITS)} (advisory)")
print(f"  • NBY% split artifacts : {n_nby_split} (target ≈ 0, advisory)")
print(f"  • GRV MoS% split artif.: {n_grv_split} (target ≈ 0, advisory)")
print(f"  • latest-FY factor≠1   : {n_viol} (must be 0)")
if _warnings:
    print(f"  {len(_warnings)} warning(s):")
    for w in _warnings:
        print(f"    ⚠ {w}")
if _issues:
    print(f"  {len(_issues)} hard issue(s):")
    for i in _issues:
        print(f"    ✗ {i}")
print("Eyeball §2 (NVDA) against the pre-fix baseline in the header to confirm the regression is gone.")

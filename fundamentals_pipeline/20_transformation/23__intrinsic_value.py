# Databricks notebook source
# MAGIC %md
# MAGIC # 20_transformation / 23__intrinsic_value
# MAGIC
# MAGIC Computes the **intrinsic value** of each company under four different lenses,
# MAGIC for **each historical fiscal year** and for **TTM** (rolling 4 quarters).
# MAGIC
# MAGIC | Method | Idea | Output |
# MAGIC |---|---|---|
# MAGIC | `graham_number` | √(22.5 × EPS × BVPS) — Graham's napkin rule | $/share |
# MAGIC | `graham_revised` | EPS × (8.5 + 2g) × 4.4 / Y_AAA — formula with growth | $/share |
# MAGIC | `dcf` | 2-stage DCF on FCF (or Owner Earnings) | $/share |
# MAGIC | `owner_earnings` | Owner Earnings × multiple (Buffett) or / discount_rate | $/share |
# MAGIC
# MAGIC **Primary output:** `{catalog}.{schema}.financials_intrinsic_value` (new)
# MAGIC with columns `period_type ∈ {'FY','TTM'}`, `fiscal_year`, `period_end`.
# MAGIC
# MAGIC Additionally, exposes the key metrics (IV per share and MoS by method and period)
# MAGIC in `financials_metrics` with suffixes `(FY)` and `(TTM)` so the dashboard can filter
# MAGIC them like any other metric.
# MAGIC
# MAGIC **Reads from:**
# MAGIC - `financials` (long-format with `period_type`, `period_end`, `fiscal_year`)
# MAGIC - `market_data` (price_close, market_cap per ticker × fiscal_year)
# MAGIC - `00_config/valuation_assumptions.json` (defaults + per-ticker overrides)
# MAGIC
# MAGIC > **Important warning:** valuations are only as good as their assumptions.
# MAGIC > The DCF in particular is **very sensitive** to `WACC` and `growth_stage1`. Change
# MAGIC > these parameters in `valuation_assumptions.json` before making real decisions.

# COMMAND ----------

# MAGIC %md ### ⚠️ Path note
# MAGIC `%run` paths are **relative to this notebook's location** in the workspace.
# MAGIC If it fails with `NameError`, adjust the path to point to `00_config/01__tickers`.

# COMMAND ----------

# MAGIC %run "../00_config/01__tickers"

# COMMAND ----------

import json
import math
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window

# ── Paths & table names ──────────────────────────────────────────────────────
ASSUMPTIONS_JSON_PATH = "../00_config/valuation_assumptions.json"

full_table  = f"{CATALOG}.{SCHEMA}.{TABLE}"
market_tbl  = f"{CATALOG}.{SCHEMA}.market_data"
iv_tbl      = f"{CATALOG}.{SCHEMA}.financials_intrinsic_value"
metrics_tbl = f"{CATALOG}.{SCHEMA}.financials_metrics"

print(f"Source        : {full_table}")
print(f"Market data   : {market_tbl}")
print(f"Target IV     : {iv_tbl}")
print(f"Metrics table : {metrics_tbl}")

# COMMAND ----------

# MAGIC %md ## 1. Load assumptions from JSON

# COMMAND ----------

def _load_assumptions(path: str) -> dict:
    """JSON loader tolerant of // comment lines and discardable _xxx keys."""
    raw   = Path(path).read_text(encoding="utf-8")
    lines = [l for l in raw.splitlines() if not l.strip().startswith("//")]
    data  = json.loads("\n".join(lines))

    def _clean(obj):
        if isinstance(obj, dict):
            return {k: _clean(v) for k, v in obj.items() if not k.startswith("_")}
        if isinstance(obj, list):
            return [_clean(x) for x in obj]
        return obj

    return _clean(data)


ASSUMPTIONS = _load_assumptions(ASSUMPTIONS_JSON_PATH)
DEFAULTS    = ASSUMPTIONS["defaults"]
OVERRIDES   = ASSUMPTIONS.get("overrides", {})

# Sector-aware flow-model skip policy. `_load_assumptions` already stripped the `_doc` key,
# so only `flow_model_skip_sectors` (sector → bool) survives. Energy/Financials/Real Estate
# skip DCF + Owner Earnings by default; a per-ticker override still wins (see _resolve_skip).
SECTOR_POLICY           = ASSUMPTIONS.get("sector_policy", {})
FLOW_MODEL_SKIP_SECTORS = SECTOR_POLICY.get("flow_model_skip_sectors", {})

print(f"✓ Loaded assumptions — {len(OVERRIDES)} ticker override(s)")
print(f"  DCF defaults  : WACC={DEFAULTS['dcf']['wacc']}, "
      f"g1={DEFAULTS['dcf']['growth_stage1']}, "
      f"g_terminal={DEFAULTS['dcf']['growth_terminal']}, "
      f"horizon={DEFAULTS['dcf']['horizon_years']}y")
print(f"  Flow-model skip sectors: "
      f"{sorted(s for s, v in FLOW_MODEL_SKIP_SECTORS.items() if v) or '(none)'}")

# ticker → GICS sector, used by the sector-aware skip below. Left-join semantics: tickers
# absent from config.tickers (or with NULL sector) resolve to None ⇒ no sector skip. Built
# once here (small table, ~3k rows) and looked up per unique ticker in _resolve_skip.
SECTOR_MAP = {
    r["ticker"]: r["sector"]
    for r in spark.table(TICKERS_TABLE).select("ticker", "sector").collect()
}
print(f"✓ Sector map built — {len(SECTOR_MAP):,} tickers")


def _merge_dicts(base: dict, over: dict) -> dict:
    out = {k: dict(v) if isinstance(v, dict) else v for k, v in base.items()}
    for k, v in over.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = {**out[k], **v}
        else:
            out[k] = v
    return out


def assumptions_for(ticker: str) -> dict:
    return _merge_dicts(DEFAULTS, OVERRIDES.get(ticker, {}))

# COMMAND ----------

# MAGIC %md ## 2. Define the concepts we need
# MAGIC
# MAGIC IMPORTANT: we include `stmt` in the join — `Net Income` appears in both
# MAGIC `Income Statement` and `Cash Flow` (reconciliation), and without filtering
# MAGIC we would duplicate rows when pivoting.

# COMMAND ----------

# (stmt, concept, alias) — controlled order, no name collisions when pivoting
NEEDED = [
    ("Income Statement", "Net Income",                  "net_income"),
    ("Income Statement", "Revenue",                     "revenue"),
    ("Income Statement", "Operating Income",            "op_income"),
    ("Income Statement", "EPS Diluted",                 "eps"),
    ("Income Statement", "Shares Diluted",              "shares"),

    ("Balance Sheet",    "Total Stockholders Equity",   "equity"),
    ("Balance Sheet",    "Total Assets",                "assets"),
    ("Balance Sheet",    "Long-term Debt",              "lt_debt"),
    ("Balance Sheet",    "Short-term Debt",             "st_debt"),
    ("Balance Sheet",    "Cash & Equivalents",          "cash"),
    ("Balance Sheet",    "Short-term Investments",      "st_inv"),
    ("Balance Sheet",    "Retained Earnings",           "retained_earnings"),  # for Graham applicability guard

    ("Cash Flow",        "Operating Cash Flow",         "ocf"),
    ("Cash Flow",        "CapEx",                       "capex"),
    ("Cash Flow",        "Depreciation & Amortization", "dna"),
    ("Cash Flow",        "Stock-based Compensation",    "sbc"),
    ("Cash Flow",        "Changes in Working Capital",  "delta_wc"),
]

# "flow" concepts (summable to TTM). Balance Sheet concepts are "stock" (snapshot).
STOCK_ALIASES = {"equity", "assets", "lt_debt", "st_debt", "cash", "st_inv", "retained_earnings"}
# Shares is also "stock" in the TTM sense: we use the most recent quarter's value,
# not the sum (the 'shares' alias is the period's weighted-average diluted count).
STOCK_ALIASES.add("shares")

ALIAS_OF = {(s, c): a for s, c, a in NEEDED}
ALL_ALIASES = [a for _, _, a in NEEDED]

# Build the filter as OR of specific (stmt, concept) pairs
filter_cond = F.lit(False)
for stmt, concept, _ in NEEDED:
    filter_cond = filter_cond | ((F.col("stmt") == stmt) & (F.col("concept") == concept))

# Relevant subset with alias in a new column
fin_subset = (
    spark.table(full_table)
    .filter(filter_cond)
    .filter(F.col("value").isNotNull())
)

# Apply stmt+concept → alias mapping using CASE WHEN
alias_col = F.lit(None).cast("string")
for stmt, concept, alias in NEEDED:
    alias_col = F.when(
        (F.col("stmt") == stmt) & (F.col("concept") == concept),
        F.lit(alias),
    ).otherwise(alias_col)
fin_subset = fin_subset.withColumn("alias", alias_col).filter(F.col("alias").isNotNull())

# Deduplicate in case a scrape introduced exact duplicates
fin_subset = fin_subset.dropDuplicates(
    ["ticker", "stmt", "concept", "fiscal_year", "period_type", "period_end"]
)

# Guard against inconsistent company per ticker — same reason as in 22__derived_metrics:
# the MERGE in 21__clean_and_merge only updates company when value changes, so some old
# rows retain a stale company (SEC entityNames with escapes, restructures, favorites.json
# overrides). Without this, groupBy(ticker, company, fiscal_year) below produces duplicate
# rows and the final MERGE fails with DELTA_MULTIPLE_SOURCE_ROW_MATCHING.
_company_w = Window.partitionBy("ticker")
fin_subset = fin_subset.withColumn(
    "company", F.first("company", ignorenulls=True).over(_company_w)
)

# Materialize fin_subset (financials scan filtered to ~16 concepts): consumed by fy_wide,
# quarters, and several count() calls → without this it would be re-scanned ~7×. localCheckpoint(eager)
# materializes it once and truncates the lineage. .cache()/.persist() do NOT work on serverless ([NOT_SUPPORTED_WITH_SERVERLESS]).
fin_subset = fin_subset.localCheckpoint(eager=True)

print(f"✓ Concept subset prepared — {fin_subset.count():,} rows")

# COMMAND ----------

# MAGIC %md ## 3. Pivot FY — one row per (ticker, fiscal_year)

# COMMAND ----------

fy_wide = (
    fin_subset
    .filter(F.col("period_type") == "FY")
    .groupBy("ticker", "company", "fiscal_year")
    .pivot("alias", ALL_ALIASES)
    .agg(F.first("value"))
    .withColumnRenamed("fiscal_year", "year")
)

print(f"✓ FY wide: {fy_wide.count():,} (ticker, year) rows")

# ── Trailing EPS CAGR per (ticker, fiscal_year) → growth input for Graham Revised ──
# Graham's revised formula uses each company's own expected growth in (8.5 + 2g); the flat
# dcf.growth_stage1 made GRV collapse to a near-constant ~24× EPS for the whole universe.
# We derive g from the FY EPS series, POINT-IN-TIME (ending at each row's own fiscal_year,
# no lookahead): a 5y trailing CAGR, falling back to the longest span available in [3,5]y.
# Both the start and end EPS must be > 0 for a CAGR to be meaningful; rows with < 3y of
# positive history get NULL and are coalesced to the dcf.growth_stage1 assumption downstream.
eps_fy = (
    fy_wide.select("ticker", "year", F.col("eps").cast("double").alias("eps"))
    .filter(F.col("eps") > 0)
)
_cagr_pairs = (
    eps_fy.join(
        eps_fy.select(
            "ticker",
            F.col("year").alias("base_year"),
            F.col("eps").alias("base_eps"),
        ),
        on="ticker",
    )
    .filter(
        (F.col("base_year") >= F.col("year") - 5)
        & (F.col("base_year") <= F.col("year") - 3)
    )
)
# Per (ticker, year), keep the longest available span (smallest base_year ⇒ closest to −5y).
_span_w = Window.partitionBy("ticker", "year").orderBy(F.col("base_year").asc())
eps_cagr = (
    _cagr_pairs.withColumn("_rn", F.row_number().over(_span_w))
    .filter(F.col("_rn") == 1)
    .withColumn("_n", (F.col("year") - F.col("base_year")).cast("double"))
    .withColumn(
        "eps_cagr",
        F.pow(F.col("eps") / F.col("base_eps"), F.lit(1.0) / F.col("_n")) - F.lit(1.0),
    )
    .select("ticker", "year", "eps_cagr")
)
fy_wide = fy_wide.join(eps_cagr, on=["ticker", "year"], how="left")

# COMMAND ----------

# MAGIC %md ## 4. Pivot TTM — one row per ticker (rolling 4 quarters)
# MAGIC
# MAGIC For each (ticker, alias) we sort quarters by `period_end DESC` and
# MAGIC keep the 4 most recent. Then:
# MAGIC - **Flow** (IS, CF) → SUM of the 4 values
# MAGIC - **Stock** (BS, Shares) → first value (the most recent)

# COMMAND ----------

quarters = (
    fin_subset
    .filter(F.col("period_type").isin("Q1", "Q2", "Q3", "Q4"))
)

w_recent = Window.partitionBy("ticker", "alias").orderBy(F.col("period_end").desc())
quarters_ranked = quarters.withColumn("rn", F.row_number().over(w_recent)) \
                          .filter(F.col("rn") <= 4)

# Materialize quarters_ranked (window of the 4 most recent quarters): feeds ttm_flow AND
# ttm_stock → two reads of the same window. localCheckpoint once (cache does not work on serverless).
quarters_ranked = quarters_ranked.localCheckpoint(eager=True)

# Aggregate: SUM if flow, FIRST (rn=1) if stock
ttm_flow = (
    quarters_ranked
    .filter(~F.col("alias").isin(list(STOCK_ALIASES)))
    .groupBy("ticker", "alias")
    .agg(
        F.count(F.lit(1)).alias("_n_quarters"),
        F.sum("value").alias("value"),
        F.max("period_end").alias("period_end"),
        F.first("company", ignorenulls=True).alias("company"),
        F.first("fiscal_year").alias("fiscal_year"),  # from the most recent
    )
    # Only keep TTM if we have 4 complete quarters
    .filter(F.col("_n_quarters") == 4)
    .drop("_n_quarters")
)

ttm_stock = (
    quarters_ranked
    .filter(F.col("alias").isin(list(STOCK_ALIASES)))
    .filter(F.col("rn") == 1)
    .select(
        "ticker", "alias", "value",
        F.col("period_end"),
        "company",
        "fiscal_year",
    )
)

ttm_long = ttm_flow.unionByName(ttm_stock)

# Pivot to wide. For period_end and fiscal_year we use the MAX across all
# aliases for the same ticker (the most recent available).
ttm_meta = ttm_long.groupBy("ticker").agg(
    F.max("period_end").alias("period_end"),
    F.first("company", ignorenulls=True).alias("company"),
    F.max("fiscal_year").alias("year"),
)

ttm_pivot = (
    ttm_long
    .groupBy("ticker")
    .pivot("alias", ALL_ALIASES)
    .agg(F.first("value"))
)

ttm_wide = ttm_meta.join(ttm_pivot, on="ticker", how="inner")

# TTM growth = the most recent FY EPS CAGR available per ticker (eps_cagr defined in §3).
_latest_cagr = (
    eps_cagr
    .withColumn("_rn", F.row_number().over(
        Window.partitionBy("ticker").orderBy(F.col("year").desc())))
    .filter(F.col("_rn") == 1)
    .select("ticker", "eps_cagr")
)
ttm_wide = ttm_wide.join(_latest_cagr, on="ticker", how="left")

print(f"✓ TTM wide: {ttm_wide.count():,} ticker rows")

# COMMAND ----------

# MAGIC %md ## 5. Join with market_data and collect to Pandas
# MAGIC
# MAGIC For **FY**: price from the same `fiscal_year`.
# MAGIC For **TTM**: price from the most recent `fiscal_year` available in market_data.

# COMMAND ----------

try:
    mkt = (
        spark.table(market_tbl)
        .select(
            "ticker",
            F.col("fiscal_year").alias("year"),
            "price_close",
            "market_cap",
        )
    )
    has_market_data = True
except Exception:
    print("⚠ market_data not available — Margin of Safety will be NULL.")
    mkt = None
    has_market_data = False

# FY: direct join on (ticker, year)
if has_market_data:
    fy_with_price = fy_wide.join(mkt, on=["ticker", "year"], how="left")
else:
    fy_with_price = (
        fy_wide
        .withColumn("price_close", F.lit(None).cast("double"))
        .withColumn("market_cap",  F.lit(None).cast("double"))
    )

# TTM: use the most recent price_close available in market_data per ticker
# (may be the current year without shares yet, or the previous year).
if has_market_data:
    w_latest_price = Window.partitionBy("ticker").orderBy(F.col("year").desc())
    latest_price = (
        mkt
        .filter(F.col("price_close").isNotNull())
        .withColumn("rn", F.row_number().over(w_latest_price))
        .filter(F.col("rn") == 1)
        .select("ticker", F.col("price_close"), F.col("year").alias("price_year"))
    )
    ttm_with_price = ttm_wide.join(latest_price, on="ticker", how="left")
    # current market_cap (for future metrics — not strictly needed here)
    ttm_with_price = ttm_with_price.withColumn("market_cap", F.lit(None).cast("double"))
else:
    ttm_with_price = (
        ttm_wide
        .withColumn("price_close", F.lit(None).cast("double"))
        .withColumn("market_cap",  F.lit(None).cast("double"))
        .withColumn("price_year",  F.lit(None).cast("int"))
    )

print(f"✓ FY rows : {fy_with_price.count():,}")
print(f"✓ TTM rows: {ttm_with_price.count():,}")

# COMMAND ----------

# Collect to pandas — reasonable volume (~30k FY + ~3k TTM)
fy_pdf  = fy_with_price.toPandas()
ttm_pdf = ttm_with_price.toPandas()

# (No unpersist: also unsupported on serverless. localCheckpoints are released when the
# session closes; from here on everything is pandas/numpy on the driver + Delta writes.)

# Ensure auxiliary columns exist
if "period_end" not in fy_pdf.columns:
    fy_pdf["period_end"] = pd.NaT
fy_pdf["period_type"] = "FY"

ttm_pdf["period_type"] = "TTM"
# Rename so both pdfs share the same schema
# (fy already has 'year'; ttm does too)

# Common derived fields
for pdf in (fy_pdf, ttm_pdf):
    pdf["fcf"]  = pdf["ocf"].astype(float) - pdf["capex"].fillna(0).astype(float)
    pdf["bvps"] = pdf["equity"] / pdf["shares"]
    # Owner Earnings $ (Buffett 1986: NI + D&A + SBC − CapEx − ΔWC). Vectorized with
    # fillna(0) = the per-row _safe(.., 0). Consumed by compute_all (via DCF
    # use_owner_earnings + owner_earnings method) and step 9 for absolute OE exposure.
    pdf["oe_dollars"] = (
        pdf["net_income"].fillna(0) + pdf["dna"].fillna(0) + pdf["sbc"].fillna(0)
        - pdf["capex"].fillna(0) - pdf["delta_wc"].fillna(0)
    )

# Universe of tickers EVALUATED this run. The exposure/iv MERGEs below only upsert, so a
# method that is now SKIPPED for a ticker (e.g. Graham Number suppressed for a distorted
# book value) would keep its previously-published rows forever. We use this view to scope
# the orphan-deletes (steps 8b / 9b) so we only clean stale rows for tickers we recomputed.
spark.createDataFrame(
    pd.DataFrame({"ticker": pd.Series(
        sorted(set(fy_pdf["ticker"]).union(set(ttm_pdf["ticker"]))), dtype="string")})
).createOrReplaceTempView("iv_processed_tickers")

print(f"✓ FY pandas : {len(fy_pdf):,} rows")
print(f"✓ TTM pandas: {len(ttm_pdf):,} rows")

# COMMAND ----------

# MAGIC %md ## 6. The four formulas

# COMMAND ----------

# The four formulas, VECTORIZED over the entire dataframe with numpy. The previous version
# iterated row-by-row (pdf.iterrows × 4 methods × one dict merge per row) — the wall-clock
# bottleneck once re-scans were eliminated. This version is row-for-row equivalent to the
# previous one (validated to diff < 1e-9 on synthetic data covering all skip-conditions and
# overrides). Each skip-condition becomes a NaN mask; a row is included only if its iv is
# not NaN and > 0, identical to the original `continue`. The DCF keeps the per-year LOOP
# (vectorized across rows, NOT closed-form) to reproduce the original loop's floating-point sum bit-for-bit.


def _resolve_skip(ticker: str, method_assumptions: dict) -> bool:
    """Precedence for the flow-model skip flag (dcf / owner_earnings):
      1. Explicit per-ticker override wins — if the merged assumptions carry a `skip` key
         (present, True OR False), use it verbatim. DEFAULTS never set `skip`, so the key is
         present here only when an `overrides[ticker]` entry set it (preserves BRK.B / JPM).
      2. Else fall through to the sector default — skip if the ticker's GICS sector is flagged
         in flow_model_skip_sectors with True.
      3. Else do not skip. Sector NULL / unknown / absent from config.tickers ⇒ no skip."""
    if "skip" in method_assumptions:
        return bool(method_assumptions["skip"])
    return bool(FLOW_MODEL_SKIP_SECTORS.get(SECTOR_MAP.get(ticker), False))


def _params_for(ticker: str) -> dict:
    """Flattens assumptions_for(ticker) into scalar columns. Called ONCE per UNIQUE ticker
    (not per row), reusing the same defaults+overrides merge logic."""
    a = assumptions_for(ticker)
    g, gr, d, oe = a["graham"], a["graham_revised"], a["dcf"], a["owner_earnings"]
    return {
        "ticker":         ticker,
        "magic":          float(g["magic_number"]),
        "gr_base_pe":     float(gr["base_pe"]),
        "gr_growth_mult": float(gr["growth_multiplier"]),
        "gr_aaa_norm":    float(gr["aaa_yield_norm"]),
        "gr_aaa_yield":   float(gr["graham_aaa_yield"]),
        "gr_growth_cap":  float(gr["growth_cap"]),
        "dcf_skip":       _resolve_skip(ticker, d),
        "dcf_wacc":       float(d["wacc"]),
        "dcf_g1":         float(d["growth_stage1"]),
        "dcf_gt":         float(d["growth_terminal"]),
        "dcf_horizon":    int(d["horizon_years"]),
        "dcf_use_oe":     bool(d.get("use_owner_earnings", False)),
        "oe_skip":        _resolve_skip(ticker, oe),
        "oe_method":      oe.get("method", "multiple"),
        "oe_multiple":    float(oe["multiple"]),
        "oe_dr":          float(oe["discount_rate"]),
    }

# COMMAND ----------

# MAGIC %md ## 7. Compute — for each row (FY or TTM) × each method (vectorized)

# COMMAND ----------

def compute_all(pdf, period_type, computed_at):
    """Computes all 4 methods for the ENTIRE dataframe (numpy, no iterrows) and builds the rows."""
    if len(pdf) == 0:
        return []

    # Parameters per unique ticker → columns; merge instead of a per-row dict-merge.
    params = pd.DataFrame([_params_for(t) for t in pdf["ticker"].unique()])
    m = pdf.merge(params, on="ticker", how="left")

    def col(name):
        return m[name].to_numpy(dtype="float64")

    eps, bvps, shares = col("eps"), col("bvps"), col("shares")
    eps_cagr          = col("eps_cagr")   # per-ticker trailing 5y EPS growth (NaN ⇒ use assumption)
    price, retained   = col("price_close"), col("retained_earnings")
    ni, dna, sbc      = col("net_income"), col("dna"), col("sbc")
    capex, dwc        = col("capex"), col("delta_wc")
    fcf               = col("fcf")
    lt_debt, st_debt  = col("lt_debt"), col("st_debt")
    cash, st_inv      = col("cash"), col("st_inv")

    magic          = col("magic")
    gr_base_pe     = col("gr_base_pe")
    gr_growth_mult = col("gr_growth_mult")
    gr_aaa_norm    = col("gr_aaa_norm")
    gr_aaa_yield   = col("gr_aaa_yield")
    gr_growth_cap  = col("gr_growth_cap")
    dcf_skip       = m["dcf_skip"].to_numpy(dtype=bool)
    dcf_wacc, dcf_g1, dcf_gt = col("dcf_wacc"), col("dcf_g1"), col("dcf_gt")
    dcf_horizon    = m["dcf_horizon"].to_numpy(dtype="int64")
    dcf_use_oe     = m["dcf_use_oe"].to_numpy(dtype=bool)
    oe_skip        = m["oe_skip"].to_numpy(dtype=bool)
    oe_method      = m["oe_method"].to_numpy(dtype=object)
    oe_multiple, oe_dr = col("oe_multiple"), col("oe_dr")

    nan = np.nan
    z = lambda arr: np.where(np.isnan(arr), 0.0, arr)   # = _safe(.., 0)
    oe_dollars = z(ni) + z(dna) + z(sbc) - z(capex) - z(dwc)

    with np.errstate(invalid="ignore", divide="ignore"):
        # ── graham_number ──  sqrt(magic·EPS·BVPS); skip if EPS/BVPS non-positive or book
        # distorted (retained < 0, or P/B > 10). bvps>0 guaranteed in the valid branch.
        gn_valid = ~np.isnan(eps) & ~np.isnan(bvps) & (eps > 0) & (bvps > 0)
        pb = np.where(bvps != 0, price / bvps, np.inf)
        distort = (~np.isnan(retained) & (retained < 0)) | (~np.isnan(price) & (pb > 10))
        gn = np.where(gn_valid & ~distort, np.sqrt(magic * eps * bvps), nan)

        # ── graham_revised ──  g = the company's own trailing 5y EPS CAGR (point-in-time),
        # floored at 0 (Graham's 8.5 base IS the no-growth P/E — a shrinking firm should not
        # push the multiple below it) and capped at growth_cap. Falls back to the
        # dcf.growth_stage1 assumption when the CAGR is undefined (NaN). DCF still reads
        # dcf_g1 directly in its loop below — only Graham Revised uses g_eff.
        g_company = np.where(np.isnan(eps_cagr), dcf_g1, eps_cagr)
        g_eff = np.clip(np.minimum(g_company, gr_growth_cap), 0.0, None)
        grv_valid = ~np.isnan(eps) & (eps > 0)
        grv = np.where(
            grv_valid,
            eps * (gr_base_pe + gr_growth_mult * g_eff * 100) * gr_aaa_norm / (gr_aaa_yield * 100),
            nan,
        )

        # ── dcf ──  per-year loop (vectorized across rows) = bit-identical to the original loop.
        starting_cf = np.where(dcf_use_oe, oe_dollars, fcf)
        dcf_valid = (
            ~dcf_skip & ~np.isnan(shares) & (shares > 0)
            & ~np.isnan(starting_cf) & (starting_cf > 0) & (dcf_wacc > dcf_gt)
        )
        cf  = np.where(np.isnan(starting_cf), 0.0, starting_cf).astype("float64")
        pv1 = np.zeros_like(cf)
        for t in range(1, int(dcf_horizon.max()) + 1):
            active = t <= dcf_horizon                       # rows whose horizon still covers t
            cf  = np.where(active, cf * (1 + dcf_g1), cf)    # stops growing past the horizon
            pv1 = np.where(active, pv1 + cf / ((1 + dcf_wacc) ** t), pv1)
        cf_term = cf * (1 + dcf_gt)
        tv      = cf_term / (dcf_wacc - dcf_gt)
        pv_term = tv / ((1 + dcf_wacc) ** dcf_horizon)
        debt    = z(lt_debt) + z(st_debt)
        cash_t  = z(cash) + z(st_inv)
        dcf_ips = (pv1 + pv_term - debt + cash_t) / shares
        dcf = np.where(dcf_valid, dcf_ips, nan)

        # ── owner_earnings ──  OE × multiple  or  OE / discount_rate (Gordon perpetuity).
        oe_valid = ~oe_skip & (oe_dollars > 0) & ~np.isnan(shares) & (shares > 0)
        total = np.where(
            oe_method == "multiple", oe_dollars * oe_multiple,
            np.where(oe_method == "perpetuity", oe_dollars / oe_dr, nan),
        )
        oev = np.where(oe_valid & ~np.isnan(total), total / shares, nan)

    company    = m["company"].to_numpy(dtype=object)
    ticker_arr = m["ticker"].to_numpy(dtype=object)
    year       = m["year"].to_numpy()
    pend_ts    = pd.to_datetime(m["period_end"], errors="coerce")
    pend       = [p.date() if pd.notna(p) else None for p in pend_ts]

    # Diagnostic metadata (`assumptions` column) — NOT consumed by the dashboard or export;
    # kept for ad-hoc table inspection. Built only for surviving rows.
    def _meta_gn(i):
        return {"magic": float(magic[i]), "eps": float(eps[i]), "bvps": float(bvps[i])}

    def _meta_grv(i):
        return {"eps": float(eps[i]), "g": float(g_eff[i]), "base_pe": float(gr_base_pe[i]),
                "growth_mult": float(gr_growth_mult[i]), "aaa_norm": float(gr_aaa_norm[i]),
                "aaa_current_pct": float(gr_aaa_yield[i] * 100)}

    def _meta_dcf(i):
        return {"cf_basis": "owner_earnings" if dcf_use_oe[i] else "fcf",
                "starting_cf": float(starting_cf[i]), "wacc": float(dcf_wacc[i]),
                "g1": float(dcf_g1[i]), "g_terminal": float(dcf_gt[i]), "horizon": int(dcf_horizon[i]),
                "pv_stage1": round(float(pv1[i]), 0), "pv_terminal": round(float(pv_term[i]), 0),
                "debt": float(debt[i]), "cash": float(cash_t[i])}

    def _meta_oe(i):
        meta = {"method": oe_method[i], "oe": float(oe_dollars[i]),
                "oe_per_share": float(oe_dollars[i] / shares[i])}
        if oe_method[i] == "multiple":
            meta["multiple"] = float(oe_multiple[i])
        elif oe_method[i] == "perpetuity":
            meta["discount_rate"] = float(oe_dr[i])
        return meta

    rows = []
    methods = (
        ("graham_number",  gn,  _meta_gn),
        ("graham_revised", grv, _meta_grv),
        ("dcf",            dcf, _meta_dcf),
        ("owner_earnings", oev, _meta_oe),
    )
    for method_name, iv, meta_fn in methods:
        keep = ~np.isnan(iv) & (iv > 0)
        for i in np.nonzero(keep)[0]:
            i   = int(i)
            ivv = float(iv[i])
            sh  = shares[i]
            pr  = price[i]
            rows.append({
                "ticker":                    ticker_arr[i],
                "company":                   company[i],
                "period_type":               period_type,
                "fiscal_year":               int(year[i]) if pd.notna(year[i]) else None,
                "period_end":                pend[i],
                "method":                    method_name,
                "intrinsic_value_per_share": ivv,
                "intrinsic_value_total":     float(ivv * sh) if not np.isnan(sh) else None,
                "price_close":               float(pr) if not np.isnan(pr) else None,
                "margin_of_safety_pct":      float((ivv - pr) / ivv * 100) if not np.isnan(pr) else None,
                "assumptions":               json.dumps(meta_fn(i), default=str),
                "computed_at":               computed_at,
            })
    return rows


computed_at = datetime.utcnow()

fy_rows  = compute_all(fy_pdf,  "FY",  computed_at)
ttm_rows = compute_all(ttm_pdf, "TTM", computed_at)
all_rows = fy_rows + ttm_rows

iv_pdf = pd.DataFrame(all_rows)
print(f"✓ Computed {len(iv_pdf):,} valuations ({len(fy_rows):,} FY + {len(ttm_rows):,} TTM)")
if len(iv_pdf):
    print(iv_pdf.groupby(["period_type", "method"]).size().unstack(fill_value=0))

# COMMAND ----------

# MAGIC %md ## 8. Write to Delta (MERGE — idempotent)

# COMMAND ----------

schema = T.StructType([
    T.StructField("ticker",                    T.StringType(),    False),
    T.StructField("company",                   T.StringType(),    True),
    T.StructField("period_type",               T.StringType(),    False),
    T.StructField("fiscal_year",               T.IntegerType(),   True),
    T.StructField("period_end",                T.DateType(),      True),
    T.StructField("method",                    T.StringType(),    False),
    T.StructField("intrinsic_value_per_share", T.DoubleType(),    True),
    T.StructField("intrinsic_value_total",     T.DoubleType(),    True),
    T.StructField("price_close",               T.DoubleType(),    True),
    T.StructField("margin_of_safety_pct",      T.DoubleType(),    True),
    T.StructField("assumptions",               T.StringType(),    True),
    T.StructField("computed_at",               T.TimestampType(), True),
])

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {iv_tbl} (
        ticker                    STRING    NOT NULL,
        company                   STRING,
        period_type               STRING    NOT NULL,
        fiscal_year               INT,
        period_end                DATE,
        method                    STRING    NOT NULL,
        intrinsic_value_per_share DOUBLE,
        intrinsic_value_total     DOUBLE,
        price_close               DOUBLE,
        margin_of_safety_pct      DOUBLE,
        assumptions               STRING,
        computed_at               TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (ticker)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

if len(iv_pdf):
    iv_sdf = spark.createDataFrame(iv_pdf, schema=schema)
    iv_sdf.createOrReplaceTempView("incoming_iv")

    # MERGE: unique key is (ticker, period_type, fiscal_year, method).
    # For TTM, fiscal_year is that of the most recent quarter, so each TTM run
    # upserts the same row (if the quarter hasn't changed) or inserts a new one
    # (if there is a more recent quarter).
    spark.sql(f"""
        MERGE INTO {iv_tbl} AS target
        USING incoming_iv AS source
        ON  target.ticker      = source.ticker
        AND target.period_type = source.period_type
        AND COALESCE(target.fiscal_year, -1) = COALESCE(source.fiscal_year, -1)
        AND target.method      = source.method

        WHEN MATCHED THEN UPDATE SET
            target.intrinsic_value_per_share = source.intrinsic_value_per_share,
            target.intrinsic_value_total     = source.intrinsic_value_total,
            target.price_close               = source.price_close,
            target.margin_of_safety_pct      = source.margin_of_safety_pct,
            target.assumptions               = source.assumptions,
            target.computed_at               = source.computed_at,
            target.period_end                = source.period_end,
            target.company                   = source.company

        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"✓ Merged into {iv_tbl}")

    # ── 8b. Orphan cleanup ──────────────────────────────────────────────────────
    # The MERGE above only upserts. For tickers EVALUATED this run, delete (method,
    # period, year) combinations absent from the freshly-computed `incoming_iv` — i.e.
    # methods that became inapplicable (e.g. Graham Number skipped on a distorted book
    # value) or turned non-positive. Mirrors 21's step-4 orphan DELETE. Scoped to
    # iv_processed_tickers so it never touches tickers not recomputed this run.
    n_orphan_iv = spark.sql(f"""
        SELECT t.ticker FROM {iv_tbl} t
        JOIN iv_processed_tickers p ON t.ticker = p.ticker
        WHERE NOT EXISTS (
            SELECT 1 FROM incoming_iv s
            WHERE s.ticker = t.ticker AND s.period_type = t.period_type
              AND COALESCE(s.fiscal_year, -1) = COALESCE(t.fiscal_year, -1)
              AND s.method = t.method)
    """).count()
    spark.sql(f"""
        MERGE INTO {iv_tbl} AS t
        USING (
            SELECT t.ticker, t.period_type, t.fiscal_year, t.method
            FROM {iv_tbl} t
            JOIN iv_processed_tickers p ON t.ticker = p.ticker
            WHERE NOT EXISTS (
                SELECT 1 FROM incoming_iv s
                WHERE s.ticker = t.ticker AND s.period_type = t.period_type
                  AND COALESCE(s.fiscal_year, -1) = COALESCE(t.fiscal_year, -1)
                  AND s.method = t.method)
        ) AS s
        ON  t.ticker = s.ticker AND t.period_type = s.period_type
        AND COALESCE(t.fiscal_year, -1) = COALESCE(s.fiscal_year, -1)
        AND t.method = s.method
        WHEN MATCHED THEN DELETE
    """)
    print(f"✓ Orphan cleanup on {iv_tbl}: {n_orphan_iv:,} stale method-rows deleted")
else:
    print(f"⊘ No valuations computed — {iv_tbl} unchanged.")

# COMMAND ----------

# MAGIC %md ## 9. Expose metrics in `financials_metrics`
# MAGIC
# MAGIC Two variants per method: `(FY)` and `(TTM)`. FY rows are associated with their
# MAGIC corresponding `fiscal_year`; TTM rows are written with the `fiscal_year` of the
# MAGIC ticker's most recent quarter.

# COMMAND ----------

# Mapping: (method, output_field, period_type) → metric_name
EXPOSED = [
    # ── FY ──
    ("graham_number",  "FY",  "intrinsic_value_per_share", "Graham Number (FY)"),
    ("graham_number",  "FY",  "margin_of_safety_pct",      "MoS % (Graham Number, FY)"),
    ("graham_revised", "FY",  "intrinsic_value_per_share", "Graham Revised Value (FY)"),
    ("graham_revised", "FY",  "margin_of_safety_pct",      "MoS % (Graham Revised, FY)"),
    ("dcf",            "FY",  "intrinsic_value_per_share", "DCF Value per Share (FY)"),
    ("dcf",            "FY",  "margin_of_safety_pct",      "MoS % (DCF, FY)"),
    ("owner_earnings", "FY",  "intrinsic_value_per_share", "Owner Earnings Value/Share (FY)"),
    ("owner_earnings", "FY",  "margin_of_safety_pct",      "MoS % (Owner Earnings, FY)"),
    # ── TTM ──
    ("graham_number",  "TTM", "intrinsic_value_per_share", "Graham Number (TTM)"),
    ("graham_number",  "TTM", "margin_of_safety_pct",      "MoS % (Graham Number, TTM)"),
    ("graham_revised", "TTM", "intrinsic_value_per_share", "Graham Revised Value (TTM)"),
    ("graham_revised", "TTM", "margin_of_safety_pct",      "MoS % (Graham Revised, TTM)"),
    ("dcf",            "TTM", "intrinsic_value_per_share", "DCF Value per Share (TTM)"),
    ("dcf",            "TTM", "margin_of_safety_pct",      "MoS % (DCF, TTM)"),
    ("owner_earnings", "TTM", "intrinsic_value_per_share", "Owner Earnings Value/Share (TTM)"),
    ("owner_earnings", "TTM", "margin_of_safety_pct",      "MoS % (Owner Earnings, TTM)"),
]

# Detect the actual financials_metrics schema (may have fiscal_year or year)
try:
    metrics_cols = {f.name for f in spark.table(metrics_tbl).schema.fields}
except Exception:
    metrics_cols = set()

year_col = "fiscal_year" if "fiscal_year" in metrics_cols else "year"
print(f"  financials_metrics year column: '{year_col}'")

exposed_frames = []
if len(iv_pdf):
    for method, ptype, field, metric_label in EXPOSED:
        subset = iv_pdf[
            (iv_pdf["method"] == method) & (iv_pdf["period_type"] == ptype)
        ][["ticker", "company", "fiscal_year", field]].copy()

        subset = subset.rename(columns={field: "value", "fiscal_year": year_col})
        subset["metric"] = metric_label
        subset = subset[["ticker", "company", year_col, "metric", "value"]]
        exposed_frames.append(subset)

# Absolute Owner Earnings (FY and TTM) — useful standalone metric. `oe_dollars` is already
# a column (vectorized, = _owner_earnings_dollars with _safe→0), never NaN → every row
# produces its metric, same as the previous per-row version. The dropna(value) below is a no-op here (never NaN).
oe_frames = []
for _pdf, ptype in ((fy_pdf, "FY"), (ttm_pdf, "TTM")):
    if not len(_pdf):
        continue
    _yr = [int(y) if pd.notna(y) else None for y in _pdf["year"].to_numpy()]
    oe_frames.append(pd.DataFrame({
        "ticker":  _pdf["ticker"].to_numpy(),
        "company": _pdf["company"].to_numpy(),
        year_col:  _yr,
        "metric":  f"Owner Earnings ({ptype})",
        "value":   _pdf["oe_dollars"].to_numpy(dtype=float),
    }))

exposed_frames.extend(oe_frames)

if exposed_frames:
    exposed_pdf = pd.concat(exposed_frames, ignore_index=True).dropna(subset=["value"])

    if metrics_cols:
        exposed_sdf = spark.createDataFrame(exposed_pdf)
        exposed_sdf.createOrReplaceTempView("incoming_iv_metrics")

        spark.sql(f"""
            MERGE INTO {metrics_tbl} AS target
            USING incoming_iv_metrics AS source
            ON  target.ticker = source.ticker
            AND target.{year_col} = source.{year_col}
            AND target.metric = source.metric

            WHEN MATCHED AND target.value != source.value THEN
                UPDATE SET target.value = source.value, target.company = source.company

            WHEN NOT MATCHED THEN
                INSERT (ticker, company, {year_col}, metric, value)
                VALUES (source.ticker, source.company, source.{year_col}, source.metric, source.value)
        """)

        print(f"✓ Exposed {len(exposed_pdf):,} rows in {metrics_tbl}")

        # ── 9b. Orphan cleanup ──────────────────────────────────────────────────────
        # Same rationale as 8b: the exposure MERGE only upserts, so a skipped method's
        # label rows (e.g. "Graham Number (FY)" / "MoS % (Graham Number, FY)") would
        # linger in financials_metrics. Delete IV-label rows absent from the fresh set,
        # for evaluated tickers only. The IN-list confines the delete to the intrinsic
        # labels this notebook owns — it never touches metrics produced by 22.
        _iv_labels = [lbl for *_, lbl in EXPOSED] + ["Owner Earnings (FY)", "Owner Earnings (TTM)"]
        _iv_labels_sql = ", ".join("'" + lbl.replace("'", "''") + "'" for lbl in _iv_labels)
        spark.sql(f"""
            MERGE INTO {metrics_tbl} AS t
            USING (
                SELECT t.ticker, t.{year_col} AS yr, t.metric
                FROM {metrics_tbl} t
                JOIN iv_processed_tickers p ON t.ticker = p.ticker
                WHERE t.metric IN ({_iv_labels_sql})
                  AND NOT EXISTS (
                    SELECT 1 FROM incoming_iv_metrics s
                    WHERE s.ticker = t.ticker AND s.{year_col} = t.{year_col}
                      AND s.metric = t.metric)
            ) AS s
            ON  t.ticker = s.ticker AND t.{year_col} = s.yr AND t.metric = s.metric
            WHEN MATCHED THEN DELETE
        """)
        print(f"✓ Orphan IV-metric cleanup on {metrics_tbl} complete")
    else:
        print(f"⊘ {metrics_tbl} not found — skipping exposure step (run 22__derived_metrics first).")
else:
    print("⊘ No metrics to expose.")

# COMMAND ----------

# MAGIC %md ## 10. Preview

# COMMAND ----------

spark.sql(f"""
    SELECT period_type, fiscal_year, method,
           ROUND(intrinsic_value_per_share, 2) AS iv_per_share,
           ROUND(price_close,               2) AS price,
           ROUND(margin_of_safety_pct,      1) AS mos_pct,
           period_end
    FROM {iv_tbl}
    WHERE ticker = 'AAPL'
    ORDER BY period_type DESC, fiscal_year DESC, method
    LIMIT 40
""").display()

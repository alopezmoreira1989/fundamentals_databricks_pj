# Databricks notebook source
# MAGIC %md
# MAGIC # 20__transformation / 22__derived_metrics
# MAGIC
# MAGIC Reads from the clean `financials` fact table (FY rows only) and computes:
# MAGIC - **Margins** — Gross, Operating, Net, FCF (as %)
# MAGIC - **Returns** — ROA, ROE, ROIC, ROCE, CROIC (as %, end-of-year denominators)
# MAGIC - **Free Cash Flow** — Operating CF − CapEx
# MAGIC - **YoY Growth** — Revenue, Net Income, Operating CF, FCF
# MAGIC - **Leverage** — Debt/Equity, Debt/Assets
# MAGIC - **Liquidity** — Current Ratio
# MAGIC - **Valuation multiples** — P/E, P/S, P/FCF, P/B, EV, EV/EBITDA *(requires `market_data`)*
# MAGIC - **Yields** — Earnings, Sales, FCF, Op Cash Flow, Book, EBITDA Yield *(requires `market_data`)*
# MAGIC
# MAGIC **Output table:** `{catalog}.{schema}.financials_metrics` (long format, one row per
# MAGIC ticker / fiscal_year / metric).

# COMMAND ----------

# MAGIC %run "../00__config/01__tickers"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

full_tbl    = f"{CATALOG}.{SCHEMA}.{TABLE}"
market_tbl  = f"{CATALOG}.{SCHEMA}.market_data"            # legacy (deprecated; not read here)
prices_tbl  = f"{CATALOG}.{SCHEMA}.market_prices_daily"   # daily price store — pricing source
splits_tbl  = f"{CATALOG}.{SCHEMA}.stock_splits"          # for split-adjusting cross-year share metrics
metrics_tbl = f"{CATALOG}.{SCHEMA}.financials_metrics"
mca_tbl     = f"{CATALOG}.{SCHEMA}.market_cap_asof"       # period_end-aligned price + market cap

# COMMAND ----------

# MAGIC %md ## 1. Read FY-only rows and pivot to wide format
# MAGIC
# MAGIC Guard against inconsistent company per ticker: the MERGE in `21__clean_and_merge.py`
# MAGIC only updates `company` when `value` changes, so old rows can retain a stale company
# MAGIC name (rare SEC escapes, LLC→Inc. restructures, favorites.json overrides).
# MAGIC We normalize to the first non-null per ticker before the pivot —
# MAGIC otherwise `groupBy(ticker, company, fy)` produces duplicate rows and the final MERGE
# MAGIC fails with DELTA_MULTIPLE_SOURCE_ROW_MATCHING.

# COMMAND ----------

_raw_full = (
    spark.table(full_tbl)
    .filter(F.col("period_type") == "FY")
    .filter(F.col("value").isNotNull())
)

_company_w = Window.partitionBy("ticker")
raw = _raw_full.withColumn(
    "company", F.first("company", ignorenulls=True).over(_company_w)
)

# Net Income is reported in BOTH the Income Statement and the Cash Flow statement (CF carries
# consolidated ProfitLoss), and the two can differ (parent-only vs consolidated). The pivot
# below groups by `concept` alone, so without this its Net Income would be a nondeterministic
# F.first() across the IS and CF rows. Pin it to the IS row when present so ROE / Net Margin
# here agree with the P/E in the valuation block (which takes Net Income from the IS). Targeted
# dedup of the Net-Income concept only — every other concept is single-statement, so the rest
# of the pivot is untouched. Filers reporting Net Income ONLY in the CF keep that row (no loss).
_ni = F.col("concept") == "Net Income"
_ni_pref = F.when(F.col("stmt") == "Income Statement", 0).otherwise(1)
_w_ni = Window.partitionBy("ticker", "fiscal_year").orderBy(_ni_pref.asc())
_ni_rows = (
    raw.filter(_ni)
    .withColumn("_rn", F.row_number().over(_w_ni))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)
raw = raw.filter(~_ni).unionByName(_ni_rows)

# `raw` (FY long-format facts, value-non-null, company resolved) is reused 3×: the `wide`
# pivot, the valuation concepts, and the company lookup downstream. Materialize once so those
# don't each re-scan `financials`. Serverless: localCheckpoint(eager), not .cache()/.persist().
raw = raw.localCheckpoint(eager=True)

wide = (
    raw
    .groupBy("ticker", "company", "fiscal_year")
    .pivot("concept")
    .agg(F.first("value"))
)

# Coalesce the two Revenue columns (companies use different XBRL tags)
if "Revenue" in wide.columns and "Revenue (contract)" in wide.columns:
    wide = wide.withColumn(
        "Revenue",
        F.coalesce(F.col("Revenue"), F.col("Revenue (contract)"))
    ).drop("Revenue (contract)")

# The `wide` pivot (FY full-scan + pivot over all concepts) is expensive and consumed
# 2×: the sanity count() here and the entire `metrics_wide` block downstream. localCheckpoint
# (eager) materializes it once. .cache()/.persist() do NOT work on serverless; localCheckpoint does.
wide = wide.localCheckpoint(eager=True)

print(f"Wide table: {wide.count():,} rows × {len(wide.columns)} columns")

# ── Split-adjusted diluted share count (internal `_shares_adj`) ────────────────────────────
# Used ONLY by the CROSS-YEAR share metrics — Net Buyback Yield % and the Piotroski no-dilution
# signal — where a stock split otherwise reads as a huge YoY jump (NVDA's 10:1 → +900% "dilution").
# factor(period_end) = ∏ ratio for splits with ex_date > period_end; Shares_adj = Shares × factor
# (see _core/splits.py). The most recent year has factor 1. NOT used by the market-cap path
# (asof_shares below stays raw) nor by per-row metrics (NCAV/Share, TBV/Share) — those are
# own-period-consistent. `_shares_adj` is internal (absent from base_metric_cols → never exported).
_fy_pe = raw.groupBy("ticker", "fiscal_year").agg(F.max("period_end").alias("_pe"))
try:
    _splits = (spark.table(splits_tbl)
               .select("ticker", "split_date", "ratio").filter(F.col("ratio") > 0))
    _split_factor = (
        _fy_pe.join(F.broadcast(_splits), on="ticker", how="left")
        .withColumn("_lr", F.when(F.col("split_date") > F.col("_pe"), F.log(F.col("ratio"))))
        .groupBy("ticker", "fiscal_year")
        .agg(F.exp(F.sum("_lr")).alias("_f"))                 # NULL when no qualifying split → 1.0
        .withColumn("_split_factor", F.coalesce(F.col("_f"), F.lit(1.0)))
        .select("ticker", "fiscal_year", "_split_factor")
    )
except Exception as _e:
    print(f"⚠ stock_splits unavailable ({_e}); cross-year share metrics use raw counts.")
    _split_factor = _fy_pe.select("ticker", "fiscal_year").withColumn("_split_factor", F.lit(1.0))

wide = (
    wide.join(_split_factor, on=["ticker", "fiscal_year"], how="left")
    .withColumn(
        "_shares_adj",
        F.when(
            F.col("Shares Diluted").isNotNull(),
            F.col("Shares Diluted") * F.coalesce(F.col("_split_factor"), F.lit(1.0)),
        ),
    )
    .drop("_split_factor")
)

# COMMAND ----------

# MAGIC %md ## 2. Compute margins, FCF, leverage, liquidity, returns

# COMMAND ----------

def safe_div(num, den):
    return F.when(
        F.col(den).isNotNull() & (F.col(den) != 0),
        F.col(num) / F.col(den)
    ).otherwise(F.lit(None))

# Guarded refs for concepts NOT already consumed unguarded in the base block: Inventory
# (Quick Ratio) and Total Liabilities (NCAV fallback) may be absent from the wide pivot for a
# small universe. lit(None) when absent → the dependent metric NULLs out instead of raising.
# (Total Current Assets/Liabilities, Total Assets, Total Stockholders Equity, Shares Diluted are
# already referenced unguarded above by Current Ratio / ROA / ROE / Net Buyback Yield, so present.)
_inv_expr = F.col("Inventory")         if "Inventory"         in wide.columns else F.lit(None)
_tl_expr  = F.col("Total Liabilities") if "Total Liabilities" in wide.columns else F.lit(None)
_gw_expr     = F.col("Goodwill")           if "Goodwill"           in wide.columns else F.lit(None)
_intang_expr = F.col("Intangible Assets")  if "Intangible Assets"  in wide.columns else F.lit(None)

metrics_wide = (
    wide
    # ── Margins (%) ───────────────────────────────────────────────────────────
    .withColumn("Gross Margin %",     safe_div("Gross Profit",    "Revenue") * 100)
    .withColumn("Operating Margin %", safe_div("Operating Income","Revenue") * 100)
    .withColumn("Net Margin %",       safe_div("Net Income",      "Revenue") * 100)

    # ── Free Cash Flow ────────────────────────────────────────────────────────
    .withColumn("Free Cash Flow",
        F.when(
            F.col("Operating Cash Flow").isNotNull() & F.col("CapEx").isNotNull(),
            F.col("Operating Cash Flow") - F.col("CapEx")
        )
    )

    # ── Leverage ──────────────────────────────────────────────────────────────
    # Total Debt = Long-term (noncurrent) + Short-term (current). A missing side is
    # treated as 0 (a co. may genuinely report only one). BUT if BOTH debt tags are
    # absent for the year we leave Total Debt NULL — reporting 0 would mask genuinely
    # missing data as "debt-free" and yield a misleading Debt/Equity ≈ 0.00x for a
    # levered issuer (the original AT&T symptom). NULL then propagates through safe_div
    # so Debt/Equity and Debt/Assets are NULL (absent) rather than 0.
    .withColumn("Total Debt",
        F.when(
            F.col("Long-term Debt").isNotNull() | F.col("Short-term Debt").isNotNull(),
            F.coalesce(F.col("Long-term Debt"),  F.lit(0)) +
            F.coalesce(F.col("Short-term Debt"), F.lit(0))
        )
    )
    .withColumn("Debt / Equity",  safe_div("Total Debt", "Total Stockholders Equity"))
    .withColumn("Debt / Assets",  safe_div("Total Debt", "Total Assets"))

    # ── Solvency / Coverage ─────────────────────────────────────────────────────
    # All reuse the existing `Total Debt` column (NULL when both debt tags absent).
    # EBITDA approximated as Operating Income + D&A (same definition as the EV/EBITDA
    # block). Interest Expense is stored as a positive magnitude → F.abs() is belt-and-
    # suspenders in case any issuer reports it signed. safe_div takes column NAMES, so the
    # intermediates are materialized as named columns first, then divided (not in
    # base_metric_cols — they're internals like _fcf in the val block).
    .withColumn("_ebitda_fh",
        F.when(
            F.col("Operating Income").isNotNull(),
            F.col("Operating Income")
            + F.coalesce(F.col("Depreciation & Amortization"), F.lit(0))
        )
    )
    .withColumn("_net_debt",
        F.when(
            F.col("Total Debt").isNotNull(),
            F.col("Total Debt")
            - F.coalesce(F.col("Cash & Equivalents"),     F.lit(0))
            - F.coalesce(F.col("Short-term Investments"), F.lit(0))
        )
    )
    # NULL when interest is absent or ~0 (net-cash firms like AAPL report negligible
    # interest, which would otherwise explode the ratio); safe_div NULLs on 0 denominator.
    .withColumn("_abs_interest",
        F.when(F.col("Interest Expense").isNotNull(), F.abs(F.col("Interest Expense")))
    )
    .withColumn("Interest Coverage", safe_div("Operating Income", "_abs_interest"))
    .withColumn("Net Debt / EBITDA", safe_div("_net_debt", "_ebitda_fh"))
    .withColumn("Cash Flow to Debt", safe_div("Operating Cash Flow", "Total Debt"))

    # ── Liquidity ─────────────────────────────────────────────────────────────
    .withColumn("Current Ratio",  safe_div("Total Current Assets", "Total Current Liabilities"))
    # Quick (acid-test) Ratio: liquid current assets (ex-inventory) over current liabilities.
    # Inventory missing → coalesce to 0 (treat as no inventory); NULL if either current-side
    # column is absent or Total Current Liabilities is 0.
    .withColumn("Quick Ratio",
        F.when(
            F.col("Total Current Assets").isNotNull()
            & F.col("Total Current Liabilities").isNotNull()
            & (F.col("Total Current Liabilities") != 0),
            (F.col("Total Current Assets") - F.coalesce(_inv_expr, F.lit(0)))
            / F.col("Total Current Liabilities")
        ))

    # ── Cash-flow margins ───────────────────────────────────────────────────
    .withColumn("FCF Margin %",          safe_div("Free Cash Flow",      "Revenue") * 100)
    .withColumn("Op Cash Flow Margin %", safe_div("Operating Cash Flow", "Revenue") * 100)

    # ── Returns (%) — end-of-year denominators ───────────────────────────────
    .withColumn("Invested Capital",
        F.coalesce(F.col("Total Debt"),                F.lit(0)) +
        F.coalesce(F.col("Total Stockholders Equity"), F.lit(0)) -
        F.coalesce(F.col("Cash & Equivalents"),        F.lit(0)) -
        F.coalesce(F.col("Short-term Investments"),    F.lit(0))
    )
    .withColumn("Capital Employed",
        F.when(
            F.col("Total Assets").isNotNull() & F.col("Total Current Liabilities").isNotNull(),
            F.col("Total Assets") - F.col("Total Current Liabilities")
        )
    )
    .withColumn("ROA %",   safe_div("Net Income",         "Total Assets")              * 100)
    .withColumn("ROE %",   safe_div("Net Income",         "Total Stockholders Equity") * 100)
    .withColumn("ROIC %",  safe_div("Operating Income",   "Invested Capital")          * 100)
    .withColumn("ROCE %",  safe_div("Operating Income",   "Capital Employed")          * 100)
    .withColumn("CROIC %", safe_div("Free Cash Flow",     "Invested Capital")          * 100)

    # ── Capital Returns — payout ratios (base block; no market data) ─────────────
    # SEC reports Dividends Paid / Share Repurchases as positive magnitudes (the repo
    # flips their sign only at display time), so wrap both in F.abs(...) to be
    # sign-agnostic. NULL guards are on the DENOMINATOR: a loss (Net Income ≤ 0) or a
    # negative FCF returns NULL instead of a misleading negative ratio. The two ADDITIVE
    # numerators coalesce a missing side to 0 so a dividend-only or buyback-only issuer
    # still gets a Total figure (a firm returning nothing → 0); single-source ratios stay
    # NULL when their one component is absent (a non-payer has no Dividend Payout Ratio).
    .withColumn("Dividend Payout Ratio",
        F.when(F.col("Net Income") > 0,
               F.abs(F.col("Dividends Paid")) / F.col("Net Income")))
    .withColumn("Buyback Payout Ratio",
        F.when(F.col("Net Income") > 0,
               F.abs(F.col("Share Repurchases")) / F.col("Net Income")))
    .withColumn("Total Payout Ratio",
        F.when(F.col("Net Income") > 0,
               (F.coalesce(F.abs(F.col("Dividends Paid")),    F.lit(0)) +
                F.coalesce(F.abs(F.col("Share Repurchases")), F.lit(0))) / F.col("Net Income")))
    .withColumn("Payout / FCF",
        F.when(F.col("Free Cash Flow") > 0,
               (F.coalesce(F.abs(F.col("Dividends Paid")),    F.lit(0)) +
                F.coalesce(F.abs(F.col("Share Repurchases")), F.lit(0))) / F.col("Free Cash Flow")))
    .withColumn("Dividend Coverage (FCF)",
        F.when(F.abs(F.col("Dividends Paid")) != 0,
               F.col("Free Cash Flow") / F.abs(F.col("Dividends Paid"))))

    # ── Quality & Risk — Accruals Ratio (base block; no market data) ────────────
    # (Net Income − Operating Cash Flow) / Total Assets. High positive accruals = earnings
    # not backed by cash → lower quality, so LOWER is better. safe_div takes column names,
    # so the signed numerator is materialized first; NULL if a component or Total Assets
    # is missing (no coalescing — a real gap should read NULL, not 0).
    .withColumn("_accruals_num", F.col("Net Income") - F.col("Operating Cash Flow"))
    .withColumn("Accruals Ratio", safe_div("_accruals_num", "Total Assets"))

    # ── Net-Net (Graham NCAV; base — no market data) ────────────────────────────
    # NCAV = Total Current Assets − Total Liabilities. Reuses the SAME Total-Liabilities
    # fallback as Altman Z (coalesce to Total Assets − Total Stockholders Equity) for issuers
    # like VZ that don't tag us-gaap:Liabilities directly. NOT clamped: NCAV is negative for
    # most firms — only genuine net-nets are positive, which is the whole signal.
    .withColumn("_ncav_liab",
        F.coalesce(
            _tl_expr,
            F.when(
                F.col("Total Assets").isNotNull() & F.col("Total Stockholders Equity").isNotNull(),
                F.col("Total Assets") - F.col("Total Stockholders Equity"),
            ),
        ))
    .withColumn("NCAV",
        F.when(
            F.col("Total Current Assets").isNotNull() & F.col("_ncav_liab").isNotNull(),
            F.col("Total Current Assets") - F.col("_ncav_liab"),
        ))
    .withColumn("NCAV / Share",
        F.when(
            F.col("NCAV").isNotNull()
            & F.col("Shares Diluted").isNotNull()
            & (F.col("Shares Diluted") != 0),
            F.col("NCAV") / F.col("Shares Diluted"),
        ))

    # ── Tangible Value & Goodwill Risk (base; no market data) ───────────────────
    # Re-materialize Goodwill/Intangible Assets as guaranteed-present columns (guards above
    # handle universes where a ticker never reports either tag) so downstream safe_div() calls
    # can reference them by name like any other column.
    .withColumn("Goodwill", _gw_expr)
    .withColumn("Intangible Assets", _intang_expr)
    # Tangible Book Value = reported equity stripped of Goodwill and Intangible Assets —
    # Graham's "real" floor on a liquidation basis. Goodwill/Intangibles coalesce a missing
    # side to 0 (a real, common case — not every company carries both); Total Stockholders
    # Equity itself is NOT coalesced, so TBV is NULL when equity is absent (mirrors P/B).
    .withColumn("Tangible Book Value",
        F.when(
            F.col("Total Stockholders Equity").isNotNull(),
            F.col("Total Stockholders Equity")
            - F.coalesce(F.col("Goodwill"), F.lit(0))
            - F.coalesce(F.col("Intangible Assets"), F.lit(0))
        ))
    .withColumn("Tangible Book Value / Share",
        F.when(
            F.col("Tangible Book Value").isNotNull()
            & F.col("Shares Diluted").isNotNull()
            & (F.col("Shares Diluted") != 0),
            F.col("Tangible Book Value") / F.col("Shares Diluted")
        ))
    # Goodwill / Total Assets %: composition risk — what fraction of the balance sheet is
    # unverifiable acquisition residue. Goodwill coalesced to 0 in the numerator: the
    # us-gaap:Goodwill tag is standard/reliable enough that "absent" reads as "true zero"
    # (unlike the multi-tag debt case, where absence was ambiguous).
    .withColumn("Goodwill / Total Assets %",
        safe_div("Goodwill", "Total Assets") * 100)
    # Goodwill / Tangible Equity %: how much of "real" equity Goodwill alone would wipe out
    # on a liquidation basis. >100% = Goodwill exceeds the entire tangible equity base, i.e.
    # the company is more intangible than real. Gated on Tangible Book Value > 0 — when TBV
    # is negative the ratio sign flips and becomes uninterpretable; the negative TBV itself
    # is the real signal there (surfaced via Tangible Book Value directly).
    .withColumn("Goodwill / Tangible Equity %",
        F.when(F.col("Tangible Book Value") > 0,
               safe_div("Goodwill", "Tangible Book Value") * 100))
    # ROTCE % — Return on Tangible Capital Employed. Mirrors ROCE % (Operating Income /
    # Capital Employed) with Goodwill + Intangibles stripped from the denominator. If this
    # comfortably exceeds the cost of capital, the excess return over ROCE is real,
    # capitalizable goodwill rather than an artifact of the intangible asset base.
    .withColumn("Tangible Capital Employed",
        F.when(
            F.col("Capital Employed").isNotNull(),
            F.col("Capital Employed")
            - F.coalesce(F.col("Goodwill"), F.lit(0))
            - F.coalesce(F.col("Intangible Assets"), F.lit(0))
        ))
    .withColumn("ROTCE %", safe_div("Operating Income", "Tangible Capital Employed") * 100)
    # Return on Tangible Equity % vs. ROE %: a large gap shows how much reported ROE is
    # inflated by the intangible base rather than real capital efficiency. Both columns land
    # in "Tangible Returns" next to ROE % in metrics_hierarchy.json for direct comparison.
    .withColumn("Return on Tangible Equity %",
        F.when(F.col("Tangible Book Value") > 0,
               safe_div("Net Income", "Tangible Book Value") * 100))
)

# COMMAND ----------

# MAGIC %md ## 3. YoY Growth metrics
# MAGIC Computed via window functions over `fiscal_year` for each ticker.

# COMMAND ----------

from pyspark.sql.window import Window

w_yoy = Window.partitionBy("ticker").orderBy("fiscal_year")

def yoy(col_name):
    prev = F.lag(F.col(col_name)).over(w_yoy)
    return F.when(
        prev.isNotNull() & (prev != 0),
        (F.col(col_name) - prev) / F.abs(prev) * 100
    )

metrics_wide = (
    metrics_wide
    .withColumn("Revenue YoY %",             yoy("Revenue"))
    .withColumn("Net Income YoY %",          yoy("Net Income"))
    .withColumn("Operating Cash Flow YoY %", yoy("Operating Cash Flow"))
    .withColumn("Free Cash Flow YoY %",      yoy("Free Cash Flow"))
    # Net Buyback Yield % = negative of the YoY % change in diluted share count.
    # A shrinking share count → positive yield. Dilution-aware (it nets SBC/issuance
    # against buybacks), unlike the gross cash-based Buyback Yield %. Reuses w_yoy via yoy().
    # Uses the SPLIT-ADJUSTED count so a split isn't mistaken for a +900% issuance.
    .withColumn("Net Buyback Yield %", -yoy("_shares_adj"))

    # ── Quality & Risk — Piotroski F-Score (base; uses w_yoy lag) ───────────────
    # Nine 1-pt signals. Profitability: ROA>0, CFO>0, ΔROA>0, CFO>NI (accrual quality).
    # Leverage/liquidity/dilution: ΔDebt/Assets<0, ΔCurrentRatio>0, no share dilution
    # (≤ +0.1% tolerance). Efficiency: ΔGrossMargin>0, ΔAssetTurnover>0. Each
    # F.when(cond,1).otherwise(0); the whole score is set NULL in a ticker's first year
    # (no prior to compare) via the lag(Total Assets) guard below — can't score 9 on year 1.
    # _pf_* / _pf_at are internals, NOT exported (absent from base_metric_cols).
    .withColumn("_pf_at", safe_div("Revenue", "Total Assets"))    # asset turnover (current yr)
    .withColumn("_pf_roa_pos", F.when(F.col("ROA %") > 0, 1).otherwise(0))
    .withColumn("_pf_cfo_pos", F.when(F.col("Operating Cash Flow") > 0, 1).otherwise(0))
    .withColumn("_pf_d_roa",   F.when(F.col("ROA %") > F.lag(F.col("ROA %")).over(w_yoy), 1).otherwise(0))
    .withColumn("_pf_accr",    F.when(F.col("Operating Cash Flow") > F.col("Net Income"), 1).otherwise(0))
    .withColumn("_pf_d_lev",   F.when(F.col("Debt / Assets") < F.lag(F.col("Debt / Assets")).over(w_yoy), 1).otherwise(0))
    .withColumn("_pf_d_cr",    F.when(F.col("Current Ratio") > F.lag(F.col("Current Ratio")).over(w_yoy), 1).otherwise(0))
    .withColumn("_pf_no_dil",  F.when(F.col("_shares_adj") <= F.lag(F.col("_shares_adj")).over(w_yoy) * 1.001, 1).otherwise(0))
    .withColumn("_pf_d_gm",    F.when(F.col("Gross Margin %") > F.lag(F.col("Gross Margin %")).over(w_yoy), 1).otherwise(0))
    .withColumn("_pf_d_at",    F.when(F.col("_pf_at") > F.lag(F.col("_pf_at")).over(w_yoy), 1).otherwise(0))
    .withColumn("Piotroski F-Score",
        F.when(
            F.lag(F.col("Total Assets")).over(w_yoy).isNotNull(),   # need a prior year to score
            (F.col("_pf_roa_pos") + F.col("_pf_cfo_pos") + F.col("_pf_d_roa")
             + F.col("_pf_accr") + F.col("_pf_d_lev") + F.col("_pf_d_cr")
             + F.col("_pf_no_dil") + F.col("_pf_d_gm") + F.col("_pf_d_at"))
        ).cast("double")   # double so the unpivot stack() matches the other (double) metrics
    )
)

# Outlier caps for the coverage ratios most prone to blow-ups (mirrors the EV/EBITDA
# |x|>500 filter in the val block). Cash Flow to Debt is left uncapped — it's naturally
# bounded for any real debt load; a huge value just means trivially small debt.
metrics_wide = (
    metrics_wide
    .withColumn("Interest Coverage",
        F.when(F.abs(F.col("Interest Coverage")) > 1000, F.lit(None)).otherwise(F.col("Interest Coverage")))
    .withColumn("Net Debt / EBITDA",
        F.when(F.abs(F.col("Net Debt / EBITDA")) > 100, F.lit(None)).otherwise(F.col("Net Debt / EBITDA")))
)

# COMMAND ----------

# MAGIC %md ## 4. Unpivot to long format (without valuation)

# COMMAND ----------

base_metric_cols = [
    # Profitability — Margins
    "Gross Margin %", "Operating Margin %", "Net Margin %", "FCF Margin %",
    # Returns
    "ROA %", "ROE %", "ROIC %", "ROCE %", "CROIC %",
    # Cash Flow — Absolute (passthrough from the wide FY pivot) + cash-flow margin
    "Operating Cash Flow", "CapEx", "Free Cash Flow",
    "Op Cash Flow Margin %",
    # Growth — YoY
    "Revenue YoY %", "Net Income YoY %", "Operating Cash Flow YoY %", "Free Cash Flow YoY %",
    # Leverage
    "Debt / Equity", "Debt / Assets",
    # Financial Health — Solvency / Coverage
    "Interest Coverage", "Net Debt / EBITDA", "Cash Flow to Debt",
    # Liquidity
    "Current Ratio", "Quick Ratio",
    # Capital Returns — payout ratios + net buyback yield (all base / no market data)
    "Dividend Payout Ratio", "Buyback Payout Ratio", "Total Payout Ratio",
    "Payout / FCF", "Dividend Coverage (FCF)", "Net Buyback Yield %",
    # Quality & Risk — base (Altman Z is market-gated → val block / val_metric_cols)
    "Piotroski F-Score", "Accruals Ratio",
    # Net-Net — base (NCAV Ratio is market-gated → val block / val_metric_cols)
    "NCAV", "NCAV / Share",
    # Tangible Value & Goodwill Risk
    "Tangible Book Value", "Tangible Book Value / Share",
    "Goodwill / Total Assets %", "Goodwill / Tangible Equity %",
    "ROTCE %", "Return on Tangible Equity %",
]

def unpivot(df, metric_cols):
    stack_expr = "stack({n}, {pairs}) AS (metric, value)".format(
        n=len(metric_cols),
        pairs=", ".join([f"'{c}', `{c}`" for c in metric_cols])
    )
    return df.select(
        "ticker", "company", "fiscal_year",
        F.expr(stack_expr)
    ).filter(F.col("value").isNotNull())

long_base = unpivot(metrics_wide, base_metric_cols)
# long_base is reused 4×: count() here, the unionByName with long_val, the final count(), and the MERGE.
# Materializing avoids re-deriving the entire metrics block (withColumns + stack) each time.
long_base = long_base.localCheckpoint(eager=True)
print(f"Base metrics long: {long_base.count():,} rows")

# COMMAND ----------

# MAGIC %md ## 5. Valuation metrics & Yields (requires `market_prices_daily`)
# MAGIC
# MAGIC `market_cap` is computed **as-of each FY `period_end`** from the daily price store —
# MAGIC NOT the calendar-Dec-31 `market_data` join. This removes the 0–11 month fiscal/calendar
# MAGIC offset that distorted every multiple for non-December filers. `market_cap` = raw `close`
# MAGIC on the latest trading day ≤ `period_end` × `Shares Diluted` reported as-of `period_end`.
# MAGIC Raw `close` (not `adj_close`): the historical market cap that actually existed at `t` uses
# MAGIC the unadjusted price; `adj_close` would fold future splits back into it.

# COMMAND ----------

try:
    _prices = (
        spark.table(prices_tbl)
        .select("ticker", "date", "close")
        .filter(F.col("close").isNotNull())
    )
    _has_prices = _prices.limit(1).count() > 0
except Exception:
    _has_prices = False

if not _has_prices:
    print("⚠ market_prices_daily not found/empty — skipping valuation metrics.")
    pe_mcap = None
else:
    # FY period_end per (ticker, fiscal_year). One fiscal close per FY; MAX is defensive.
    # ~30k rows → broadcast it on both as-of joins below so the multi-million-row daily
    # price / shares stores aren't shuffled on `ticker` before the date filter.
    fy_dates = F.broadcast(
        spark.table(full_tbl)
        .filter(F.col("period_type") == "FY")
        .groupBy("ticker", "fiscal_year")
        .agg(F.max("period_end").alias("period_end"))
    )

    # As-of close: latest trading day on/before period_end (handles weekends/holidays).
    _w_px = Window.partitionBy("ticker", "fiscal_year").orderBy(F.col("date").desc())
    asof_close = (
        fy_dates.join(_prices, on="ticker", how="inner")
        .filter(F.col("date") <= F.col("period_end"))
        .withColumn("_rn", F.row_number().over(_w_px))
        .filter(F.col("_rn") == 1)
        .select("ticker", "fiscal_year", F.col("close").alias("asof_close"))
    )

    # As-of shares: most recent Shares Diluted (ANY period_type) with period_end ≤ FY period_end;
    # FY-preferred on a tie so a normal FY uses its own reported figure, falling back across
    # periods when the FY value is NULL (approved fallback). NOTE: Shares Diluted is the
    # weighted-average diluted count, not shares-outstanding-at-date — see the out-of-scope note.
    _sh = (
        spark.table(full_tbl)
        .filter((F.col("concept") == "Shares Diluted") & F.col("value").isNotNull())
        .select("ticker", F.col("period_end").alias("sh_end"),
                F.col("value").alias("shares"),
                (F.col("period_type") == "FY").alias("_is_fy"))
    )
    _w_sh = Window.partitionBy("ticker", "fiscal_year").orderBy(
        F.col("sh_end").desc(), F.col("_is_fy").desc())
    asof_shares = (
        fy_dates.join(_sh, on="ticker", how="inner")
        .filter(F.col("sh_end") <= F.col("period_end"))
        .withColumn("_rn", F.row_number().over(_w_sh))
        .filter(F.col("_rn") == 1)
        .select("ticker", "fiscal_year", F.col("shares").alias("asof_shares"))
    )

    pe_mcap = (
        asof_close.join(asof_shares, on=["ticker", "fiscal_year"], how="inner")
        .withColumn("market_cap",
            F.when(
                F.col("asof_close").isNotNull() & F.col("asof_shares").isNotNull(),
                F.col("asof_close") * F.col("asof_shares"),
            ))
        .filter(F.col("market_cap").isNotNull())
        .select("ticker", "fiscal_year", "market_cap")
    )
    # Range-join over the daily store is reused by every downstream count/MERGE; materialize
    # once. Serverless: localCheckpoint(eager), not .cache()/.persist().
    pe_mcap = pe_mcap.localCheckpoint(eager=True)
    print(f"✓ period_end-aligned market_cap — {pe_mcap.count():,} (ticker, fy) rows")

    # Persist the period_end-aligned price + market cap as a small table so the intrinsic-value
    # step (23) and the export (51) can read fiscal-close-correct values instead of the legacy,
    # CALENDAR-aligned `market_data` (which `12` no longer rebuilds). Keyed by (ticker,
    # fiscal_year) like market_data, but `period_end` is the REAL fiscal close and `price_close`
    # / `market_cap` are priced as-of it (the same basis as every multiple in financials_metrics).
    # Full overwrite each run — 22 recomputes all FY. saveAsTable(overwrite) creates if absent
    # (no MERGE → no schema-evolution trap; no CREATE SCHEMA — main.financials is provisioned).
    market_cap_asof = (
        fy_dates
        .join(asof_close, on=["ticker", "fiscal_year"], how="inner")
        .join(pe_mcap,    on=["ticker", "fiscal_year"], how="inner")
        .select(
            "ticker", "fiscal_year", "period_end",
            F.col("asof_close").alias("price_close"),
            "market_cap",
        )
    )
    (market_cap_asof.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(mca_tbl))
    print(f"✓ wrote {mca_tbl}")

# COMMAND ----------

if pe_mcap is not None:

    # Reuse the already-materialized `raw` (FY rows, value-non-null) instead of re-scanning
    # `financials` — the explicit per-concept stmt filters below are unchanged, so the selected
    # (concept, stmt) values are identical (Net Income from IS, OCF/CapEx/D&A from CF, etc.).
    val_concepts = (
        raw
        .filter(
            (
                (F.col("stmt") == "Income Statement")
                & (F.col("concept").isin("Net Income", "Revenue", "Operating Income"))
            ) | (
                (F.col("stmt") == "Cash Flow")
                & (F.col("concept").isin("Operating Cash Flow", "CapEx", "Depreciation & Amortization",
                                          "Dividends Paid", "Share Repurchases"))
            ) | (
                (F.col("stmt") == "Balance Sheet")
                & (F.col("concept").isin(
                    "Total Stockholders Equity", "Cash & Equivalents",
                    "Short-term Investments", "Long-term Debt", "Short-term Debt",
                    # Altman Z inputs (Revenue / Operating Income already in the IS branch):
                    "Total Assets", "Total Liabilities", "Total Current Assets",
                    "Total Current Liabilities", "Retained Earnings",
                    # Tangible Value / Goodwill Risk inputs:
                    "Goodwill", "Intangible Assets"
                ))
            )
        )
        .filter(F.col("value").isNotNull())
    )

    val_wide = (
        val_concepts
        .groupBy("ticker", "fiscal_year")
        .pivot("concept")
        .agg(F.first("value"))
        .join(pe_mcap, on=["ticker", "fiscal_year"], how="inner")
    )

    def safe_div_col(num, den):
        return F.when(
            den.isNotNull() & (den != 0),
            num / den
        ).otherwise(F.lit(None))

    val_wide = (
        val_wide
        .withColumn("_fcf",
            F.when(
                F.col("Operating Cash Flow").isNotNull() & F.col("CapEx").isNotNull(),
                F.col("Operating Cash Flow") - F.col("CapEx")
            )
        )
        # EV debt bridge. Reads the SAME "Long-term Debt"/"Short-term Debt" concept
        # columns from `financials`, so it AUTOMATICALLY picks up the per-period tag
        # fallback fixed in 11__fetch_sec_xbrl.extract_series_multi — AT&T/VZ now carry
        # their real long-term debt here instead of NULL. Unlike the leverage "Total Debt"
        # above we deliberately keep coalesce-to-0 (not NULL): EV must stay defined for
        # genuinely low-/no-debt firms, and the understatement risk for levered issuers is
        # gone now that their debt is captured per year.
        .withColumn("_total_debt",
            F.coalesce(F.col("Long-term Debt"),  F.lit(0))
            + F.coalesce(F.col("Short-term Debt"), F.lit(0))
        )
        .withColumn("_ev",
            F.col("market_cap")
            + F.col("_total_debt")
            - F.coalesce(F.col("Cash & Equivalents"),     F.lit(0))
            - F.coalesce(F.col("Short-term Investments"), F.lit(0))
        )
        .withColumn("_ebitda",
            F.when(
                F.col("Operating Income").isNotNull(),
                F.col("Operating Income")
                + F.coalesce(F.col("Depreciation & Amortization"), F.lit(0))
            )
        )
        # Tangible Book Value, recomputed here (val_wide does NOT see the base-block column).
        # Goodwill/Intangible Assets coalesce to 0 — same true-zero rationale as the base block.
        .withColumn("_tbv_val",
            F.when(
                F.col("Total Stockholders Equity").isNotNull(),
                F.col("Total Stockholders Equity")
                - F.coalesce(F.col("Goodwill"), F.lit(0))
                - F.coalesce(F.col("Intangible Assets"), F.lit(0))
            )
        )
    )

    val_metric_cols = [
        "P/E", "P/S", "P/FCF", "P/B", "EV", "EV/EBITDA",
        "Earnings Yield %", "Sales Yield %", "FCF Yield %",
        "Op Cash Flow Yield %", "Book Yield %", "EBITDA Yield %",
        "Dividend Yield %", "Buyback Yield %", "Shareholder Yield %",
        "Altman Z-Score",
        "NCAV Ratio",
        "Price / Tangible Book Value", "Goodwill / Market Cap %",
    ]

    val_metrics = (
        val_wide
        # Price multiples
        # NULL when Net Income ≤ 0: a negative P/E is meaningless (loss-making company).
        # safe_div_col only guards 0/missing denominators, so the >0 gate is added here and
        # NOT in safe_div_col (other multiples — P/B, P/FCF — accept negative denominators).
        .withColumn("P/E",
            F.when(
                F.col("Net Income").isNotNull() & (F.col("Net Income") > 0),
                safe_div_col(F.col("market_cap"), F.col("Net Income")),
            ))
        .withColumn("P/S",       safe_div_col(F.col("market_cap"), F.col("Revenue")))
        .withColumn("P/FCF",     safe_div_col(F.col("market_cap"), F.col("_fcf")))
        .withColumn("P/B",       safe_div_col(F.col("market_cap"), F.col("Total Stockholders Equity")))
        .withColumn("EV",        F.col("_ev"))
        .withColumn("EV/EBITDA", safe_div_col(F.col("_ev"),        F.col("_ebitda")))
        # Price / Tangible Book Value — Graham's preferred substitute for P/B when Goodwill is
        # large. NOT gated on _tbv_val > 0: same convention as P/B above, which is also
        # ungated — a negative tangible book value still produces a (negative) ratio, which is
        # itself the signal (balance-sheet insolvent on a tangible basis).
        .withColumn("Price / Tangible Book Value", safe_div_col(F.col("market_cap"), F.col("_tbv_val")))
        # Goodwill / Market Cap %: how much of what you're paying for today is unverifiable
        # accounting residue. Goodwill coalesced to 0 — true-zero rationale, see base block.
        .withColumn("Goodwill / Market Cap %",
            safe_div_col(F.coalesce(F.col("Goodwill"), F.lit(0)), F.col("market_cap")) * 100)
        # Yields
        # NULL when Net Income ≤ 0: a negative earnings yield is equally misleading
        # (inverse of P/E — same loss-making guard).
        .withColumn("Earnings Yield %",
            F.when(
                F.col("Net Income").isNotNull() & (F.col("Net Income") > 0),
                safe_div_col(F.col("Net Income"), F.col("market_cap")) * 100,
            ))
        .withColumn("Sales Yield %",        safe_div_col(F.col("Revenue"),                   F.col("market_cap")) * 100)
        .withColumn("FCF Yield %",          safe_div_col(F.col("_fcf"),                      F.col("market_cap")) * 100)
        .withColumn("Op Cash Flow Yield %", safe_div_col(F.col("Operating Cash Flow"),       F.col("market_cap")) * 100)
        .withColumn("Book Yield %",         safe_div_col(F.col("Total Stockholders Equity"), F.col("market_cap")) * 100)
        .withColumn("EBITDA Yield %",       safe_div_col(F.col("_ebitda"),                   F.col("market_cap")) * 100)
        # Capital Returns — cash yields. Sign-agnostic via F.abs; the additive Shareholder
        # Yield coalesces a missing side to 0 so buyback-only / dividend-only firms still
        # get a value. Single yields stay NULL when their component is absent.
        .withColumn("Dividend Yield %",     safe_div_col(F.abs(F.col("Dividends Paid")),    F.col("market_cap")) * 100)
        .withColumn("Buyback Yield %",      safe_div_col(F.abs(F.col("Share Repurchases")), F.col("market_cap")) * 100)
        .withColumn("Shareholder Yield %",  safe_div_col(
            F.coalesce(F.abs(F.col("Dividends Paid")),    F.lit(0)) +
            F.coalesce(F.abs(F.col("Share Repurchases")), F.lit(0)),    F.col("market_cap")) * 100)
        # ── Quality & Risk — Altman Z-Score (market-gated; X4 needs market_cap) ─────
        # Original 5-factor MANUFACTURING model. Less meaningful for financial firms /
        # non-manufacturers (known limitation, same caveat class as EV/EBITDA for banks) —
        # documented, not sector-branched. NULL unless Total Assets and Total Liabilities are
        # both present and > 0; no coalescing, so any missing component (e.g. Retained
        # Earnings, working-capital pieces) makes Z NULL rather than silently understating it.
        .withColumn("_altman_wc",
            F.when(
                F.col("Total Current Assets").isNotNull() & F.col("Total Current Liabilities").isNotNull(),
                F.col("Total Current Assets") - F.col("Total Current Liabilities")
            ))
        # Total Liabilities fallback: many issuers (e.g. VZ) don't tag us-gaap:Liabilities
        # directly (they report Total Liabilities & Equity instead), which left Altman Z NULL.
        # Fall back to the balance-sheet identity Total Assets − Total Stockholders Equity so
        # X4 can still compute. Approximation: with attributable-only equity this folds any
        # (usually small) noncontrolling interest into liabilities — acceptable for a risk score.
        .withColumn("_total_liab",
            F.coalesce(
                F.col("Total Liabilities"),
                F.when(
                    F.col("Total Assets").isNotNull() & F.col("Total Stockholders Equity").isNotNull(),
                    F.col("Total Assets") - F.col("Total Stockholders Equity")
                )
            ))
        .withColumn("Altman Z-Score",
            F.when(
                (F.col("Total Assets") > 0) & (F.col("_total_liab") > 0),
                1.2 * (F.col("_altman_wc")        / F.col("Total Assets"))
              + 1.4 * (F.col("Retained Earnings") / F.col("Total Assets"))
              + 3.3 * (F.col("Operating Income")  / F.col("Total Assets"))
              + 0.6 * (F.col("market_cap")        / F.col("_total_liab"))
              + 1.0 * (F.col("Revenue")           / F.col("Total Assets"))
            ))
        # ── Net-Net — NCAV Ratio (market-gated; needs market_cap) ───────────────────
        # Price-over-NCAV, matching the P/E, P/S, P/B convention: lower = cheaper, < 1 = market
        # cap below net current assets, classic net-net buy ≈ ≤ 0.67. Reuses the Altman
        # `_total_liab` (same Total-Liabilities fallback). ONLY defined when _ncav > 0 — a
        # negative-NCAV firm (the vast majority) yields NULL, never a misleading negative ratio.
        .withColumn("_ncav",
            F.when(
                F.col("Total Current Assets").isNotNull(),
                F.col("Total Current Assets") - F.col("_total_liab"),
            ))
        .withColumn("NCAV Ratio",
            F.when(F.col("_ncav") > 0, F.col("market_cap") / F.col("_ncav")))
    )

    # Filter EV/EBITDA outliers (|x| > 500)
    val_metrics = val_metrics.withColumn(
        "EV/EBITDA",
        F.when(F.abs(F.col("EV/EBITDA")) > 500, F.lit(None)).otherwise(F.col("EV/EBITDA"))
    )

    # Need company column for downstream join — reuse the materialized `raw` (company already
    # resolved to the first non-null per ticker by the window above) instead of re-scanning
    # financials. One row per ticker so the left join can't duplicate.
    company_lookup = (
        raw
        .groupBy("ticker")
        .agg(F.first("company", ignorenulls=True).alias("company"))
    )
    val_metrics = val_metrics.join(company_lookup, on="ticker", how="left")

    stack_expr = "stack({n}, {pairs}) AS (metric, value)".format(
        n=len(val_metric_cols),
        pairs=", ".join([f"'{c}', `{c}`" for c in val_metric_cols])
    )
    long_val = val_metrics.select(
        "ticker", "company", "fiscal_year",
        F.expr(stack_expr)
    ).filter(F.col("value").isNotNull())

    # long_val carries its own FY full-scan + val pivot + withColumns + stack, and is reused 4×
    # (count here, union, final count, MERGE). Checkpoint to avoid recomputing the val pivot each time.
    long_val = long_val.localCheckpoint(eager=True)
    print(f"Valuation metrics long: {long_val.count():,} rows")
else:
    long_val = None

# COMMAND ----------

# MAGIC %md ## 6. Combine & write

# COMMAND ----------

if long_val is not None:
    final_long = long_base.unionByName(long_val)
else:
    final_long = long_base

print(f"Final metrics: {final_long.count():,} rows")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {metrics_tbl} (
        ticker       STRING    NOT NULL,
        company      STRING,
        fiscal_year  INT       NOT NULL,
        metric       STRING    NOT NULL,
        value        DOUBLE
    )
    USING DELTA
    PARTITIONED BY (ticker)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

final_long.createOrReplaceTempView("incoming_metrics")

# `unpivot()` drops NULL-valued metrics before they ever reach `incoming_metrics` (NULL = "absent",
# no row stored). Combined with `raw`/`_raw_full` above always reading the FULL financials table
# (no tickers_override in this notebook — every run recomputes the entire universe), that means a
# row already in `target` whose metric later recomputes to NULL (e.g. a leverage-formula fix that
# now treats a missing debt tag as NULL instead of 0) has no corresponding source row: `WHEN MATCHED`
# can't fire (no match) and a plain MERGE has no other path to remove it, so the stale value would
# persist forever even though the current code no longer produces it. `WHEN NOT MATCHED BY SOURCE`
# deletes exactly those orphans. Safe here specifically because source is a full-universe rebuild
# each run — a partial/incremental source would incorrectly delete untouched tickers.
spark.sql(f"""
    MERGE INTO {metrics_tbl} AS target
    USING incoming_metrics AS source
    ON  target.ticker      = source.ticker
    AND target.fiscal_year = source.fiscal_year
    AND target.metric      = source.metric

    WHEN MATCHED AND target.value != source.value THEN
        UPDATE SET
            target.value   = source.value,
            target.company = source.company

    WHEN NOT MATCHED THEN
        INSERT (ticker, company, fiscal_year, metric, value)
        VALUES (source.ticker, source.company, source.fiscal_year, source.metric, source.value)

    WHEN NOT MATCHED BY SOURCE THEN
        DELETE
""")

print(f"✓ MERGE complete → {metrics_tbl}")

# COMMAND ----------

# MAGIC %md ## 7. Verify

# COMMAND ----------

spark.sql(f"""
    SELECT
        metric,
        COUNT(*)              AS n_rows,
        COUNT(DISTINCT ticker) AS n_tickers,
        ROUND(MIN(value), 2)  AS min_val,
        ROUND(MAX(value), 2)  AS max_val
    FROM {metrics_tbl}
    GROUP BY metric
    ORDER BY metric
""").display()

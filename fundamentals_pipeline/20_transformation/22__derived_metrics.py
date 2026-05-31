# Databricks notebook source
# MAGIC %md
# MAGIC # 20_transformation / 22__derived_metrics
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

# MAGIC %run "../00_config/01__tickers"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

full_tbl    = f"{CATALOG}.{SCHEMA}.{TABLE}"
market_tbl  = f"{CATALOG}.{SCHEMA}.market_data"
metrics_tbl = f"{CATALOG}.{SCHEMA}.financials_metrics"

# COMMAND ----------

# MAGIC %md ## 1. Read FY-only rows and pivot to wide format
# MAGIC
# MAGIC Defensa contra company inconsistente por ticker: el MERGE de `21__clean_and_merge.py`
# MAGIC solo actualiza `company` cuando cambia `value`, así que filas viejas pueden conservar
# MAGIC un company stale (escapes raros de SEC, restructures LLC→Inc., overrides en
# MAGIC favorites.json). Aquí normalizamos al primer non-null por ticker antes del pivot —
# MAGIC si no, el `groupBy(ticker, company, fy)` produce filas duplicadas y el MERGE final
# MAGIC peta con DELTA_MULTIPLE_SOURCE_ROW_MATCHING.

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

print(f"Wide table: {wide.count():,} rows × {len(wide.columns)} columns")

# COMMAND ----------

# MAGIC %md ## 2. Compute margins, FCF, leverage, liquidity, returns

# COMMAND ----------

def safe_div(num, den):
    return F.when(
        F.col(den).isNotNull() & (F.col(den) != 0),
        F.col(num) / F.col(den)
    ).otherwise(F.lit(None))

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

    # ── Liquidity ─────────────────────────────────────────────────────────────
    .withColumn("Current Ratio",  safe_div("Total Current Assets", "Total Current Liabilities"))

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
    # Liquidity
    "Current Ratio",
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
print(f"Base metrics long: {long_base.count():,} rows")

# COMMAND ----------

# MAGIC %md ## 5. Valuation metrics & Yields (requires `market_data`)

# COMMAND ----------

try:
    mkt = (
        spark.table(market_tbl)
        .select("ticker", "fiscal_year", "market_cap")
        .filter(F.col("market_cap").isNotNull())
    )
    print(f"✓ market_data loaded — {mkt.count():,} rows")
except Exception:
    print("⚠ market_data not found — skipping valuation metrics.")
    mkt = None

# COMMAND ----------

if mkt is not None:

    val_concepts = (
        spark.table(full_tbl)
        .filter(F.col("period_type") == "FY")
        .filter(
            (
                (F.col("stmt") == "Income Statement")
                & (F.col("concept").isin("Net Income", "Revenue", "Operating Income"))
            ) | (
                (F.col("stmt") == "Cash Flow")
                & (F.col("concept").isin("Operating Cash Flow", "CapEx", "Depreciation & Amortization"))
            ) | (
                (F.col("stmt") == "Balance Sheet")
                & (F.col("concept").isin(
                    "Total Stockholders Equity", "Cash & Equivalents",
                    "Short-term Investments", "Long-term Debt", "Short-term Debt"
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
        .join(mkt, on=["ticker", "fiscal_year"], how="inner")
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
    )

    val_metric_cols = [
        "P/E", "P/S", "P/FCF", "P/B", "EV", "EV/EBITDA",
        "Earnings Yield %", "Sales Yield %", "FCF Yield %",
        "Op Cash Flow Yield %", "Book Yield %", "EBITDA Yield %",
    ]

    val_metrics = (
        val_wide
        # Price multiples
        .withColumn("P/E",       safe_div_col(F.col("market_cap"), F.col("Net Income")))
        .withColumn("P/S",       safe_div_col(F.col("market_cap"), F.col("Revenue")))
        .withColumn("P/FCF",     safe_div_col(F.col("market_cap"), F.col("_fcf")))
        .withColumn("P/B",       safe_div_col(F.col("market_cap"), F.col("Total Stockholders Equity")))
        .withColumn("EV",        F.col("_ev"))
        .withColumn("EV/EBITDA", safe_div_col(F.col("_ev"),        F.col("_ebitda")))
        # Yields
        .withColumn("Earnings Yield %",     safe_div_col(F.col("Net Income"),                F.col("market_cap")) * 100)
        .withColumn("Sales Yield %",        safe_div_col(F.col("Revenue"),                   F.col("market_cap")) * 100)
        .withColumn("FCF Yield %",          safe_div_col(F.col("_fcf"),                      F.col("market_cap")) * 100)
        .withColumn("Op Cash Flow Yield %", safe_div_col(F.col("Operating Cash Flow"),       F.col("market_cap")) * 100)
        .withColumn("Book Yield %",         safe_div_col(F.col("Total Stockholders Equity"), F.col("market_cap")) * 100)
        .withColumn("EBITDA Yield %",       safe_div_col(F.col("_ebitda"),                   F.col("market_cap")) * 100)
    )

    # Filter EV/EBITDA outliers (|x| > 500)
    val_metrics = val_metrics.withColumn(
        "EV/EBITDA",
        F.when(F.abs(F.col("EV/EBITDA")) > 500, F.lit(None)).otherwise(F.col("EV/EBITDA"))
    )

    # Need company column for downstream join — pull from financials.
    # Misma defensa que arriba: si un ticker tiene >1 valor de company en financials
    # (escapes raros de SEC, restructures, etc.), .distinct() devolvería múltiples
    # filas y el join duplicaría. Tomamos el primer non-null por ticker.
    company_lookup = (
        spark.table(full_tbl)
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

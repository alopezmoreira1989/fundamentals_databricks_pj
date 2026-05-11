# Databricks notebook source
# MAGIC %md
# MAGIC # 20_transformation / 22_derived_metrics
# MAGIC
# MAGIC Reads from the clean `financials` fact table and computes:
# MAGIC - **Margins** — Gross, Operating, Net (as %)
# MAGIC - **Free Cash Flow** — Operating CF − CapEx
# MAGIC - **YoY Growth** — Revenue, Net Income, Operating CF
# MAGIC - **Leverage** — Debt/Equity, Debt/Assets
# MAGIC - **Liquidity** — Current Ratio
# MAGIC
# MAGIC Output table: `{catalog}.{schema}.financials_metrics`
# MAGIC Same long format: ticker | company | year | metric | value

# COMMAND ----------
# MAGIC %md ### ⚠️ Path note
# MAGIC `%run` paths are **relative to this notebook's location** in the Databricks workspace.
# MAGIC If you get a `NameError`, adjust the path below to match your folder structure.
# MAGIC Example: if this notebook is at `FA_PJ/10_ingestion/11__fetch_sec_xbrl`,
# MAGIC the config path should be `../00_config/01__tickers`.

# COMMAND ----------

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/FA PJ (Basic)/00_config/01__tickers"

# COMMAND ----------
# Validate config loaded — if this fails, fix the %run path in the cell above
try:
    _ = ACTIVE_TICKERS
    print(f"✓ Config loaded — {len(ACTIVE_TICKERS)} active tickers")
except NameError:
    raise NameError(
        "ACTIVE_TICKERS not defined — the %run above did not load 00_tickers correctly.\n"
        f"Current path used: '../00_config/01__tickers'\n"
        "Fix: right-click 00_tickers in your workspace → Copy URL/Path, "
        "then adjust the %run path to match."
    )

# COMMAND ----------

full_table  = f"{CATALOG}.{SCHEMA}.{TABLE}"
metrics_tbl = f"{CATALOG}.{SCHEMA}.financials_metrics"

# COMMAND ----------
# MAGIC %md ### Pull the concepts we need into a wide pivot

# COMMAND ----------

needed_concepts = [
    # Income Statement
    ("Income Statement", "Revenue"),
    ("Income Statement", "Revenue (contract)"),
    ("Income Statement", "Gross Profit"),
    ("Income Statement", "Operating Income"),
    ("Income Statement", "Net Income"),
    # Balance Sheet
    ("Balance Sheet",    "Total Current Assets"),
    ("Balance Sheet",    "Total Current Liabilities"),
    ("Balance Sheet",    "Total Assets"),
    ("Balance Sheet",    "Total Liabilities"),
    ("Balance Sheet",    "Total Stockholders Equity"),
    ("Balance Sheet",    "Long-term Debt"),
    ("Balance Sheet",    "Short-term Debt"),
    # Cash Flow
    ("Cash Flow",        "Operating Cash Flow"),
    ("Cash Flow",        "CapEx"),
]

# Build a filter condition
from pyspark.sql import functions as F

condition = F.lit(False)
for stmt, concept in needed_concepts:
    condition = condition | (
        (F.col("stmt") == stmt) & (F.col("concept") == concept)
    )

raw = (
    spark.table(full_table)
    .filter(condition)
    .select("ticker", "company", "year", "stmt", "concept", "value")
)

# Pivot to wide: one row per (ticker, year), one column per concept
wide = (
    raw
    .groupBy("ticker", "company", "year")
    .pivot("concept")
    .agg(F.first("value"))
)

# Coalesce the two Revenue columns (companies use different XBRL tags)
if "Revenue" in wide.columns and "Revenue (contract)" in wide.columns:
    wide = wide.withColumn(
        "Revenue",
        F.coalesce(F.col("Revenue"), F.col("Revenue (contract)"))
    ).drop("Revenue (contract)")

print(f"Wide table: {wide.count()} rows × {len(wide.columns)} columns")

# COMMAND ----------
# MAGIC %md ### Compute metrics

# COMMAND ----------

def safe_div(num, den):
    """Null-safe division."""
    return F.when(F.col(den).isNotNull() & (F.col(den) != 0),
                  F.col(num) / F.col(den)).otherwise(F.lit(None))

metrics_wide = (
    wide
    # ── Margins (%) ───────────────────────────────────────────────────────────
    .withColumn("Gross Margin %",     safe_div("Gross Profit",    "Revenue") * 100)
    .withColumn("Operating Margin %", safe_div("Operating Income","Revenue") * 100)
    .withColumn("Net Margin %",       safe_div("Net Income",      "Revenue") * 100)

    # ── Free Cash Flow ────────────────────────────────────────────────────────
    .withColumn("Free Cash Flow",
        F.when(F.col("Operating Cash Flow").isNotNull() & F.col("CapEx").isNotNull(),
               F.col("Operating Cash Flow") - F.col("CapEx"))
    )

    # ── Leverage ──────────────────────────────────────────────────────────────
    .withColumn("Total Debt",
        F.coalesce(F.col("Long-term Debt"), F.lit(0)) +
        F.coalesce(F.col("Short-term Debt"), F.lit(0))
    )
    .withColumn("Debt / Equity",  safe_div("Total Debt", "Total Stockholders Equity"))
    .withColumn("Debt / Assets",  safe_div("Total Debt", "Total Assets"))

    # ── Liquidity ─────────────────────────────────────────────────────────────
    .withColumn("Current Ratio",  safe_div("Total Current Assets", "Total Current Liabilities"))

    # ── FCF Margin ────────────────────────────────────────────────────────────
    .withColumn("FCF Margin %",   safe_div("Free Cash Flow", "Revenue") * 100)
)

# COMMAND ----------
# MAGIC %md ### Compute YoY growth (window function)

# COMMAND ----------

from pyspark.sql.window import Window

grow_cols = ["Revenue", "Net Income", "Operating Cash Flow", "Free Cash Flow"]
w = Window.partitionBy("ticker").orderBy("year")

for col in grow_cols:
    if col in metrics_wide.columns:
        lag_col = f"_lag_{col}"
        metrics_wide = (
            metrics_wide
            .withColumn(lag_col, F.lag(F.col(col)).over(w))
            .withColumn(
                f"{col} YoY %",
                F.when(
                    F.col(lag_col).isNotNull() & (F.col(lag_col) != 0),
                    ((F.col(col) - F.col(lag_col)) / F.abs(F.col(lag_col))) * 100
                )
            )
            .drop(lag_col)
        )

# COMMAND ----------
# MAGIC %md ### Melt back to long format

# COMMAND ----------

# Columns that are metrics (everything except ticker/company/year + raw inputs)
raw_concept_cols = [
    "Revenue", "Gross Profit", "Operating Income", "Net Income",
    "Total Current Assets", "Total Current Liabilities", "Total Assets",
    "Total Liabilities", "Total Stockholders Equity", "Long-term Debt",
    "Short-term Debt", "Operating Cash Flow", "CapEx", "Total Debt",
]

id_cols     = ["ticker", "company", "year"]
metric_cols = [c for c in metrics_wide.columns
               if c not in id_cols and c not in raw_concept_cols]

print(f"Computed metrics: {metric_cols}")

# Stack into long format
stack_expr = ", ".join([f"'{c}', `{c}`" for c in metric_cols])
n = len(metric_cols)

metrics_long = metrics_wide.selectExpr(
    "ticker", "company", "year",
    f"stack({n}, {stack_expr}) as (metric, value)"
).filter(F.col("value").isNotNull())

print(f"Long metrics table: {metrics_long.count()} rows")

# COMMAND ----------
# MAGIC %md ### Write to Delta (MERGE — idempotent)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {metrics_tbl} (
        ticker  STRING  NOT NULL,
        company STRING,
        year    INT     NOT NULL,
        metric  STRING  NOT NULL,
        value   DOUBLE
    )
    USING DELTA
    PARTITIONED BY (ticker)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

metrics_long.createOrReplaceTempView("incoming_metrics")

spark.sql(f"""
    MERGE INTO {metrics_tbl} AS target
    USING incoming_metrics AS source
    ON  target.ticker = source.ticker
    AND target.year   = source.year
    AND target.metric = source.metric

    WHEN MATCHED AND target.value != source.value THEN
        UPDATE SET target.value = source.value, target.company = source.company

    WHEN NOT MATCHED THEN
        INSERT (ticker, company, year, metric, value)
        VALUES (source.ticker, source.company, source.year, source.metric, source.value)
""")

print(f"✓ Merged into {metrics_tbl}")

# COMMAND ----------
# MAGIC %md ### Preview

# COMMAND ----------

spark.sql(f"""
    SELECT ticker, year, metric, ROUND(value, 2) AS value
    FROM {metrics_tbl}
    WHERE ticker = 'AAPL'
      AND metric IN ('Gross Margin %', 'Net Margin %', 'Free Cash Flow',
                     'Current Ratio', 'Revenue YoY %')
    ORDER BY metric, year
""").display()


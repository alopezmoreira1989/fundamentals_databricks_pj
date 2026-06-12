# Databricks notebook source
# MAGIC %md
# MAGIC # 30_analysis / 33__fy_staleness_check
# MAGIC
# MAGIC Detects **core concepts with missing or stale FY** in `main.financials.financials`
# MAGIC — the symptom of an XBRL synonym gap (an issuer that switched, e.g., `NetIncomeLoss`
# MAGIC to `ProfitLoss`, or `NetCashProvidedByUsedInOperatingActivities` to its
# MAGIC `…ContinuingOperations` variant). These gaps produce **stale or null** ROE / Net Margin / P/E
# MAGIC in the screener and detail page.
# MAGIC
# MAGIC **Method:** for each ticker, the latest fiscal year with a `Revenue` FY row is the
# MAGIC "active company" reference. A concept whose latest FY lags that reference (or is absent
# MAGIC altogether) is flagged as *stale*/*missing*. This is **informational** (does not fail)
# MAGIC — it validates the effect of adding synonyms in `00_config/01__tickers.py` and monitors
# MAGIC for future regressions.
# MAGIC
# MAGIC **Note:** some "missing" entries are legitimate (banks/REITs without Gross Profit /
# MAGIC Operating Income). The goal is to track the TREND in the count after each synonym change,
# MAGIC not to chase absolute zero.

# COMMAND ----------

# MAGIC %run "../00_config/01__tickers"

# COMMAND ----------

from pyspark.sql import functions as F

full_tbl = f"{CATALOG}.{SCHEMA}.{TABLE}"

# Core concepts to monitor (canonical label as stored in `financials`).
# The "active company" reference is the latest FY with Revenue.
CORE_CONCEPTS = [
    ("Income Statement", "Net Income"),
    ("Income Statement", "Operating Income"),
    ("Cash Flow",        "Operating Cash Flow"),
    ("Balance Sheet",    "Total Stockholders Equity"),
    ("Balance Sheet",    "Total Assets"),
    ("Cash Flow",        "CapEx"),
]

# COMMAND ----------

# MAGIC %md ## 1. Reference per ticker: latest FY with Revenue

# COMMAND ----------

fy = spark.table(full_tbl).filter(F.col("period_type") == "FY")

ref = (
    fy.filter((F.col("stmt") == "Income Statement") & (F.col("concept") == "Revenue"))
      .groupBy("ticker")
      .agg(F.max("fiscal_year").alias("ref_fy"))
)

# COMMAND ----------

# MAGIC %md ## 2. Per concept: latest FY vs reference → missing / stale

# COMMAND ----------

summary_rows = []
detail = None

for stmt, concept in CORE_CONCEPTS:
    c_max = (
        fy.filter((F.col("stmt") == stmt) & (F.col("concept") == concept))
          .groupBy("ticker")
          .agg(F.max("fiscal_year").alias("concept_fy"))
    )
    j = ref.join(c_max, on="ticker", how="left")

    missing = j.filter(F.col("concept_fy").isNull())
    stale   = j.filter(F.col("concept_fy").isNotNull() & (F.col("concept_fy") < F.col("ref_fy")))

    n_missing = missing.count()
    n_stale   = stale.count()
    summary_rows.append((stmt, concept, n_missing, n_stale, n_missing + n_stale))

    affected = (
        j.filter(F.col("concept_fy").isNull() | (F.col("concept_fy") < F.col("ref_fy")))
         .withColumn("stmt", F.lit(stmt))
         .withColumn("concept", F.lit(concept))
         .withColumn("status", F.when(F.col("concept_fy").isNull(), F.lit("missing")).otherwise(F.lit("stale")))
         .withColumn("years_behind", F.col("ref_fy") - F.coalesce(F.col("concept_fy"), F.lit(0)))
         .select("ticker", "stmt", "concept", "ref_fy", "concept_fy", "status", "years_behind")
    )
    detail = affected if detail is None else detail.unionByName(affected)

# COMMAND ----------

# MAGIC %md ## 3. Report

# COMMAND ----------

summary = spark.createDataFrame(
    summary_rows, ["stmt", "concept", "missing", "stale", "total_affected"]
).orderBy(F.col("total_affected").desc())

print("FY staleness by core concept (vs latest FY with Revenue):")
summary.display()

print("\nDetail — worst cases (most years behind):")
detail.orderBy(F.col("years_behind").desc()).display()

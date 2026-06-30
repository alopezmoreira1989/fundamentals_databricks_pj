# Databricks notebook source
# MAGIC %md
# MAGIC # 20__transformation / 21c__prune_quarterly
# MAGIC
# MAGIC Enforces the **rolling window** of the last `QUARTERLY_WINDOW` quarters per ticker.
# MAGIC
# MAGIC **Logic:**
# MAGIC - For each ticker, rank quarterly rows by `period_end` descending
# MAGIC - DELETE rows where rank > `QUARTERLY_WINDOW`
# MAGIC - FY rows are **never touched** (full history kept)
# MAGIC
# MAGIC **Rationale:** SEC keeps all historical 10-Qs forever. Storing all of them
# MAGIC across ~3000 tickers inflates the table without analytical benefit beyond
# MAGIC the most recent 3 years. The `financials_raw` audit log keeps full history
# MAGIC for back-derivation if ever needed.

# COMMAND ----------

# MAGIC %run "../00__config/01__tickers"

# COMMAND ----------

from pyspark.sql import functions as F

full_tbl = f"{CATALOG}.{SCHEMA}.{TABLE}"

print(f"Pruning quarterly rows beyond last {QUARTERLY_WINDOW} per ticker")

# COMMAND ----------

# MAGIC %md ## 1. Identify rows to keep
# MAGIC
# MAGIC A "quarter slot" is a unique `(ticker, period_end)` for quarterly rows.
# MAGIC We rank slots per ticker by recency and keep top N.

# COMMAND ----------

# Get distinct (ticker, period_end) for quarterly rows, rank by recency
to_keep = spark.sql(f"""
    WITH ranked_slots AS (
        SELECT
            ticker,
            period_end,
            ROW_NUMBER() OVER (
                PARTITION BY ticker
                ORDER BY period_end DESC
            ) AS slot_rank
        FROM (
            SELECT DISTINCT ticker, period_end
            FROM {full_tbl}
            WHERE period_type IN ('Q1','Q2','Q3','Q4')
        )
    )
    SELECT ticker, period_end
    FROM ranked_slots
    WHERE slot_rank <= {QUARTERLY_WINDOW}
""")

to_keep.createOrReplaceTempView("keep_slots")
print(f"Slots to keep across all tickers: {to_keep.count():,}")

# COMMAND ----------

# MAGIC %md ## 2. Count rows before/after for audit

# COMMAND ----------

before = spark.sql(f"""
    SELECT COUNT(*) AS n
    FROM {full_tbl}
    WHERE period_type IN ('Q1','Q2','Q3','Q4')
""").collect()[0]["n"]

# COMMAND ----------

# MAGIC %md ## 3. DELETE quarterly rows outside the window

# COMMAND ----------

spark.sql(f"""
    DELETE FROM {full_tbl}
    WHERE period_type IN ('Q1','Q2','Q3','Q4')
      AND NOT EXISTS (
          SELECT 1 FROM keep_slots k
          WHERE k.ticker     = {full_tbl}.ticker
            AND k.period_end = {full_tbl}.period_end
      )
""")

after = spark.sql(f"""
    SELECT COUNT(*) AS n
    FROM {full_tbl}
    WHERE period_type IN ('Q1','Q2','Q3','Q4')
""").collect()[0]["n"]

print(f"Quarterly rows before : {before:,}")
print(f"Quarterly rows after  : {after:,}")
print(f"Rows deleted          : {before - after:,}")

# COMMAND ----------

# MAGIC %md ## 4. Sanity check — distribution of quarters per ticker

# COMMAND ----------

spark.sql(f"""
    SELECT
        ticker,
        COUNT(DISTINCT period_end) AS n_quarters,
        MIN(period_end)            AS oldest_q,
        MAX(period_end)            AS newest_q
    FROM {full_tbl}
    WHERE period_type IN ('Q1','Q2','Q3','Q4')
    GROUP BY ticker
    ORDER BY n_quarters DESC, ticker
    LIMIT 20
""").display()

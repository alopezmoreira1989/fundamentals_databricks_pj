# Databricks notebook source
# MAGIC %md
# MAGIC # 30_analysis / 31__company_analysis
# MAGIC
# MAGIC Ready-made queries for visualization in Databricks.
# MAGIC All queries filter `period_type = 'FY'` — for quarterly, use `31b__company_analysis_quarterly`.
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. Single company deep-dive
# MAGIC 2. Cross-company comparisons
# MAGIC 3. Growth analysis
# MAGIC 4. Profitability
# MAGIC 5. Cash flow & capex
# MAGIC 6. Balance sheet & leverage

# COMMAND ----------

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/FA PJ (Basic)/00_config/01__tickers"

# COMMAND ----------

COMPANY     = "AAPL"
PEERS       = ["AAPL", "MSFT", "GOOGL", "META", "NVDA"]
START_YEAR  = 2015

FIN  = f"{CATALOG}.{SCHEMA}.financials"
MET  = f"{CATALOG}.{SCHEMA}.financials_metrics"

print(f"Analysing : {COMPANY}")
print(f"Peers     : {PEERS}")
print(f"From year : {START_YEAR}")

# COMMAND ----------
# MAGIC %md ---
# MAGIC ## 1. Single company — Income Statement overview

# COMMAND ----------

spark.sql(f"""
    SELECT
        fiscal_year,
        concept,
        ROUND(value / 1e9, 2) AS value_bn
    FROM {FIN}
    WHERE ticker      = '{COMPANY}'
      AND period_type = 'FY'
      AND stmt        = 'Income Statement'
      AND concept     IN ('Revenue', 'Gross Profit', 'Operating Income', 'Net Income')
      AND fiscal_year >= {START_YEAR}
    ORDER BY fiscal_year, concept
""").display()

# COMMAND ----------
# MAGIC %md ## 2. Single company — Margins over time

# COMMAND ----------

spark.sql(f"""
    SELECT
        fiscal_year,
        metric,
        ROUND(value, 1) AS value
    FROM {MET}
    WHERE ticker      = '{COMPANY}'
      AND metric      IN ('Gross Margin %', 'Operating Margin %', 'Net Margin %', 'FCF Margin %')
      AND fiscal_year >= {START_YEAR}
    ORDER BY fiscal_year, metric
""").display()

# COMMAND ----------
# MAGIC %md ## 3. Single company — Cash flow breakdown

# COMMAND ----------

spark.sql(f"""
    SELECT
        fiscal_year,
        concept,
        ROUND(value / 1e9, 2) AS value_bn
    FROM {FIN}
    WHERE ticker      = '{COMPANY}'
      AND period_type = 'FY'
      AND stmt        = 'Cash Flow'
      AND concept     IN ('Operating Cash Flow', 'CapEx', 'Financing Cash Flow')
      AND fiscal_year >= {START_YEAR}
    ORDER BY fiscal_year, concept
""").display()

# COMMAND ----------
# MAGIC %md ## 4. Peer comparison — Revenue growth

# COMMAND ----------

peers_sql = "', '".join(PEERS)
spark.sql(f"""
    SELECT
        ticker,
        fiscal_year,
        ROUND(value, 1) AS revenue_yoy_pct
    FROM {MET}
    WHERE ticker      IN ('{peers_sql}')
      AND metric      = 'Revenue YoY %'
      AND fiscal_year >= {START_YEAR}
    ORDER BY fiscal_year, ticker
""").display()

# COMMAND ----------
# MAGIC %md ## 5. Peer comparison — Net Margin

# COMMAND ----------

spark.sql(f"""
    SELECT
        ticker,
        fiscal_year,
        ROUND(value, 1) AS net_margin_pct
    FROM {MET}
    WHERE ticker      IN ('{peers_sql}')
      AND metric      = 'Net Margin %'
      AND fiscal_year >= {START_YEAR}
    ORDER BY fiscal_year, ticker
""").display()

# COMMAND ----------
# MAGIC %md ## 6. Peer comparison — ROIC

# COMMAND ----------

spark.sql(f"""
    SELECT
        ticker,
        fiscal_year,
        ROUND(value, 1) AS roic_pct
    FROM {MET}
    WHERE ticker      IN ('{peers_sql}')
      AND metric      = 'ROIC %'
      AND fiscal_year >= {START_YEAR}
    ORDER BY fiscal_year, ticker
""").display()

# COMMAND ----------
# MAGIC %md ## 7. Single company — Balance Sheet snapshot (latest year)

# COMMAND ----------

spark.sql(f"""
    WITH latest AS (
        SELECT MAX(fiscal_year) AS yr
        FROM {FIN}
        WHERE ticker = '{COMPANY}' AND period_type = 'FY'
    )
    SELECT
        f.concept,
        ROUND(f.value / 1e9, 2) AS value_bn
    FROM {FIN} f, latest
    WHERE f.ticker      = '{COMPANY}'
      AND f.period_type = 'FY'
      AND f.stmt        = 'Balance Sheet'
      AND f.fiscal_year = latest.yr
    ORDER BY f.concept
""").display()

# COMMAND ----------
# MAGIC %md ## 8. Single company — Leverage over time

# COMMAND ----------

spark.sql(f"""
    SELECT
        fiscal_year,
        metric,
        ROUND(value, 2) AS value
    FROM {MET}
    WHERE ticker      = '{COMPANY}'
      AND metric      IN ('Debt / Equity', 'Debt / Assets', 'Current Ratio')
      AND fiscal_year >= {START_YEAR}
    ORDER BY fiscal_year, metric
""").display()

# COMMAND ----------
# MAGIC %md ## 9. Single company — Valuation multiples

# COMMAND ----------

spark.sql(f"""
    SELECT
        fiscal_year,
        metric,
        ROUND(value, 2) AS value
    FROM {MET}
    WHERE ticker      = '{COMPANY}'
      AND metric      IN ('P/E', 'P/S', 'P/FCF', 'EV/EBITDA')
      AND fiscal_year >= {START_YEAR}
    ORDER BY fiscal_year, metric
""").display()


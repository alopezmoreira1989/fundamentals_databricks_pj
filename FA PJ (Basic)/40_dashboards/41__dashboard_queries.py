# Databricks notebook source
# MAGIC %md
# MAGIC # 40_dashboards / 41__dashboard_queries
# MAGIC
# MAGIC This notebook contains all SQL queries used in the **Financial Analysis** Databricks Dashboard.
# MAGIC
# MAGIC **How to use:**
# MAGIC 1. Go to Databricks sidebar → **Dashboards** → **Financial Analysis**
# MAGIC 2. To add/edit a dataset, copy the relevant query from here and paste it into the dashboard
# MAGIC
# MAGIC **Dashboard link:** (paste your dashboard URL here after creating it)
# MAGIC
# MAGIC **Tables used:**
# MAGIC - `main.financials.financials`         ← raw fact table (Balance Sheet, Income Statement, Cash Flow)
# MAGIC - `main.financials.financials_metrics` ← derived metrics (margins, FCF, YoY growth, leverage)

# COMMAND ----------
# MAGIC %md ---
# MAGIC ## Dataset 1 — Income Statement
# MAGIC *Used by: Revenue chart, Gross Profit chart, Net Income chart*

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ticker,
# MAGIC     year,
# MAGIC     concept,
# MAGIC     ROUND(value / 1e9, 2) AS value_bn
# MAGIC FROM main.financials.financials
# MAGIC WHERE stmt = 'Income Statement'
# MAGIC   AND concept IN ('Revenue', 'Revenue (contract)', 'Gross Profit', 'Operating Income', 'Net Income')
# MAGIC ORDER BY ticker, year, concept

# COMMAND ----------
# MAGIC %md ## Dataset 2 — Margins
# MAGIC *Used by: Gross / Operating / Net / FCF margin charts*

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ticker,
# MAGIC     company,
# MAGIC     year,
# MAGIC     metric,
# MAGIC     ROUND(value, 1) AS value
# MAGIC FROM main.financials.financials_metrics
# MAGIC WHERE metric IN ('Gross Margin %', 'Operating Margin %', 'Net Margin %', 'FCF Margin %')
# MAGIC ORDER BY ticker, year, metric

# COMMAND ----------
# MAGIC %md ## Dataset 3 — Cash Flow
# MAGIC *Used by: Operating CF, CapEx, FCF charts*

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ticker,
# MAGIC     year,
# MAGIC     concept,
# MAGIC     ROUND(value / 1e9, 2) AS value_bn
# MAGIC FROM main.financials.financials
# MAGIC WHERE stmt = 'Cash Flow'
# MAGIC   AND concept IN ('Operating Cash Flow', 'CapEx', 'Investing Cash Flow', 'Financing Cash Flow')
# MAGIC ORDER BY ticker, year, concept

# COMMAND ----------
# MAGIC %md ## Dataset 4 — Free Cash Flow vs Net Income
# MAGIC *Used by: FCF vs Net Income line chart*

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ticker, year, 'Operating Cash Flow' AS metric, ROUND(value / 1e9, 2) AS value_bn
# MAGIC FROM main.financials.financials
# MAGIC WHERE stmt = 'Cash Flow' AND concept = 'Operating Cash Flow'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT ticker, year, 'CapEx (negative)' AS metric, ROUND(-value / 1e9, 2) AS value_bn
# MAGIC FROM main.financials.financials
# MAGIC WHERE stmt = 'Cash Flow' AND concept = 'CapEx'
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT ticker, year, 'Net Income' AS metric, ROUND(value / 1e9, 2) AS value_bn
# MAGIC FROM main.financials.financials
# MAGIC WHERE stmt = 'Income Statement' AND concept = 'Net Income'
# MAGIC
# MAGIC ORDER BY ticker, year, metric

# COMMAND ----------
# MAGIC %md ## Dataset 5 — Balance Sheet
# MAGIC *Used by: Assets, Liabilities, Equity charts*

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ticker,
# MAGIC     year,
# MAGIC     concept,
# MAGIC     ROUND(value / 1e9, 2) AS value_bn
# MAGIC FROM main.financials.financials
# MAGIC WHERE stmt = 'Balance Sheet'
# MAGIC   AND concept IN (
# MAGIC       'Cash & Equivalents', 'Short-term Investments',
# MAGIC       'Total Current Assets', 'PP&E Net', 'Goodwill',
# MAGIC       'Total Assets', 'Total Current Liabilities',
# MAGIC       'Long-term Debt', 'Total Liabilities',
# MAGIC       'Total Stockholders Equity'
# MAGIC   )
# MAGIC ORDER BY ticker, year, concept

# COMMAND ----------
# MAGIC %md ## Dataset 6 — YoY Growth rates
# MAGIC *Used by: Revenue growth, Net Income growth charts*

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ticker,
# MAGIC     company,
# MAGIC     year,
# MAGIC     metric,
# MAGIC     ROUND(value, 1) AS value
# MAGIC FROM main.financials.financials_metrics
# MAGIC WHERE metric IN (
# MAGIC     'Revenue YoY %', 'Net Income YoY %',
# MAGIC     'Operating Cash Flow YoY %', 'Free Cash Flow YoY %'
# MAGIC )
# MAGIC ORDER BY ticker, year, metric

# COMMAND ----------
# MAGIC %md ## Dataset 7 — Leverage & Liquidity
# MAGIC *Used by: Debt/Equity, Current Ratio charts*

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ticker,
# MAGIC     company,
# MAGIC     year,
# MAGIC     metric,
# MAGIC     ROUND(value, 2) AS value
# MAGIC FROM main.financials.financials_metrics
# MAGIC WHERE metric IN ('Debt / Equity', 'Debt / Assets', 'Current Ratio')
# MAGIC ORDER BY ticker, year, metric

# COMMAND ----------
# MAGIC %md ## Dataset 8 — Scorecard (latest year per company)
# MAGIC *Used by: summary scorecard table*

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest AS (
# MAGIC     SELECT ticker, MAX(year) AS max_year
# MAGIC     FROM main.financials.financials_metrics
# MAGIC     GROUP BY ticker
# MAGIC )
# MAGIC SELECT
# MAGIC     m.ticker,
# MAGIC     m.company,
# MAGIC     m.year,
# MAGIC     ROUND(MAX(CASE WHEN metric = 'Gross Margin %'      THEN value END), 1) AS `Gross Margin %`,
# MAGIC     ROUND(MAX(CASE WHEN metric = 'Operating Margin %'  THEN value END), 1) AS `Op Margin %`,
# MAGIC     ROUND(MAX(CASE WHEN metric = 'Net Margin %'        THEN value END), 1) AS `Net Margin %`,
# MAGIC     ROUND(MAX(CASE WHEN metric = 'FCF Margin %'        THEN value END), 1) AS `FCF Margin %`,
# MAGIC     ROUND(MAX(CASE WHEN metric = 'Revenue YoY %'       THEN value END), 1) AS `Revenue YoY %`,
# MAGIC     ROUND(MAX(CASE WHEN metric = 'Net Income YoY %'    THEN value END), 1) AS `NI YoY %`,
# MAGIC     ROUND(MAX(CASE WHEN metric = 'Free Cash Flow'      THEN value END) / 1e9, 1) AS `FCF ($bn)`,
# MAGIC     ROUND(MAX(CASE WHEN metric = 'Current Ratio'       THEN value END), 2) AS `Current Ratio`,
# MAGIC     ROUND(MAX(CASE WHEN metric = 'Debt / Equity'       THEN value END), 2) AS `Debt/Equity`
# MAGIC FROM main.financials.financials_metrics m
# MAGIC JOIN latest l ON m.ticker = l.ticker AND m.year = l.max_year
# MAGIC GROUP BY m.ticker, m.company, m.year
# MAGIC ORDER BY `Net Margin %` DESC


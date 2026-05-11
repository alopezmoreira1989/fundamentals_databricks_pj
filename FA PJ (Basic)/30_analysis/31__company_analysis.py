# Databricks notebook source
# MAGIC %md
# MAGIC # 30_analysis / 31__company_analysis
# MAGIC
# MAGIC Ready-made queries for visualization in Databricks.
# MAGIC Each section produces a table you can chart using the **+** button below any result.
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

# ── Change these two variables to focus the analysis ─────────────────────────

COMPANY     = "AAPL"                          # single-company sections
PEERS       = ["AAPL", "MSFT", "GOOGL", "META", "NVDA"]   # comparison sections
START_YEAR  = 2015

# ── Table references ──────────────────────────────────────────────────────────
F  = f"{CATALOG}.{SCHEMA}.financials"          # raw fact table
M  = f"{CATALOG}.{SCHEMA}.financials_metrics"  # derived metrics

print(f"Analysing : {COMPANY}")
print(f"Peers     : {PEERS}")
print(f"From year : {START_YEAR}")

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 1. Single company — Income Statement overview
# MAGIC > Change `COMPANY` and `START_YEAR` in the cell above to switch company.
# MAGIC
# MAGIC **How to chart:** after running, click **+** → Bar chart → X: `year`, Y: `value_bn`, Series: `concept`

# COMMAND ----------

spark.sql(f"""
    SELECT
        year,
        concept,
        ROUND(value / 1e9, 2) AS value_bn
    FROM {F}
    WHERE ticker    = '{COMPANY}'
      AND statement = 'Income Statement'
      AND concept   IN ('Revenue', 'Gross Profit', 'Operating Income', 'Net Income')
      AND year     >= {START_YEAR}
    ORDER BY year, concept
""").display()

# COMMAND ----------

# MAGIC %md ## 2. Single company — Margins over time
# MAGIC **How to chart:** Line chart → X: `year`, Y: `value`, Series: `metric`

# COMMAND ----------

spark.sql(f"""
    SELECT
        year,
        metric,
        ROUND(value, 1) AS value
    FROM {M}
    WHERE ticker = '{COMPANY}'
      AND metric IN ('Gross Margin %', 'Operating Margin %', 'Net Margin %', 'FCF Margin %')
      AND year  >= {START_YEAR}
    ORDER BY year, metric
""").display()

# COMMAND ----------

# MAGIC %md ## 3. Single company — Cash flow breakdown
# MAGIC **How to chart:** Bar chart → X: `year`, Y: `value_bn`, Series: `concept`

# COMMAND ----------

spark.sql(f"""
    SELECT
        year,
        concept,
        ROUND(value / 1e9, 2) AS value_bn
    FROM {F}
    WHERE ticker    = '{COMPANY}'
      AND statement = 'Cash Flow'
      AND concept   IN ('Operating Cash Flow', 'CapEx', 'Financing Cash Flow')
      AND year     >= {START_YEAR}
    ORDER BY year, concept
""").display()

# COMMAND ----------

# MAGIC %md ## 4. Single company — Free Cash Flow vs Net Income
# MAGIC **How to chart:** Line chart → X: `year`, Y: `value_bn`, Series: `metric`

# COMMAND ----------

spark.sql(f"""
    SELECT year, 'Free Cash Flow' AS metric, ROUND(value / 1e9, 2) AS value_bn
    FROM {F}
    WHERE ticker = '{COMPANY}' AND statement = 'Cash Flow'
      AND concept = 'Operating Cash Flow' AND year >= {START_YEAR}

    UNION ALL

    SELECT year, 'CapEx' AS metric, ROUND(-value / 1e9, 2) AS value_bn
    FROM {F}
    WHERE ticker = '{COMPANY}' AND statement = 'Cash Flow'
      AND concept = 'CapEx' AND year >= {START_YEAR}

    UNION ALL

    SELECT year, 'Net Income' AS metric, ROUND(value / 1e9, 2) AS value_bn
    FROM {F}
    WHERE ticker = '{COMPANY}' AND statement = 'Income Statement'
      AND concept = 'Net Income' AND year >= {START_YEAR}

    ORDER BY year, metric
""").display()

# COMMAND ----------

# MAGIC %md ## 5. Single company — Balance sheet composition
# MAGIC **How to chart:** Bar chart (stacked) → X: `year`, Y: `value_bn`, Series: `concept`

# COMMAND ----------

spark.sql(f"""
    SELECT
        year,
        concept,
        ROUND(value / 1e9, 2) AS value_bn
    FROM {F}
    WHERE ticker    = '{COMPANY}'
      AND statement = 'Balance Sheet'
      AND concept   IN (
            'Cash & Equivalents', 'Short-term Investments',
            'Total Current Assets', 'PP&E Net', 'Goodwill',
            'Total Current Liabilities', 'Long-term Debt',
            'Total Stockholders Equity'
          )
      AND year >= {START_YEAR}
    ORDER BY year, concept
""").display()

# COMMAND ----------

# MAGIC %md ## 6. Single company — YoY growth rates
# MAGIC **How to chart:** Line chart → X: `year`, Y: `value`, Series: `metric`

# COMMAND ----------

spark.sql(f"""
    SELECT
        year,
        metric,
        ROUND(value, 1) AS value
    FROM {M}
    WHERE ticker = '{COMPANY}'
      AND metric IN ('Revenue YoY %', 'Net Income YoY %',
                     'Operating Cash Flow YoY %', 'Free Cash Flow YoY %')
      AND year  >= {START_YEAR}
    ORDER BY year, metric
""").display()

# COMMAND ----------

# MAGIC %md ## 7. Single company — Leverage & liquidity
# MAGIC **How to chart:** Line chart → X: `year`, Y: `value`, Series: `metric`

# COMMAND ----------

spark.sql(f"""
    SELECT
        year,
        metric,
        ROUND(value, 2) AS value
    FROM {M}
    WHERE ticker = '{COMPANY}'
      AND metric IN ('Debt / Equity', 'Debt / Assets', 'Current Ratio')
      AND year  >= {START_YEAR}
    ORDER BY year, metric
""").display()

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 8. Cross-company — Revenue comparison
# MAGIC **How to chart:** Bar chart → X: `year`, Y: `revenue_bn`, Series: `ticker`

# COMMAND ----------

peers_str = ", ".join(f"'{t}'" for t in PEERS)

spark.sql(f"""
    SELECT
        ticker,
        year,
        ROUND(value / 1e9, 2) AS revenue_bn
    FROM {F}
    WHERE ticker    IN ({peers_str})
      AND statement = 'Income Statement'
      AND concept   IN ('Revenue', 'Revenue (contract)')
      AND year     >= {START_YEAR}
    ORDER BY year, ticker
""").display()

# COMMAND ----------

# MAGIC %md ## 9. Cross-company — Net margin comparison
# MAGIC **How to chart:** Line chart → X: `year`, Y: `value`, Series: `ticker`

# COMMAND ----------

spark.sql(f"""
    SELECT
        ticker,
        year,
        ROUND(value, 1) AS net_margin_pct
    FROM {M}
    WHERE ticker IN ({peers_str})
      AND metric  = 'Net Margin %'
      AND year   >= {START_YEAR}
    ORDER BY year, ticker
""").display()

# COMMAND ----------

# MAGIC %md ## 10. Cross-company — Free Cash Flow comparison
# MAGIC **How to chart:** Bar chart → X: `year`, Y: `fcf_bn`, Series: `ticker`

# COMMAND ----------

spark.sql(f"""
    SELECT
        ticker,
        year,
        ROUND(value / 1e9, 2) AS fcf_bn
    FROM {M}
    WHERE ticker IN ({peers_str})
      AND metric  = 'Free Cash Flow'
      AND year   >= {START_YEAR}
    ORDER BY year, ticker
""").display()

# COMMAND ----------

# MAGIC %md ## 11. Cross-company — Snapshot scorecard (latest year)
# MAGIC Single table with the most recent values for all key metrics across all peers.
# MAGIC **How to use:** sort by any column to rank companies.

# COMMAND ----------

spark.sql(f"""
    WITH latest AS (
        SELECT ticker, MAX(year) AS max_year
        FROM {M}
        WHERE ticker IN ({peers_str})
        GROUP BY ticker
    ),
    metrics AS (
        SELECT m.ticker, m.year, m.metric, m.value
        FROM {M} m
        JOIN latest l ON m.ticker = l.ticker AND m.year = l.max_year
        WHERE m.ticker IN ({peers_str})
          AND m.metric IN (
                'Gross Margin %', 'Operating Margin %', 'Net Margin %',
                'FCF Margin %', 'Revenue YoY %', 'Net Income YoY %',
                'Free Cash Flow', 'Current Ratio', 'Debt / Equity'
              )
    )
    SELECT
        ticker,
        year,
        ROUND(MAX(CASE WHEN metric = 'Gross Margin %'      THEN value END), 1) AS `Gross Margin %`,
        ROUND(MAX(CASE WHEN metric = 'Operating Margin %'  THEN value END), 1) AS `Op Margin %`,
        ROUND(MAX(CASE WHEN metric = 'Net Margin %'        THEN value END), 1) AS `Net Margin %`,
        ROUND(MAX(CASE WHEN metric = 'FCF Margin %'        THEN value END), 1) AS `FCF Margin %`,
        ROUND(MAX(CASE WHEN metric = 'Revenue YoY %'       THEN value END), 1) AS `Revenue YoY %`,
        ROUND(MAX(CASE WHEN metric = 'Net Income YoY %'    THEN value END), 1) AS `NI YoY %`,
        ROUND(MAX(CASE WHEN metric = 'Free Cash Flow'      THEN value END) / 1e9, 1) AS `FCF ($bn)`,
        ROUND(MAX(CASE WHEN metric = 'Current Ratio'       THEN value END), 2) AS `Current Ratio`,
        ROUND(MAX(CASE WHEN metric = 'Debt / Equity'       THEN value END), 2) AS `Debt/Equity`
    FROM metrics
    GROUP BY ticker, year
    ORDER BY `Net Margin %` DESC
""").display()

# COMMAND ----------

# MAGIC %md ## 12. Cross-company — Revenue growth ranking over time
# MAGIC **How to chart:** Bar chart → X: `ticker`, Y: `revenue_yoy_pct`, Series: `year`

# COMMAND ----------

spark.sql(f"""
    SELECT
        ticker,
        year,
        ROUND(value, 1) AS revenue_yoy_pct
    FROM {M}
    WHERE ticker IN ({peers_str})
      AND metric  = 'Revenue YoY %'
      AND year   >= {START_YEAR}
    ORDER BY year DESC, value DESC
""").display()

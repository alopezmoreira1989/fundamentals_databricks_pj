# Databricks notebook source
# MAGIC %md
# MAGIC # 30_analysis / 32__coverage_check
# MAGIC
# MAGIC Post-pipeline coverage check: verifies that **favorite tickers** made it
# MAGIC through the full pipeline (config → financials → financials_metrics).
# MAGIC Also surfaces any ingestion failures from the latest run.
# MAGIC
# MAGIC **Behavior:**
# MAGIC - 0 issues → `✓ All favorites present`
# MAGIC - Issues found → prints table and summary
# MAGIC - >5% of favorites missing → raises `RuntimeError` (hard fail)
# MAGIC - ≤5% → warning only (pipeline continues)

# COMMAND ----------

# MAGIC %run "../00_config/01__tickers"

# COMMAND ----------

from pyspark.sql import functions as F

_failures_tbl = f"{CATALOG}.{SCHEMA}.ingestion_failures"

# COMMAND ----------

# MAGIC %md ## 1. Check favorite coverage

# COMMAND ----------

issues = spark.sql(f"""
    -- Favoritos que NO llegaron a financials
    SELECT t.ticker, t.company, 'missing_from_financials' AS issue
    FROM {CATALOG}.config.tickers t
    LEFT JOIN (SELECT DISTINCT ticker FROM {CATALOG}.{SCHEMA}.financials) f
      ON f.ticker = t.ticker
    WHERE t.is_favorite = true AND f.ticker IS NULL

    UNION ALL

    -- Favoritos en financials pero no en financials_metrics
    SELECT t.ticker, t.company, 'missing_from_metrics' AS issue
    FROM {CATALOG}.config.tickers t
    JOIN (SELECT DISTINCT ticker FROM {CATALOG}.{SCHEMA}.financials) f
      ON f.ticker = t.ticker
    LEFT JOIN (SELECT DISTINCT ticker FROM {CATALOG}.{SCHEMA}.financials_metrics) m
      ON m.ticker = t.ticker
    WHERE t.is_favorite = true AND m.ticker IS NULL
""")

n_issues = issues.count()

# COMMAND ----------

# MAGIC %md ## 2. Check recent ingestion failures

# COMMAND ----------

try:
    recent_failures = spark.sql(f"""
        SELECT ticker, error_type, error_message, step
        FROM {_failures_tbl}
        WHERE scraped_at = (SELECT MAX(scraped_at) FROM {_failures_tbl})
    """)
    n_failures = recent_failures.count()
except Exception:
    recent_failures = None
    n_failures = 0

# COMMAND ----------

# MAGIC %md ## 3. Report

# COMMAND ----------

total_favorites = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {CATALOG}.config.tickers WHERE is_favorite = true
""").collect()[0]["n"]

if n_issues == 0 and n_failures == 0:
    print(f"✓ All {total_favorites} favorite(s) present in financials and financials_metrics")
    print(f"✓ No ingestion failures in latest run")
else:
    if n_issues > 0:
        print(f"\n⚠ {n_issues} coverage issue(s) found for favorites:")
        issues.display()

    if n_failures > 0:
        print(f"\n⚠ {n_failures} ingestion failure(s) in latest run:")
        recent_failures.display()

    # Hard fail if >5% of favorites are missing from financials
    missing_financials = issues.filter(F.col("issue") == "missing_from_financials").count()
    if total_favorites > 0 and missing_financials / total_favorites > 0.05:
        raise RuntimeError(
            f"HARD FAIL: {missing_financials}/{total_favorites} favorites "
            f"({missing_financials/total_favorites:.0%}) missing from financials — "
            f"exceeds 5% threshold. Check ingestion_failures for details."
        )
    else:
        print(f"\n⊘ Coverage gaps within tolerance — pipeline continues")

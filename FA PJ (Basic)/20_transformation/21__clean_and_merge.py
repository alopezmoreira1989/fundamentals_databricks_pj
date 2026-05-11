# Databricks notebook source
# MAGIC %md
# MAGIC # 20_transformation / 21_clean_and_merge
# MAGIC
# MAGIC Reads from `financials_raw` (append-only) and merges into the clean
# MAGIC `financials` fact table — the one your dashboards query.
# MAGIC
# MAGIC **Logic:**
# MAGIC - UPDATE if a value changed (e.g. SEC restated a filing)
# MAGIC - INSERT if it's a new ticker / year / concept
# MAGIC - Leave everything else untouched
# MAGIC
# MAGIC Re-running this notebook is always safe — fully idempotent.

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

from pyspark.sql import functions as F

raw_full  = f"{CATALOG}.{SCHEMA}.{RAW_TABLE}"
full_tbl  = f"{CATALOG}.{SCHEMA}.{TABLE}"

# Read only the latest scrape from raw
# (if you want to reprocess all history, remove the scraped_at filter)
latest_scrape = spark.sql(f"SELECT MAX(scraped_at) AS ts FROM {raw_full}").collect()[0]["ts"]

incoming = (
    spark.table(raw_full)
    .filter(F.col("scraped_at") == latest_scrape)

    # ── Cleaning steps ────────────────────────────────────────────────────────

    # Drop rows with null values — no point storing them
    .filter(F.col("value").isNotNull())

    # Drop obvious duplicates (shouldn't exist in raw, but defensive)
    .dropDuplicates(["ticker", "statement", "year", "concept"])

    # Normalise company name capitalisation
    .withColumn("company", F.initcap(F.col("company")))
)

print(f"Latest scrape  : {latest_scrape}")
print(f"Incoming rows  : {incoming.count():,}")

# COMMAND ----------

# MAGIC %md ## Create clean fact table if first run

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_tbl} (
        ticker     STRING    NOT NULL,
        company    STRING,
        statement  STRING    NOT NULL,
        year       INT       NOT NULL,
        concept    STRING    NOT NULL,
        value      DOUBLE,
        scraped_at TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (ticker, statement)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

# COMMAND ----------

# MAGIC %md ## MERGE — upsert into clean fact table

# COMMAND ----------

incoming.createOrReplaceTempView("incoming_clean")

merge_result = spark.sql(f"""
    MERGE INTO {full_tbl} AS target
    USING incoming_clean AS source
    ON  target.ticker    = source.ticker
    AND target.statement = source.statement
    AND target.year      = source.year
    AND target.concept   = source.concept

    -- Value changed (e.g. restated filing) → update
    WHEN MATCHED AND target.value != source.value THEN
        UPDATE SET
            target.value      = source.value,
            target.company    = source.company,
            target.scraped_at = source.scraped_at

    -- New row → insert
    WHEN NOT MATCHED THEN
        INSERT (ticker, company, statement, year, concept, value, scraped_at)
        VALUES (source.ticker, source.company, source.statement,
                source.year,  source.concept,  source.value, source.scraped_at)
""")

print(f"✓ MERGE complete → {full_tbl}")

# COMMAND ----------

# MAGIC %md ## Sanity check

# COMMAND ----------

spark.sql(f"""
    SELECT
        ticker,
        COUNT(DISTINCT statement)   AS statements,
        COUNT(DISTINCT year)        AS years,
        MIN(year)                   AS first_year,
        MAX(year)                   AS last_year,
        COUNT(*)                    AS total_rows
    FROM {full_tbl}
    GROUP BY ticker
    ORDER BY ticker
""").display()

# Databricks notebook source
# MAGIC %md
# MAGIC # 10_ingestion / 11_fetch_sec_xbrl
# MAGIC
# MAGIC Fetches the **Holy Trinity** (Balance Sheet, Income Statement, Cash Flow)
# MAGIC from SEC EDGAR's XBRL API for all active tickers in `config/tickers`.
# MAGIC
# MAGIC **Writes to:**
# MAGIC - `{catalog}.{schema}.financials_raw`  ← append-only audit log
# MAGIC
# MAGIC **Called by:** `pipelines/full_pipeline` or run standalone.

# COMMAND ----------

# MAGIC %md ## 0. Load config

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

# Confirm config loaded correctly — if ACTIVE_TICKERS is missing, the %run above failed.
# Fix: check that the path ../00_config/01__tickers matches your actual folder name exactly.
assert "ACTIVE_TICKERS" in dir(), (
    "Config not loaded. Check that the %run path matches your workspace folder name exactly. "
    "In Databricks the path is case-sensitive and relative to this notebook's location."
)
print(f"Config loaded — {len(ACTIVE_TICKERS)} active tickers: {ACTIVE_TICKERS}")

# COMMAND ----------

import requests
import time
import pandas as pd
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)

# If called from 91__full_pipeline, run_tickers is already defined in the shared namespace
# If run standalone, fall back to all active tickers from config
try:
    _ = run_tickers
    RUN_TICKERS = run_tickers
    print(f"Running pipeline subset: {RUN_TICKERS}")
except NameError:
    RUN_TICKERS = ACTIVE_TICKERS
    print(f"Running standalone — all active tickers: {RUN_TICKERS}")

HEADERS = {"User-Agent": SEC_USER_AGENT}

# COMMAND ----------

# MAGIC %md ## 1. SEC EDGAR fetch helpers

# COMMAND ----------

def get_cik(ticker: str) -> tuple:
    """Resolve ticker → (cik_str, company_name) using SEC's company index."""
    url  = "https://www.sec.gov/files/company_tickers.json"
    data = requests.get(url, headers=HEADERS, timeout=10).json()
    for entry in data.values():
        if entry["ticker"].upper() == ticker.upper():
            return str(entry["cik_str"]).zfill(10), entry["title"]
    raise ValueError(f"Ticker '{ticker}' not found in SEC database.")


def get_facts(cik: str) -> dict:
    """
    Fetch the full XBRL company facts JSON from SEC.
    One call returns every financial figure ever reported — all years, all concepts.
    """
    url  = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    time.sleep(0.15)   # SEC rate limit: max 10 req/s
    return resp.json()


def extract_annual_series(facts: dict, concept: str, namespace: str = "us-gaap") -> pd.DataFrame:
    """
    Extract one XBRL concept from the facts blob.

    - Keeps only annual filings (10-K / 20-F / 10-K/A)
    - One row per fiscal year end — if restated, the latest filed version wins
    - Returns columns: date (fiscal year end), value
    """
    try:
        units    = facts["facts"][namespace][concept]["units"]
        unit_key = "USD" if "USD" in units else list(units.keys())[0]
        rows     = units[unit_key]
    except KeyError:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Annual filings only
    df = df[df["form"].isin(["10-K", "20-F", "10-K/A"])].copy()
    df = df.dropna(subset=["end"])
    df["end"] = pd.to_datetime(df["end"])

    # Latest filed version wins per fiscal year end
    df = (
        df.sort_values("filed")
          .drop_duplicates(subset=["end"], keep="last")
    )

    return df[["end", "val"]].rename(columns={"end": "date", "val": "value"})

# COMMAND ----------

# MAGIC %md ## 2. Scrape all tickers

# COMMAND ----------

SCHEMA_DEF = StructType([
    StructField("ticker",     StringType(),    False),
    StructField("company",    StringType(),    True),
    StructField("stmt",  StringType(),    False),
    StructField("year",       IntegerType(),   False),
    StructField("concept",    StringType(),    False),
    StructField("value",      DoubleType(),    True),
    StructField("scraped_at", TimestampType(), True),
])

scraped_at = datetime.utcnow()
all_records = []
failed      = []

for ticker in RUN_TICKERS:
    print(f"\n── {ticker} ──────────────────────────────────────")
    try:
        # 1. Resolve CIK
        cik, company_name = get_cik(ticker)
        print(f"  CIK  : {cik}  |  {company_name}")

        # 2. Fetch all XBRL facts (single API call per company)
        facts = get_facts(cik)

        # 3. Extract each concept for each statement
        ticker_rows = 0
        for stmt_name, concept_map in STATEMENTS.items():
            for label, xbrl_concept in concept_map.items():
                series = extract_annual_series(facts, xbrl_concept)
                if series.empty:
                    continue
                for _, row in series.iterrows():
                    all_records.append({
                        "ticker":     ticker.upper(),
                        "company":    company_name,
                        "stmt":  stmt_name,
                        "year":       int(row["date"].year),
                        "concept":    label,
                        "value":      float(row["value"]),
                        "scraped_at": scraped_at,
                    })
                    ticker_rows += 1

        print(f"  ✓  {ticker_rows} rows collected")

    except Exception as e:
        print(f"  ✗  ERROR: {e}")
        failed.append({"ticker": ticker, "error": str(e)})

print(f"\n{'='*55}")
print(f"  Scraped  : {len(RUN_TICKERS) - len(failed)} / {len(RUN_TICKERS)} tickers")
print(f"  Records  : {len(all_records)}")
if failed:
    print(f"  Failed   : {[f['ticker'] for f in failed]}")
print(f"{'='*55}")

# COMMAND ----------

# MAGIC %md ## 3. Convert to Spark DataFrame

# COMMAND ----------

if not all_records:
    raise RuntimeError("No records scraped — check SEC_USER_AGENT and network access.")

pandas_df = pd.DataFrame(all_records)
spark_df  = spark.createDataFrame(pandas_df, schema=SCHEMA_DEF)

print(f"Spark DataFrame: {spark_df.count():,} rows")
spark_df.printSchema()

# COMMAND ----------

# MAGIC %md ## 4. Write to raw layer (append-only)
# MAGIC
# MAGIC `financials_raw` is **never updated or deleted from** — every run appends.
# MAGIC This gives you:
# MAGIC - Full audit trail of every scrape
# MAGIC - Ability to reprocess if concept maps change
# MAGIC - Protection against accidental data loss downstream

# COMMAND ----------

raw_full = f"{CATALOG}.{SCHEMA}.{RAW_TABLE}"

# Create table if first run
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {raw_full} (
        ticker     STRING    NOT NULL,
        company    STRING,
        stmt       STRING    NOT NULL,
        year       INT       NOT NULL,
        concept    STRING    NOT NULL,
        value      DOUBLE,
        scraped_at TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (ticker, stmt)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

(
    spark_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(raw_full)
)

print(f"✓ Appended {spark_df.count():,} rows → {raw_full}")

# COMMAND ----------

# MAGIC %md ## 5. Sanity check

# COMMAND ----------

spark.sql(f"""
    SELECT
        ticker,
        stmt,
        COUNT(DISTINCT year)  AS years,
        MIN(year)             AS first_year,
        MAX(year)             AS last_year,
        MAX(scraped_at)       AS last_scraped,
        COUNT(*)              AS rows
    FROM {raw_full}
    WHERE ticker IN ({", ".join(f"'{t}'" for t in RUN_TICKERS)})
      AND scraped_at = (SELECT MAX(scraped_at) FROM {raw_full})
    GROUP BY ticker, stmt
    ORDER BY ticker, stmt
""").display()

# COMMAND ----------

# MAGIC %md ## 6. Log failed tickers (if any)

# COMMAND ----------

if failed:
    failed_df = spark.createDataFrame(failed)
    print("Failed tickers — investigate manually:")
    failed_df.display()

    # Optionally persist failures to a log table for monitoring
    # failed_df.withColumn("run_at", F.lit(scraped_at)) \
    #          .write.format("delta").mode("append") \
    #          .saveAsTable(f"{CATALOG}.{SCHEMA}.ingestion_errors")

# Databricks notebook source
# MAGIC %md
# MAGIC # 10_ingestion / 11__fetch_sec_xbrl
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

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/FA PJ (Basic)/00_config/01__tickers"

# COMMAND ----------

# Load ticker universe from main.config.tickers
tickers_df = spark.table(f"{CATALOG}.config.tickers")
ACTIVE_TICKERS = [row.ticker for row in tickers_df.select("ticker").collect()]
print(f"✓ Config loaded — {len(ACTIVE_TICKERS)} active tickers")

# COMMAND ----------

import requests
import time
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore, Lock
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)

HEADERS = {"User-Agent": SEC_USER_AGENT}

MAX_WORKERS        = 8      # concurrent threads (SEC allows 10 req/s, stay safe at 8)
RATE_LIMIT_DELAY   = 0.13   # seconds between requests across all threads combined
FORCE_FULL_REFRESH = False  # set True to re-fetch everything regardless of age
STALENESS_DAYS     = 7      # re-fetch tickers not scraped within this many days

raw_full = f"{CATALOG}.{SCHEMA}.{RAW_TABLE}"
cutoff   = datetime.utcnow() - timedelta(days=STALENESS_DAYS)

try:
    recently_scraped = {
        row.ticker
        for row in spark.sql(f"""
            SELECT DISTINCT ticker FROM {raw_full}
            WHERE scraped_at >= '{cutoff.isoformat()}'
        """).collect()
    }
except Exception:
    recently_scraped = set()

if FORCE_FULL_REFRESH:
    RUN_TICKERS = ACTIVE_TICKERS
    print(f"Force refresh — fetching all {len(RUN_TICKERS):,} tickers")
else:
    RUN_TICKERS = [t for t in ACTIVE_TICKERS if t not in recently_scraped]
    skipped = len(ACTIVE_TICKERS) - len(RUN_TICKERS)
    print(f"Incremental run:")
    print(f"  Total universe : {len(ACTIVE_TICKERS):,} tickers")
    print(f"  Already fresh  : {skipped:,} tickers (scraped in last {STALENESS_DAYS} days)")
    print(f"  To fetch       : {len(RUN_TICKERS):,} tickers")

if not RUN_TICKERS:
    print("\n✓ All tickers are up to date — skipping SEC fetch.")

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
    """
    try:
        units    = facts["facts"][namespace][concept]["units"]
        unit_key = "USD" if "USD" in units else list(units.keys())[0]
        rows     = units[unit_key]
    except KeyError:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df = df[df["form"].isin(["10-K", "20-F", "10-K/A"])].copy()
    df = df.dropna(subset=["end"])
    df["end"] = pd.to_datetime(df["end"])
    df = (
        df.sort_values("filed")
          .drop_duplicates(subset=["end"], keep="last")
    )
    return df[["end", "val"]].rename(columns={"end": "date", "val": "value"})

# COMMAND ----------
# MAGIC %md ## 2. Scrape all tickers

# COMMAND ----------

if RUN_TICKERS:

    SCHEMA_DEF = StructType([
        StructField("ticker",     StringType(),    False),
        StructField("company",    StringType(),    True),
        StructField("stmt",       StringType(),    False),
        StructField("year",       IntegerType(),   False),
        StructField("concept",    StringType(),    False),
        StructField("value",      DoubleType(),    True),
        StructField("scraped_at", TimestampType(), True),
    ])

    scraped_at  = datetime.utcnow()
    all_records = []
    failed      = []

    for ticker in RUN_TICKERS:
        print(f"\n── {ticker} ──────────────────────────────────────")
        try:
            cik, company_name = get_cik(ticker)
            print(f"  CIK  : {cik}  |  {company_name}")

            facts = get_facts(cik)

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
                            "stmt":       stmt_name,
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

if RUN_TICKERS and all_records:

    pandas_df = pd.DataFrame(all_records)
    spark_df  = spark.createDataFrame(pandas_df, schema=SCHEMA_DEF)

    print(f"Spark DataFrame: {spark_df.count():,} rows")
    spark_df.printSchema()

# COMMAND ----------
# MAGIC %md ## 4. Write to raw layer (append-only)

# COMMAND ----------

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

if RUN_TICKERS and all_records:

    (
        spark_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(raw_full)
    )

    print(f"✓ Appended {spark_df.count():,} rows → {raw_full}")

else:
    print("✓ No new data to append — financials_raw already up to date.")

# COMMAND ----------
# MAGIC %md ## 5. Sanity check

# COMMAND ----------

if RUN_TICKERS:
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
        WHERE ticker IN ({", ".join(f"'{t}'" for t in RUN_TICKERS[:20])})
          AND scraped_at = (SELECT MAX(scraped_at) FROM {raw_full})
        GROUP BY ticker, stmt
        ORDER BY ticker, stmt
    """).display()

# COMMAND ----------
# MAGIC %md ## 6. Log failed tickers (if any)

# COMMAND ----------

if RUN_TICKERS and failed:
    failed_df = spark.createDataFrame(failed)
    print("Failed tickers — investigate manually:")
    failed_df.display()


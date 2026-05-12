# Databricks notebook source
# MAGIC %md
# MAGIC # 10_ingestion / 12__fetch_market_data
# MAGIC
# MAGIC Fetches **year-end closing prices** from Yahoo Finance (yfinance) for all
# MAGIC active tickers, then joins with `Shares Diluted` already in `financials`
# MAGIC to compute an annual market cap.
# MAGIC
# MAGIC **Writes to:**
# MAGIC - `{catalog}.{schema}.market_data`  ← year-end price + market cap, one row per ticker/year
# MAGIC
# MAGIC **Unlocks in 22__derived_metrics:**
# MAGIC - P/E Ratio, P/S Ratio, Price/FCF, EV/EBITDA
# MAGIC
# MAGIC **Called by:** `pipelines/full_pipeline` or run standalone.

# COMMAND ----------

# MAGIC %pip install yfinance

# COMMAND ----------

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/FA PJ (Basic)/00_config/01__tickers"

# COMMAND ----------

# Load ticker universe from main.config.tickers
tickers_df = spark.table(f"{CATALOG}.config.tickers")
ACTIVE_TICKERS = [row.ticker for row in tickers_df.select("ticker").collect()]
print(f"✓ Config loaded — {len(ACTIVE_TICKERS)} active tickers")

# COMMAND ----------

import yfinance as yf
import pandas as pd
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from pyspark.sql import functions as F

market_tbl = f"{CATALOG}.{SCHEMA}.market_data"
full_tbl   = f"{CATALOG}.{SCHEMA}.{TABLE}"

MAX_WORKERS        = 3      # conservative to avoid Yahoo Finance rate limits
STALENESS_DAYS     = 7      # re-fetch tickers not updated within this many days
FORCE_FULL_REFRESH = False  # set True to re-fetch everything

# COMMAND ----------

# MAGIC %md ## 1. Incremental filter — skip recently fetched tickers

# COMMAND ----------

cutoff = datetime.utcnow() - timedelta(days=STALENESS_DAYS)

try:
    recently_fetched = {
        row.ticker
        for row in spark.sql(f"""
            SELECT DISTINCT ticker FROM {market_tbl}
            WHERE fetched_at >= '{cutoff.isoformat()}'
        """).collect()
    }
except Exception:
    recently_fetched = set()

if FORCE_FULL_REFRESH:
    RUN_TICKERS = ACTIVE_TICKERS
    print(f"Force refresh — fetching all {len(RUN_TICKERS):,} tickers")
else:
    RUN_TICKERS = [t for t in ACTIVE_TICKERS if t not in recently_fetched]
    skipped = len(ACTIVE_TICKERS) - len(RUN_TICKERS)
    print(f"Incremental run:")
    print(f"  Total universe  : {len(ACTIVE_TICKERS):,} tickers")
    print(f"  Already fresh   : {skipped:,} tickers")
    print(f"  To fetch        : {len(RUN_TICKERS):,} tickers")

if not RUN_TICKERS:
    print("\n✓ All tickers are up to date — skipping fetch.")

# COMMAND ----------

# MAGIC %md ## 2. Fetch year-end closing prices from Yahoo Finance

# COMMAND ----------

if RUN_TICKERS:

    fetched_at  = datetime.utcnow()
    all_prices  = []
    failed      = []
    lock        = Lock()
    completed   = 0

    def fetch_year_end_prices(ticker: str) -> tuple[str, list[dict]]:
        """
        Returns (ticker, rows) — one row per calendar year with year-end price.
        Uses 'max' history so we get the full backfill in one API call per ticker.
        """
        try:
            hist = yf.Ticker(ticker).history(period="max", auto_adjust=True)
            time.sleep(0.5)
            if hist.empty:
                return ticker, []

            hist.index = pd.to_datetime(hist.index).tz_localize(None)
            hist["year"] = hist.index.year

            year_end = (
                hist.groupby("year")["Close"]
                .last()
                .reset_index()
                .rename(columns={"Close": "price_close"})
            )
            year_end["ticker"] = ticker.upper()
            rows = year_end.to_dict("records")
            for r in rows:
                r["fetched_at"] = fetched_at
            return ticker, rows

        except Exception:
            return ticker, []

    print(f"Fetching {len(RUN_TICKERS):,} tickers with {MAX_WORKERS} parallel workers...\n")
    fetch_start = datetime.utcnow()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_year_end_prices, t): t for t in RUN_TICKERS}
        for future in as_completed(futures):
            ticker, rows = future.result()
            with lock:
                completed += 1
                if rows:
                    all_prices.extend(rows)
                else:
                    failed.append(ticker)
                if completed % 100 == 0 or completed == len(RUN_TICKERS):
                    elapsed = (datetime.utcnow() - fetch_start).total_seconds()
                    rate    = completed / elapsed if elapsed > 0 else 0
                    eta     = (len(RUN_TICKERS) - completed) / rate if rate > 0 else 0
                    print(f"  [{completed:>4}/{len(RUN_TICKERS)}]  "
                          f"{rate:.1f} tickers/s  —  ETA {eta:.0f}s")

    elapsed_total = (datetime.utcnow() - fetch_start).total_seconds()
    print(f"\n{'='*55}")
    print(f"  Fetched  : {len(RUN_TICKERS) - len(failed):,} / {len(RUN_TICKERS):,} tickers")
    print(f"  Records  : {len(all_prices):,}")
    print(f"  Duration : {elapsed_total:.0f}s  ({elapsed_total/60:.1f} min)")
    if failed:
        print(f"  Failed   : {len(failed)} tickers")
    print(f"{'='*55}")

# COMMAND ----------

# MAGIC %md ## 3. Join with Shares Diluted → compute Market Cap

# COMMAND ----------

if RUN_TICKERS and all_prices:

    prices_pd = pd.DataFrame(all_prices)
    prices_pd["year"]        = prices_pd["year"].astype("int32")
    prices_pd["fetched_at"]  = pd.to_datetime(prices_pd["fetched_at"])
    prices_pd["ticker"]      = prices_pd["ticker"].astype(str)
    prices_pd["price_close"] = prices_pd["price_close"].astype(float)

    prices_df = spark.createDataFrame(prices_pd)

    shares_df = (
        spark.table(full_tbl)
        .filter(
            (F.col("stmt") == "Income Statement") &
            (F.col("concept") == "Shares Diluted")
        )
        .select("ticker", "year", F.col("value").alias("shares_diluted"))
    )

    market_df = (
        prices_df
        .join(shares_df, on=["ticker", "year"], how="left")
        .withColumn(
            "market_cap",
            F.when(
                F.col("shares_diluted").isNotNull() & F.col("price_close").isNotNull(),
                F.col("price_close") * F.col("shares_diluted")
            )
        )
    )

    print(f"Market data rows: {market_df.count():,}")

# COMMAND ----------

# MAGIC %md ## 4. Write to Delta (MERGE — idempotent)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {market_tbl} (
        ticker         STRING    NOT NULL,
        year           INT       NOT NULL,
        price_close    DOUBLE,
        shares_diluted DOUBLE,
        market_cap     DOUBLE,
        fetched_at     TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (ticker)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

if RUN_TICKERS and all_prices:

    market_df.createOrReplaceTempView("incoming_market")

    spark.sql(f"""
        MERGE INTO {market_tbl} AS target
        USING incoming_market AS source
        ON  target.ticker = source.ticker
        AND target.year   = source.year

        WHEN MATCHED AND (
            target.price_close    != source.price_close OR
            target.shares_diluted != source.shares_diluted
        ) THEN
            UPDATE SET
                target.price_close    = source.price_close,
                target.shares_diluted = source.shares_diluted,
                target.market_cap     = source.market_cap,
                target.fetched_at     = source.fetched_at

        WHEN NOT MATCHED THEN
            INSERT (ticker, year, price_close, shares_diluted, market_cap, fetched_at)
            VALUES (source.ticker, source.year, source.price_close,
                    source.shares_diluted, source.market_cap, source.fetched_at)
    """)

    print(f"✓ Merged into {market_tbl}")

else:
    print("✓ No new data to merge — market_data already up to date.")

# COMMAND ----------

# MAGIC %md ## 5. Sanity check

# COMMAND ----------

spark.sql(f"""
    SELECT
        ticker,
        COUNT(DISTINCT year)            AS years,
        MIN(year)                       AS first_year,
        MAX(year)                       AS last_year,
        ROUND(MAX(market_cap) / 1e9, 1) AS peak_mktcap_bn,
        ROUND(MAX(price_close), 2)      AS peak_price
    FROM {market_tbl}
    GROUP BY ticker
    ORDER BY peak_mktcap_bn DESC
    LIMIT 20
""").display()

# COMMAND ----------

# MAGIC %md ## 6. Log failed tickers (if any)

# COMMAND ----------

if RUN_TICKERS and failed:
    print(f"Failed tickers ({len(failed)}) — investigate manually:")
    for t in failed:
        print(f"  {t}")

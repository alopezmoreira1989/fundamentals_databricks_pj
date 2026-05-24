# Databricks notebook source
# MAGIC %md
# MAGIC # 10_ingestion / 12__fetch_market_data
# MAGIC
# MAGIC Fetches **year-end closing prices** from Yahoo Finance (yfinance) for all
# MAGIC active tickers, then joins with `Shares Diluted` already in `financials`
# MAGIC to compute an annual market cap.
# MAGIC
# MAGIC **Writes to:** `{catalog}.{schema}.market_data`
# MAGIC
# MAGIC **Note on `fiscal_year`:** prices are fetched on a calendar-year basis (Dec 31)
# MAGIC because yfinance has no notion of fiscal years. The column is named
# MAGIC `fiscal_year` only for schema consistency with `financials` — the value still
# MAGIC represents calendar year. For companies with non-December fiscal year-ends
# MAGIC (AAPL, MSFT, WMT…), this introduces a known 0–11 month offset between
# MAGIC fundamentals (fiscal) and price (calendar). Acceptable for trend analysis;
# MAGIC for precise valuation use `period_end`-based pricing in a future revision.

# COMMAND ----------

# MAGIC %pip install yfinance

# COMMAND ----------

# MAGIC %run "/Workspace/Users/al.lopez.moreira@gmail.com/fundamentals_databricks_pj/fundamentals_pipeline/00_config/01__tickers"

# COMMAND ----------

if "ACTIVE_TICKERS" not in globals() or not ACTIVE_TICKERS:
    tickers_df = spark.table(f"{CATALOG}.config.tickers")
    ACTIVE_TICKERS = [row.ticker for row in tickers_df.select("ticker").collect()]
    print(f"✓ Config loaded — {len(ACTIVE_TICKERS)} active tickers from {CATALOG}.config.tickers")
else:
    print(f"✓ Inherited {len(ACTIVE_TICKERS)} tickers from parent (override mode)")

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

MAX_WORKERS        = 3
STALENESS_DAYS     = 7
FORCE_FULL_REFRESH = False

# COMMAND ----------

# MAGIC %md ## 1. Incremental filter

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

# MAGIC %md ## 2. Fetch year-end prices

# COMMAND ----------

def fetch_year_end_prices(ticker: str) -> list:
    """Fetch annual year-end prices for one ticker. Returns list of dicts."""
    try:
        tk = yf.Ticker(ticker)
        hist = tk.history(period="max", auto_adjust=True)
        if hist.empty:
            return []
        # Take last close of each calendar year
        hist = hist.copy()
        hist["fiscal_year"] = hist.index.year
        yearly = hist.groupby("fiscal_year").tail(1)
        return [
            {
                "ticker":      ticker.upper(),
                "fiscal_year": int(row["fiscal_year"]),
                "price_close": float(row["Close"]),
                "fetched_at":  datetime.utcnow(),
            }
            for _, row in yearly.iterrows()
        ]
    except Exception as e:
        print(f"  ✗ {ticker}: {e}")
        return []

# COMMAND ----------

if RUN_TICKERS:
    all_prices = []
    failed = []
    progress_lock = Lock()
    completed = [0]
    total = len(RUN_TICKERS)

    def worker(ticker):
        rows = fetch_year_end_prices(ticker)
        with progress_lock:
            completed[0] += 1
            n = completed[0]
            if not rows:
                failed.append(ticker)
            else:
                all_prices.extend(rows)
            if n % 50 == 0 or n == total:
                print(f"  [{n:>4}/{total}]")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(worker, t) for t in RUN_TICKERS]
        for _ in as_completed(futures):
            pass

    print(f"\n  Fetched : {len(all_prices):,} price rows")
    print(f"  Failed  : {len(failed)} tickers")

# COMMAND ----------

# MAGIC %md ## 3. Join with Shares Diluted → compute Market Cap

# COMMAND ----------

if RUN_TICKERS and all_prices:

    prices_pd = pd.DataFrame(all_prices)
    prices_pd["fiscal_year"] = prices_pd["fiscal_year"].astype("int32")
    prices_pd["fetched_at"]  = pd.to_datetime(prices_pd["fetched_at"])
    prices_pd["ticker"]      = prices_pd["ticker"].astype(str)
    prices_pd["price_close"] = prices_pd["price_close"].astype(float)

    prices_df = spark.createDataFrame(prices_pd)

    shares_df = (
        spark.table(full_tbl)
        .filter(
            (F.col("stmt") == "Income Statement")
            & (F.col("concept") == "Shares Diluted")
            & (F.col("period_type") == "FY")
        )
        .select("ticker", "fiscal_year", F.col("value").alias("shares_diluted"))
    )

    market_df = (
        prices_df
        .join(shares_df, on=["ticker", "fiscal_year"], how="left")
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
        fiscal_year    INT       NOT NULL,
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
        ON  target.ticker      = source.ticker
        AND target.fiscal_year = source.fiscal_year

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
            INSERT (ticker, fiscal_year, price_close, shares_diluted, market_cap, fetched_at)
            VALUES (source.ticker, source.fiscal_year, source.price_close,
                    source.shares_diluted, source.market_cap, source.fetched_at)
    """)

    print(f"✓ Merged into {market_tbl}")

else:
    print("✓ No new data to merge — market_data already up to date.")

# COMMAND ----------

# MAGIC %md ## 5. Verify

# COMMAND ----------

spark.sql(f"""
    SELECT
        COUNT(*)                     AS rows,
        COUNT(DISTINCT ticker)       AS tickers,
        MIN(fiscal_year)             AS min_year,
        MAX(fiscal_year)             AS max_year,
        COUNT(market_cap)            AS rows_with_market_cap
    FROM {market_tbl}
""").display()

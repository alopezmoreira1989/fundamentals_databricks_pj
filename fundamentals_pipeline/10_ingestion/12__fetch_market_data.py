# Databricks notebook source
# MAGIC %md
# MAGIC # 10_ingestion / 12__fetch_market_data
# MAGIC
# MAGIC Two phases:
# MAGIC 1. **Fetch → `market_prices_daily`** — the daily price store (one row per
# MAGIC    `(ticker, date)`), the single source of truth for all pricing. Raw `close`
# MAGIC    (unadjusted) drives market cap; `adj_close` is split/dividend-adjusted for
# MAGIC    future return work.
# MAGIC 2. **Derive → `market_data`** (legacy) — last raw close per calendar year × FY
# MAGIC    `Shares Diluted`. Retained for backward compatibility only (deprecated; nothing
# MAGIC    new should depend on it). `22__derived_metrics` now prices off
# MAGIC    `market_prices_daily` as-of each fiscal `period_end`.
# MAGIC
# MAGIC **Why daily + period_end pricing:** the old model stored one Dec-31 price per
# MAGIC calendar year and joined it to FY fundamentals on `(ticker, fiscal_year)`. For a
# MAGIC non-December filer (AAPL/Sep, MSFT/Jun, WMT/Jan) that paired e.g. a 2023-09-30
# MAGIC fundamentals row with a 2023-12-31 price — a 0–11 month offset that distorts every
# MAGIC market-cap-derived multiple. A daily store lets each FY be priced at its real
# MAGIC `period_end`.
# MAGIC
# MAGIC **Fetch strategy:** one **batched** `yf.download(...)` per ~60 tickers (yfinance
# MAGIC threads within a batch). Full history collapses to ~50 batched calls.

# COMMAND ----------

# MAGIC %pip install yfinance

# COMMAND ----------

# MAGIC %run "../00_config/01__tickers"

# COMMAND ----------

if "ACTIVE_TICKERS" not in globals() or not ACTIVE_TICKERS:
    tickers_df = spark.table(f"{CATALOG}.config.tickers")
    ACTIVE_TICKERS = [row.ticker for row in tickers_df.select("ticker").collect()]
    print(f"✓ Config loaded — {len(ACTIVE_TICKERS)} active tickers from {CATALOG}.config.tickers")
else:
    print(f"✓ Inherited {len(ACTIVE_TICKERS)} tickers from parent (override mode)")

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from threading import Lock

import pandas as pd
import yfinance as yf
from pyspark.sql import functions as F
from pyspark.sql.types import (DateType, DoubleType, LongType, StringType,
                               StructField, StructType, TimestampType)
from pyspark.sql.window import Window

prices_tbl = f"{CATALOG}.{SCHEMA}.market_prices_daily"
market_tbl = f"{CATALOG}.{SCHEMA}.market_data"
full_tbl   = f"{CATALOG}.{SCHEMA}.{TABLE}"

# ── Batched download config ──────────────────────────────────────────────────────
# yf.download already parallelizes WITHIN a batch (threads=True), so batch-level
# parallelism is kept conservative to avoid Yahoo throttling.
BATCH_SIZE     = 60      # tickers per yf.download call
BATCH_WORKERS  = 2       # concurrent batches (yf threads internally on top of this)
STALENESS_DAYS = 7       # incremental: a ticker whose latest stored date is within this is fresh
INCR_BUFFER_DAYS = 7     # incremental: refetch from (max stored date − buffer) to fill the gap

# ── Refresh policy ────────────────────────────────────────────────────────────
# Inherit force_full_refresh from the parent 91 via globals() — SAME handoff as
# 11__fetch_sec_xbrl. dbutils.widgets.get() does NOT reliably read the parent's widget
# under %run, so we read the variable 91 already set. Standalone runs fall back to this
# notebook's own widget, else False (normal incremental behaviour). Now MATERIAL: a full
# refresh re-fetches the entire daily history, so this must be correct.
if "force_full_refresh" in globals():
    FORCE_FULL_REFRESH = str(force_full_refresh).strip().lower() == "true"
else:
    try:
        FORCE_FULL_REFRESH = dbutils.widgets.get("force_full_refresh").strip().lower() == "true"
    except Exception:
        FORCE_FULL_REFRESH = False

# COMMAND ----------

# MAGIC %md ## 1. Create `market_prices_daily` if first run
# MAGIC
# MAGIC Liquid clustering on `(ticker, date)` — NOT `PARTITIONED BY (ticker)`: ~3k tiny
# MAGIC partitions at daily grain create a small-file problem. **Fallback** if liquid
# MAGIC clustering is unavailable on this workspace: drop `CLUSTER BY`, leave the table
# MAGIC unpartitioned, and run `OPTIMIZE {prices_tbl} ZORDER BY (ticker, date)` periodically.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {prices_tbl} (
        ticker      STRING  NOT NULL,
        date        DATE    NOT NULL,
        close       DOUBLE,
        adj_close   DOUBLE,
        volume      BIGINT,
        fetched_at  TIMESTAMP
    )
    USING DELTA
    CLUSTER BY (ticker, date)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

# COMMAND ----------

# MAGIC %md ## 2. Decide fetch mode + window
# MAGIC
# MAGIC - **full** (force refresh OR empty table) → `period="max"` for every ticker → overwrite.
# MAGIC - **incremental** → new tickers `period="max"`; stale tickers (latest stored date older
# MAGIC   than STALENESS_DAYS) from a global `start = min(latest stored date) − buffer`. Batched
# MAGIC   `yf.download` can't take a per-ticker start, so the global window over-fetches a few
# MAGIC   recent days; the MERGE on `(ticker, date)` makes that idempotent.

# COMMAND ----------

try:
    _has_prices = spark.table(prices_tbl).limit(1).count() > 0
except Exception:
    _has_prices = False

MODE = "full" if (FORCE_FULL_REFRESH or not _has_prices) else "incremental"

full_tickers, incr_tickers, incr_start = [], [], None

if MODE == "full":
    full_tickers = list(ACTIVE_TICKERS)
    print(f"FULL refresh — {len(full_tickers):,} tickers, period=max (overwrite)")
else:
    maxd = {
        r["ticker"]: r["max_date"]
        for r in spark.sql(f"SELECT ticker, MAX(date) AS max_date FROM {prices_tbl} GROUP BY ticker").collect()
    }
    cutoff = date.today() - timedelta(days=STALENESS_DAYS)
    full_tickers = [t for t in ACTIVE_TICKERS if t not in maxd]                       # never seen → full hist
    incr_tickers = [t for t in ACTIVE_TICKERS if t in maxd and maxd[t] < cutoff]      # stale → gap-fill
    stale_maxes  = [maxd[t] for t in incr_tickers]
    if stale_maxes:
        incr_start = min(stale_maxes) - timedelta(days=INCR_BUFFER_DAYS)
    print("INCREMENTAL run:")
    print(f"  Universe         : {len(ACTIVE_TICKERS):,}")
    print(f"  New (period=max) : {len(full_tickers):,}")
    print(f"  Stale (gap-fill) : {len(incr_tickers):,}  start={incr_start}")
    print(f"  Fresh (skipped)  : {len(ACTIVE_TICKERS) - len(full_tickers) - len(incr_tickers):,}")

# COMMAND ----------

# MAGIC %md ## 3. Fetch daily prices (batched)

# COMMAND ----------

def _classify_mkt_error(e: Exception) -> dict:
    """Classify a market data fetch exception."""
    msg = str(e)[:500]
    if isinstance(e, (ConnectionError, TimeoutError)):
        error_type = "timeout"
    elif "404" in msg or "Not Found" in msg:
        error_type = "http_404"
    elif "5" in msg[:1] and "Server" in msg:
        error_type = "http_5xx"
    else:
        error_type = "other"
    return {"error_type": error_type, "error_message": msg, "step": "market_data"}


def _empty_err(ticker: str) -> dict:
    """Same shape as the old per-ticker empty-history failure."""
    return {"error_type": "empty_facts",
            "error_message": f"No price history for {ticker}",
            "step": "market_data"}


def _daily_frame(ticker: str, sub: pd.DataFrame, fetched_at) -> pd.DataFrame:
    """Vectorized: a ticker's OHLCV frame → tidy daily rows. No per-row loop (daily grain
    over the full universe is millions of rows — building dicts row-by-row would blow up
    the driver). `close` = raw Close; `adj_close` = Adj Close (auto_adjust=False)."""
    if "Close" not in sub.columns:
        return pd.DataFrame()
    adj = sub["Adj Close"] if "Adj Close" in sub.columns else pd.Series(index=sub.index, dtype="float64")
    vol = sub["Volume"] if "Volume" in sub.columns else pd.Series(index=sub.index, dtype="float64")
    out = pd.DataFrame({
        "ticker":     ticker.upper(),
        "date":       pd.to_datetime(sub.index).date,
        "close":      pd.to_numeric(sub["Close"], errors="coerce"),
        "adj_close":  pd.to_numeric(adj, errors="coerce"),
        "volume":     pd.to_numeric(vol, errors="coerce"),
        "fetched_at": fetched_at,
    })
    return out.dropna(subset=["close"]).reset_index(drop=True)


def fetch_batch(batch: list, period: str | None = None, start=None) -> tuple:
    """Download a batch in ONE yf.download call (auto_adjust=False → raw Close + Adj Close)
    and return (list_of_per_ticker_frames, failed). Either `period` (e.g. 'max') or `start`
    is used. MultiIndex (multi-ticker) and flat (single-ticker) shapes both handled; a ticker
    yf drops or that returns no/all-NaN Close is logged via _classify_mkt_error / _empty_err."""
    fetched_at = datetime.utcnow()
    kw = dict(group_by="ticker", threads=True, progress=False, auto_adjust=False)
    if start is not None:
        kw["start"] = start.isoformat() if hasattr(start, "isoformat") else start
    else:
        kw["period"] = period or "max"
    try:
        data = yf.download(batch, **kw)
    except Exception as e:
        err = _classify_mkt_error(e)
        return [], [{"ticker": t, "error": err} for t in batch]

    if data is None or data.empty:
        return [], [{"ticker": t, "error": _empty_err(t)} for t in batch]

    multi = isinstance(data.columns, pd.MultiIndex)
    present = set(data.columns.get_level_values(0)) if multi else set(batch)

    frames, failed_local = [], []
    for t in batch:
        try:
            if multi:
                if t not in present:                 # yf dropped this ticker entirely
                    failed_local.append({"ticker": t, "error": _empty_err(t)})
                    continue
                sub = data[t]
            else:
                sub = data                           # single-ticker batch → flat columns
            fr = _daily_frame(t, sub, fetched_at)
            if not fr.empty:
                frames.append(fr)
            else:                                    # present but all-NaN Close
                failed_local.append({"ticker": t, "error": _empty_err(t)})
        except Exception as e:
            failed_local.append({"ticker": t, "error": _classify_mkt_error(e)})
    return frames, failed_local


def _run_fetch(tickers: list, period=None, start=None, label="") -> tuple:
    """Batch + thread a fetch set. Returns (frames, failed)."""
    if not tickers:
        return [], []
    frames, failed = [], []
    lock = Lock()
    done = [0]
    total = len(tickers)
    batches = [tickers[i:i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]
    print(f"  {label}: {total:,} tickers → {len(batches)} batches of ≤{BATCH_SIZE}")
    with ThreadPoolExecutor(max_workers=BATCH_WORKERS) as pool:
        futs = {pool.submit(fetch_batch, b, period, start): b for b in batches}
        for fut in as_completed(futs):
            b = futs[fut]
            try:
                fr, fl = fut.result()
            except Exception as e:   # defensive — fetch_batch catches internally
                fr, fl = [], [{"ticker": t, "error": _classify_mkt_error(e)} for t in b]
            with lock:
                frames.extend(fr)
                failed.extend(fl)
                done[0] += len(b)
                print(f"    [{done[0]:>4}/{total}] (+{len(fr)} tickers, +{len(fl)} failed)")
    return frames, failed

# COMMAND ----------

_mkt_scraped_at = datetime.utcnow()
all_frames, failed = [], []

_f1, _e1 = _run_fetch(full_tickers, period="max", label="full-history")
_f2, _e2 = _run_fetch(incr_tickers, start=incr_start, label="gap-fill") if incr_start else ([], [])
all_frames = _f1 + _f2
failed = _e1 + _e2

_n_rows = sum(len(f) for f in all_frames)
print(f"\n  Fetched : {_n_rows:,} daily rows across {len(all_frames):,} tickers")
print(f"  Failed  : {len(failed)} tickers")

# COMMAND ----------

# MAGIC %md ## 4. Write `market_prices_daily` (overwrite on full, MERGE on incremental)

# COMMAND ----------

_wrote_prices = False
if all_frames:
    prices_pd = pd.concat(all_frames, ignore_index=True)
    prices_pd["ticker"]    = prices_pd["ticker"].astype(str)
    prices_pd["date"]      = pd.to_datetime(prices_pd["date"]).dt.date
    prices_pd["close"]     = prices_pd["close"].astype(float)
    prices_pd["adj_close"] = prices_pd["adj_close"].astype(float)
    prices_pd["volume"]    = prices_pd["volume"].astype("Int64")
    prices_pd["fetched_at"] = pd.to_datetime(prices_pd["fetched_at"])

    _price_schema = StructType([
        StructField("ticker",     StringType(),    False),
        StructField("date",       DateType(),      False),
        StructField("close",      DoubleType(),    True),
        StructField("adj_close",  DoubleType(),    True),
        StructField("volume",     LongType(),      True),
        StructField("fetched_at", TimestampType(), True),
    ])
    # volume Int64 (nullable) → object with None for NaN so Spark casts cleanly to LongType.
    prices_pd["volume"] = prices_pd["volume"].astype("object").where(prices_pd["volume"].notna(), None)
    prices_sdf = spark.createDataFrame(prices_pd, schema=_price_schema)

    if MODE == "full":
        # Initial / full backfill: single overwrite, NOT a MERGE (MERGE is for incrementals).
        (prices_sdf.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(prices_tbl))
        print(f"✓ OVERWROTE {prices_tbl} — {prices_sdf.count():,} rows")
    else:
        prices_sdf.createOrReplaceTempView("incoming_prices")
        spark.sql(f"""
            MERGE INTO {prices_tbl} AS t
            USING incoming_prices AS s
            ON t.ticker = s.ticker AND t.date = s.date
            WHEN MATCHED THEN UPDATE SET
                t.close = s.close, t.adj_close = s.adj_close,
                t.volume = s.volume, t.fetched_at = s.fetched_at
            WHEN NOT MATCHED THEN
                INSERT (ticker, date, close, adj_close, volume, fetched_at)
                VALUES (s.ticker, s.date, s.close, s.adj_close, s.volume, s.fetched_at)
        """)
        print(f"✓ MERGED {prices_sdf.count():,} rows into {prices_tbl}")
    _wrote_prices = True
else:
    print("✓ No new daily prices to write (incremental, all fresh).")

# COMMAND ----------

# MAGIC %md ## 5. Derive legacy `market_data` from `market_prices_daily`
# MAGIC
# MAGIC Last **raw** close per calendar year × FY `Shares Diluted`. Always rebuilt from the
# MAGIC FULL daily table (overwrite — idempotent), so it stays complete even on incremental
# MAGIC runs. For December filers these values are identical to the previous implementation
# MAGIC (regression-tested). Deprecated: nothing new should read `market_data`.

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

if spark.table(prices_tbl).limit(1).count() > 0:
    _yr = Window.partitionBy("ticker", "cy").orderBy(F.col("date").desc())
    year_end = (
        spark.table(prices_tbl)
        .withColumn("cy", F.year("date"))
        .withColumn("_rn", F.row_number().over(_yr))
        .filter(F.col("_rn") == 1)
        .select(
            "ticker",
            F.col("cy").alias("fiscal_year"),          # calendar year (name kept for schema compat)
            F.col("close").alias("price_close"),
            F.col("fetched_at"),
        )
    )

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
        year_end
        .join(shares_df, on=["ticker", "fiscal_year"], how="left")
        .withColumn(
            "market_cap",
            F.when(
                F.col("shares_diluted").isNotNull() & F.col("price_close").isNotNull(),
                F.col("price_close") * F.col("shares_diluted"),
            ),
        )
        .select("ticker", "fiscal_year", "price_close", "shares_diluted", "market_cap", "fetched_at")
    )

    (market_df.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(market_tbl))
    print(f"✓ Rebuilt {market_tbl} from {prices_tbl} — {market_df.count():,} rows")
else:
    print("⊘ market_prices_daily empty — market_data not rebuilt.")

# COMMAND ----------

# MAGIC %md ## 6. Write market_data failures to ingestion_failures

# COMMAND ----------

_failures_tbl = f"{CATALOG}.{SCHEMA}.ingestion_failures"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {_failures_tbl} (
        ticker         STRING    NOT NULL,
        error_type     STRING    NOT NULL,
        error_message  STRING,
        step           STRING    NOT NULL,
        scraped_at     TIMESTAMP NOT NULL
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

if failed:
    _fail_schema = StructType([
        StructField("ticker",        StringType(),    False),
        StructField("error_type",    StringType(),    False),
        StructField("error_message", StringType(),    True),
        StructField("step",          StringType(),    False),
        StructField("scraped_at",    TimestampType(), False),
    ])

    _fail_records = [{
        "ticker":        f["ticker"],
        "error_type":    f["error"]["error_type"],
        "error_message": f["error"]["error_message"],
        "step":          f["error"]["step"],
        "scraped_at":    _mkt_scraped_at,
    } for f in failed]

    spark.createDataFrame(_fail_records, schema=_fail_schema) \
         .write.mode("append").saveAsTable(_failures_tbl)
    print(f"✓ {len(_fail_records)} market_data failure(s) written → {_failures_tbl}")
else:
    print("✓ No market_data failures to record")

# COMMAND ----------

# MAGIC %md ## 7. Verify

# COMMAND ----------

spark.sql(f"""
    SELECT 'market_prices_daily' AS tbl, COUNT(*) AS rows, COUNT(DISTINCT ticker) AS tickers,
           MIN(date) AS min_date, MAX(date) AS max_date
    FROM {prices_tbl}
""").display()

spark.sql(f"""
    SELECT 'market_data' AS tbl, COUNT(*) AS rows, COUNT(DISTINCT ticker) AS tickers,
           MIN(fiscal_year) AS min_year, MAX(fiscal_year) AS max_year,
           COUNT(market_cap) AS rows_with_market_cap
    FROM {market_tbl}
""").display()

# Databricks notebook source
# MAGIC %md
# MAGIC # 10__ingestion / 12__fetch_market_data
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
# MAGIC
# MAGIC **Benchmark:** `BENCHMARK_TICKERS` (e.g. `SPY`) are priced into `market_prices_daily`
# MAGIC alongside the fundamentals universe so `70__backtest/71__run_backtest` can compute
# MAGIC benchmark-relative returns. They are NOT in `config.tickers` (no SEC fundamentals) and are
# MAGIC excluded from the legacy `market_data` derive.

# COMMAND ----------

# MAGIC %pip install yfinance

# COMMAND ----------

# MAGIC %run "../00__config/01__tickers"

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
from pyspark.sql.types import DateType, DoubleType, LongType, StringType, StructField, StructType, TimestampType

prices_tbl = f"{CATALOG}.{SCHEMA}.market_prices_daily"
splits_tbl = f"{CATALOG}.{SCHEMA}.stock_splits"

# ── Batched download config ──────────────────────────────────────────────────────
# yf.download already parallelizes WITHIN a batch (threads=True), so batch-level
# parallelism is kept conservative to avoid Yahoo throttling.
BATCH_SIZE     = 60      # tickers per yf.download call
BATCH_WORKERS  = 2       # concurrent batches (yf threads internally on top of this)
STALENESS_DAYS = 7       # incremental: a ticker whose latest stored date is within this is fresh
INCR_BUFFER_DAYS = 7     # incremental: refetch from (max stored date − buffer) to fill the gap

# Benchmark tickers priced into market_prices_daily for the backtester (70__backtest/71). They are
# indices/ETFs with NO SEC fundamentals, so they are deliberately NOT in config.tickers — 11 must
# never try to ingest them — and they are excluded from the legacy market_data derive below.
# 71__run_backtest reads the benchmark symbol from backtest_archetypes.json (config.benchmark = SPY);
# keep this list in sync with it. Empty list ⇒ no benchmark priced (71 degrades benchmark to NULL).
BENCHMARK_TICKERS = ["SPY"]

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

# Stock splits — sparse corporate-action store (a handful of rows per ticker, most have none).
# Consumed PER PERIOD by 22 (Net Buyback Yield % / Piotroski no-dilution) and 23 (EPS-CAGR growth)
# to rescale per-share figures to a consistent current basis (see _core/splits.py). NOT used for
# market cap — that stays raw close × raw as-of shares, both period-consistent. `ratio` is yfinance
# convention: 4:1 → 4.0, 10:1 → 10.0, 1-for-10 reverse → 0.1.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {splits_tbl} (
        ticker      STRING  NOT NULL,
        split_date  DATE    NOT NULL,
        ratio       DOUBLE,
        fetched_at  TIMESTAMP
    )
    USING DELTA
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

# Price universe = active fundamentals tickers + benchmark tickers (deduped, order-preserving).
# Benchmarks are always priced — even in override mode (e.g. tickers_override=AAPL,MSFT) — so the
# backtester can always compute benchmark-relative returns.
PRICE_UNIVERSE = list(dict.fromkeys([*ACTIVE_TICKERS, *BENCHMARK_TICKERS]))

MODE = "full" if (FORCE_FULL_REFRESH or not _has_prices) else "incremental"

full_tickers, incr_tickers, incr_start = [], [], None

if MODE == "full":
    full_tickers = list(PRICE_UNIVERSE)
    print(f"FULL refresh — {len(full_tickers):,} tickers, period=max (overwrite)")
else:
    maxd = {
        r["ticker"]: r["max_date"]
        for r in spark.sql(f"SELECT ticker, MAX(date) AS max_date FROM {prices_tbl} GROUP BY ticker").collect()
    }
    cutoff = date.today() - timedelta(days=STALENESS_DAYS)
    full_tickers = [t for t in PRICE_UNIVERSE if t not in maxd]                       # never seen → full hist
    incr_tickers = [t for t in PRICE_UNIVERSE if t in maxd and maxd[t] < cutoff]      # stale → gap-fill
    stale_maxes  = [maxd[t] for t in incr_tickers]
    if stale_maxes:
        incr_start = min(stale_maxes) - timedelta(days=INCR_BUFFER_DAYS)
    print("INCREMENTAL run:")
    print(f"  Universe         : {len(PRICE_UNIVERSE):,}  (incl. {len(BENCHMARK_TICKERS)} benchmark)")
    print(f"  New (period=max) : {len(full_tickers):,}")
    print(f"  Stale (gap-fill) : {len(incr_tickers):,}  start={incr_start}")
    print(f"  Fresh (skipped)  : {len(PRICE_UNIVERSE) - len(full_tickers) - len(incr_tickers):,}")

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
        print(f"✓ OVERWROTE {prices_tbl} — {len(prices_pd):,} rows")
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
        print(f"✓ MERGED {len(prices_pd):,} rows into {prices_tbl}")
    _wrote_prices = True
else:
    print("✓ No new daily prices to write (incremental, all fresh).")

# COMMAND ----------

# MAGIC %md ## 4b. Stock splits (sparse corporate actions)
# MAGIC
# MAGIC Fetched independently of prices (its own `yf.download(actions=True)` pass) so the delicate
# MAGIC price path stays untouched. Splits seldom change, so we pull FULL history only on the first
# MAGIC run (empty `stock_splits`) or a forced refresh; otherwise we re-pull the recent window and
# MAGIC MERGE on `(ticker, split_date)` (idempotent over the over-fetch). Consumed by 22/23 to
# MAGIC split-adjust per-share figures across periods — NOT by the market-cap path.

# COMMAND ----------

def fetch_splits_batch(batch: list, period: str | None = None, start=None) -> list:
    """One `yf.download(actions=True)` → per-ticker `(ticker, split_date, ratio)` frames.
    Splits-only: prices/dividends in the response are ignored. Missing column / no split days /
    yf error → no rows (never a 'failure' — most tickers simply never split)."""
    kw = dict(group_by="ticker", threads=True, progress=False, auto_adjust=False, actions=True)
    if start is not None:
        kw["start"] = start.isoformat() if hasattr(start, "isoformat") else start
    else:
        kw["period"] = period or "max"
    try:
        data = yf.download(batch, **kw)
    except Exception:
        return []
    if data is None or data.empty:
        return []
    multi = isinstance(data.columns, pd.MultiIndex)
    out = []
    for t in batch:
        try:
            sub = data[t] if multi else data
            if "Stock Splits" not in sub.columns:
                continue
            ss = pd.to_numeric(sub["Stock Splits"], errors="coerce")
            ss = ss[ss.fillna(0) != 0]                 # split days only (0.0 on normal days)
            if ss.empty:
                continue
            out.append(pd.DataFrame({
                "ticker":     t.upper(),
                "split_date": pd.to_datetime(ss.index).date,
                "ratio":      ss.astype(float).values,
            }))
        except Exception:
            continue
    return out


def _run_splits_fetch(tickers: list, period=None, start=None, label="") -> list:
    """Batch + thread a splits fetch set (mirrors _run_fetch; no failure tracking)."""
    if not tickers:
        return []
    frames = []
    lock = Lock()
    batches = [tickers[i:i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
    print(f"  splits {label}: {len(tickers):,} tickers → {len(batches)} batches")
    with ThreadPoolExecutor(max_workers=BATCH_WORKERS) as pool:
        futs = [pool.submit(fetch_splits_batch, b, period, start) for b in batches]
        for fut in as_completed(futs):
            try:
                fr = fut.result()
            except Exception:
                fr = []
            with lock:
                frames.extend(fr)
    return frames

# COMMAND ----------

try:
    _splits_seen = spark.table(splits_tbl).limit(1).count() > 0
except Exception:
    _splits_seen = False

# Full split history on first run / forced; else the same recent window as the price gap-fill
# (catches a split announced since the last run). Whole universe each time — splits are tiny.
_splits_full = FORCE_FULL_REFRESH or not _splits_seen
if _splits_full:
    _split_frames = _run_splits_fetch(PRICE_UNIVERSE, period="max", label="full-history")
else:
    _split_cutoff = date.today() - timedelta(days=STALENESS_DAYS + INCR_BUFFER_DAYS)
    _split_frames = _run_splits_fetch(PRICE_UNIVERSE, start=_split_cutoff, label="recent-window")

_splits_schema = StructType([
    StructField("ticker",     StringType(),    False),
    StructField("split_date", DateType(),      False),
    StructField("ratio",      DoubleType(),    True),
    StructField("fetched_at", TimestampType(), True),
])

if _split_frames:
    splits_pd = pd.concat(_split_frames, ignore_index=True)
    splits_pd = splits_pd[splits_pd["ratio"] > 0].copy()
    splits_pd["ticker"]     = splits_pd["ticker"].astype(str)
    splits_pd["split_date"] = pd.to_datetime(splits_pd["split_date"]).dt.date
    splits_pd["ratio"]      = splits_pd["ratio"].astype(float)
    splits_pd["fetched_at"] = pd.Timestamp(_mkt_scraped_at)
    splits_pd = splits_pd.drop_duplicates(subset=["ticker", "split_date"], keep="last")
    splits_sdf = spark.createDataFrame(splits_pd, schema=_splits_schema)

    if _splits_full:
        (splits_sdf.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(splits_tbl))
        print(f"✓ OVERWROTE {splits_tbl} — {len(splits_pd):,} split rows "
              f"({splits_pd['ticker'].nunique():,} tickers)")
    else:
        splits_sdf.createOrReplaceTempView("incoming_splits")
        spark.sql(f"""
            MERGE INTO {splits_tbl} AS t
            USING incoming_splits AS s
            ON t.ticker = s.ticker AND t.split_date = s.split_date
            WHEN MATCHED THEN UPDATE SET t.ratio = s.ratio, t.fetched_at = s.fetched_at
            WHEN NOT MATCHED THEN
                INSERT (ticker, split_date, ratio, fetched_at)
                VALUES (s.ticker, s.split_date, s.ratio, s.fetched_at)
        """)
        print(f"✓ MERGED {len(splits_pd):,} split rows into {splits_tbl}")
else:
    print("✓ No splits fetched this run.")

# COMMAND ----------

# MAGIC %md ## 5. Legacy `market_data` — no longer rebuilt
# MAGIC
# MAGIC `market_data` (last **raw** close per CALENDAR year × FY `Shares Diluted`) is **frozen**:
# MAGIC it is no longer rebuilt here. Its calendar-vs-fiscal 0–11mo offset distorted multiples for
# MAGIC non-December filers. The period_end-aligned replacement is `market_cap_asof` (price +
# MAGIC market cap as-of each FY's real fiscal close), written by `22__derived_metrics` and read
# MAGIC by `23__intrinsic_value` (Margin of Safety) and `51__export_dashboard_data` (Market Cap).
# MAGIC The old table is left in place for any ad-hoc back-compat query but receives no new data.

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

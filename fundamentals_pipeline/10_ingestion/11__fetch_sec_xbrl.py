# Databricks notebook source
# MAGIC %md
# MAGIC # 10_ingestion / 11__fetch_sec_xbrl
# MAGIC
# MAGIC Fetches the **Holy Trinity** (Balance Sheet, Income Statement, Cash Flow)
# MAGIC from SEC EDGAR's XBRL API for all active tickers in `config/tickers`,
# MAGIC including **both annual (10-K) and quarterly (10-Q) filings**.
# MAGIC
# MAGIC **Architecture:**
# MAGIC - **Parallel fetch** with `ThreadPoolExecutor` (~15-20 min for 3000 tickers)
# MAGIC - **Global rate limiter** via Lock + monotonic clock (SEC allows 10 req/s, we cap at ~8)
# MAGIC - **Arrow-accelerated** pandas→Spark conversion
# MAGIC - **Batched writes** to Delta in chunks of 250 tickers each, avoiding
# MAGIC   the multi-million-row single-shot createDataFrame bottleneck
# MAGIC
# MAGIC **Writes to:** `{catalog}.{schema}.financials_raw` (append-only audit log)

# COMMAND ----------

# MAGIC %md ## 0. Load config

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

import json
import requests
import time
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType, DateType,
)

# Enable Arrow for fast pandas↔Spark conversion (10–30x speedup)
# Arrow is enabled by default on Databricks Runtime 13+ / Serverless
# (cannot be set at runtime on managed clusters — already on)

HEADERS = {"User-Agent": SEC_USER_AGENT}

# ── Parallelism & rate limit ──────────────────────────────────────────────────
MAX_WORKERS        = 8       # concurrent threads (SEC allows 10 req/s)
MIN_REQUEST_GAP    = 0.12    # global minimum gap between request starts (seconds)
REQUEST_TIMEOUT    = 30

# ── Write batching ────────────────────────────────────────────────────────────
# Flush a Delta append every N completed tickers. Keeps each createDataFrame
# under ~500K rows so it's fast; also gives partial-progress safety if the
# notebook is interrupted.
BATCH_TICKERS      = 250

# ── Refresh policy ────────────────────────────────────────────────────────────
# FORCE_FULL_REFRESH re-fetches every ticker, ignoring the staleness guard below.
# Inherited from 91 via %run (same globals() handoff as ACTIVE_TICKERS above) —
# dbutils.widgets.get() does NOT reliably read the parent's widget under %run, so
# we read the variable 91 already set. Standalone runs fall back to this notebook's
# own widget, else False (normal incremental behaviour).
if "force_full_refresh" in globals():
    FORCE_FULL_REFRESH = str(force_full_refresh).strip().lower() == "true"
else:
    try:
        FORCE_FULL_REFRESH = dbutils.widgets.get("force_full_refresh").strip().lower() == "true"
    except Exception:
        FORCE_FULL_REFRESH = False
STALENESS_DAYS     = 3

raw_full = f"{CATALOG}.{SCHEMA}.{RAW_TABLE}"

# COMMAND ----------

# MAGIC %md ## 1. Decide which tickers to fetch

# COMMAND ----------

cutoff = datetime.utcnow() - timedelta(days=STALENESS_DAYS)

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
    print(f"  Already fresh  : {skipped:,} tickers (last {STALENESS_DAYS}d)")
    print(f"  To fetch       : {len(RUN_TICKERS):,} tickers")

if not RUN_TICKERS:
    print("\n✓ All tickers are up to date — nothing to fetch.")

# COMMAND ----------

# MAGIC %md ## 2. Global rate limiter

# COMMAND ----------

_rate_lock = Lock()
_last_request_ts = [0.0]

def rate_limited_get(url: str, timeout: int = REQUEST_TIMEOUT) -> requests.Response:
    """Thread-safe HTTP GET enforcing global MIN_REQUEST_GAP between request starts."""
    with _rate_lock:
        wait = _last_request_ts[0] + MIN_REQUEST_GAP - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _last_request_ts[0] = time.monotonic()
    return requests.get(url, headers=HEADERS, timeout=timeout)

# COMMAND ----------

# MAGIC %md ## 3. SEC fetch helpers

# COMMAND ----------

# Prefetch ticker→CIK index once (single ~13MB download shared by all threads)
print("Loading SEC ticker index...")
_idx_resp = rate_limited_get("https://www.sec.gov/files/company_tickers.json")
_idx_resp.raise_for_status()
_idx = _idx_resp.json()

TICKER_MAP = {
    entry["ticker"].upper(): (str(entry["cik_str"]).zfill(10), entry["title"])
    for entry in _idx.values()
}
print(f"✓ Ticker index loaded — {len(TICKER_MAP):,} tickers known to SEC")

# ── CIK overrides from favorites.json ────────────────────────────────────
_FAV_CIK_OVERRIDES = {}   # ticker → (cik_padded, company)
_FAV_CIK_ALIASES   = {}   # ticker → [cik_padded, ...]  (predecesores tras fusiones/reorgs)
_ALIAS_MAP = {}            # alias → canonical_ticker
try:
    with open(FAVORITES_JSON_PATH, "r", encoding="utf-8") as f:
        _fav_raw = f.read()
    _fav_lines = [l for l in _fav_raw.splitlines() if not l.strip().startswith("/")]
    for _entry in json.loads("\n".join(_fav_lines)):
        _t = _entry["ticker"].upper().strip()
        if _entry.get("cik"):
            _FAV_CIK_OVERRIDES[_t] = (_entry["cik"].zfill(10), _entry.get("company", _t))
        _cik_aliases = [str(c).zfill(10) for c in _entry.get("cik_aliases", []) if c]
        if _cik_aliases:
            _FAV_CIK_ALIASES[_t] = _cik_aliases
        for _alias in _entry.get("aliases", []):
            _ALIAS_MAP[_alias.upper().strip()] = _t
except Exception as _e:
    print(f"  ⚠ Could not load favorites overrides: {_e}")
if _FAV_CIK_OVERRIDES:
    print(f"  ✓ CIK overrides: {list(_FAV_CIK_OVERRIDES.keys())}")
if _FAV_CIK_ALIASES:
    print(f"  ✓ CIK aliases   : {_FAV_CIK_ALIASES}")


def get_cik(ticker: str) -> tuple:
    t = ticker.upper()
    if t in _FAV_CIK_OVERRIDES:
        return _FAV_CIK_OVERRIDES[t]
    if t in TICKER_MAP:
        return TICKER_MAP[t]
    canonical = _ALIAS_MAP.get(t)
    if canonical:
        if canonical in _FAV_CIK_OVERRIDES:
            return _FAV_CIK_OVERRIDES[canonical]
        if canonical in TICKER_MAP:
            return TICKER_MAP[canonical]
    raise ValueError(f"Ticker '{ticker}' not found in SEC database.")


def get_facts(cik: str) -> dict:
    url  = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    resp = rate_limited_get(url)
    resp.raise_for_status()
    if "json" not in resp.headers.get("Content-Type", "").lower():
        raise ValueError(f"Non-JSON response (Content-Type: {resp.headers.get('Content-Type')})")
    return resp.json()


def merge_facts(*facts_dicts: dict) -> dict:
    """
    Concatena los arrays `facts[ns][concept]["units"][unit]` a través de múltiples
    JSONs de companyfacts. Útil cuando un ticker tiene CIKs predecesores (p.ej. tras
    una fusión MLP→C-corp): el histórico vive bajo el CIK viejo y los filings recientes
    bajo el nuevo. Mergeamos los facts crudos y dejamos que la dedup downstream en
    21__clean_and_merge.py — Window por (ticker, stmt, concept, fy) con latest filed —
    resuelva cualquier solape.
    """
    if not facts_dicts:
        return {}
    if len(facts_dicts) == 1:
        return facts_dicts[0]

    merged = {"facts": {}}
    # Preservar metadatos del primer dict (que será el CIK primario)
    for k, v in facts_dicts[0].items():
        if k != "facts":
            merged[k] = v

    for fd in facts_dicts:
        for ns, concepts in fd.get("facts", {}).items():
            ns_bucket = merged["facts"].setdefault(ns, {})
            for concept, payload in concepts.items():
                if concept not in ns_bucket:
                    # primera vez que vemos este concept: copiamos shallow + clonamos units
                    ns_bucket[concept] = {
                        "label":       payload.get("label"),
                        "description": payload.get("description"),
                        "units":       {u: list(rows) for u, rows in payload.get("units", {}).items()},
                    }
                else:
                    existing_units = ns_bucket[concept]["units"]
                    for unit_key, rows in payload.get("units", {}).items():
                        existing_units.setdefault(unit_key, []).extend(rows)
    return merged


def extract_series(facts: dict, concept: str, kind: str, namespace: str = "us-gaap") -> pd.DataFrame:
    """Extract all rows of one XBRL concept across 10-K/10-Q filings, classified by period shape."""
    try:
        units    = facts["facts"][namespace][concept]["units"]
        unit_key = "USD" if "USD" in units else list(units.keys())[0]
        rows     = units[unit_key]
    except KeyError:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df[df["form"].isin(["10-K", "10-Q", "10-K/A", "10-Q/A"])].copy()
    if df.empty:
        return df

    df["end"]   = pd.to_datetime(df["end"])
    df["filed"] = pd.to_datetime(df["filed"])
    if "start" in df.columns:
        df["start"] = pd.to_datetime(df["start"], errors="coerce")
    else:
        df["start"] = pd.NaT

    df["period_shape"] = classify_period_shape_series(df["start"], df["end"])

    if kind == "stock":
        df = df[df["period_shape"] == "snapshot"].copy()
    else:
        df = df[df["period_shape"] != "snapshot"].copy()

    return df[[
        "fy", "fp", "form", "start", "end", "period_shape", "val", "filed"
    ]].rename(columns={
        "start": "period_start",
        "end":   "period_end",
        "val":   "value",
    })


def extract_series_multi(facts: dict, concepts, kind: str, namespace: str = "us-gaap") -> pd.DataFrame:
    """Extract one concept from a priority list of XBRL tags — first non-empty wins.

    `concepts` may be a single tag (str) or a list[str] of fallback tags tried in
    PRIORITY ORDER. We return the FIRST tag that yields data for this company's
    filings and stop; remaining tags are ignored. We deliberately do NOT merge
    values across tags within one concept — summing e.g. ``LongTermDebtNoncurrent``
    and the aggregate ``LongTermDebt`` would double-count, since the latter already
    includes the current portion. A single str preserves the original single-tag
    behaviour exactly (one-element loop).

    Caveat (documented in the debt mapping in 01__tickers.py): if a filer reports
    ONLY the aggregate ``LongTermDebt`` (which folds in the current portion) while
    also reporting a current-debt tag under "Short-term Debt", the current portion
    can be counted twice. Acceptable approximation for a leverage ratio.
    """
    tags = [concepts] if isinstance(concepts, str) else list(concepts)
    for tag in tags:
        series = extract_series(facts, tag, kind, namespace=namespace)
        if not series.empty:
            return series
    return pd.DataFrame()

# COMMAND ----------

# MAGIC %md ## 4. Per-ticker worker

# COMMAND ----------

def _classify_error(e: Exception, step: str) -> dict:
    """Classify an exception into a structured error dict for ingestion_failures."""
    msg = str(e)[:500]
    if step == "fetch_cik":
        error_type = "cik_not_found"
    elif isinstance(e, requests.exceptions.Timeout):
        error_type = "timeout"
    elif isinstance(e, requests.exceptions.HTTPError):
        code = getattr(e.response, "status_code", 0)
        error_type = "http_404" if code == 404 else "http_5xx" if 500 <= code < 600 else f"http_{code}"
    elif "Non-JSON" in str(e):
        error_type = "non_json_response"
    elif isinstance(e, json.JSONDecodeError):
        error_type = "json_decode"
    else:
        error_type = "other"
    return {"error_type": error_type, "error_message": msg, "step": step}


def process_ticker(ticker: str, scraped_at_ts: datetime) -> tuple:
    """Returns (records, error_dict | None). error_dict has: error_type, error_message, step."""
    records = []
    try:
        cik, company_name = get_cik(ticker)
    except Exception as e:
        return [], _classify_error(e, "fetch_cik")

    try:
        facts = get_facts(cik)
    except Exception as e:
        return [], _classify_error(e, "fetch_facts")

    # Fusionar CIKs predecesores (fusiones, MLP→C-corp, spinoffs). Un alias roto
    # no debe romper la ingesta del ticker — log y seguir con lo que tengamos.
    _aliases = _FAV_CIK_ALIASES.get(ticker.upper(), [])
    if _aliases:
        _alias_facts = []
        for _alias_cik in _aliases:
            if _alias_cik == cik:
                continue
            try:
                _alias_facts.append(get_facts(_alias_cik))
            except Exception as _alias_err:
                print(f"    ⚠ {ticker}: alias CIK {_alias_cik} failed ({_alias_err})")
        if _alias_facts:
            facts = merge_facts(facts, *_alias_facts)

    try:
        for stmt_name, concept_map in STATEMENTS.items():
            for label, (xbrl_concept, kind) in concept_map.items():
                # xbrl_concept may be a single tag (str) or a priority list[str].
                series = extract_series_multi(facts, xbrl_concept, kind)
                if series.empty:
                    continue
                for _, row in series.iterrows():
                    records.append({
                        "ticker":       ticker.upper(),
                        "company":      company_name,
                        "stmt":         stmt_name,
                        "concept":      label,
                        "kind":         kind,
                        "fy":           int(row["fy"])   if pd.notna(row["fy"])   else None,
                        "fp":           row["fp"]        if pd.notna(row["fp"])   else None,
                        "form":         row["form"],
                        "period_start": row["period_start"].date() if pd.notna(row["period_start"]) else None,
                        "period_end":   row["period_end"].date(),
                        "period_shape": row["period_shape"],
                        "value":        float(row["value"]) if pd.notna(row["value"]) else None,
                        "filed":        row["filed"].date() if pd.notna(row["filed"]) else None,
                        "scraped_at":   scraped_at_ts,
                    })
        if not records:
            return [], {"error_type": "empty_facts", "error_message": f"No XBRL facts extracted for {ticker}", "step": "extract"}
        return records, None
    except Exception as e:
        return records, _classify_error(e, "extract")

# COMMAND ----------

# MAGIC %md ## 5. Ensure target Delta table & schema

# COMMAND ----------

if RUN_TICKERS:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {raw_full} (
            ticker        STRING    NOT NULL,
            company       STRING,
            stmt          STRING    NOT NULL,
            concept       STRING    NOT NULL,
            kind          STRING    NOT NULL,
            fy            INT,
            fp            STRING,
            form          STRING,
            period_start  DATE,
            period_end    DATE      NOT NULL,
            period_shape  STRING,
            value         DOUBLE,
            filed         DATE,
            scraped_at    TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (ticker)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true'
        )
    """)

    SCHEMA_DEF = StructType([
        StructField("ticker",        StringType(),    False),
        StructField("company",       StringType(),    True),
        StructField("stmt",          StringType(),    False),
        StructField("concept",       StringType(),    False),
        StructField("kind",          StringType(),    False),
        StructField("fy",            IntegerType(),   True),
        StructField("fp",            StringType(),    True),
        StructField("form",          StringType(),    True),
        StructField("period_start",  DateType(),      True),
        StructField("period_end",    DateType(),      False),
        StructField("period_shape",  StringType(),    True),
        StructField("value",         DoubleType(),    True),
        StructField("filed",         DateType(),      True),
        StructField("scraped_at",    TimestampType(), True),
    ])

# COMMAND ----------

# MAGIC %md ## 6. Batched flush helper

# COMMAND ----------

def flush_batch(batch_records: list) -> int:
    """Convert a batch of records to a Spark DataFrame (via Arrow) and append to Delta."""
    if not batch_records:
        return 0

    pdf = pd.DataFrame(batch_records)
    pdf["fy"] = pdf["fy"].astype("Int64")   # nullable int for clean Arrow null handling

    sdf = spark.createDataFrame(pdf, schema=SCHEMA_DEF)

    (sdf
        .write
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(raw_full)
    )
    return len(pdf)

# COMMAND ----------

# MAGIC %md ## 7. Parallel scrape with incremental batched writes

# COMMAND ----------

if RUN_TICKERS:

    scraped_at   = datetime.utcnow()
    buffer       = []
    failed       = []

    state_lock   = Lock()
    completed    = [0]
    rows_written = [0]
    total        = len(RUN_TICKERS)
    started_at   = time.monotonic()

    def submit_and_collect(ticker: str):
        records, err = process_ticker(ticker, scraped_at)

        should_flush = False
        to_write     = None

        with state_lock:
            completed[0] += 1
            n = completed[0]

            if err:
                failed.append({"ticker": ticker, "error": err})
                print(f"  [{n:>4}/{total}] ✗  {ticker:<6} [{err['error_type']}] {err['error_message'][:80]}")
            else:
                buffer.extend(records)

                if n % 25 == 0 or n == total:
                    elapsed = time.monotonic() - started_at
                    rate    = n / elapsed if elapsed else 0
                    eta_s   = (total - n) / rate if rate else 0
                    print(f"  [{n:>4}/{total}] ✓  {ticker:<6} "
                          f"({rate:.1f} t/s, ETA {eta_s/60:.1f} min, "
                          f"buffer {len(buffer):,} rows)")

                if (n % BATCH_TICKERS == 0) or (n == total):
                    should_flush = True
                    to_write     = buffer.copy()
                    buffer.clear()

        # Flush outside the state_lock so other threads can keep buffering
        if should_flush and to_write:
            wrote = flush_batch(to_write)
            with state_lock:
                rows_written[0] += wrote
            print(f"  ── flushed {wrote:,} rows to Delta "
                  f"(total written: {rows_written[0]:,}) ──")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(submit_and_collect, t) for t in RUN_TICKERS]
        for _ in as_completed(futures):
            pass

    # Safety net: if any records remain (shouldn't happen since last ticker triggers flush)
    if buffer:
        wrote = flush_batch(buffer)
        rows_written[0] += wrote
        buffer.clear()
        print(f"  ── final safety flush: {wrote:,} rows ──")

    total_elapsed = time.monotonic() - started_at
    print(f"\n{'='*60}")
    print(f"  Scraped     : {total - len(failed)} / {total} tickers")
    print(f"  Rows written: {rows_written[0]:,}")
    print(f"  Elapsed     : {total_elapsed/60:.1f} min "
          f"({total/total_elapsed:.1f} tickers/sec)")
    if failed:
        print(f"  Failed      : {len(failed)} tickers")
        for f in failed[:10]:
            print(f"                {f['ticker']:<6} [{f['error']['error_type']}] {f['error']['error_message'][:60]}")
        if len(failed) > 10:
            print(f"                ... and {len(failed)-10} more")
    print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md ## 8. Write ingestion failures to Delta

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

if RUN_TICKERS and failed:
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType as _TS

    _fail_schema = StructType([
        StructField("ticker",        StringType(), False),
        StructField("error_type",    StringType(), False),
        StructField("error_message", StringType(), True),
        StructField("step",          StringType(), False),
        StructField("scraped_at",    _TS(),        False),
    ])

    _fail_records = [{
        "ticker":        f["ticker"],
        "error_type":    f["error"]["error_type"],
        "error_message": f["error"]["error_message"],
        "step":          f["error"]["step"],
        "scraped_at":    scraped_at,
    } for f in failed]

    spark.createDataFrame(_fail_records, schema=_fail_schema) \
         .write.mode("append").saveAsTable(_failures_tbl)
    print(f"✓ {len(_fail_records)} failure(s) written → {_failures_tbl}")
else:
    print(f"✓ No ingestion failures to record")

# COMMAND ----------

# MAGIC %md ## 9. Verify

# COMMAND ----------

if RUN_TICKERS:
    spark.sql(f"""
        SELECT
            COUNT(*)               AS total_rows,
            COUNT(DISTINCT ticker)  AS distinct_tickers,
            MIN(scraped_at)        AS first_scrape,
            MAX(scraped_at)        AS last_scrape
        FROM {raw_full}
        WHERE scraped_at >= '{scraped_at.isoformat()}'
    """).display()

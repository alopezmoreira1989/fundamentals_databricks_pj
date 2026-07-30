# Databricks notebook source
# MAGIC %md
# MAGIC # 10__ingestion / 15__fetch_sec_filings
# MAGIC
# MAGIC Fetches each ticker's real SEC filing list (10-K/10-Q — form, filing date, report date,
# MAGIC and a direct document link) for the company page's **Filings tab**.
# MAGIC
# MAGIC **Why this lives in the pipeline, not a frontend (found 2026-07 the hard way):** both
# MAGIC consumer apps originally fetched this themselves — `web/` live, per request
# MAGIC (`infrastructure/filings.py`); `fundamentals_screener` with a lighter sync-time
# MAGIC ticker→CIK-only fetch, needing its own `SEC_USER_AGENT` setting on every consumer host.
# MAGIC The second one caused a real production incident: an unconfigured `SEC_USER_AGENT` on
# MAGIC the consumer crashed its whole deploy script. The pipeline already has a real, working
# MAGIC `SEC_USER_AGENT` (`00__config/01__tickers.py`) and proven rate-limited fetch
# MAGIC infrastructure for this exact ~2,600-ticker universe (see `11__fetch_sec_xbrl.py`) —
# MAGIC doing this fetch ONCE here and publishing it (`51__export_dashboard_data.py`'s
# MAGIC `dashboard_filings` artifact) means NEITHER frontend needs its own SEC credentials or
# MAGIC live network call ever again; both just read a published field, like every other
# MAGIC descriptive fact.
# MAGIC
# MAGIC **Scope, deliberately narrow:** 10-K/10-Q only (matches both frontends' existing filter,
# MAGIC no scope creep to other form types), the 40 most recent filings per ticker (matches
# MAGIC `web/`'s existing cap).
# MAGIC
# MAGIC **Self-contained, mirroring `13__fetch_dimensional_10k.py`'s established pattern:** its
# MAGIC own CIK resolution and rate limiter, not cross-imported from `11__fetch_sec_xbrl.py` —
# MAGIC these notebook stages are independent Databricks job tasks, not importable Python
# MAGIC modules.
# MAGIC
# MAGIC **Writes to:** `{catalog}.{schema}.sec_filings` — full overwrite each run. Filings
# MAGIC change slowly (~quarterly per ticker), so re-fetching the whole universe daily is cheap
# MAGIC relative to the rest of the pipeline and needs no incremental-staleness tracking (unlike
# MAGIC `11`'s much heavier companyfacts fetch).

# COMMAND ----------

# MAGIC %run "../00__config/01__tickers"

# COMMAND ----------

# Inherit ACTIVE_TICKERS from the parent orchestrator (91__full_pipeline runs 11 first, in the
# same notebook globals) when available — same idiom 11__fetch_sec_xbrl.py itself uses — else
# derive fresh for a standalone run of this notebook.
if "ACTIVE_TICKERS" not in globals() or not ACTIVE_TICKERS:
    ACTIVE_TICKERS = [row.ticker for row in spark.table(f"{CATALOG}.config.tickers").select("ticker").collect()]
    print(f"✓ Config loaded — {len(ACTIVE_TICKERS)} active tickers from {CATALOG}.config.tickers")
else:
    print(f"✓ Inherited {len(ACTIVE_TICKERS)} tickers from parent (override mode)")

# COMMAND ----------

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from threading import Lock

import requests
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

HEADERS = {"User-Agent": SEC_USER_AGENT}

# Same rate-limiting discipline as 11__fetch_sec_xbrl.py (SEC allows 10 req/s, cap at ~8).
MAX_WORKERS     = 8
MIN_REQUEST_GAP = 0.12
REQUEST_TIMEOUT = 30
MAX_FILINGS     = 40                # per ticker — matches web/'s existing cap
FORM_TYPES      = ("10-K", "10-Q")  # matches both frontends' existing filter

filings_tbl  = f"{CATALOG}.{SCHEMA}.sec_filings"
failures_tbl = f"{CATALOG}.{SCHEMA}.ingestion_failures"

# COMMAND ----------

# MAGIC %md ## 1. Rate limiter + CIK resolution

# COMMAND ----------

_rate_lock = Lock()
_last_request_ts = [0.0]


def rate_limited_get(url: str) -> requests.Response:
    """Thread-safe HTTP GET enforcing a global minimum gap between request starts."""
    with _rate_lock:
        wait = _last_request_ts[0] + MIN_REQUEST_GAP - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _last_request_ts[0] = time.monotonic()
    return requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)


print("Loading SEC ticker index...")
_idx_resp = rate_limited_get("https://www.sec.gov/files/company_tickers.json")
_idx_resp.raise_for_status()
_idx = _idx_resp.json()
TICKER_MAP = {entry["ticker"].upper(): str(entry["cik_str"]).zfill(10) for entry in _idx.values()}
print(f"✓ Ticker index loaded — {len(TICKER_MAP):,} tickers known to SEC")


def get_cik(ticker: str) -> str:
    cik = TICKER_MAP.get(ticker.upper())
    if not cik:
        raise ValueError(f"Ticker '{ticker}' not found in SEC's ticker index.")
    return cik

# COMMAND ----------

# MAGIC %md ## 2. Per-ticker worker

# COMMAND ----------

def _classify_error(e: Exception, phase: str) -> dict:
    """Classify an exception into a structured error dict for ingestion_failures."""
    msg = str(e)[:500]
    if phase == "fetch_cik":
        error_type = "cik_not_found"
    elif isinstance(e, requests.exceptions.Timeout):
        error_type = "timeout"
    elif isinstance(e, requests.exceptions.HTTPError):
        code = getattr(e.response, "status_code", 0)
        error_type = "http_404" if code == 404 else "http_5xx" if 500 <= code < 600 else f"http_{code}"
    elif isinstance(e, requests.exceptions.RequestException):
        error_type = "connection_error"
    else:
        error_type = "other"
    return {"error_type": error_type, "error_message": msg}


def process_ticker(ticker: str) -> tuple:
    """Returns (records: list[dict], error_dict | None)."""
    try:
        cik = get_cik(ticker)
    except Exception as e:
        return [], _classify_error(e, "fetch_cik")

    try:
        resp = rate_limited_get(f"https://data.sec.gov/submissions/CIK{cik}.json")
        resp.raise_for_status()
        subs = resp.json()
    except Exception as e:
        return [], _classify_error(e, "fetch_submissions")

    rec    = subs.get("filings", {}).get("recent", {})
    forms  = rec.get("form", [])
    accs   = rec.get("accessionNumber", [])
    docs   = rec.get("primaryDocument", [])
    fdates = rec.get("filingDate", [])
    rdates = rec.get("reportDate", [])
    descs  = rec.get("primaryDocDescription", [])

    cik_int = int(cik)
    records = []
    for form, acc, doc, fdate, rdate, desc in zip(forms, accs, docs, fdates, rdates, descs, strict=False):
        if form not in FORM_TYPES or not doc:
            continue
        acc_nodash = acc.replace("-", "")
        url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nodash}/{doc}"
        records.append({
            "ticker": ticker.upper(),
            "form": form,
            "filing_date": fdate or None,
            "report_date": rdate or None,
            "description": desc or form,
            "url": url,
        })
        if len(records) >= MAX_FILINGS:
            break
    return records, None

# COMMAND ----------

# MAGIC %md ## 3. Parallel fetch

# COMMAND ----------

all_records: list = []
failed: list = []
completed = [0]
state_lock = Lock()
total = len(ACTIVE_TICKERS)
started_at = time.monotonic()


def submit_and_collect(ticker: str) -> None:
    records, err = process_ticker(ticker)
    with state_lock:
        completed[0] += 1
        n = completed[0]
        if err:
            failed.append({"ticker": ticker, "error": err})
        else:
            all_records.extend(records)
        if n % 250 == 0 or n == total:
            elapsed = time.monotonic() - started_at
            rate = n / elapsed if elapsed else 0
            print(f"  [{n:>4}/{total}] {rate:.1f} t/s, {len(all_records):,} filing rows so far")


with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
    futures = [pool.submit(submit_and_collect, t) for t in ACTIVE_TICKERS]
    for _ in as_completed(futures):
        pass

print(f"\n✓ Fetched filings for {total - len(failed)}/{total} tickers — {len(all_records):,} filing rows total")
if failed:
    print(f"  {len(failed)} ticker(s) failed:")
    for f in failed[:10]:
        print(f"    {f['ticker']:<6} [{f['error']['error_type']}] {f['error']['error_message'][:60]}")
    if len(failed) > 10:
        print(f"    ... and {len(failed) - 10} more")

# COMMAND ----------

# MAGIC %md ## 4. Write sec_filings (full overwrite)

# COMMAND ----------

_filings_schema = StructType([
    StructField("ticker",      StringType(), False),
    StructField("form",        StringType(), False),
    StructField("filing_date", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("description", StringType(), True),
    StructField("url",         StringType(), False),
])

if all_records:
    (spark.createDataFrame(all_records, schema=_filings_schema)
     .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(filings_tbl))
    print(f"✓ wrote {len(all_records):,} rows → {filings_tbl}")
else:
    print("⚠ No filing rows fetched this run — leaving any existing sec_filings table untouched")

# COMMAND ----------

# MAGIC %md ## 5. Log failures

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {failures_tbl} (
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
    _scraped_at = datetime.utcnow()
    _fail_schema = StructType([
        StructField("ticker",        StringType(),    False),
        StructField("error_type",    StringType(),    False),
        StructField("error_message", StringType(),    True),
        StructField("step",          StringType(),    False),
        StructField("scraped_at",    TimestampType(), False),
    ])
    _fail_records = [{
        "ticker": f["ticker"],
        "error_type": f["error"]["error_type"],
        "error_message": f["error"]["error_message"],
        "step": "sec_filings",
        "scraped_at": _scraped_at,
    } for f in failed]
    (spark.createDataFrame(_fail_records, schema=_fail_schema)
     .write.mode("append").saveAsTable(failures_tbl))
    print(f"✓ Logged {len(_fail_records):,} failure(s) to {failures_tbl}")
else:
    print("✓ No failures to log")

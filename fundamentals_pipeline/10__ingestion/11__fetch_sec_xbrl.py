# Databricks notebook source
# MAGIC %md
# MAGIC # 10__ingestion / 11__fetch_sec_xbrl
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

# MAGIC %run "../00__config/01__tickers"

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
    Concatenates the `facts[ns][concept]["units"][unit]` arrays across multiple
    companyfacts JSONs. Useful when a ticker has predecessor CIKs (e.g. after an
    MLP→C-corp conversion): historical data lives under the old CIK and recent
    filings under the new one. We merge the raw facts and let the downstream dedup in
    21__clean_and_merge.py — Window by (ticker, stmt, concept, fy) with latest filed —
    resolve any overlap.
    """
    if not facts_dicts:
        return {}
    if len(facts_dicts) == 1:
        return facts_dicts[0]

    merged = {"facts": {}}
    # Preserve metadata from the first dict (the primary CIK)
    for k, v in facts_dicts[0].items():
        if k != "facts":
            merged[k] = v

    for fd in facts_dicts:
        for ns, concepts in fd.get("facts", {}).items():
            ns_bucket = merged["facts"].setdefault(ns, {})
            for concept, payload in concepts.items():
                if concept not in ns_bucket:
                    # first time we see this concept: shallow copy + clone units
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


def _pick_unit(units: dict) -> str:
    """Choose the unit bucket that carries a concept's real series.

    A fact may expose several unit buckets. Monetary concepts use ``USD``; per-share
    concepts (EPS) use ``USD/shares``; share counts use ``shares``. We must NOT blindly
    take ``list(units.keys())[0]``: many filers (KO/PEP/WMT/DHR…) mistagged a handful of
    early (≈2009) per-share facts under the dimensionless ``pure`` unit, and that bogus
    bucket can sort FIRST in dict order. The old first-key fallback then grabbed those 2–4
    junk rows and dropped the entire real ``USD/shares`` series (every year) — nulling EPS
    Diluted/Basic, and with it Graham Number, Graham Revised and P/E, for ~870 tickers.

    Prefer the correct measure for the concept, then fall back to the largest bucket (the
    erroneous one is always tiny). ``units.get(pref)`` is truthy only when the bucket is
    present AND non-empty, so an empty preferred bucket doesn't shadow a populated one.
    """
    for pref in ("USD", "USD/shares", "shares"):
        if units.get(pref):
            return pref
    return max(units, key=lambda u: len(units[u]))


def extract_series(facts: dict, concept: str, kind: str, namespace: str = "us-gaap") -> pd.DataFrame:
    """Extract all rows of one XBRL concept across 10-K/10-Q filings, classified by period shape."""
    try:
        units    = facts["facts"][namespace][concept]["units"]
        rows     = units[_pick_unit(units)]
    except (KeyError, ValueError):
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
    """Extract one concept from a priority list of XBRL tags, resolved PER PERIOD.

    `concepts` may be a single tag (str) or a list[str] of fallback tags in PRIORITY
    ORDER (index 0 = most preferred). For each reporting period we keep the value from
    the HIGHEST-priority tag that actually carries data for THAT period.

    Why per-period and NOT first-non-empty-wins-for-the-whole-company: issuers switch
    tags across years. AT&T/VZ tag the long-term line ``LongTermDebtNoncurrent`` in old
    filings (≤2011) but ``LongTermDebt`` in recent ones. The old "first tag with any rows
    anywhere wins" locked onto ``LongTermDebtNoncurrent`` — which only has the stale early
    years — and never fell back, so recent long-term debt came back NULL → Total Debt =
    current portion only → Debt/Equity ≈ 0.07x. Resolving per period fixes the recent
    years while leaving single-tag concepts (the common case) byte-identical.

    No-double-count is preserved: only the best-priority tag's rows survive for each
    period — we never SUM two candidates within a period. So the aggregate ``LongTermDebt``
    (which folds in the current maturities) is used only when the noncurrent split is
    absent for that period; adding the separate "Short-term Debt" concept then does not
    count the current portion twice.

    A single str preserves the original single-tag behaviour exactly (fast path, no merge).

    Caveat (documented in the debt mapping in 01__tickers.py): if a filer reports ONLY the
    aggregate ``LongTermDebt`` (already incl. the current portion) for a period AND a
    separate current-debt tag under "Short-term Debt", the current portion can be counted
    twice that year. Acceptable approximation for a leverage ratio.
    """
    tags = [concepts] if isinstance(concepts, str) else list(concepts)
    if len(tags) == 1:
        # Single-tag concept (every non-debt concept): unchanged behaviour, no overhead.
        return extract_series(facts, tags[0], kind, namespace=namespace)

    frames = []
    for priority, tag in enumerate(tags):
        series = extract_series(facts, tag, kind, namespace=namespace)
        if not series.empty:
            series = series.copy()
            series["_priority"] = priority   # 0 = most preferred
            frames.append(series)
    if not frames:
        return pd.DataFrame()
    if len(frames) == 1:
        return frames[0].drop(columns="_priority")

    merged = pd.concat(frames, ignore_index=True)
    # Per reporting period, keep only the rows from the best (lowest-index) tag present.
    # Key on (fy, period span): fy MUST be in the key, not just period_start|period_end.
    # A single balance-sheet date appears in several filings — the current-year filing and
    # later 10-Ks that re-report it as a comparative — and an issuer can switch tags across
    # those filings. AT&T fy2019: the current-year (fy=2019) long-term line is tagged
    # ``LongTermDebtAndCapitalLeaseObligations`` (priority 2), while the higher-priority
    # ``LongTermDebt`` (priority 1) exists for 2019-12-31 ONLY as the fy=2020 comparative.
    # Keying on period_end alone let that fy=2020 comparative suppress the fy=2019 row; 21's
    # comparative guard (year(period_end) >= fy) then dropped the fy=2020 row → fy2019 NULL.
    # Keying on fy resolves priority WITHIN each fiscal-year occurrence, so fy2019 keeps its
    # only tag. Finer-grained than before → cannot reintroduce the global-suppression bug.
    # The period span stays in the key so flows that share a period_end across shapes
    # (Q_standalone vs YTD) don't collapse. String key is NaT-safe (NaT/NA → "").
    period_key = (
        merged["fy"].astype("string").fillna("")
        + "|" + merged["period_start"].astype("string").fillna("")
        + "|" + merged["period_end"].astype("string")
    )
    best = merged.groupby(period_key)["_priority"].transform("min")
    merged = merged[merged["_priority"] == best]
    return merged.drop(columns="_priority").reset_index(drop=True)


def extract_series_aggregate_or_sum(facts, aggregate_tag, component_tags, kind, namespace="us-gaap"):
    """Resolve a concept as the AGGREGATE tag when the filer reports it, else the per-context
    SUM of its disjoint component tags.

    Unlike ``extract_series_multi`` (which COALESCES — one tag wins per period and the rest are
    dropped), some balance-sheet lines are an aggregate over genuinely ADDITIVE sub-lines. Short-term
    Debt is the canonical case: total current debt = ``ShortTermBorrowings`` (commercial paper /
    revolver) + the current maturities of long-term debt (``LongTermDebtCurrent``). ``DebtCurrent``
    is the us-gaap AGGREGATE of both. Filers that present ONLY the two components (e.g. LIN, WMT)
    made the coalesce keep one and silently DROP the other → Short-term Debt — and Total Debt /
    Debt-to-Equity / leverage — understated. (Confirmed via the linkbase-oracle reconciliation:
    LIN FY24 published 2.057B = current maturities only, omitting 4.223B of short-term borrowings.)

    Rule, resolved PER FILING CONTEXT (fy, fp, form, period_start, period_end, filed):
      • if the aggregate tag carries a value for that context → use it (it already folds in the
        parts), so no double-count;
      • else → SUM the component tags present for that context.

    Two distinct component tags in ONE filing are genuinely separate line items (a single XBRL tag
    has one value per context), so summing them is exact. Across filings each context is kept
    separate, so the downstream dedup still picks the latest-filed snapshot (restatements win). A
    context with a single component sums to that component → byte-identical to the old coalesce for
    single-component filers (the overwhelming majority).
    """
    _CTX = ["fy", "fp", "form", "period_start", "period_end", "period_shape", "filed"]

    agg = extract_series(facts, aggregate_tag, kind, namespace=namespace)

    comp_frames = [
        f for f in (extract_series(facts, t, kind, namespace=namespace) for t in component_tags)
        if not f.empty
    ]
    if comp_frames:
        comps = pd.concat(comp_frames, ignore_index=True)
        # Sum the (disjoint) component tags within each filing context.
        comp_sum = comps.groupby(_CTX, dropna=False, as_index=False)["value"].sum()
    else:
        comp_sum = pd.DataFrame(columns=_CTX + ["value"])

    if agg.empty:
        return comp_sum[_CTX + ["value"]].reset_index(drop=True)
    if comp_sum.empty:
        return agg[_CTX + ["value"]].reset_index(drop=True)

    # Aggregate wins per context; components fill only the contexts the aggregate does not cover.
    have_agg = agg[_CTX].drop_duplicates()
    comp_only = (
        comp_sum.merge(have_agg, on=_CTX, how="left", indicator=True)
        .query("_merge == 'left_only'")
        .drop(columns="_merge")
    )
    return pd.concat([agg[_CTX + ["value"]], comp_only], ignore_index=True).reset_index(drop=True)

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

    # Merge predecessor CIKs (mergers, MLP→C-corp, spinoffs). A broken alias
    # must not abort ingestion for the ticker — log and continue with what we have.
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
        # VECTORIZED row construction. Previously: series.iterrows() per concept — ~42% of
        # per-ticker CPU. CPU is the REAL ingestion bottleneck: it is GIL-serialized, so the
        # 8 threads only overlap network downloads (I/O releases the GIL), NOT
        # parsing/pandas/row building → throughput ≈ 1 core (~1.5 t/s vs the 8 req/s ceiling).
        # Each `series` is already a DataFrame; we add constant columns vectorially and dump
        # with to_dict instead of row-by-row. Parity validated on 12 real tickers (incl. multi-tag
        # T/VZ/WMB): same counts and values. (extract — the other ~50% — would need real
        # parallelism to improve; see ingestion bottleneck investigation.)
        frames = []
        _agg_sum = globals().get("AGGREGATE_OR_SUM_CONCEPTS", {})
        for stmt_name, concept_map in STATEMENTS.items():
            for label, (xbrl_concept, kind) in concept_map.items():
                # Most concepts COALESCE a priority list (extract_series_multi). A few are an
                # aggregate over additive sub-lines (e.g. Short-term Debt) and must SUM the
                # components when the aggregate tag is absent — see AGGREGATE_OR_SUM_CONCEPTS.
                _spec = _agg_sum.get(label)
                if _spec is not None:
                    series = extract_series_aggregate_or_sum(
                        facts, _spec["aggregate"], _spec["sum"], kind)
                else:
                    # xbrl_concept may be a single tag (str) or a priority list[str].
                    series = extract_series_multi(facts, xbrl_concept, kind)
                if series.empty:
                    continue
                # Share Repurchases is a positive cash-outflow MAGNITUDE by definition. Some filers
                # report it with the treasury / contra-equity (negative) convention — both in the cash
                # tags (e.g. GE PaymentsForRepurchaseOfCommonStock = -22.6B) and in the
                # StockRepurchasedDuringPeriodValue fallback (HON -1,085M, where the real buyback is
                # +1,085M). A negative "repurchase" is never a genuine value, so normalise to magnitude.
                if label == "Share Repurchases":
                    series = series.assign(value=series["value"].abs())
                frames.append(series.assign(stmt=stmt_name, concept=label, kind=kind))

        if not frames:
            return [], {"error_type": "empty_facts", "error_message": f"No XBRL facts extracted for {ticker}", "step": "extract"}

        allf = pd.concat(frames, ignore_index=True)
        allf["ticker"]     = ticker.upper()
        allf["company"]    = company_name
        allf["scraped_at"] = scraped_at_ts
        # Normalize types to match EXACTLY the previous per-row dict output (consumed later
        # by flush_batch): fy → nullable Int; dates → date|None; value/fp → value|None.
        allf["fy"] = allf["fy"].astype("Int64")
        for _dc in ("period_start", "period_end", "filed"):
            allf[_dc] = allf[_dc].dt.date.where(allf[_dc].notna(), None)
        allf["value"] = allf["value"].astype(float).where(allf["value"].notna(), None)
        allf["fp"]    = allf["fp"].where(allf["fp"].notna(), None)

        records = allf[[
            "ticker", "company", "stmt", "concept", "kind", "fy", "fp", "form",
            "period_start", "period_end", "period_shape", "value", "filed", "scraped_at",
        ]].to_dict("records")
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

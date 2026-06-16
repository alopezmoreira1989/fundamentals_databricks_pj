# Databricks notebook source
# MAGIC %md
# MAGIC # 00_config / 02__tickers_master
# MAGIC
# MAGIC Builds the unified ticker universe by merging:
# MAGIC - **S&P 500** — 500 large-cap US stocks (Wikipedia)
# MAGIC - **Russell 3000** — broad US market ~3000 stocks (iShares IWV ETF)
# MAGIC - **Favorites** — from `00_config/favorites.json` in the repo root
# MAGIC
# MAGIC **Output table:** `main.config.tickers`
# MAGIC
# MAGIC ```
# MAGIC ticker | company    | sector                 | in_sp500 | in_r3000 | is_favorite
# MAGIC AAPL   | Apple      | Information Technology | true     | true     | false
# MAGIC TSM    | TSMC       | NULL                   | false    | false    | true
# MAGIC ```
# MAGIC
# MAGIC `sector` is one of the 11 canonical **GICS sectors** (or NULL/Unknown). Precedence
# MAGIC when a ticker appears in multiple sources: **Wikipedia GICS (S&P) → IWV normalized
# MAGIC → favorites.json → NULL**.
# MAGIC
# MAGIC ### ✏️ How to add/remove favorites
# MAGIC Edit `00_config/favorites.json` in the Git repository and re-run this notebook.
# MAGIC No Databricks notebook needs to be touched.

# COMMAND ----------

# MAGIC %run "./01__tickers"

# COMMAND ----------

import json
import os
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

# `fetch_sp500()` parses the Wikipedia table with `pd.read_html(flavor="lxml")`, but
# lxml is NOT in the Databricks serverless base image (ImportError: lxml not found).
# Install it defensively with a plain subprocess pip — NOT `%pip`, which is unsupported
# when this notebook is pulled in via `%run` (e.g. from 91__full_pipeline). No-op on
# clusters that already ship lxml; idempotent on re-runs.
try:
    import lxml  # noqa: F401
except ImportError:
    import subprocess
    import sys

    subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "lxml"])

INGEST_SP500  = True
INGEST_R3000  = True

# FAVORITES_JSON_PATH inherited from 01__tickers via %run

TARGET_TABLE = f"{CATALOG}.config.tickers"

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; research-bot/1.0)"}

# The 11 canonical GICS sectors — the normalization target for every source.
_CANONICAL_SECTORS = {
    "Energy",
    "Materials",
    "Industrials",
    "Consumer Discretionary",
    "Consumer Staples",
    "Health Care",
    "Financials",
    "Information Technology",
    "Communication Services",
    "Utilities",
    "Real Estate",
}

# Known IWV / BlackRock (and other GICS-derived) labels → canonical GICS sector.
# Seeded with the obvious mismatches; extend it whenever `fetch_russell3000()` prints
# an unmapped label. Canonical labels themselves are handled by `_normalize_sector`
# (identity) and need not be repeated here.
_SECTOR_NORMALIZE: dict[str, str] = {
    "Technology":               "Information Technology",
    "Information Technology":    "Information Technology",
    "Communication":            "Communication Services",
    "Communication Services":    "Communication Services",
    "Telecommunications":       "Communication Services",
    "Telecommunication Services": "Communication Services",
    "Financial":                "Financials",
    "Financials":               "Financials",
    "Financial Services":       "Financials",
    "Health Care":              "Health Care",
    "Healthcare":               "Health Care",
    "Consumer Discretionary":    "Consumer Discretionary",
    "Consumer, Cyclical":       "Consumer Discretionary",
    "Consumer Staples":         "Consumer Staples",
    "Consumer, Non-cyclical":    "Consumer Staples",
    "Industrials":              "Industrials",
    "Industrial":               "Industrials",
    "Materials":                "Materials",
    "Basic Materials":          "Materials",
    "Energy":                   "Energy",
    "Utilities":                "Utilities",
    "Real Estate":              "Real Estate",
}


def _normalize_sector(raw: object) -> str | None:
    """Map a raw source sector label to one of the canonical 11 GICS sectors.

    Returns the canonical label, or ``None`` when the input is missing/blank or
    unknown. Unknown labels are deliberately dropped to NULL (the app renders them
    as "Unknown") and surfaced by the caller so the map can be extended.
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    s = str(raw).strip()
    if not s or s in ("-", "—"):
        return None
    if s in _CANONICAL_SECTORS:
        return s
    return _SECTOR_NORMALIZE.get(s)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0b. Logo.dev presence probe (`has_logo`)
# MAGIC
# MAGIC Probe Logo.dev by ticker and persist only a boolean `has_logo` — **never** the image
# MAGIC bytes (free-tier terms forbid self-hosting/caching; only the flag is stored). The
# MAGIC Streamlit masthead uses it to fall back to our editorial monogram for known misses
# MAGIC instead of Logo.dev's generic one. Key from the `LOGO_DEV_PUBLISHABLE_KEY` env var;
# MAGIC absent → `has_logo` is NULL for every ticker and the app degrades gracefully (hotlink,
# MAGIC Logo.dev covers misses).
# MAGIC
# MAGIC Databricks-only note: the probe is plain `requests` (no `spark`/`dbutils`), so it runs
# MAGIC fine under Databricks Connect — but set `LOGO_DEV_PUBLISHABLE_KEY` in the Job/cluster
# MAGIC environment before the run, or every ticker resolves to NULL.

# COMMAND ----------

# fallback=404 + a tiny size so a miss is an empty 404 (not a monogram) and we transfer
# almost no bytes. Logo.dev has no per-minute rate limit; keep concurrency modest and polite.
_LOGO_DEV_PROBE     = "https://img.logo.dev/ticker/{t}?token={k}&fallback=404&size=16&format=png"
_LOGO_PROBE_WORKERS = 8


def _probe_has_logo(ticker: str, key: str, session: requests.Session) -> bool | None:
    """True if Logo.dev has a real logo for the ticker, False if 404, None on error.

    We persist only the boolean — the image itself is never downloaded or stored.
    """
    try:
        r = session.get(_LOGO_DEV_PROBE.format(t=ticker, k=key), timeout=8)
        if r.status_code == 200:
            return True
        if r.status_code == 404:
            return False
        return None
    except requests.RequestException:
        return None


def resolve_has_logo(tickers: list[str]) -> dict[str, bool | None]:
    """Map each ticker → has_logo (True hit / False miss / None error). All None when no key."""
    key = os.environ.get("LOGO_DEV_PUBLISHABLE_KEY")
    if not key:
        print("  ⚠ LOGO_DEV_PUBLISHABLE_KEY not set — has_logo = None for all tickers")
        return {t: None for t in tickers}
    session = requests.Session()
    session.headers.update(_HEADERS)
    with ThreadPoolExecutor(max_workers=_LOGO_PROBE_WORKERS) as ex:
        results = list(ex.map(lambda t: (t, _probe_has_logo(t, key, session)), tickers))
    out    = dict(results)
    hits   = sum(1 for v in out.values() if v is True)
    misses = sum(1 for v in out.values() if v is False)
    errs   = sum(1 for v in out.values() if v is None)
    print(f"  Logo.dev: {hits}/{len(tickers)} have a real logo "
          f"({hits / max(len(tickers), 1):.1%}) · {misses} miss · {errs} error/None")
    return out

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Pull index constituents + favorites

# COMMAND ----------

def fetch_sp500() -> pd.DataFrame:
    """Return S&P 500 constituents as [ticker, company, sector].

    The Wikipedia table's `GICS Sector` column already carries canonical GICS
    labels, so this is the authoritative sector source.
    """
    url  = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    html = requests.get(url, headers=_HEADERS).text
    df   = pd.read_html(html, flavor="lxml")[0][["Symbol", "Security", "GICS Sector"]].copy()
    df.columns = ["ticker", "company", "sector"]
    df["ticker"] = df["ticker"].str.replace(".", "-", regex=False)
    # Already canonical, but normalize defensively so a stray label lands as NULL
    # rather than leaking a non-canonical value into the table.
    df["sector"] = df["sector"].map(_normalize_sector)
    return df.dropna(subset=["ticker"])


def fetch_russell3000() -> pd.DataFrame:
    """
    Fetch IWV (iShares Russell 3000 ETF) holdings via the BlackRock fundDownload API.
    Returns a DataFrame with columns [ticker, company, sector].

    The response is XML Excel (SpreadsheetML). The header row is detected
    dynamically by scanning for a row containing a recognised ticker column
    name, so the parser is resilient to metadata rows being added or removed.

    A `Sector` column (BlackRock labels, GICS-derived) is parsed when present and
    normalized to the canonical 11 via `_SECTOR_NORMALIZE`; if the column is absent
    the parse still succeeds with `sector = None` for every row.
    """
    import re

    url = (
        "https://www.blackrock.com/varnish-api/blk-one01-product-data/"
        "product-data/api/v1/get-fund-document"
        "?appType=PRODUCT_PAGE&appSubType=ISHARES&targetSite=us-ishares"
        "&locale=en_US&portfolioId=239714&component=fundDownload&userType=individual"
    )
    resp = requests.get(url, headers=_HEADERS, timeout=30)
    resp.raise_for_status()

    if "<ss:Worksheet" not in resp.text[:2000]:
        raise ValueError(
            f"IWV response is not SpreadsheetML (first 200 chars: {resp.text[:200]})"
        )

    # Locate the Holdings worksheet
    ws_start = resp.text.find('ss:Name="Holdings"')
    if ws_start == -1:
        raise ValueError("Holdings worksheet not found in IWV SpreadsheetML response")
    ws_end = resp.text.find("</ss:Worksheet>", ws_start)
    ws_xml = resp.text[ws_start:ws_end]

    rows = re.findall(r"<ss:Row[^>]*>(.*?)</ss:Row>", ws_xml, re.DOTALL)

    # Dynamic header detection: find row containing a recognized ticker column
    TICKER_COLS  = {"Ticker", "Symbol", "Holding Ticker"}
    COMPANY_COLS = {"Name", "Issuer Name", "Security Name", "Description"}
    SECTOR_COLS  = {"Sector", "GICS Sector", "Sector Name", "Industry Sector"}

    header_idx = None
    header_cells = []
    for i, row in enumerate(rows):
        cells = re.findall(r'<ss:Data[^>]*>([^<]*)</ss:Data>', row)
        if TICKER_COLS & set(cells):
            header_idx = i
            header_cells = cells
            break

    if header_idx is None:
        all_row_samples = []
        for i, row in enumerate(rows[:15]):
            cells = re.findall(r'<ss:Data[^>]*>([^<]*)</ss:Data>', row)
            all_row_samples.append(f"  Row {i}: {cells}")
        raise ValueError(
            f"Could not find header row with any of {TICKER_COLS}. "
            f"First 15 rows:\n" + "\n".join(all_row_samples)
        )

    # Resolve column indices with flexible mapping
    ticker_col = next((j for j, c in enumerate(header_cells) if c in TICKER_COLS), None)
    company_col = next((j for j, c in enumerate(header_cells) if c in COMPANY_COLS), None)
    sector_col = next((j for j, c in enumerate(header_cells) if c in SECTOR_COLS), None)

    if ticker_col is None:
        raise ValueError(f"Ticker column not found. Header: {header_cells}")
    if company_col is None:
        raise ValueError(f"Company column not found. Header: {header_cells}")

    used_ticker_name = header_cells[ticker_col]
    used_company_name = header_cells[company_col]
    if sector_col is None:
        print(f"  ⚠ IWV holdings: no Sector column in header {header_cells} — "
              f"sector will be NULL for all R3000-only tickers")

    # Parse data rows
    records = []
    for row in rows[header_idx + 1:]:
        cells = re.findall(r'<ss:Data[^>]*>([^<]*)</ss:Data>', row)
        if len(cells) <= max(ticker_col, company_col):
            continue
        t = cells[ticker_col].strip()
        c = cells[company_col].strip()
        sec = cells[sector_col].strip() if (sector_col is not None and len(cells) > sector_col) else None
        if t and re.match(r"^[A-Z][A-Z0-9.\-]{0,6}$", t):
            records.append({"ticker": t, "company": c, "sector_raw": sec})

    df = pd.DataFrame(records)

    # Validation
    if len(df) < 1500:
        raise ValueError(
            f"IWV holdings too few: {len(df)} (expected ~2500+). "
            f"Possible format change or partial response."
        )

    nan_pct = df["ticker"].isna().sum() / len(df) if len(df) > 0 else 0
    if nan_pct > 0.05:
        raise ValueError(f"IWV holdings: {nan_pct:.1%} of tickers are NaN (>5% threshold)")

    # Normalize sector + surface unmapped labels so the map can be extended next run.
    df["sector"] = df["sector_raw"].map(_normalize_sector)
    if sector_col is not None:
        raw = df["sector_raw"].astype("object")
        # A raw label is "unmapped" only if it's present/non-blank yet still landed NULL.
        is_unmapped = df["sector"].isna() & raw.map(
            lambda x: bool(x) and str(x).strip() not in ("", "-", "—")
        )
        unmapped = sorted({str(x).strip() for x in raw[is_unmapped]})
        if unmapped:
            print(f"  ⚠ {len(unmapped)} unmapped IWV sector label(s) "
                  f"→ extend _SECTOR_NORMALIZE: {unmapped}")
        else:
            print("  ✓ All IWV sector labels mapped to canonical GICS")
    df = df.drop(columns=["sector_raw"])

    print(f"  ✓ Parsed {len(df)} holdings from IWV fundDownload API")
    print(f"    Header at row {header_idx}, columns: {used_ticker_name}/{used_company_name}"
          f"{'/' + header_cells[sector_col] if sector_col is not None else ' (no sector)'}")
    return df.reset_index(drop=True)


def fetch_favorites() -> pd.DataFrame:
    """
    Read favorites from 00_config/favorites.json in the repository.
    The JSON is an array of objects: [{"ticker": "TSM", "company": "...", "note": "..."}]
    Comments with // are ignored (stripped before parsing).

    The `sector` field is **optional**: when present it is used as the lowest-precedence
    fallback (below S&P and Russell 3000). Returns NULL when absent.
    """
    empty = pd.DataFrame(columns=["ticker", "company", "sector"])
    try:
        with open(FAVORITES_JSON_PATH, encoding="utf-8") as f:
            raw = f.read()

        # Strip comment lines (// ...) to allow annotated JSON
        lines   = [ln for ln in raw.splitlines() if not ln.strip().startswith("/")]
        cleaned = "\n".join(lines)
        data    = json.loads(cleaned)

        if not data:
            print("  ℹ favorites.json is empty — no favorites")
            return empty

        full = pd.DataFrame(data)
        df = full[["ticker", "company"]].copy()
        df["ticker"] = df["ticker"].str.upper().str.strip()
        # Optional sector field — normalize to canonical GICS (NULL when absent/unknown).
        df["sector"] = full["sector"].map(_normalize_sector) if "sector" in full.columns else None
        print(f"  ✓ {len(df)} favorite(s) loaded from favorites.json")
        return df

    except FileNotFoundError:
        print(f"  ⚠ {FAVORITES_JSON_PATH} not found — no favorites")
        return empty
    except json.JSONDecodeError as e:
        print(f"  ✗ Error parsing favorites.json: {e}")
        return empty

# COMMAND ----------

raw_sources: dict[str, pd.DataFrame] = {}

if INGEST_SP500:
    print("Fetching S&P 500...")
    raw_sources["sp500"] = fetch_sp500()
    print(f"  ✓ {len(raw_sources['sp500'])} tickers")

if INGEST_R3000:
    print("Fetching Russell 3000...")
    try:
        raw_sources["r3000"] = fetch_russell3000()
        print(f"  ✓ {len(raw_sources['r3000'])} tickers")
    except Exception as _r3k_err:
        print(f"  ✗ Russell 3000 fetch failed: {_r3k_err}")
        print("  ⚠ Continuing with S&P 500 + favorites only — R3000 will be missing")

print("Loading favorites from favorites.json...")
favorites_df = fetch_favorites()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Merge into unified ticker universe
# MAGIC
# MAGIC `sector` precedence (COALESCE): **Wikipedia GICS (S&P) → IWV normalized →
# MAGIC favorites.json → NULL**. Per-source sector columns are kept distinct through the
# MAGIC merges and collapsed once at the end, so the middle (IWV) precedence is preserved
# MAGIC — a plain sequential fillna would let a favorites value block a higher-priority
# MAGIC IWV value.

# COMMAND ----------

master = favorites_df.copy()
master["is_favorite"] = True
# Park favorites' optional sector at the bottom of the precedence chain.
master = master.rename(columns={"sector": "sector_fav"})
if "sector_fav" not in master.columns:
    master["sector_fav"] = None

for source_name in ["sp500", "r3000"]:
    flag_col = f"in_{source_name}"
    if source_name in raw_sources:
        src = raw_sources[source_name]
        cols = ["ticker", "company"] + (["sector"] if "sector" in src.columns else [])
        idx_df = src[cols].copy().rename(columns={"sector": f"sector_{source_name}"})
        idx_df[flag_col] = True
        master = master.merge(idx_df, on="ticker", how="outer", suffixes=("", f"_{source_name}"))
        if f"company_{source_name}" in master.columns:
            master["company"] = master["company"].fillna(master[f"company_{source_name}"])
            master.drop(columns=[f"company_{source_name}"], inplace=True)
    else:
        master[flag_col] = False

# COALESCE per-source sector columns in strict precedence order → single `sector`.
master["sector"] = None
for col in ["sector_sp500", "sector_r3000", "sector_fav"]:
    if col in master.columns:
        master["sector"] = master["sector"].combine_first(master[col])
master.drop(columns=[c for c in ("sector_sp500", "sector_r3000", "sector_fav") if c in master.columns],
            inplace=True)

bool_cols = ["is_favorite", "in_sp500", "in_r3000"]
for col in bool_cols:
    if col not in master.columns:
        master[col] = False
    master[col] = master[col].fillna(False)

master = (
    master[["ticker", "company", "sector"] + bool_cols]
    .drop_duplicates("ticker")
    .sort_values("ticker")
)

print(f"\nUnified universe: {len(master):,} unique tickers")
print(master[bool_cols].sum().to_string())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2b. Resolve `has_logo` (Logo.dev presence probe)
# MAGIC
# MAGIC Sample-validate a spread of tickers (named large caps + a sweep across the universe
# MAGIC to catch obscure tail names) and print the result, then probe the full universe and
# MAGIC attach a nullable boolean `has_logo` column. NULL for every ticker when no key is set.

# COMMAND ----------

# Sample validation before the full sweep: AAPL/MSFT must resolve True with a key; an evenly
# spread sample exercises the miss path (no market cap here to pick true micro-caps, so the
# stride sample stands in for the long tail). Printed for eyeballing, then discarded.
_SAMPLE_NAMED = ["AAPL", "MSFT"]
_universe     = master["ticker"].tolist()
_step         = max(len(_universe) // 48, 1)
_sample       = _SAMPLE_NAMED + [t for t in _universe[::_step] if t not in _SAMPLE_NAMED][:48]
_sample_res   = resolve_has_logo(_sample)
for _t in _SAMPLE_NAMED:
    print(f"    {_t}: has_logo={_sample_res.get(_t)}")
_sh = sum(1 for v in _sample_res.values() if v is True)
_sm = sum(1 for v in _sample_res.values() if v is False)
_sn = sum(1 for v in _sample_res.values() if v is None)
print(f"  sample({len(_sample)}): {_sh} hit · {_sm} miss · {_sn} none")

# COMMAND ----------

# Full-universe resolution → nullable boolean column on master (object dtype keeps None as
# NULL, not NaN, for the BooleanType write).
has_logo_map = resolve_has_logo(_universe)
master["has_logo"] = master["ticker"].map(has_logo_map).astype("object")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write to Delta

# COMMAND ----------

schema = StructType([
    StructField("ticker",      StringType(),  False),
    StructField("company",     StringType(),  True),
    StructField("sector",      StringType(),  True),
    StructField("is_favorite", BooleanType(), False),
    StructField("in_sp500",    BooleanType(), False),
    StructField("in_r3000",    BooleanType(), False),
    StructField("has_logo",    BooleanType(), True),
])

sdf = spark.createDataFrame(master, schema=schema)
spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")

(
    sdf.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("ticker")
    .saveAsTable(TARGET_TABLE)
)

print(f"✓ {sdf.count():,} tickers written → {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Sanity check

# COMMAND ----------

spark.sql(f"""
    SELECT
        COUNT(*)                                                AS total_tickers,
        SUM(CAST(is_favorite AS INT))                           AS favorites,
        SUM(CAST(in_sp500    AS INT))                           AS in_sp500,
        SUM(CAST(in_r3000    AS INT))                           AS in_r3000,
        SUM(CASE WHEN in_sp500 AND in_r3000 THEN 1 ELSE 0 END) AS in_both,
        SUM(CASE WHEN is_favorite AND NOT in_sp500
                  AND NOT in_r3000 THEN 1 ELSE 0 END)           AS favorites_only
    FROM {TARGET_TABLE}
""").show()

# Sector coverage + distribution (NULL bucketed as "Unknown", matching the app).
spark.sql(f"""
    SELECT
        COUNT(*)                                                  AS total_tickers,
        SUM(CASE WHEN sector IS NOT NULL THEN 1 ELSE 0 END)       AS with_sector,
        ROUND(100.0 * SUM(CASE WHEN sector IS NOT NULL THEN 1 ELSE 0 END)
              / COUNT(*), 1)                                      AS pct_coverage
    FROM {TARGET_TABLE}
""").show()

print("Sector distribution:")
spark.sql(f"""
    SELECT COALESCE(sector, 'Unknown') AS sector, COUNT(*) AS n
    FROM {TARGET_TABLE}
    GROUP BY COALESCE(sector, 'Unknown')
    ORDER BY n DESC
""").show(20, truncate=False)

# has_logo coverage (NULL = probe skipped / errored — e.g. LOGO_DEV_PUBLISHABLE_KEY unset).
print("Logo.dev has_logo coverage:")
spark.sql(f"""
    SELECT
        SUM(CASE WHEN has_logo = true  THEN 1 ELSE 0 END) AS with_logo,
        SUM(CASE WHEN has_logo = false THEN 1 ELSE 0 END) AS no_logo,
        SUM(CASE WHEN has_logo IS NULL THEN 1 ELSE 0 END) AS null_logo,
        ROUND(100.0 * SUM(CASE WHEN has_logo = true THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_with_logo
    FROM {TARGET_TABLE}
""").show()

fav_count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE} WHERE is_favorite").collect()[0][0]
if fav_count > 0:
    print("\nFavoritos activos:")
    spark.sql(f"""
        SELECT ticker, company, sector, in_sp500, in_r3000
        FROM {TARGET_TABLE} WHERE is_favorite ORDER BY ticker
    """).show()
else:
    print("\nℹ No favorites — edit 00_config/favorites.json to add some")

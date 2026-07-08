# Databricks notebook source
# MAGIC %md
# MAGIC # 00__config / 02__tickers_master
# MAGIC
# MAGIC Builds the unified ticker universe by merging:
# MAGIC - **S&P 500** — 500 large-cap US stocks (Wikipedia)
# MAGIC - **Russell 3000** — broad US market ~3000 stocks (iShares IWV ETF)
# MAGIC - **S&P/TSX Composite** — ~220 Canadian equities (via XIC holdings), gated to
# MAGIC   SEC-registered (MJDS/40-F) filers only — see section 2a
# MAGIC - **Favorites** — from `00__config/favorites.json` in the repo root
# MAGIC
# MAGIC **Output table:** `main.config.tickers`
# MAGIC
# MAGIC ```
# MAGIC ticker | company    | sector                 | market | in_sp500 | in_r3000 | is_favorite
# MAGIC AAPL   | Apple      | Information Technology | US     | true     | true     | false
# MAGIC TSM    | TSMC       | NULL                   | US     | false    | false    | true
# MAGIC RY     | Royal Bank | Financials              | CA     | false    | false    | false
# MAGIC ```
# MAGIC
# MAGIC `sector` is one of the 11 canonical **GICS sectors** (or NULL/Unknown). Precedence
# MAGIC when a ticker appears in multiple sources: **Wikipedia GICS (S&P) → IWV normalized
# MAGIC → favorites.json → NULL**.
# MAGIC
# MAGIC `market` is the ticker-identity collision-guard key (`fundamentals_pipeline/identity.py`) —
# MAGIC which sourcing pipeline a row came from, `"US"` or `"CA"` today. A bare ticker can
# MAGIC legitimately collide across markets (Magna Intl `MG` on the TSX vs Mistras Group `MG` on
# MAGIC the NYSE) or refer to the same dual-listed company (`BAM`, `SHOP`); section 2a's admission
# MAGIC gate and `check_no_cross_market_collision()` tell the two apart.
# MAGIC
# MAGIC ### ✏️ How to add/remove favorites
# MAGIC Edit `00__config/favorites.json` in the Git repository and re-run this notebook.
# MAGIC No Databricks notebook needs to be touched.

# COMMAND ----------

# MAGIC %run "./01__tickers"

# COMMAND ----------

import json
import os
import re
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

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

# yfinance powers the per-ticker company-info probe (resolve_company_info below). Same defensive
# subprocess install as lxml — %pip is unsupported when this notebook is pulled in via %run
# (e.g. from 91). No-op where yfinance already ships; idempotent on re-runs.
try:
    import yfinance as yf  # noqa: F401
except ImportError:
    import subprocess
    import sys

    subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "yfinance"])
    import yfinance as yf  # noqa: F401

# fundamentals_pipeline is normally pip-installed once per pipeline session (91's %pip cell),
# but 02 is a MANUAL step not chained through 91 — same defensive install as lxml/yfinance
# above, using the repo root two levels up from this notebook's CWD (00__config/).
try:
    from fundamentals_pipeline.identity import check_no_cross_market_collision, classify_company_match
    from fundamentals_pipeline.tickers_universe import parse_tsx_composite_csv
except ImportError:
    import subprocess
    import sys

    subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "-e", "../.."])
    from fundamentals_pipeline.identity import check_no_cross_market_collision, classify_company_match
    from fundamentals_pipeline.tickers_universe import parse_tsx_composite_csv

INGEST_SP500         = True
INGEST_R3000         = True
INGEST_TSX_COMPOSITE = True

# ── Refresh policy for the industry probe ──────────────────────────────────────
# Inherit force_full_refresh from the parent 91 via globals() — SAME handoff as 11/12
# (dbutils.widgets.get() does not reliably read the parent's widget under %run). Standalone
# runs fall back to this notebook's own widget, else False. When False, `industry` is fetched
# only for tickers without a cached value; when True, every ticker is re-probed.
if "force_full_refresh" in globals():
    FORCE_FULL_REFRESH = str(force_full_refresh).strip().lower() == "true"
else:
    try:
        FORCE_FULL_REFRESH = dbutils.widgets.get("force_full_refresh").strip().lower() == "true"
    except Exception:
        FORCE_FULL_REFRESH = False

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
# MAGIC Key resolution (first hit wins): the `LOGO_DEV_PUBLISHABLE_KEY` env var, else the
# MAGIC Databricks secret scope `logo_dev` / key `publishable_key`. The env var path keeps this
# MAGIC runnable under local Databricks Connect (no `dbutils`); the secret scope is how the
# MAGIC serverless Job gets it (matching the `github_pat` pattern in `52`). Neither present →
# MAGIC `has_logo` is NULL for every ticker and the app degrades gracefully (hotlink, Logo.dev
# MAGIC covers misses).
# MAGIC
# MAGIC ```
# MAGIC databricks secrets create-scope logo_dev
# MAGIC databricks secrets put-secret  logo_dev publishable_key
# MAGIC ```

# COMMAND ----------

# fallback=404 + a tiny size so a miss is an empty 404 (not a monogram) and we transfer
# almost no bytes. Logo.dev has no per-minute rate limit; keep concurrency modest and polite.
_LOGO_DEV_PROBE     = "https://img.logo.dev/ticker/{t}?token={k}&fallback=404&size=16&format=png"
_LOGO_PROBE_WORKERS = 8


def _resolve_logo_key() -> str | None:
    """Logo.dev publishable key: env var first (local Connect), then the `logo_dev` secret
    scope (serverless Job). None if neither is set. `dbutils` is undefined under local Connect,
    so the secret lookup is guarded."""
    key = os.environ.get("LOGO_DEV_PUBLISHABLE_KEY")
    if key:
        return key
    try:
        return dbutils.secrets.get(scope="logo_dev", key="publishable_key") or None
    except Exception:
        return None


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
    key = _resolve_logo_key()
    if not key:
        print("  ⚠ no Logo.dev key (env LOGO_DEV_PUBLISHABLE_KEY or secret logo_dev/publishable_key)"
              " — has_logo = None for all tickers")
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
# MAGIC ## 0c. Industry probe (`industry`)
# MAGIC
# MAGIC Per-ticker Yahoo Finance sub-sector label (`Ticker.info["industry"]`) — the grouping key
# MAGIC for sub-sector valuation comparisons (median P/E, P/FCF *by industry group*), finer than
# MAGIC the 11 broad GICS sectors. `.info` is one HTTP call per ticker (no batch form) and the
# MAGIC flakiest yfinance endpoint, so the probe is threaded with graceful `None` on any failure
# MAGIC and is **cache-aware**: only tickers without a cached industry are fetched (new names +
# MAGIC prior misses), unless `force_full_refresh` re-probes the whole universe. Stored raw (Yahoo
# MAGIC display string); NULL → the app renders "Unknown".
# MAGIC
# MAGIC ⚠ Yahoo's `industry` nests under Yahoo's OWN sector taxonomy, which is NOT the GICS `sector`
# MAGIC column (Wikipedia/IWV). Group valuation comps by `industry` directly; keep `sector` for the
# MAGIC broad rollup.

# COMMAND ----------

_INDUSTRY_PROBE_WORKERS = 8

# The 7 company-info fields resolved from a single `yf.Ticker(ticker).info` dict. `industry` is
# the legacy field (drives the Sectors page); the other 6 power the Company Overview tab. Stored
# verbatim on config.tickers and surfaced in the meta artifact (51 → ticker_meta).
_COMPANY_INFO_FIELDS = ("industry", "description", "exchange", "country", "employees", "website", "founded")
# `industry` is the carry-forward ANCHOR: a ticker with a non-NULL industry is considered resolved
# and is NOT re-probed (the proven behavior that accumulated ~95% industry coverage over 3 runs).
# The other obtainable fields ride the SAME `.info` call, so they fill alongside industry; `founded`
# is ~never exposed by yfinance, so it is deliberately NOT part of the gate — gating on it would
# leave every ticker permanently "unresolved" and force a full universe re-probe every run.
_PROBE_ANCHOR_FIELD = "industry"
_DESCRIPTION_MAX_CHARS = 2000


def _read_known_company_info() -> dict:
    """Prior {ticker: {field: value}} from the existing config.tickers, to avoid re-probing every run.

    Returns EVERY ticker that already has a non-NULL anchor (`industry`), carrying all 7 of its
    stored fields forward. Guarded: the table may not exist (first run) or may predate these columns
    — either way returns {} (→ full probe). MUST be called BEFORE the table is overwritten in
    section 3.
    """
    try:
        cols = ", ".join(_COMPANY_INFO_FIELDS)
        rows = spark.sql(f"SELECT ticker, {cols} FROM {TARGET_TABLE}").collect()
    except Exception:
        return {}
    known = {}
    for r in rows:
        rec = {f: r[f] for f in _COMPANY_INFO_FIELDS}
        if rec[_PROBE_ANCHOR_FIELD] is not None:
            known[r["ticker"]] = rec
    return known


def _clean_str(v):
    """Trim a yfinance string field to None when missing/blank, else the stripped value."""
    return v.strip() if isinstance(v, str) and v.strip() else None


def _clean_int(v):
    """Coerce a yfinance numeric field to int, or None when missing/non-numeric."""
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _probe_company_info(ticker: str) -> dict:
    """Yahoo Finance company metadata for one ticker from a SINGLE `.info` call.

    Returns all 7 fields (None each on any error or missing/blank field). `founded` is rarely
    exposed by yfinance — left None when absent, never fabricated.
    """
    blank = {f: None for f in _COMPANY_INFO_FIELDS}
    try:
        info = yf.Ticker(ticker).info or {}
    except Exception:
        return blank
    desc = _clean_str(info.get("longBusinessSummary"))
    return {
        "industry":    _clean_str(info.get("industry")),
        "description": desc[:_DESCRIPTION_MAX_CHARS] if desc else None,
        "exchange":    _clean_str(info.get("exchange")),
        "country":     _clean_str(info.get("country")),
        "employees":   _clean_int(info.get("fullTimeEmployees")),
        "website":     _clean_str(info.get("website")),
        "founded":     _clean_int(info.get("founded")),
    }


def resolve_company_info(tickers: list, known: dict) -> dict:
    """Map each ticker → {field: value} dict of the 7 company-info fields (industry + 6 more, None ok).

    CACHE-AWARE + COVERAGE-MONOTONIC: tickers with a known anchor (`industry`) are carried forward
    and NOT re-fetched unless FORCE_FULL_REFRESH; only anchor-less tickers hit `.info` (one HTTP
    call/ticker — no batch form, the flakiest yfinance endpoint, hence threaded + graceful None).

    Merge is FIELD-LEVEL: a freshly-probed value is used only when non-NULL, otherwise the cached
    value is kept. So a flaky probe that returns NULL for a field can never *erase* a value an
    earlier run resolved — coverage only ever grows across runs, never regresses.
    """
    to_fetch = list(tickers) if FORCE_FULL_REFRESH else [t for t in tickers if t not in known]
    carried  = 0 if FORCE_FULL_REFRESH else sum(1 for t in tickers if t in known)
    print(f"  Company-info probe: {len(to_fetch):,} to fetch, {carried:,} carried forward"
          f"{' (force refresh — all re-probed)' if FORCE_FULL_REFRESH else ''}")

    fetched = {}
    if to_fetch:
        with ThreadPoolExecutor(max_workers=_INDUSTRY_PROBE_WORKERS) as ex:
            fetched = dict(ex.map(lambda t: (t, _probe_company_info(t)), to_fetch))

    blank = {f: None for f in _COMPANY_INFO_FIELDS}
    out   = {}
    for t in tickers:
        prior = known.get(t, blank)
        fresh = fetched.get(t)
        if fresh is None:
            out[t] = dict(prior)
        else:
            # Keep a freshly-resolved value; fall back to the cached one where the probe came back NULL.
            out[t] = {f: (fresh[f] if fresh[f] is not None else prior.get(f)) for f in _COMPANY_INFO_FIELDS}
    hits  = sum(1 for v in out.values() if v.get(_PROBE_ANCHOR_FIELD))
    print(f"  Industry: {hits:,}/{len(out):,} resolved ({hits / max(len(out), 1):.1%}) · "
          f"{len(out) - hits:,} NULL")
    return out

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0d. Founded probe (`founded`, Wikidata inception)
# MAGIC
# MAGIC yfinance never exposes a founding year (`info["founded"]` is essentially always absent —
# MAGIC see `_probe_company_info`), so `founded` is sourced from **Wikidata**: the `inception`
# MAGIC property (`P571`) of the entity carrying the `ticker symbol` (`P249`). One batched SPARQL
# MAGIC call per ~300 tickers (POST, no API key). Best-effort: Wikidata's ticker coverage is
# MAGIC partial (~50–70%), so unmatched tickers stay NULL — never fabricated. When a ticker maps
# MAGIC to multiple entities (reused symbols / ADRs), the **earliest** inception year wins.
# MAGIC Cache-aware: only tickers without a cached `founded` are queried unless `force_full_refresh`.

# COMMAND ----------

_WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
_WIKIDATA_BATCH      = 300
# Wikidata blocks generic/empty User-Agents — identify the project + a contact per their policy.
_WIKIDATA_UA = (
    "fundamentals-databricks-pj/1.0 "
    "(https://github.com/alopezmoreira1989/fundamentals_databricks_pj; al.lopez.moreira@gmail.com)"
)


def _founded_year_from_inception(value: str) -> int | None:
    """Parse a Wikidata inception literal (e.g. '1976-04-01T00:00:00Z') → 4-digit year, or None.

    Guards BCE/garbage values: only plausible CE company years (1..2100) pass.
    """
    m = re.match(r"^(-?\d{1,4})", value or "")
    if not m:
        return None
    yr = int(m.group(1))
    return yr if 0 < yr <= 2100 else None


def _wikidata_founded_batch(tickers: list, session: requests.Session) -> dict:
    """One SPARQL call → {ticker_variant: earliest_year}. Returns {} on any failure (best-effort).

    Queries both the canonical (dash) and dotted forms of class-share tickers (BRK-B / BRK.B),
    since Wikidata stores `P249` in either style; the caller maps variants back to our ticker.
    """
    variants = set()
    for t in tickers:
        variants.add(t)
        if "-" in t:
            variants.add(t.replace("-", "."))
    values = " ".join(f'"{v}"' for v in sorted(variants))
    # The ticker symbol (P249) lives as a QUALIFIER on the company's stock-exchange statement
    # (P414), NOT as a top-level property — direct `wdt:P249` is deprecated (~37 entities total).
    # `inception` (P571) is a normal truthy statement.
    query = (
        "SELECT ?ticker ?inception WHERE { "
        f"VALUES ?ticker {{ {values} }} "
        "?company p:P414 ?stmt . ?stmt pq:P249 ?ticker . "
        "?company wdt:P571 ?inception . }"
    )
    try:
        resp = session.post(
            _WIKIDATA_SPARQL_URL,
            data={"query": query, "format": "json"},
            headers={"Accept": "application/sparql-results+json", "User-Agent": _WIKIDATA_UA},
            timeout=90,
        )
        resp.raise_for_status()
        rows = resp.json()["results"]["bindings"]
    except Exception:
        return {}
    out: dict = {}
    for row in rows:
        tk = row["ticker"]["value"]
        yr = _founded_year_from_inception(row["inception"]["value"])
        if yr is None:
            continue
        if tk not in out or yr < out[tk]:   # earliest inception wins
            out[tk] = yr
    return out


def resolve_founded(tickers: list, known: dict) -> dict:
    """Map each ticker → founding year (int) from Wikidata inception (P571), earliest match.

    CACHE-AWARE: only tickers without a cached `founded` are queried (unless FORCE_FULL_REFRESH);
    `known` is the {ticker: {field: value}} dict already read for the industry probe. Best-effort —
    Wikidata's ticker coverage is partial, so unmatched tickers stay None (never fabricated).
    """
    to_fetch = (
        list(tickers) if FORCE_FULL_REFRESH
        else [t for t in tickers if known.get(t, {}).get("founded") is None]
    )
    carried = len(tickers) - len(to_fetch)
    print(f"  Founded probe (Wikidata): {len(to_fetch):,} to query, {carried:,} carried forward"
          f"{' (force refresh — all re-queried)' if FORCE_FULL_REFRESH else ''}")

    raw: dict = {}
    if to_fetch:
        session = requests.Session()
        for i in range(0, len(to_fetch), _WIKIDATA_BATCH):
            raw.update(_wikidata_founded_batch(to_fetch[i:i + _WIKIDATA_BATCH], session))

    # Map results (which may be in dotted form) back to our canonical ticker; earliest year wins.
    fetched: dict = {}
    for t in to_fetch:
        cands = [raw.get(t)]
        if "-" in t:
            cands.append(raw.get(t.replace("-", ".")))
        yrs = [y for y in cands if y is not None]
        if yrs:
            fetched[t] = min(yrs)

    out = {t: fetched.get(t, known.get(t, {}).get("founded")) for t in tickers}
    hits = sum(1 for v in out.values() if v is not None)
    print(f"  Founded: {hits:,}/{len(out):,} resolved ({hits / max(len(out), 1):.1%})")
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
    Read favorites from 00__config/favorites.json in the repository.
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

# MAGIC %md
# MAGIC ## 1b. S&P/TSX Composite (via XIC) + SEC ticker index
# MAGIC
# MAGIC No ETF tracks the S&P/TSX Composite exactly; **XIC** (iShares Core S&P/TSX Capped
# MAGIC Composite Index ETF) matches its membership closely (weights capped at 10%, membership
# MAGIC effectively the same) and is run by BlackRock — but unlike IWV (Russell 3000), XIC is
# MAGIC NOT served by the varnish-api `fundDownload` SpreadsheetML endpoint (confirmed 2026-07:
# MAGIC that URL 400s — `BAD_REQUEST_INVALID_PARAM_VALUES` — for this fund regardless of
# MAGIC `targetSite`/`locale`). The real mechanism is a plain CSV at a different `blackrock.com/
# MAGIC ca/investors/...` path; see `fundamentals_pipeline/tickers_universe.py` for the parser
# MAGIC and the exact format.
# MAGIC
# MAGIC Every XIC candidate is gated through SEC's own `company_tickers.json` (the same map
# MAGIC `11__fetch_sec_xbrl.py` builds) before admission — this is deliberately narrow: Phase 1
# MAGIC of the multi-market roadmap is Canadian **MJDS/40-F filers**, i.e. companies that also
# MAGIC register with the SEC, not the whole TSX. A ticker resolving in that map is NOT
# MAGIC sufficient on its own, though — SEC's map is bare-ticker-keyed across its entire ~10,000
# MAGIC company universe, so plenty of XIC tickers coincidentally resolve to an unrelated US
# MAGIC filer (confirmed real examples: XIC's `MG` = Magna International, but SEC's `MG` = a
# MAGIC stale entry for Mistras Group, taken private in 2023; `TVE` = Tamarack Valley Energy vs
# MAGIC SEC's `TVE` = the Tennessee Valley Authority). `classify_company_match` (imported above)
# MAGIC adjudicates every resolution before admission.

# COMMAND ----------

_XIC_HOLDINGS_URL = (
    "https://www.blackrock.com/ca/investors/en/products/239837/"
    "ishares-sptsx-capped-composite-index-etf/1464253357814.ajax"
    "?fileType=csv&fileName=XIC_holdings&dataType=fund"
)

_SEC_TICKER_INDEX_URL = "https://www.sec.gov/files/company_tickers.json"

# XIC's live holdings count fluctuates modestly (~220-250); this is a live-response sanity
# floor, not a parsing concern — kept separate from parse_tsx_composite_csv so that function
# stays testable against small fixtures (see tests/test_tickers_universe.py).
_XIC_MIN_HOLDINGS = 150


def fetch_tsx_composite() -> pd.DataFrame:
    """Fetch XIC's live holdings (S&P/TSX Composite proxy) as [ticker, company, sector].

    See `fundamentals_pipeline/tickers_universe.py::parse_tsx_composite_csv` for the parser
    and why this is a plain CSV, not the varnish-api SpreadsheetML `fetch_russell3000()` uses.
    """
    resp = requests.get(_XIC_HOLDINGS_URL, headers=_HEADERS, timeout=30)
    resp.raise_for_status()
    df = parse_tsx_composite_csv(resp.text)
    if len(df) < _XIC_MIN_HOLDINGS:
        raise ValueError(
            f"XIC holdings too few: {len(df)} (expected ~200+). "
            f"Possible format change or partial response."
        )
    print(f"  ✓ Parsed {len(df)} equity holdings from XIC")
    return df


def fetch_sec_ticker_map() -> dict:
    """SEC's bare-ticker -> (cik, title) map, same source `11__fetch_sec_xbrl.py`'s
    `TICKER_MAP` uses. Used here ONLY to gate Canadian admission to tickers that are actually
    SEC-registered filers (Phase-1 MJDS/40-F scope) and to resolve the company-identity match
    (`fundamentals_pipeline.identity.classify_company_match`) — a ticker resolving here is
    necessary but not sufficient for admission; see section 2a.
    """
    resp = requests.get(_SEC_TICKER_INDEX_URL, headers=_HEADERS, timeout=30)
    resp.raise_for_status()
    idx = resp.json()
    return {entry["ticker"].upper(): (str(entry["cik_str"]).zfill(10), entry["title"]) for entry in idx.values()}

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

# TSX Composite candidates are NOT merged into raw_sources like sp500/r3000 (a plain outer-merge
# would blindly trust every ticker) — they go through the SEC-registration + company-identity
# admission gate in section 2a instead, run against the US-only universe assembled below.
tsx_composite_df = pd.DataFrame(columns=["ticker", "company", "sector"])
if INGEST_TSX_COMPOSITE:
    print("Fetching S&P/TSX Composite (via XIC)...")
    try:
        tsx_composite_df = fetch_tsx_composite()
    except Exception as _tsx_err:
        print(f"  ✗ TSX Composite fetch failed: {_tsx_err}")
        print("  ⚠ Continuing without Canadian tickers this run")

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

# `market` is the collision-guard identity key — which listing jurisdiction a ticker's row was
# sourced from. The whole current universe is US SEC filers (SP500/R3000/favorites), so it's a
# static literal today; a future Canadian TSX/MJDS source would set "CA" for its rows. Kept
# separate from the display-oriented `exchange` column (Yahoo per-venue mnemonic, resolved in
# 2c below) so this never collides with — or overwrites — that already-live display data.
master["market"] = "US"

master = (
    master[["ticker", "company", "sector", "market"] + bool_cols]
    .drop_duplicates(["ticker", "market"])
    .sort_values("ticker")
)

# Fail loud if the same bare ticker symbol claims more than one market (e.g. a future Canadian
# source colliding with an existing US ticker — Magna Intl 'MG' on the TSX vs Mistras Group
# 'MG' on the NYSE) instead of silently overwriting one company's row with another's. At this
# point every row is market="US", so this is a no-op pass-through — cheap, and consistent with
# calling the guard right after every merge step that could in principle introduce a collision.
master = check_no_cross_market_collision(master)

print(f"\nUnified universe: {len(master):,} unique tickers")
print(master[bool_cols].sum().to_string())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2a. Canadian ticker universe (S&P/TSX Composite via XIC)
# MAGIC
# MAGIC Three-way admission per XIC candidate:
# MAGIC 1. **No SEC CIK at all** → not a Phase-1 (MJDS/40-F) candidate → excluded, logged.
# MAGIC 2. **Resolves, but `classify_company_match` says "different" or "ambiguous"** → the
# MAGIC    bare ticker collides with an unrelated US-registered filer (confirmed real cases:
# MAGIC    `MG`→Mistras Group, `TVE`→Tennessee Valley Authority, `IAU`→the iShares Gold Trust
# MAGIC    ETF, among others) → excluded, logged — never guessed.
# MAGIC 3. **Resolves and `classify_company_match` says "same"** → genuine MJDS/40-F filer.
# MAGIC    If the ticker is already in `master` (a dual-listed company, e.g. `BAM`, `SHOP`,
# MAGIC    `GFL`), no new row is added — it's already covered via the US path; only the
# MAGIC    `in_tsx_composite` flag is set. Otherwise it's admitted as a new `market="CA"` row.
# MAGIC
# MAGIC `accounting_standard` / `reporting_currency` are deliberately left `NULL` here — real
# MAGIC per-ticker detection needs each filer's actual `companyfacts` (ifrs-full vs us-gaap;
# MAGIC CAD vs USD is NOT a function of namespace alone — e.g. Nutrien/Gildan file ifrs-full in
# MAGIC USD). `11__fetch_sec_xbrl.py` already fetches that response per ticker for real
# MAGIC ingestion, so it derives and writes these two fields back rather than this notebook
# MAGIC fetching `companyfacts` again just for metadata.
# MAGIC
# MAGIC ⚠ **Known limitation:** the `has_logo` / company-info (`industry`, `description`,
# MAGIC `employees`, `website`) / `founded` probes below (2b-2d) call `yf.Ticker(ticker)` with
# MAGIC the BARE ticker — Yahoo Finance requires a `.TO` suffix for TSX-listed symbols, so these
# MAGIC probes may silently return `NULL` (or, rarely, data for an unrelated Yahoo-side symbol)
# MAGIC for newly-admitted Canadian tickers. `country`/`exchange` are protected below (seeded to
# MAGIC "Canada"/"TSX" and never overwritten by these probes for `market="CA"` rows); `industry`/
# MAGIC `description`/`employees`/`website`/`founded` are NOT — tracked as follow-up work, not
# MAGIC fixed in this pass (see CLAUDE.md).

# COMMAND ----------

_new_ca_rows: list[dict] = []
_ca_excluded: list[tuple] = []
_ca_dual_listed: list[str] = []

if INGEST_TSX_COMPOSITE and not tsx_composite_df.empty:
    print("Loading SEC ticker index for Canadian admission gate...")
    _sec_ticker_map = fetch_sec_ticker_map()
    _existing_tickers = set(master["ticker"])

    for _, _row in tsx_composite_df.iterrows():
        _t, _company, _sector = _row["ticker"], _row["company"], _row["sector"]
        _hit = _sec_ticker_map.get(_t.upper())
        if _hit is None:
            _ca_excluded.append((_t, _company, "no SEC CIK — not a Phase-1 MJDS/40-F candidate"))
            continue

        _cik, _sec_title = _hit
        _verdict = classify_company_match(_company, _sec_title)
        if _verdict != "same":
            _ca_excluded.append(
                (_t, _company, f"{_verdict} match vs SEC {_sec_title!r} (CIK {_cik})")
            )
            continue

        if _t in _existing_tickers:
            _ca_dual_listed.append(_t)
            continue

        _new_ca_rows.append({
            "ticker": _t, "company": _company, "sector": _sector, "market": "CA",
            "is_favorite": False, "in_sp500": False, "in_r3000": False,
            "country": "Canada", "exchange": "TSX",
            "accounting_standard": None, "reporting_currency": None,
        })

    print(f"  Admitted new Canadian tickers : {len(_new_ca_rows)}")
    print(f"  Already covered (dual-listed) : {len(_ca_dual_listed)} — {_ca_dual_listed}")
    print(f"  Excluded (see detail below)   : {len(_ca_excluded)}")
    for _t, _company, _reason in _ca_excluded:
        print(f"    ✗ {_t:<8} {_company:<40} {_reason}")

    if _new_ca_rows:
        master = pd.concat([master, pd.DataFrame(_new_ca_rows)], ignore_index=True)

_tsx_tickers = set(tsx_composite_df["ticker"]) if not tsx_composite_df.empty else set()
master["in_tsx_composite"] = master["ticker"].isin(_tsx_tickers)

# Defense in depth: re-run the guard over the fully-assembled (US + admitted CA) universe.
# Should be a no-op given the admission gate above already excludes/dedupes collisions and
# dual-listings — this is the final fail-safe, not the primary mechanism.
master = check_no_cross_market_collision(master)
master = master.sort_values("ticker").reset_index(drop=True)

print(f"\nUniverse after Canadian admission: {len(master):,} unique tickers "
      f"({int(master['market'].eq('CA').sum())} market=CA)")

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
# MAGIC ## 2c. Resolve `industry` (Yahoo sub-sector, cache-aware)
# MAGIC
# MAGIC Read prior industries from the existing table (BEFORE the overwrite in section 3) and probe
# MAGIC only the tickers without a cached value, so steady-state rebuilds fetch just the new names.

# COMMAND ----------

_known_company_info = _read_known_company_info()
company_info_map    = resolve_company_info(_universe, _known_company_info)
for _field in _COMPANY_INFO_FIELDS:
    master[_field] = master["ticker"].map(lambda t, f=_field: company_info_map[t][f]).astype("object")

# Protect the seeded country="Canada"/exchange="TSX" (set in section 2a) from this probe:
# yfinance's .info requires a ".TO" suffix for TSX-listed symbols (see 12__fetch_market_data.py
# for the analogous fix on the pricing side), so a BARE-ticker lookup for a Canadian symbol can
# return NULL or, rarely, data for an unrelated Yahoo-side symbol — never something more
# reliable than the authoritative XIC/TSX source. industry/description/employees/website are
# NOT protected (they have no equally-authoritative fallback here) — see section 2a's note.
_is_ca = master["market"] == "CA"
master.loc[_is_ca, "country"]  = "Canada"
master.loc[_is_ca, "exchange"] = "TSX"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2d. Resolve `founded` (Wikidata inception, cache-aware)
# MAGIC
# MAGIC Overlay the founding year from Wikidata (yfinance can't supply it). Reuses the same
# MAGIC `_known_company_info` cache read in 2c, so only tickers without a cached `founded`
# MAGIC are queried. This OVERRIDES the always-NULL `founded` that 2c set from the yfinance probe.

# COMMAND ----------

founded_map       = resolve_founded(_universe, _known_company_info)
master["founded"] = master["ticker"].map(founded_map).astype("object")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2e. Accounting standard + reporting currency (multi-market foundation)
# MAGIC
# MAGIC US rows (`market="US"`) are always `"us-gaap"`/`"USD"` — every US SEC filer reports
# MAGIC that way, so this stays a blanket literal for them, as before. Canadian rows
# MAGIC (`market="CA"`, admitted in section 2a) are left `NULL` here on purpose: real Canadian
# MAGIC MJDS/40-F filers are a genuine mix (confirmed 2026-07 against real `companyfacts` —
# MAGIC most banks/energy file `ifrs-full`/CAD, but IMO/Shopify/BlackBerry file `us-gaap`/USD,
# MAGIC and Nutrien/Gildan file `ifrs-full`/**USD** — currency is NOT a function of namespace
# MAGIC alone), so a blanket literal would be wrong for a large share of them. Real per-ticker
# MAGIC detection needs each filer's actual `companyfacts` response, which
# MAGIC `11__fetch_sec_xbrl.py` already fetches per ticker for ingestion — it derives and
# MAGIC writes these two fields back to `main.config.tickers` rather than this notebook
# MAGIC fetching `companyfacts` again just for metadata.
# MAGIC
# MAGIC (`market` — the ticker-identity collision-guard key, `(ticker, market)` — was already set
# MAGIC in section 2, before the first dedup; see `fundamentals_pipeline/identity.py`.)

# COMMAND ----------

_is_us = master["market"] == "US"
master.loc[_is_us, "accounting_standard"] = "us-gaap"
master.loc[_is_us, "reporting_currency"]  = "USD"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write to Delta

# COMMAND ----------

schema = StructType([
    StructField("ticker",      StringType(),  False),
    StructField("company",     StringType(),  True),
    StructField("sector",      StringType(),  True),
    StructField("market",      StringType(),  False),
    StructField("is_favorite", BooleanType(), False),
    StructField("in_sp500",    BooleanType(), False),
    StructField("in_r3000",    BooleanType(), False),
    StructField("in_tsx_composite", BooleanType(), False),
    StructField("has_logo",    BooleanType(), True),
    StructField("industry",    StringType(),  True),
    StructField("description", StringType(),  True),
    StructField("exchange",    StringType(),  True),
    StructField("country",     StringType(),  True),
    StructField("employees",   LongType(),    True),
    StructField("website",     StringType(),  True),
    StructField("founded",     IntegerType(), True),
    # Nullable now (were NOT NULL literals pre-Canadian-onboarding): admitted CA rows start
    # NULL here and are backfilled by 11__fetch_sec_xbrl.py from real per-ticker companyfacts.
    StructField("accounting_standard", StringType(), True),
    StructField("reporting_currency",  StringType(), True),
])

# createDataFrame(pandas_df, schema=StructType(...)) binds schema fields to DataFrame
# columns POSITIONALLY, not by name — `master`'s columns were built up incrementally
# across sections 2-2e in whatever order each field was first assigned, which does not
# match the field order declared in `schema` above. Reindex explicitly or the write
# silently scrambles columns into the wrong typed slots (or throws, if the scrambled
# types are incompatible).
master = master[[f.name for f in schema.fields]]
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

# Market breakdown — CA rows admitted in section 2a; accounting_standard/reporting_currency
# for CA rows are NULL until 11__fetch_sec_xbrl.py's next run backfills them from companyfacts.
print("Market breakdown:")
spark.sql(f"""
    SELECT
        market, COUNT(*) AS n,
        SUM(CAST(in_tsx_composite AS INT))                        AS in_tsx_composite,
        SUM(CASE WHEN accounting_standard IS NULL THEN 1 ELSE 0 END) AS pending_accounting_standard
    FROM {TARGET_TABLE}
    GROUP BY market
    ORDER BY market
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

# Industry coverage + top groups (NULL bucketed as "Unknown", matching the app).
spark.sql(f"""
    SELECT
        COUNT(*)                                                  AS total_tickers,
        SUM(CASE WHEN industry IS NOT NULL THEN 1 ELSE 0 END)     AS with_industry,
        ROUND(100.0 * SUM(CASE WHEN industry IS NOT NULL THEN 1 ELSE 0 END)
              / COUNT(*), 1)                                      AS pct_coverage
    FROM {TARGET_TABLE}
""").show()

print("Top industries:")
spark.sql(f"""
    SELECT COALESCE(industry, 'Unknown') AS industry, COUNT(*) AS n
    FROM {TARGET_TABLE}
    GROUP BY COALESCE(industry, 'Unknown')
    ORDER BY n DESC
""").show(25, truncate=False)

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
    print("\nℹ No favorites — edit 00__config/favorites.json to add some")

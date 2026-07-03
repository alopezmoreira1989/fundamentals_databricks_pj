# Databricks notebook source
# MAGIC %md
# MAGIC # 30__analysis / 38__history_audit
# MAGIC
# MAGIC Audits the historical coverage of each active ticker against the SEC companyfacts API
# MAGIC and flags those that likely have truncated history due to one of three causes:
# MAGIC
# MAGIC 1. **Missing predecessor CIK** (`flag_short_history`) — after a merger or
# MAGIC    MLP→C-corp conversion, the ticker points to a new CIK whose `companyfacts`
# MAGIC    only contains recent filings; the historical data lives under the old CIK.
# MAGIC 2. **Concept renaming** (`flag_concept_gap`) — the canonical XBRL tag we
# MAGIC    ingest (e.g. `Revenues`) has fewer years than a synonym (e.g.
# MAGIC    `RevenueFromContractWithCustomerExcludingAssessedTax`); the synonym needs to be
# MAGIC    added to `CONCEPT_SYNONYMS` in `01__tickers.py`.
# MAGIC 3. **Stub/transition year** (`flag_stub_years`) — rows with `fp='FY'` in
# MAGIC    10-K filings with a duration outside 350–380d (e.g. fiscal year-end change);
# MAGIC    the old strict filter discarded them.
# MAGIC
# MAGIC **Writes to**: `main.financials.history_audit` (Delta, overwrite each run —
# MAGIC snapshot of SEC state at the time of the run).
# MAGIC
# MAGIC **Does NOT write** to `financials_raw`, `financials`, or any other pipeline table.
# MAGIC
# MAGIC **How to run**:
# MAGIC - Default: audits all `ACTIVE_TICKERS` (~3000 tickers, ~20–25 min with 8 workers).
# MAGIC - Ad-hoc override: set `TICKERS_OVERRIDE = ["VNOM", "AAPL"]` before executing.

# COMMAND ----------

# MAGIC %md ## 0. Load config

# COMMAND ----------

# MAGIC %run "../00__config/01__tickers"

# COMMAND ----------

# ── Manual override: list of tickers; None = all active ──────────────────────
TICKERS_OVERRIDE: "list[str] | None" = None

# ── Heuristics ───────────────────────────────────────────────────────────────
MIN_EXPECTED_YEARS = 5     # fewer years than this → flag_short_history
MIN_REVENUE_GAP    = 2     # years difference between synonyms → flag_concept_gap

# ── Parallelism & rate limit (same pattern as 11__fetch_sec_xbrl.py) ─────────
MAX_WORKERS     = 8
MIN_REQUEST_GAP = 0.12
REQUEST_TIMEOUT = 30

# ── Output ───────────────────────────────────────────────────────────────────
AUDIT_TABLE = f"{CATALOG}.{SCHEMA}.history_audit"

# COMMAND ----------

import json
import sys
import time
import urllib.parse
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from threading import Lock

import pandas as pd
import requests
from requests.adapters import HTTPAdapter

# Windows console (cp1252) chokes on → ✓ ✗ ⚠ — force UTF-8 if the stream supports it.
# No-op in Databricks.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HEADERS = {"User-Agent": SEC_USER_AGENT}

# ── Shared HTTP session ───────────────────────────────────────────────────────
# Reusing the same Session across all workers saves the TCP+TLS handshake
# (~50–150ms per request) on every call after the first. The pool must be
# ≥ MAX_WORKERS to avoid becoming a hidden serialisation bottleneck.
SESSION = requests.Session()
SESSION.headers.update(HEADERS)
SESSION.mount("https://", HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS * 2))

# Latency buffer for the p50/p95/p99 summary at the end. Capped so it
# doesn't grow unbounded on long runs.
_LATENCIES: "deque[float]" = deque(maxlen=20000)

# Cache of "does this CIK have ingestible XBRL?" used by pass 3 (FTS).
# Shared across workers; single-key dict ops are atomic in CPython under
# the GIL — no lock needed.
_XBRL_PROBE_CACHE: "dict[str, bool]" = {}

# COMMAND ----------

# MAGIC %md ## 1. Lista de tickers a auditar

# COMMAND ----------

if TICKERS_OVERRIDE:
    AUDIT_TICKERS = [t.upper().strip() for t in TICKERS_OVERRIDE]
    print(f"✓ Manual override — auditing {len(AUDIT_TICKERS)} ticker(s): {AUDIT_TICKERS[:20]}{'…' if len(AUDIT_TICKERS) > 20 else ''}")
else:
    tickers_df = spark.table(f"{CATALOG}.config.tickers").select("ticker").collect()
    AUDIT_TICKERS = [row.ticker.upper() for row in tickers_df]
    print(f"✓ Full universe — auditing {len(AUDIT_TICKERS):,} tickers from {CATALOG}.config.tickers")

# COMMAND ----------

# MAGIC %md ## 2. Concepts to probe
# MAGIC
# MAGIC Broad families to detect renaming. If the canonical tag has less coverage
# MAGIC than any alternative in the same group, we flag it.

# COMMAND ----------

REVENUE_FAMILY = [
    "Revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "SalesRevenueNet",
    "SalesRevenueGoodsNet",
    "SalesRevenueServicesNet",
    "OilAndGasRevenue",
    "InterestAndDividendIncomeOperating",
]

NET_INCOME_FAMILY = [
    "NetIncomeLoss",
    "ProfitLoss",
    "NetIncomeLossAttributableToParent",
    "NetIncomeLossAvailableToCommonStockholdersBasic",
]

ASSETS_FAMILY = [
    "Assets",
]

ALL_PROBES = REVENUE_FAMILY + NET_INCOME_FAMILY + ASSETS_FAMILY

# ── XBRL tags the pipeline ALREADY ingests and collapses to "Revenue" ─────────
# Derived from INCOME_STATEMENT + CONCEPT_SYNONYMS (inherited from the %run of 01__tickers).
# If a tag from REVENUE_FAMILY is not here, the pipeline doesn't know it and we
# will fire flag_concept_gap. Keeping this set in sync with the pipeline is
# automatic — adding a synonym to 01__tickers.py updates this set automatically.
INGESTED_REVENUE_TAGS = {
    tag
    for label, (tag, _kind) in INCOME_STATEMENT.items()
    if label == "Revenue" or CONCEPT_SYNONYMS.get(label) == "Revenue"
}
print(f"  Pipeline reconoce {len(INGESTED_REVENUE_TAGS)} tag(s) como Revenue: {sorted(INGESTED_REVENUE_TAGS)}")

# COMMAND ----------

# MAGIC %md ## 3. Rate limiter + ticker index (cached)

# COMMAND ----------

_rate_lock = Lock()
_last_request_ts = [0.0]

def rate_limited_get(url: str, timeout: int = REQUEST_TIMEOUT) -> requests.Response:
    with _rate_lock:
        wait = _last_request_ts[0] + MIN_REQUEST_GAP - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _last_request_ts[0] = time.monotonic()
    _t = time.monotonic()
    resp = SESSION.get(url, timeout=timeout)
    _LATENCIES.append(time.monotonic() - _t)
    return resp


print("Loading SEC ticker index...")
_idx_resp = rate_limited_get("https://www.sec.gov/files/company_tickers.json")
_idx_resp.raise_for_status()
_idx = _idx_resp.json()

TICKER_MAP = {
    entry["ticker"].upper(): (str(entry["cik_str"]).zfill(10), entry["title"])
    for entry in _idx.values()
}
print(f"✓ Ticker index loaded — {len(TICKER_MAP):,} tickers known to SEC")

# ── CIK aliases from favorites.json ──────────────────────────────────────────
# Allows the audit to see the combined history of predecessor CIKs
# (MLP→C-corp mergers, spinoffs). Without this, a ticker with cik_aliases
# configured in the pipeline would falsely appear with flag_short_history because
# the current SEC CIK only has recent filings (e.g. VNOM).
_FAV_CIK_ALIASES = {}
try:
    with open(FAVORITES_JSON_PATH, encoding="utf-8") as f:
        _fav_raw = f.read()
    _fav_lines = [l for l in _fav_raw.splitlines() if not l.strip().startswith("/")]
    for _entry in json.loads("\n".join(_fav_lines)):
        _t = _entry["ticker"].upper().strip()
        _aliases = [str(c).zfill(10) for c in _entry.get("cik_aliases", []) if c]
        if _aliases:
            _FAV_CIK_ALIASES[_t] = _aliases
except Exception as _e:
    print(f"  ⚠ Could not load cik_aliases from favorites.json: {_e}")
if _FAV_CIK_ALIASES:
    print(f"  ✓ Active CIK aliases for audit: {_FAV_CIK_ALIASES}")

# COMMAND ----------

# MAGIC %md ## 4. Helpers — SEC fetch + ticker analysis

# COMMAND ----------

def classify_duration(start, end):
    if pd.isna(start):
        return "snapshot"
    days = (pd.to_datetime(end) - pd.to_datetime(start)).days
    if   70  <= days <=  100: return "Q_standalone"
    elif 160 <= days <=  200: return "YTD_6M"
    elif 250 <= days <=  290: return "YTD_9M"
    elif 350 <= days <=  380: return "FY_or_TTM"
    else:                     return f"other_{days}d"


def concept_fy_coverage(facts: dict, concept: str) -> "tuple[set[int], int]":
    """
    Returns (set of distinct fy with fp='FY' in the 10-K family, n rows with period_shape='other_*').
    For balance-sheet concepts (snapshot), returns years derived from period_end.
    """
    try:
        units    = facts["facts"]["us-gaap"][concept]["units"]
        unit_key = "USD" if "USD" in units else list(units.keys())[0]
        rows     = units[unit_key]
    except KeyError:
        return set(), 0

    if not rows:
        return set(), 0

    df = pd.DataFrame(rows)
    df = df[df["form"].isin(["10-K", "10-K/A"])]
    if df.empty:
        return set(), 0

    df["end"] = pd.to_datetime(df["end"], errors="coerce")
    if "start" in df.columns:
        df["start"] = pd.to_datetime(df["start"], errors="coerce")
    else:
        df["start"] = pd.NaT

    df["period_shape"] = df.apply(lambda r: classify_duration(r["start"], r["end"]), axis=1)

    # Stocks: snapshots, year = end.year. Flows: fp='FY' and period_shape!='snapshot'
    is_stock = df["period_shape"].eq("snapshot").all()
    if is_stock:
        years = set(df["end"].dt.year.dropna().astype(int).tolist())
        n_stub = 0
    else:
        df_fy = df[(df["fp"] == "FY") & (df["period_shape"] != "snapshot")].copy()
        if df_fy.empty:
            return set(), 0
        years = set(df_fy["fy"].dropna().astype(int).tolist())
        n_stub = int(df_fy["period_shape"].astype(str).str.startswith("other_").sum())

    return years, n_stub


def merge_facts(*facts_dicts: dict) -> dict:
    """
    Concatenates `facts[ns][concept]["units"][unit]` arrays across multiple
    companyfacts JSONs. Same logic as 11__fetch_sec_xbrl.merge_facts —
    duplicated here to avoid coupling this script (read-only) with the
    ingestion notebook (which has side effects when run via %run).
    """
    if not facts_dicts:
        return {}
    if len(facts_dicts) == 1:
        return facts_dicts[0]

    merged = {"facts": {}}
    for k, v in facts_dicts[0].items():
        if k != "facts":
            merged[k] = v

    for fd in facts_dicts:
        for ns, concepts in fd.get("facts", {}).items():
            ns_bucket = merged["facts"].setdefault(ns, {})
            for concept, payload in concepts.items():
                if concept not in ns_bucket:
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


def derive_10k_stats(facts: dict) -> "tuple[int|None, int|None, int]":
    """
    Derives (first_10k_year, last_10k_year, n_10k) from companyfacts rows
    without calling /submissions. Iterates all probed concepts and unions
    distinct (fy, accn) for form='10-K'/'10-K/A'.

    Trade-off vs. the old method (counting filings from /submissions):
    if a 10-K reports NONE of the concepts in ALL_PROBES, it is not counted.
    For normal issuers this doesn't happen (all report Revenue/NetIncome/Assets).
    n_10k is now "number of 10-K accessions observed in probed concepts".
    """
    fys: set[int] = set()
    accns: set[str] = set()
    ns_bucket = facts.get("facts", {}).get("us-gaap", {})
    for concept in ALL_PROBES:
        units = ns_bucket.get(concept, {}).get("units", {})
        for unit_rows in units.values():
            for row in unit_rows:
                if row.get("form") not in ("10-K", "10-K/A"):
                    continue
                fy = row.get("fy")
                if fy is not None:
                    fys.add(int(fy))
                accn = row.get("accn")
                if accn:
                    accns.add(accn)
    return (min(fys) if fys else None, max(fys) if fys else None, len(accns))


def _build_action_recommended(rec: dict) -> "str | None":
    actions = []
    if rec["flag_short_history"]:
        suggested = rec.get("suggested_predecessor_ciks") or []
        had_raw = rec.get("_had_raw_predecessor_candidates", False)
        if suggested:
            csv = ", ".join(suggested)
            actions.append(
                f"Likely predecessor (EDGAR FTS): CIK(s) {csv} — verify and add to cik_aliases in favorites.json"
            )
        elif had_raw:
            actions.append(
                "Possible predecessor without XBRL: hits in EDGAR FTS but no CIK with usable companyfacts "
                "(likely pre-XBRL mandate 2009-2011). Review manually."
            )
        elif rec["former_names"]:
            actions.append(
                "Likely predecessor CIK — search EDGAR full-text by formerNames "
                f"({rec['former_names']}) and add to cik_aliases in favorites.json"
            )
        else:
            actions.append(
                "Likely predecessor CIK — no formerNames; check Previous CIKs in EDGAR full-text "
                "and add to cik_aliases in favorites.json"
            )
    if rec["flag_concept_gap"]:
        best_tag = rec["concept_max_coverage"]
        actions.append(
            f"Synonym to add: tag {best_tag} covers more years than the canonical one — "
            "add it to INCOME_STATEMENT in 01__tickers.py and to the CONCEPT_SYNONYMS dict pointing to Revenue"
        )
    if rec["flag_stub_years"]:
        actions.append(
            "There are years with period_shape=other_* (stubs/transitions). Layer c of 21__clean_and_merge.py "
            "(max-duration window) already captures them — verify that ticker has FY after re-ingestion."
        )
    return " | ".join(actions) if actions else None


def fetch_former_names(current_cik: str, aliased_ciks: "list[str]") -> "list[str]":
    """
    Lazy call to /submissions to populate formerNames. Invoked ONLY for
    tickers with flag_short_history in the second pass (post-parallel). Errors
    on aliases are silent; an error on the primary CIK returns [].
    """
    names: set[str] = set()
    for c in [current_cik] + list(aliased_ciks or []):
        if not c:
            continue
        try:
            resp = rate_limited_get(f"https://data.sec.gov/submissions/CIK{c}.json")
            resp.raise_for_status()
            for fn in resp.json().get("formerNames", []):
                if fn.get("name"):
                    names.add(fn["name"])
        except Exception:
            continue
    return sorted(names)


# ── Pass 3: fuzzy predecessor search by name (EDGAR FTS) ─────────────────────
# Covers the case where the predecessor is a DIFFERENT CIK (LLC→Inc. change,
# debt issuer→equity issuer), not a rename within the same CIK. SEC's formerNames
# only captures the latter. Canonical example: ALH (Alliance Laundry Holdings)
# whose predecessor "Alliance Laundry Systems LLC" CIK 0001063699 does not appear
# in formerNames because it is a distinct CIK.

_ENTITY_SUFFIXES = {
    "INC", "INC.", "CORP", "CORP.", "CORPORATION", "COMPANY", "CO", "CO.",
    "HOLDINGS", "HOLDING", "GROUP", "LLC", "L.L.C.", "LTD", "LTD.",
    "LIMITED", "PLC", "TRUST", "LP", "L.P.", "PARTNERS", "PARTNERSHIP",
    "SA", "S.A.", "NV", "N.V.", "AG", "THE",
}

_NAME_STOPWORDS = {
    # connectors / articles
    "AND", "OF", "FOR", "NEW", "THE",
    # geographic / highly recurrent generic terms in company names
    "AMERICAN", "UNITED", "GLOBAL", "INTERNATIONAL", "NATIONAL", "FIRST",
    # industry noise: appear in thousands of names and match anything.
    # If the issuer name ONLY contains these tokens, the overlap filter
    # will reduce candidates to [], which is correct — for those cases we
    # cannot search for a predecessor by name because the name is non-distinctive.
    "TECHNOLOGIES", "TECHNOLOGY", "HEALTHCARE", "SYSTEMS", "SOLUTIONS",
    "INDUSTRIES", "ENTERPRISES", "RESOURCES", "SERVICES", "ENERGY",
    "ENTERTAINMENT", "FINANCIAL", "FINANCE", "PHARMACEUTICALS",
    "PHARMACEUTICAL", "MEDICAL", "COMMUNICATIONS", "BANCORP",
    "BANCSHARES", "BANKSHARES", "CAPITAL", "PROPERTIES", "REALTY",
}


def _normalize_company_name(title: str) -> str:
    """
    Strips trademark glyphs, parens, and recurrent entity suffixes to
    obtain the "core" of the name for use with EDGAR FTS.
    "ALLIANCE LAUNDRY HOLDINGS INC." → "ALLIANCE LAUNDRY".
    """
    if not title:
        return ""
    s = title.upper()
    # strip trademarks and parens
    for ch in ("™", "®", "©"):
        s = s.replace(ch, "")
    # cut any paren (typically (TICKER) or (CIK ...))
    if "(" in s:
        s = s.split("(", 1)[0]
    # remove commas and collapse whitespace
    s = s.replace(",", " ")
    tokens = [t for t in s.split() if t]
    # remove leading "THE" — common in names like "The Walt Disney Company"
    if tokens and tokens[0] == "THE":
        tokens.pop(0)
    # remove entity suffixes right-to-left iteratively
    while tokens and tokens[-1] in _ENTITY_SUFFIXES:
        tokens.pop()
    return " ".join(tokens).strip()


def _substantive_tokens(name: str) -> "set[str]":
    """
    Tokens >=3 chars, alphanumeric, excluding generic stopwords. Used to
    filter EDGAR FTS hits by overlap with the issuer name.
    """
    out: set[str] = set()
    for tok in (name or "").upper().split():
        cleaned = "".join(c for c in tok if c.isalnum())
        if len(cleaned) >= 3 and cleaned not in _NAME_STOPWORDS:
            out.add(cleaned)
    return out


def _cik_has_xbrl(cik: str, cache: "dict[str, bool]") -> bool:
    """
    Memoized check: does companyfacts/CIK{cik}.json return non-empty us-gaap
    data? True only if status 200 + json + facts.us-gaap non-empty.
    404 → False (and cached). 5xx or others → False but NOT cached
    (to avoid poisoning the run with a transient failure).
    """
    cik = str(cik).zfill(10)
    cached = cache.get(cik)
    if cached is not None:
        return cached
    try:
        resp = rate_limited_get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json")
        if resp.status_code == 404:
            cache[cik] = False
            return False
        if resp.status_code != 200:
            return False  # no cachear: transient
        if "json" not in resp.headers.get("Content-Type", "").lower():
            cache[cik] = False
            return False
        body = resp.json()
        usgaap = body.get("facts", {}).get("us-gaap", {})
        ok = bool(usgaap)
        cache[cik] = ok
        return ok
    except Exception:
        return False  # no cachear


def _edgar_fts_search(normalized_name: str) -> "list[dict]":
    """
    GET to EDGAR full-text search for a quoted phrase, restricted to 10-K.
    Returns the list of hits (each hit has _source with ciks, display_names,
    file_date). On any error → [].
    """
    if not normalized_name:
        return []
    quoted = f'"{normalized_name}"'
    url = "https://efts.sec.gov/LATEST/search-index?q=" + urllib.parse.quote_plus(quoted) + "&forms=10-K"
    try:
        resp = rate_limited_get(url)
        resp.raise_for_status()
        return resp.json().get("hits", {}).get("hits", []) or []
    except Exception:
        return []


def _rank_predecessor_candidates(
    hits: "list[dict]",
    current_cik: str,
    exclude_ciks: "set[str]",
    issuer_tokens: "set[str]",
) -> "list[tuple[str, int, str]]":
    """
    Aggregates hits by CIK (all CIKs per hit — some filings are multi-issuer),
    filters out the current CIK and already-configured aliases, requires
    >=1 substantive token overlap with the issuer name to reduce false positives
    from generic names. Sorts by (filings desc, latest_date desc).

    Deliberate: we do NOT fetch the candidate's SIC. That would be an extra GET
    per candidate and the token filter already eliminates most cross-industry noise.
    """
    agg: dict[str, dict] = {}
    for h in hits:
        src = h.get("_source", {}) or {}
        ciks = src.get("ciks", []) or []
        names = src.get("display_names", []) or []
        date = src.get("file_date", "")
        for i, cik in enumerate(ciks):
            cik_pad = str(cik).zfill(10)
            if cik_pad == current_cik or cik_pad in exclude_ciks:
                continue
            display = names[i] if i < len(names) else (names[0] if names else "")
            entry = agg.setdefault(cik_pad, {"count": 0, "latest_date": "", "display_name": display})
            entry["count"] += 1
            if date and date > entry["latest_date"]:
                entry["latest_date"] = date
            if not entry["display_name"] and display:
                entry["display_name"] = display
    out: list[tuple[str, int, str]] = []
    for cik, info in agg.items():
        cand_tokens = _substantive_tokens(_normalize_company_name(info["display_name"]))
        if not (cand_tokens & issuer_tokens):
            continue
        out.append((cik, info["count"], info["latest_date"]))
    out.sort(key=lambda t: (t[1], t[2]), reverse=True)  # count desc, latest_date desc
    return out


def find_predecessor_candidates(
    ticker: str,
    current_cik: str,
    issuer_title: str,
    aliased_ciks: "list[str]",
    xbrl_cache: "dict[str, bool]",
    max_results: int = 3,
) -> "tuple[list[str], bool]":
    """
    Orchestrator for pass 3. Returns (suggested_ciks, had_raw_candidates).
    had_raw_candidates distinguishes "FTS returned nothing" from "FTS returned
    hits but none have XBRL". The latter is the signature of the ALH problem
    (predecessor exists but predates the XBRL mandate).
    """
    normalized = _normalize_company_name(issuer_title)
    if not normalized:
        return [], False
    issuer_tokens = _substantive_tokens(normalized)
    if not issuer_tokens:
        return [], False
    hits = _edgar_fts_search(normalized)
    exclude = {str(c).zfill(10) for c in (aliased_ciks or []) if c}
    ranked = _rank_predecessor_candidates(hits, current_cik, exclude, issuer_tokens)
    had_raw = bool(ranked)
    out: list[str] = []
    for cik, _count, _date in ranked:
        if len(out) >= max_results:
            break
        if _cik_has_xbrl(cik, xbrl_cache):
            out.append(cik)
    return out, had_raw


def audit_ticker(ticker: str) -> dict:
    """
    Audits a ticker. Returns a dict with all fields required by the destination
    table schema. On error, returns a dict with `error` populated and numeric
    fields set to NULL.

    NOTE: this pass does NOT call /submissions. former_names is left empty and
    populated in a second lazy pass only for tickers with flag_short_history
    (the only flag whose action_recommended depends on formerNames).
    """
    base = {
        "ticker":                    ticker,
        "current_cik":               None,
        "aliased_ciks":              [],
        "former_names":              [],
        "suggested_predecessor_ciks": [],
        "first_10k_year":            None,
        "last_10k_year":             None,
        "n_10k":                     0,
        "years_revenue":             [],
        "years_net_income":          [],
        "years_assets":              [],
        "flag_short_history":        False,
        "flag_concept_gap":          False,
        "flag_stub_years":           False,
        "n_flags":                   0,
        "concept_max_coverage":      None,
        "action_recommended":        None,
        "error":                     None,
    }

    # Resolve primary CIK
    cik_info = TICKER_MAP.get(ticker.upper())
    if cik_info is None:
        base["error"] = "ticker_not_in_sec_index"
        base["action_recommended"] = "Ticker does not resolve in SEC ticker index — add a 'cik' override in favorites.json"
        return base
    cik, _ = cik_info
    base["current_cik"] = cik

    # CIKs to audit: primary + aliases configured in favorites.json.
    # The primary is mandatory; aliases are best-effort (a broken alias does not
    # abort the ticker audit — only its contribution is lost).
    aliases = _FAV_CIK_ALIASES.get(ticker.upper(), [])
    base["aliased_ciks"] = aliases
    cik_list = [cik] + [a for a in aliases if a != cik]

    # ── Companyfacts: fetch per CIK and merge ─────────────────────────────
    facts_list = []
    for c in cik_list:
        try:
            facts_resp = rate_limited_get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{c}.json")
            facts_resp.raise_for_status()
            if "json" not in facts_resp.headers.get("Content-Type", "").lower():
                if c == cik:
                    base["error"] = "non_json_facts"
                    return base
                continue
            facts_list.append(facts_resp.json())
        except requests.exceptions.HTTPError as e:
            code = getattr(e.response, "status_code", 0)
            if c == cik:
                if code == 404:
                    base["error"] = "no_companyfacts"
                    base["action_recommended"] = "Issuer has no companyfacts (may be very small or newly listed)"
                else:
                    base["error"] = f"facts_http_{code}"
                return base
            # broken alias — silent, continue
        except Exception as e:
            if c == cik:
                base["error"] = f"facts_fetch_failed: {str(e)[:100]}"
                return base

    facts = merge_facts(*facts_list) if facts_list else {}
    if not facts.get("facts"):
        base["error"] = "no_facts_after_merge"
        return base

    # ── 10-K stats derived from companyfacts (avoids a /submissions fetch) ──
    first_y, last_y, n_10k = derive_10k_stats(facts)
    base["first_10k_year"] = first_y
    base["last_10k_year"]  = last_y
    base["n_10k"]          = n_10k

    # Coverage per family: keep the best-coverage synonym per family
    family_results = {}  # family_name → {best_concept, best_years, all_concept_years}
    family_stubs = 0

    for family_name, family_concepts in [
        ("revenue", REVENUE_FAMILY),
        ("net_income", NET_INCOME_FAMILY),
        ("assets", ASSETS_FAMILY),
    ]:
        per_concept = {}
        for concept in family_concepts:
            years, n_stub = concept_fy_coverage(facts, concept)
            if years:
                per_concept[concept] = years
            family_stubs += n_stub

        if per_concept:
            best_concept = max(per_concept.items(), key=lambda kv: len(kv[1]))[0]
            best_years   = per_concept[best_concept]
        else:
            best_concept, best_years = None, set()

        family_results[family_name] = {
            "best_concept": best_concept,
            "best_years":   best_years,
            "per_concept":  per_concept,
        }

    base["years_revenue"]    = sorted(family_results["revenue"]["best_years"])
    base["years_net_income"] = sorted(family_results["net_income"]["best_years"])
    base["years_assets"]     = sorted(family_results["assets"]["best_years"])
    base["concept_max_coverage"] = family_results["revenue"]["best_concept"]

    # ── Flag 1: short history ──────────────────────────────────────────────
    all_years = (
        family_results["revenue"]["best_years"]
        | family_results["net_income"]["best_years"]
        | family_results["assets"]["best_years"]
    )
    if all_years and (max(all_years) - min(all_years) + 1) < MIN_EXPECTED_YEARS:
        base["flag_short_history"] = True

    # ── Flag 2: concept gap (what the pipeline ingests vs the best synonym) ──
    # If the best synonym in the Revenue family covers MIN_REVENUE_GAP more years
    # than the tags the pipeline recognises (INGESTED_REVENUE_TAGS, derived from
    # CONCEPT_SYNONYMS) → flag. After adding a synonym to 01__tickers.py the flag
    # disappears automatically.
    revenue_per_concept = family_results["revenue"]["per_concept"]
    if revenue_per_concept:
        canonical_years = set()
        for tag in INGESTED_REVENUE_TAGS:
            canonical_years |= revenue_per_concept.get(tag, set())
        best_years = family_results["revenue"]["best_years"]
        gap = len(best_years - canonical_years)
        if gap >= MIN_REVENUE_GAP:
            base["flag_concept_gap"] = True

    # ── Flag 3: stub years ────────────────────────────────────────────────
    if family_stubs > 0:
        base["flag_stub_years"] = True

    base["n_flags"] = int(base["flag_short_history"]) + int(base["flag_concept_gap"]) + int(base["flag_stub_years"])

    # action_recommended is computed here; if flag_short_history fires, the
    # lazy /submissions pass will regenerate it later with formerNames populated.
    base["action_recommended"] = _build_action_recommended(base)
    return base

# COMMAND ----------

# MAGIC %md ## 5. Run audit en paralelo

# COMMAND ----------

started_at = time.monotonic()
results    = []
state_lock = Lock()
completed  = [0]
total      = len(AUDIT_TICKERS)

def _worker(ticker: str):
    rec = audit_ticker(ticker)
    with state_lock:
        completed[0] += 1
        n = completed[0]
        results.append(rec)
        if n % 100 == 0 or n == total:
            elapsed = time.monotonic() - started_at
            rate    = n / elapsed if elapsed else 0
            eta_s   = (total - n) / rate if rate else 0
            print(f"  [{n:>5}/{total}]  ({rate:.1f} t/s, ETA {eta_s/60:.1f} min)")

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
    futures = [pool.submit(_worker, t) for t in AUDIT_TICKERS]
    for _ in as_completed(futures):
        pass

elapsed = time.monotonic() - started_at
print(f"\n✓ Main pass complete in {elapsed/60:.1f} min ({total/elapsed:.1f} t/s)")

# ── Lazy pass: /submissions only for flag_short_history ──────────────────────
# formerNames only affects action_recommended when flag_short_history fires.
# Skipping it in the main pass saves ~3000 requests (one per ticker). Here we
# fetch it only for flagged tickers (typically <100) and regenerate
# action_recommended for those.
flagged_idxs = [i for i, r in enumerate(results) if r.get("flag_short_history")]
if flagged_idxs:
    print(f"\n  Pase lazy /submissions para {len(flagged_idxs)} ticker(s) flageados…")
    lazy_started = time.monotonic()
    with ThreadPoolExecutor(max_workers=4) as pool:
        fut_to_i = {
            pool.submit(fetch_former_names, results[i]["current_cik"], results[i].get("aliased_ciks") or []): i
            for i in flagged_idxs
        }
        for fut in as_completed(fut_to_i):
            i = fut_to_i[fut]
            try:
                results[i]["former_names"] = fut.result()
            except Exception as e:
                results[i]["former_names"] = []
                print(f"    ⚠ {results[i]['ticker']}: {str(e)[:80]}")
            results[i]["action_recommended"] = _build_action_recommended(results[i])
    print(f"  ✓ Lazy pass complete in {(time.monotonic()-lazy_started):.1f}s")

# ── FTS pass: search for predecessors by name via EDGAR full-text ────────────
# Invoked for tickers with flag_short_history that have NO alias configured.
# NOTE: formerNames is NOT a good filter — SEC records it for renames within the
# same CIK (e.g. JNTL Inc.→Kenvue Inc., same CIK), not for predecessors in a
# DIFFERENT CIK (ALH case: LLC→Inc., distinct CIK). The two columns (former_names
# vs suggested_predecessor_ciks) are complementary, not mutually exclusive.
fts_candidates_idxs = [
    i for i in flagged_idxs
    if results[i]["ticker"] not in _FAV_CIK_ALIASES
    and results[i].get("current_cik")
]
if fts_candidates_idxs:
    print(f"\n  FTS pass for {len(fts_candidates_idxs)} candidate(s) without a configured cik_alias…")
    fts_started = time.monotonic()
    n_with_suggestions = 0

    def _fts_worker(idx: int):
        rec = results[idx]
        ticker = rec["ticker"]
        title_info = TICKER_MAP.get(ticker.upper())
        title = title_info[1] if title_info else ""
        return idx, find_predecessor_candidates(
            ticker=ticker,
            current_cik=rec["current_cik"],
            issuer_title=title,
            aliased_ciks=rec.get("aliased_ciks") or [],
            xbrl_cache=_XBRL_PROBE_CACHE,
            max_results=3,
        )

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(_fts_worker, i) for i in fts_candidates_idxs]
        for fut in as_completed(futures):
            try:
                idx, (suggested, had_raw) = fut.result()
            except Exception as e:
                print(f"    ⚠ pase FTS exception: {str(e)[:80]}")
                continue
            results[idx]["suggested_predecessor_ciks"] = suggested
            results[idx]["_had_raw_predecessor_candidates"] = had_raw
            if suggested:
                n_with_suggestions += 1
            results[idx]["action_recommended"] = _build_action_recommended(results[idx])

    # Clean up the transient field before building the DataFrame (not in the schema)
    for r in results:
        r.pop("_had_raw_predecessor_candidates", None)

    print(
        f"  ✓ FTS pass complete in {(time.monotonic()-fts_started):.1f}s — "
        f"XBRL suggestions for {n_with_suggestions}/{len(fts_candidates_idxs)} candidates "
        f"(XBRL probe cache: {len(_XBRL_PROBE_CACHE)} CIKs)"
    )

# HTTP latency summary (cap: last 20k requests of the run)
if _LATENCIES:
    lats_ms = sorted(l * 1000 for l in _LATENCIES)
    n_lat   = len(lats_ms)
    p50     = lats_ms[n_lat // 2]
    p95     = lats_ms[min(n_lat - 1, int(n_lat * 0.95))]
    p99     = lats_ms[min(n_lat - 1, int(n_lat * 0.99))]
    print(f"  HTTP latency  p50: {p50:.0f}ms  p95: {p95:.0f}ms  p99: {p99:.0f}ms  (n={n_lat})")

total_elapsed = time.monotonic() - started_at
print(f"✓ Auditoría completa en {total_elapsed/60:.1f} min (total)")

# COMMAND ----------

# MAGIC %md ## 6. Build pandas DF + write to Delta

# COMMAND ----------

audit_df = pd.DataFrame(results)
audit_df["audited_at"] = datetime.utcnow()

# Coerce nullable int columns so createDataFrame doesn't complain
for col in ("first_10k_year", "last_10k_year", "n_10k", "n_flags"):
    audit_df[col] = audit_df[col].astype("Int64")

# Explicit schema (order and nullability to match the table exactly)
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

schema = StructType([
    StructField("ticker",                     StringType(),               False),
    StructField("current_cik",                StringType(),               True),
    StructField("aliased_ciks",               ArrayType(StringType()),    True),
    StructField("former_names",               ArrayType(StringType()),    True),
    StructField("suggested_predecessor_ciks", ArrayType(StringType()),    True),
    StructField("first_10k_year",             IntegerType(),              True),
    StructField("last_10k_year",        IntegerType(),              True),
    StructField("n_10k",                IntegerType(),              True),
    StructField("years_revenue",        ArrayType(IntegerType()),   True),
    StructField("years_net_income",     ArrayType(IntegerType()),   True),
    StructField("years_assets",         ArrayType(IntegerType()),   True),
    StructField("flag_short_history",   BooleanType(),              True),
    StructField("flag_concept_gap",     BooleanType(),              True),
    StructField("flag_stub_years",      BooleanType(),              True),
    StructField("n_flags",              IntegerType(),              True),
    StructField("concept_max_coverage", StringType(),               True),
    StructField("action_recommended",   StringType(),               True),
    StructField("error",                StringType(),               True),
    StructField("audited_at",           TimestampType(),            False),
])

# Reorder columns to match schema order
audit_df = audit_df[[f.name for f in schema.fields]]

sdf = spark.createDataFrame(audit_df, schema=schema)

# Guard: when TICKERS_OVERRIDE is active (test mode), do NOT overwrite the
# table — preserve the last full-run snapshot and only print.
if TICKERS_OVERRIDE:
    print(f"⚠ TICKERS_OVERRIDE active — skipping write to {AUDIT_TABLE} to avoid clobbering the full snapshot.")
    print(f"\n── Run result ({len(audit_df)} ticker(s)) ──")
    show_cols = ["ticker", "current_cik", "aliased_ciks", "suggested_predecessor_ciks",
                 "n_flags", "flag_short_history", "flag_concept_gap", "flag_stub_years",
                 "n_10k", "first_10k_year", "last_10k_year",
                 "concept_max_coverage", "error"]
    print(audit_df[show_cols].to_string(index=False))
    print("\n── action_recommended by ticker ──")
    for _, row in audit_df.iterrows():
        if row.get("action_recommended"):
            print(f"  {row['ticker']}: {row['action_recommended']}")
else:
    (
        sdf.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(AUDIT_TABLE)
    )
    print(f"✓ {sdf.count():,} rows written → {AUDIT_TABLE}")

# COMMAND ----------

# MAGIC %md ## 7. Summary — top suspects

# COMMAND ----------

print("\n" + "="*78)
print(f"  AUDIT SUMMARY  —  {AUDIT_TABLE}")
print("="*78)

n_total      = len(audit_df)
n_errors     = audit_df["error"].notna().sum()
n_flagged    = (audit_df["n_flags"] > 0).sum()
n_3flags     = (audit_df["n_flags"] == 3).sum()
n_2flags     = (audit_df["n_flags"] == 2).sum()
n_1flag      = (audit_df["n_flags"] == 1).sum()
n_short      = audit_df["flag_short_history"].sum()
n_gap        = audit_df["flag_concept_gap"].sum()
n_stub       = audit_df["flag_stub_years"].sum()

print(f"  Tickers audited          : {n_total:,}")
print(f"  With fetch errors        : {n_errors:,}")
print(f"  With at least 1 flag     : {n_flagged:,}")
print(f"    - 3 flags (critical)   : {n_3flags:,}")
print(f"    - 2 flags              : {n_2flags:,}")
print(f"    - 1 flag               : {n_1flag:,}")
print("  Breakdown by flag:")
print(f"    flag_short_history     : {n_short:,}")
print(f"    flag_concept_gap       : {n_gap:,}")
print(f"    flag_stub_years        : {n_stub:,}")

# COMMAND ----------

top_suspects = (
    audit_df[audit_df["n_flags"] > 0]
    .sort_values(["n_flags", "n_10k"], ascending=[False, True])
    .head(20)
)

if not top_suspects.empty:
    print("\n── TOP-20 MOST SUSPICIOUS TICKERS ──")
    show_cols = ["ticker", "current_cik", "n_flags", "flag_short_history",
                 "flag_concept_gap", "flag_stub_years", "n_10k",
                 "first_10k_year", "last_10k_year", "concept_max_coverage",
                 "suggested_predecessor_ciks"]
    print(top_suspects[show_cols].to_string(index=False))

    print("\n── RECOMMENDED ACTION (top 10) ──")
    for _, row in top_suspects.head(10).iterrows():
        print(f"\n  {row['ticker']}  (CIK {row['current_cik']}, flags={row['n_flags']})")
        if row.get("suggested_predecessor_ciks"):
            print(f"    suggested_predecessor_ciks: {list(row['suggested_predecessor_ciks'])}")
        if row["former_names"]:
            print(f"    formerNames: {row['former_names']}")
        print(f"    → {row['action_recommended']}")
else:
    print("\n✓ No ticker flagged — the full history looks complete")

# COMMAND ----------

# MAGIC %md ## 8. Quick SQL for later use
# MAGIC
# MAGIC ```sql
# MAGIC -- Critical tickers (3 flags)
# MAGIC SELECT * FROM main.financials.history_audit
# MAGIC WHERE n_flags = 3 ORDER BY n_10k ASC;
# MAGIC
# MAGIC -- Candidates to add a synonym (flag_concept_gap)
# MAGIC SELECT ticker, current_cik, concept_max_coverage, years_revenue
# MAGIC FROM main.financials.history_audit
# MAGIC WHERE flag_concept_gap = TRUE;
# MAGIC
# MAGIC -- Candidates likely to have a predecessor CIK (flag_short_history)
# MAGIC SELECT ticker, current_cik, former_names, first_10k_year, last_10k_year
# MAGIC FROM main.financials.history_audit
# MAGIC WHERE flag_short_history = TRUE ORDER BY n_10k ASC;
# MAGIC
# MAGIC -- Work-queue: tickers with a predecessor suggested by EDGAR FTS (Pass 3).
# MAGIC -- These are the actionable candidates to add to cik_aliases in favorites.json.
# MAGIC SELECT ticker, current_cik, suggested_predecessor_ciks, n_10k, last_10k_year, action_recommended
# MAGIC FROM main.financials.history_audit
# MAGIC WHERE flag_short_history = TRUE
# MAGIC   AND ARRAY_SIZE(suggested_predecessor_ciks) > 0
# MAGIC ORDER BY ARRAY_SIZE(suggested_predecessor_ciks) DESC, n_10k ASC
# MAGIC LIMIT 40;
# MAGIC ```

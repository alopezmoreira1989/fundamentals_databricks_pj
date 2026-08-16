# Databricks notebook source
# MAGIC %md
# MAGIC # 10__ingestion / 18__fetch_eu_market_data
# MAGIC
# MAGIC **Phase 5.6 — bounded European market-data ingestion.** Fetches real daily prices/splits
# MAGIC for the currently `admission_status = 'admitted'` European issuers
# MAGIC (`main.config.eu_admission_candidates`) into the SAME `market_prices_daily`/`stock_splits`
# MAGIC tables `12__fetch_market_data.py` writes — no new table, `source_id`-free (these tables
# MAGIC never carried one; a row's ticker is enough, exactly as for US/CA rows).
# MAGIC
# MAGIC **Not** wired into `12`'s own scheduled run, **not** added to the DAG, **not** writing to
# MAGIC `config.tickers`. A deliberately separate, bounded, non-scheduled notebook — the same
# MAGIC pattern `16__fetch_eu_xbrl.py`/`17__firds_admission.py` already established for European
# MAGIC ingestion.
# MAGIC
# MAGIC **Why a separate notebook rather than adding Europe to `12`'s own run**: unlike Canada
# MAGIC (one uniform Yahoo `.TO` suffix for every CA ticker, keyed off `config.tickers.market`),
# MAGIC each EU MIC needs its OWN Yahoo suffix (Madrid `.MC`, Paris `.PA`, Amsterdam `.AS`, Milan
# MAGIC `.MI`) — a per-ticker, not per-market, mapping `12`'s existing `MARKET_MAP`-keyed logic
# MAGIC doesn't express. More importantly: **a bare ticker symbol is not safely resolvable via
# MAGIC Yahoo Finance without independent verification** — Phase 5.2's own research found real,
# MAGIC live global ticker collisions for two of these exact eight tickers (`FCC` also matches an
# MAGIC unrelated Vietnamese company; `FCT` matches ≥5 unrelated global companies). This notebook
# MAGIC therefore adds a real, additional eligibility gate beyond `admission_status = 'admitted'`
# MAGIC (which only proves ESEF/fundamentals eligibility, not market-data safety): every fetched
# MAGIC Yahoo record's own company name must independently, confidently match the admission
# MAGIC record's `issuer_name` via `classify_company_match()` (the same conservative, reject-
# MAGIC don't-guess matcher `identity.py` already uses for the US/CA cross-market collision guard)
# MAGIC before its price/split data is trusted and written. A ticker that fails this check is
# MAGIC logged to `ingestion_failures` and excluded — never silently accepted.
# MAGIC
# MAGIC **Architecture**: reuses `12__fetch_market_data.py`'s own battle-tested fetch/MERGE logic
# MAGIC directly (loaded via the same `importlib` + pre-seeded-globals mechanism Phase 5.4 already
# MAGIC established for `16__fetch_eu_xbrl.py`/`21__clean_and_merge.py`) rather than duplicating
# MAGIC it — `12` already supports an "override mode" (`ACTIVE_TICKERS` pre-seeded by a parent),
# MAGIC extended here with one small, additive sibling override (`YAHOO_SYMBOL`, same pattern) so
# MAGIC a caller can supply explicit per-ticker Yahoo symbols instead of the market-keyed default.

# COMMAND ----------

# MAGIC %md ## 0. Load config

# COMMAND ----------

# MAGIC %run "../00__config/01__tickers"

# COMMAND ----------

import importlib.util
from datetime import datetime
from pathlib import Path

# Defensive install, same pattern as 02__tickers_master.py's own yfinance guard -- this
# notebook is a standalone, non-%run-chained entry point (like 16__fetch_eu_xbrl.py), so it
# cannot assume 12's own `%pip install yfinance` cell already ran in this session.
try:
    import yfinance as yf  # noqa: F401
except ImportError:
    import subprocess
    import sys

    subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "yfinance"])

try:
    from fundamentals_pipeline.identity import classify_company_match
except ImportError:
    import subprocess
    import sys

    subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "-e", "../.."])
    from fundamentals_pipeline.identity import classify_company_match

# Gates the network/Spark sections below, mirroring every other Phase 5.x EU notebook's own
# RUN_* pattern -- lets a local import load this module's helper functions/constants without a
# live fetch or a Spark session.
RUN_EU_MARKET_DATA = globals().get("RUN_EU_MARKET_DATA", True)

# Real, verified Yahoo Finance market suffixes for the 4 currently-admitted primary-listing
# MICs -- confirmed LIVE this session (not assumed): a real yfinance query for all 8 admitted
# tickers under these exact suffixes returned each company's correct longName with zero
# collisions (e.g. FCC.MC -> "Fomento de Construcciones y Contratas, S.A.", ALO.PA ->
# "Alstom SA"). Extend only after the same live verification for a newly-admitted MIC.
EU_MIC_YAHOO_SUFFIX = {
    "XMAD": ".MC",  # Madrid
    "XPAR": ".PA",  # Paris (Euronext)
    "XAMS": ".AS",  # Amsterdam (Euronext)
    "MTAA": ".MI",  # Milan (Borsa Italiana)
}

# COMMAND ----------

# MAGIC %md ## 1. Load admitted EU issuers + verify each Yahoo symbol resolves to the RIGHT company
# MAGIC
# MAGIC The real market-data-safety gate this notebook adds (see module docstring): a Yahoo
# MAGIC company-name match, not just `admission_status = 'admitted'`.

# COMMAND ----------


def resolve_eu_yahoo_symbol(ticker: str, mic: str) -> str | None:
    """`{ticker}{suffix}` for a real, verified MIC -- `None` for an unmapped MIC (never a
    guessed suffix)."""
    suffix = EU_MIC_YAHOO_SUFFIX.get(mic)
    return f"{ticker}{suffix}" if suffix else None


def verify_yahoo_company_match(yahoo_symbol: str, expected_name: str) -> tuple[bool, str | None, str]:
    """Real Yahoo Finance company-name lookup via `yfinance.Ticker(...).info` -- returns
    (is_safe_match, returned_name, detail). `is_safe_match` is True only for
    `classify_company_match() == "same"` -- "ambiguous" is treated identically to "different"
    (never silently accepted), per this project's own established, conservative collision-
    matching convention (`identity.py`'s own docstring).

    Deliberately uses the `yfinance` package rather than a raw `requests` call to Yahoo's
    `quoteSummary` endpoint -- confirmed live this session that a bare unauthenticated request
    to that endpoint returns `401 Unauthorized` (Yahoo requires a session crumb/cookie dance),
    which `yfinance`'s own `Ticker.info` already handles internally. `yf.Ticker(sym).info` was
    independently verified live for all 8 real admitted tickers before this function was
    written (see docs/phase5-6-european-dashboard-data-integration.md) -- this is the same
    call, not a new, unverified path.
    """
    try:
        import yfinance as yf

        info = yf.Ticker(yahoo_symbol).info
        returned_name = info.get("longName") or info.get("shortName")
        if not returned_name:
            return False, None, "no company name in response"
        verdict = classify_company_match(expected_name, returned_name)
        return verdict == "same", returned_name, f"classify_company_match={verdict!r}"
    except Exception as e:
        return False, None, f"yfinance lookup failed: {str(e)[:200]}"


# COMMAND ----------

# MAGIC %md ## 2. Run

# COMMAND ----------

if RUN_EU_MARKET_DATA:
    admitted = spark.sql(f"""
        SELECT ticker, mic, issuer_name
        FROM {CATALOG}.config.eu_admission_candidates
        WHERE admission_status = 'admitted'
        ORDER BY ticker
    """).collect()
    print(f"Loaded {len(admitted)} admitted EU issuer(s) from {CATALOG}.config.eu_admission_candidates")

    EU_ACTIVE_TICKERS: list[str] = []
    EU_YAHOO_SYMBOL: dict[str, str] = {}
    eu_market_data_failures: list[dict] = []

    for row in admitted:
        ticker, mic, issuer_name = row["ticker"], row["mic"], row["issuer_name"]
        yahoo_symbol = resolve_eu_yahoo_symbol(ticker, mic)
        if yahoo_symbol is None:
            print(f"  {ticker} ({mic}): SKIPPED — no verified Yahoo suffix mapping for this MIC")
            eu_market_data_failures.append({
                "ticker": ticker, "error_type": "unmapped_mic",
                "error_message": f"No verified Yahoo suffix for MIC {mic!r}",
                "step": "resolve_yahoo_symbol",
            })
            continue

        is_safe, returned_name, detail = verify_yahoo_company_match(yahoo_symbol, issuer_name or "")
        if not is_safe:
            print(f"  {ticker} ({yahoo_symbol}): REJECTED — {detail} "
                  f"(expected {issuer_name!r}, got {returned_name!r})")
            eu_market_data_failures.append({
                "ticker": ticker, "error_type": "company_name_mismatch",
                "error_message": (
                    f"Yahoo symbol {yahoo_symbol!r} company-name check failed ({detail}): "
                    f"expected {issuer_name!r}, got {returned_name!r} — excluded from "
                    f"market-data ingestion, never silently accepted"
                ),
                "step": "verify_yahoo_company_match",
            })
            continue

        print(f"  {ticker} ({yahoo_symbol}): VERIFIED — {returned_name!r} matches {issuer_name!r}")
        EU_ACTIVE_TICKERS.append(ticker)
        EU_YAHOO_SYMBOL[ticker] = yahoo_symbol

    print(f"\n{len(EU_ACTIVE_TICKERS)} of {len(admitted)} admitted issuer(s) passed the "
          f"market-data safety check: {EU_ACTIVE_TICKERS}")

    if eu_market_data_failures:
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
        _scraped_at = datetime.utcnow()
        _fail_records = [{**f, "scraped_at": _scraped_at} for f in eu_market_data_failures]
        spark.createDataFrame(_fail_records).write.mode("append").saveAsTable(_failures_tbl)
        print(f"  {len(_fail_records)} failure(s) logged to {_failures_tbl}")

# COMMAND ----------

# MAGIC %md ## 3. Delegate the real fetch/write to `12__fetch_market_data.py`
# MAGIC
# MAGIC Reuses its exact `market_prices_daily`/`stock_splits` schema, batching, and `(ticker,
# MAGIC date)`/`(ticker, split_date)` MERGE semantics — no duplicated fetch/write logic. Benchmark
# MAGIC tickers are deliberately empty here (`12`'s own `BENCHMARK_TICKERS = ["SPY"]` global stays
# MAGIC untouched at its own default unless a parent already overrode it — this run must not
# MAGIC re-price SPY under EU-specific settings).

# COMMAND ----------

if RUN_EU_MARKET_DATA and EU_ACTIVE_TICKERS:
    _path = Path("12__fetch_market_data.py").resolve()
    if not _path.exists():
        _path = (Path(__file__).resolve().parent / "12__fetch_market_data.py")
    _spec = importlib.util.spec_from_file_location("_eu_market_data_delegate", _path)
    _module = importlib.util.module_from_spec(_spec)
    _module.spark = spark
    _module.CATALOG = CATALOG
    _module.SCHEMA = SCHEMA
    _module.ACTIVE_TICKERS = EU_ACTIVE_TICKERS
    _module.YAHOO_SYMBOL = EU_YAHOO_SYMBOL
    _module.BENCHMARK_TICKERS = []
    # Deliberately NOT setting force_full_refresh here (2026-08-16 incident, see
    # docs/phase5-6-european-dashboard-data-integration.md §10). `12` treats
    # force_full_refresh as BOTH "fetch full per-ticker history" AND "overwrite the whole
    # table" -- combined with this notebook's narrow EU_ACTIVE_TICKERS, that emptied
    # market_prices_daily to just these tickers. It's also unnecessary: `12`'s own
    # incremental branch already detects a ticker with zero existing rows (`t not in maxd`)
    # and fetches its full history via the SAFE, ticker-scoped MERGE write path -- exactly
    # what a brand-new EU ticker needs, with no risk to the rest of the table. `12` also
    # now hard-aborts (rather than silently overwriting) if a narrower-than-full-universe
    # ACTIVE_TICKERS ever reaches its MODE=="full" write path, as defense in depth.
    _spec.loader.exec_module(_module)
elif RUN_EU_MARKET_DATA:
    print("No EU tickers passed the market-data safety check this run — nothing to fetch.")

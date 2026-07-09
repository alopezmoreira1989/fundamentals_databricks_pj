"""Pure parsing helpers for non-US ticker-universe sources (multi-market onboarding).

`00__config/02__tickers_master.py` builds `main.config.tickers` from several sources
(S&P 500, Russell 3000, favorites.json, and now the S&P/TSX Composite via XIC). Its own
network-facing scraper functions (`fetch_sp500()`, `fetch_russell3000()`) live inline in that
notebook and are untestable — the file is a Databricks notebook-source `.py` with top-level
side-effecting code (`spark.sql(...)`, `%run`, and the fetch calls themselves execute at
import time), so it can never be safely `import`ed by a test.

This module holds the pure, network-free parsing logic for new non-US sources so they *can*
be unit-tested, following the same "notebook imports a pure module" split already established
by `fundamentals_pipeline/identity.py`. `02__tickers_master.py` does the actual HTTP fetch and
passes the raw response text in here.

Import-safe: depends only on `pandas`, no Spark/`dbutils`/network I/O.
"""

from __future__ import annotations

import io

import pandas as pd

# The 11 canonical GICS sectors — the single normalization target for every ticker source
# (US or otherwise). #218: this taxonomy never grows as new markets are added; a source's own
# labels are remapped into these 11, never grafted in as new distinct values.
CANONICAL_SECTORS = {
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

# Known IWV / BlackRock (and other GICS-derived) labels -> canonical GICS sector. Seeded from
# observing fetch_russell3000()'s (IWV, also an iShares/BlackRock fund) unmapped-label reports
# — the same vendor family as XIC (parse_tsx_composite_csv below), so this table is shared
# across every BlackRock-sourced ticker universe rather than duplicated per market (#218's
# rule: extend one maintained table, never a market-specific one-off). Canonical labels
# themselves are handled by normalize_sector() as an identity match and need not be repeated.
SECTOR_NORMALIZE: dict[str, str] = {
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


def normalize_sector(raw: object) -> str | None:
    """Map a raw source sector label to one of the canonical 11 GICS sectors.

    Returns the canonical label, or ``None`` when the input is missing/blank or unknown.
    Unknown labels are deliberately dropped to NULL (the app renders them as "Unknown") rather
    than guessed — callers should surface an unmapped-label report so SECTOR_NORMALIZE can be
    extended (see fetch_russell3000()/parse_tsx_composite_csv's warnings).
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    s = str(raw).strip()
    if not s or s in ("-", "—"):
        return None
    if s in CANONICAL_SECTORS:
        return s
    return SECTOR_NORMALIZE.get(s)

# Index-mechanics rows that are not real equity holdings — the same kind of noise
# `fetch_russell3000()` already tolerates (cash sleeves, futures overlay, collateral).
NON_EQUITY_ASSET_CLASSES = {"Cash", "Cash Collateral and Margins", "Futures"}

_HEADER_PREFIX = "Ticker,Name,Sector"


def parse_tsx_composite_csv(raw_text: str) -> pd.DataFrame:
    """Parse an XIC (iShares Core S&P/TSX Capped Composite Index ETF) holdings CSV export
    into `[ticker, company, sector]` — the ticker-universe proxy for the S&P/TSX Composite.

    The BlackRock Canada holdings download
    (`blackrock.com/ca/investors/.../<fund-id>.ajax?fileType=csv&fileName=<TICKER>_holdings
    &dataType=fund`) is a **plain CSV**, not the varnish-api `fundDownload` SpreadsheetML
    `fetch_russell3000()` parses — that endpoint 400s (`BAD_REQUEST_INVALID_PARAM_VALUES`)
    for this Canadian fund regardless of `targetSite`/`locale`, confirmed 2026-07. The CSV has
    two banner rows before the real header (`Fund Holdings as of,...`, a blank/NBSP row), then
    `Ticker,Name,Sector,Asset Class,Market Value,Weight (%),Notional Value,Shares,Price,
    Location,Exchange,Currency,FX Rate,Market Currency`, one row per holding, then a trailing
    NBSP footer row.

    Filters out non-equity asset classes (cash, cash collateral, futures overlay — see
    `NON_EQUITY_ASSET_CLASSES`). `sector` is BlackRock's raw label, remapped to one of the 11
    canonical GICS labels via `normalize_sector()` (#218) — the same shared table
    `fetch_russell3000()` uses, since XIC and IWV are both iShares/BlackRock funds and have
    been observed to share label variants (e.g. "Telecommunication Services"). A label with no
    mapping is dropped to `None` and printed as a warning so it can be investigated, mirroring
    `fetch_russell3000()`'s own unmapped-sector report.

    Raises `ValueError` if the expected header row can't be found (structural parse failure —
    e.g. BlackRock changes the export format). Does NOT validate row count — that's a
    live-response sanity check the caller (`fetch_tsx_composite()`) applies, so this function
    stays testable against small fixtures.
    """
    lines = raw_text.splitlines()
    header_idx = next((i for i, line in enumerate(lines) if line.startswith(_HEADER_PREFIX)), None)
    if header_idx is None:
        raise ValueError(
            f"XIC holdings CSV: could not find a header row starting with {_HEADER_PREFIX!r}"
        )

    data_lines = [line for line in lines[header_idx:] if line.strip() and line.strip() != "\xa0"]
    df = pd.read_csv(io.StringIO("\n".join(data_lines)))

    required_cols = {"Ticker", "Name", "Sector", "Asset Class"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"XIC holdings CSV: missing expected column(s) {sorted(missing)}")

    df = df[~df["Asset Class"].isin(NON_EQUITY_ASSET_CLASSES)].copy()

    sector = df["Sector"].map(normalize_sector)
    unmapped = sorted(set(df.loc[sector.isna(), "Sector"].dropna().astype(str)))
    if unmapped:
        print(f"  ⚠ {len(unmapped)} unmapped XIC sector label(s) — treated as NULL, "
              f"extend SECTOR_NORMALIZE: {unmapped}")

    out = pd.DataFrame({
        "ticker":  df["Ticker"].astype(str).str.strip(),
        "company": df["Name"].astype(str).str.strip(),
        "sector":  sector,
    })
    return out.reset_index(drop=True)

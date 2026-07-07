"""Ticker identity guard — prevents a bare ticker symbol from silently colliding across markets.

`main.config.tickers` and SEC's `company_tickers.json` both key a security by its bare
ticker string. That is safe only while every row comes from a single market (today: US SEC
filers). Once a non-US source is merged into the same namespace (Canadian TSX/MJDS 40-F
filers, per the multi-market roadmap), a bare symbol can legitimately belong to two unrelated
companies — e.g. Magna International trades as `MG` on the TSX, Mistras Group as `MG` on the
NYSE. Without a guard, a naive `drop_duplicates("ticker")` or CIK lookup would silently
overwrite one company's row with the other's, corrupting every ticker-keyed join downstream
(no error, no log, just wrong numbers).

This module is the single source of truth for that check, called from both
`00__config/02__tickers_master.py` (before the `main.config.tickers` Delta write) and
`10__ingestion/11__fetch_sec_xbrl.py` (before CIK resolution). Pure Python / pandas, no Spark
or `dbutils` dependency, so it's unit-testable like `schemas.py` / `periods.py`.

Scope note: this guards identity at the `config.tickers` / ingestion layer only.
`financials_raw` / `financials` / `market_prices_daily` / `market_cap_asof` and both
frontends remain bare-`ticker`-keyed — re-keying them is real migration work that belongs to
the actual Canadian-onboarding effort, not to this preventive guard.
"""

from __future__ import annotations

import pandas as pd

DEFAULT_MARKET = "US"


class CrossMarketCollisionError(Exception):
    """Raised when the same bare ticker symbol is claimed by rows in two different markets."""


def check_no_cross_market_collision(
    df: pd.DataFrame,
    *,
    ticker_col: str = "ticker",
    market_col: str = "market",
    company_col: str = "company",
) -> None:
    """Raise CrossMarketCollisionError if any ticker in `df` appears under >1 distinct market.

    Rows sharing a ticker AND a market are NOT a collision (same security, e.g. duplicate rows
    from overlapping index sources) — only a ticker split across markets is. A missing/NULL
    market value is treated as ``DEFAULT_MARKET`` so a frame with rows that predate the
    `market` column never false-positives against rows that already carry it.
    """
    if df.empty:
        return

    working = df[[ticker_col, market_col, company_col]].copy()
    working[market_col] = working[market_col].fillna(DEFAULT_MARKET)

    n_markets = working.groupby(ticker_col)[market_col].nunique()
    colliding_tickers = n_markets[n_markets > 1].index
    if len(colliding_tickers) == 0:
        return

    lines = []
    for ticker in colliding_tickers:
        rows = working[working[ticker_col] == ticker].drop_duplicates([market_col])
        detail = "; ".join(f"{r[market_col]}: {r[company_col]!r}" for _, r in rows.iterrows())
        lines.append(f"  {ticker}: {detail}")

    raise CrossMarketCollisionError(
        "Cross-market ticker collision detected — the same bare symbol is claimed by more "
        "than one market (e.g. Magna Intl 'MG' on the TSX vs Mistras Group 'MG' on the NYSE). "
        "Resolve manually (e.g. a favorites.json alias or a ticker rename) before writing:\n"
        + "\n".join(lines)
    )

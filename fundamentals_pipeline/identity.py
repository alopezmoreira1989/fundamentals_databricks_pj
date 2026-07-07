"""Ticker identity guard — prevents a bare ticker symbol from silently colliding across markets.

`main.config.tickers` and SEC's `company_tickers.json` both key a security by its bare
ticker string. That is safe only while every row comes from a single market. Now that a
non-US source is merged in (S&P/TSX Composite via XIC, per the multi-market roadmap's
Phase 1 — Canadian MJDS/40-F filers), a bare symbol can legitimately belong to two unrelated
companies — e.g. Magna International trades as `MG` on the TSX, Mistras Group as `MG` on the
NYSE (confirmed still live in SEC's own ticker map as of 2026-07: SEC's `company_tickers.json`
maps bare `MG` to Mistras Group's CIK, a stale entry from before Mistras went private in 2023).
Without a guard, a naive `drop_duplicates("ticker")` or CIK lookup would silently overwrite
one company's row with the other's, corrupting every ticker-keyed join downstream (no error,
no log, just wrong numbers).

It can *also* legitimately be the SAME company listed on both markets (Shopify: `SHOP` on both
NYSE and TSX; Brookfield Asset Management: `BAM` on both). This module tells the two apart via
`classify_company_match()` — normalized-name token comparison, conservative by design: it only
ever returns "same" when confident, and treats anything else (including a merely plausible
partial match) as unsafe to auto-resolve.

This module is the single source of truth for the check, called from both
`00__config/02__tickers_master.py` (before the `main.config.tickers` Delta write, and as the
admission gate for new Canadian candidates) and `10__ingestion/11__fetch_sec_xbrl.py` (before
CIK resolution). Pure Python / pandas, no Spark or `dbutils` dependency, so it's unit-testable
like `schemas.py` / `periods.py`.

Scope note: this guards identity at the `config.tickers` / ingestion layer only.
`financials_raw` / `financials` / `market_prices_daily` / `market_cap_asof` and both
frontends remain bare-`ticker`-keyed — re-keying them is real migration work that belongs to
the actual Canadian-onboarding effort, not to this preventive guard.
"""

from __future__ import annotations

import re
from typing import Literal

import pandas as pd

DEFAULT_MARKET = "US"

MatchVerdict = Literal["same", "different", "ambiguous"]


class CrossMarketCollisionError(Exception):
    """Raised when the same bare ticker symbol is claimed by rows in two different markets
    that do not confidently resolve to the same company."""


# ── Company-name normalization + matching ──────────────────────────────────────────────────
# Company names come from genuinely different vendors depending on source (Wikipedia
# title-case for S&P 500, BlackRock all-caps truncated names for Russell 3000/XIC, SEC's own
# EDGAR corporate title for CIK resolution, freeform for favorites.json overrides), so
# comparing them for "is this the same legal entity" needs normalization, not exact string
# equality. Validated 2026-07 against every real S&P/TSX Composite (XIC) ticker that also
# appears in SEC's company_tickers.json — the actual Phase-1 Canadian-onboarding admission
# gate in 00__config/02__tickers_master.py.
_SEC_DISAMBIGUATION_TAG = re.compile(r"/[A-Z0-9]+/")  # SEC EDGAR tags: /CAN/, /ON/, /NEW/
_PUNCTUATION = re.compile(r"[.,'&]")
_CLASS_QUALIFIER = re.compile(r"\bCLASS\s*[A-Z]\b")
_SUBORDINATE_VOTING = re.compile(r"\bSUBORDINATE\s+VOT\w*\b")
_NONVOTING = re.compile(r"\bNON[\s-]?VOT\w*\b")
_VOTING = re.compile(r"\bVOTING\b")
_SUFFIX_WORDS = re.compile(r"\b(INC|CORP|CORPORATION|LTD|LIMITED|LLC|PLC|CO|COMPANY|SA|NV|AG|NEW)\b")


def _normalize_company_name(name: str) -> str:
    """Uppercase, strip punctuation/corporate suffixes/class qualifiers/SEC disambiguation
    tags, collapse whitespace. Tuned specifically for telling "same company, different
    market" apart from "different company, same ticker" — not a general-purpose name matcher.
    """
    s = (name or "").upper()
    s = _SEC_DISAMBIGUATION_TAG.sub("", s)
    s = _PUNCTUATION.sub("", s)
    s = _CLASS_QUALIFIER.sub("", s)
    s = _SUBORDINATE_VOTING.sub("", s)
    s = _NONVOTING.sub("", s)
    s = _VOTING.sub("", s)
    s = _SUFFIX_WORDS.sub("", s)
    return re.sub(r"\s+", " ", s).strip()


def classify_company_match(name_a: str, name_b: str) -> MatchVerdict:
    """Classify whether two company names plausibly refer to the SAME legal entity.

    Three-way and deliberately conservative — never silently guesses:
      - ``"same"``: normalized names are equal, or the shorter one's significant tokens are
        a full subset of the longer's (handles BlackRock's truncated names, e.g. "GFL
        ENVIRONMENTAL SUBORDINATE VOTI" vs SEC's "GFL Environmental Inc.").
      - ``"different"``: the two names share NO significant (length > 1) normalized token —
        e.g. "Aecon Group Inc" vs "Alexandria Real Estate Equities, Inc.".
      - ``"ambiguous"``: partial token overlap — two distinct companies that happen to share
        one generic word (e.g. "Boyd Group Services Inc" vs "Boyd Gaming Corp"), or a
        plural/truncation near-miss (e.g. XIC's "Restaurants Brands International I" vs SEC's
        "Restaurant Brands International Inc."). Callers must treat this the same as
        ``"different"`` (never merge), but it is reported distinctly so it can be logged for
        manual review rather than silently excluded as a confirmed collision.

    Validated 2026-07 against every real S&P/TSX Composite (XIC) ticker cross-referenced with
    SEC's `company_tickers.json` — see `00__config/02__tickers_master.py`'s TSX admission step.
    """
    na, nb = _normalize_company_name(name_a), _normalize_company_name(name_b)
    if not na or not nb:
        return "ambiguous"
    if na == nb:
        return "same"

    tokens_a = {t for t in na.split() if len(t) > 1}
    tokens_b = {t for t in nb.split() if len(t) > 1}
    if not tokens_a or not tokens_b:
        return "ambiguous"

    shorter, longer = (tokens_a, tokens_b) if len(tokens_a) <= len(tokens_b) else (tokens_b, tokens_a)
    shared = shorter & longer

    if shared == shorter:
        return "same"
    if not shared:
        return "different"
    return "ambiguous"


def _row_completeness(row: pd.Series) -> int:
    return int(row.notna().sum())


def check_no_cross_market_collision(
    df: pd.DataFrame,
    *,
    ticker_col: str = "ticker",
    market_col: str = "market",
    company_col: str = "company",
) -> pd.DataFrame:
    """Resolve cross-market ticker rows and return a collision-free frame.

    For each ticker present under more than one distinct market:
      - if every pair of rows classifies as the **same** company (`classify_company_match`),
        collapse the group to a single row — the more complete one (most non-NULL fields
        wins; ties prefer `DEFAULT_MARKET`, i.e. a pre-existing entry over a freshly-added
        one, since a first-pass Canadian candidate typically arrives with far fewer populated
        fields than an already-probed US row);
      - if any pair classifies as **different** or **ambiguous**, raise
        `CrossMarketCollisionError` — never silently guess.

    Rows sharing a ticker AND a market are NOT a collision (duplicate rows from overlapping
    sources, e.g. S&P 500 + Russell 3000 both carrying AAPL) — the caller's own dedup handles
    those; this function only adjudicates true cross-market ticker reuse. A missing/NULL
    market value is treated as `DEFAULT_MARKET` so a frame with rows that predate the `market`
    column never false-positives against rows that already carry it.
    """
    if df.empty:
        return df

    working = df.copy()
    working[market_col] = working[market_col].fillna(DEFAULT_MARKET)

    n_markets = working.groupby(ticker_col)[market_col].nunique()
    colliding_tickers = n_markets[n_markets > 1].index
    if len(colliding_tickers) == 0:
        return working

    drop_indices: list = []
    for ticker in colliding_tickers:
        rows = working[working[ticker_col] == ticker]
        names = rows[company_col].tolist()
        verdicts = {
            classify_company_match(names[i], names[j])
            for i in range(len(names))
            for j in range(i + 1, len(names))
        }

        if verdicts - {"same"}:
            detail = "; ".join(
                f"{r[market_col]}: {r[company_col]!r}"
                for _, r in rows.drop_duplicates([market_col]).iterrows()
            )
            reason = "ambiguous match" if verdicts == {"ambiguous"} else "different companies"
            raise CrossMarketCollisionError(
                f"Cross-market ticker collision on {ticker!r} ({reason}) — the same bare "
                "symbol is claimed by rows that do not confidently resolve to one company "
                "(e.g. Magna Intl 'MG' on the TSX vs Mistras Group 'MG' on the NYSE). Resolve "
                f"manually (e.g. a favorites.json override) before writing:\n  {ticker}: {detail}"
            )

        # Every pair confidently the same company — keep the most complete row.
        completeness = rows.apply(_row_completeness, axis=1)
        best_idx = completeness.idxmax()
        tied = completeness[completeness == completeness[best_idx]].index
        if len(tied) > 1:
            preferred = rows.loc[tied][rows.loc[tied, market_col] == DEFAULT_MARKET]
            if not preferred.empty:
                best_idx = preferred.index[0]
        drop_indices.extend(i for i in rows.index if i != best_idx)

    return working.drop(index=drop_indices).reset_index(drop=True)

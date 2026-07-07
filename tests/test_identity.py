"""Tests for the cross-market ticker-identity guard (fundamentals_pipeline/identity.py)."""

from __future__ import annotations

import pandas as pd
import pytest

from fundamentals_pipeline.identity import (
    CrossMarketCollisionError,
    check_no_cross_market_collision,
    classify_company_match,
)


def test_no_collision_passes():
    df = pd.DataFrame({
        "ticker":  ["AAPL", "MSFT", "TSM"],
        "market":  ["US", "US", "US"],
        "company": ["Apple", "Microsoft", "TSMC"],
    })
    result = check_no_cross_market_collision(df)
    assert list(result["ticker"]) == ["AAPL", "MSFT", "TSM"]


def test_same_ticker_same_market_is_not_a_collision():
    # Duplicate rows for the same (ticker, market) — e.g. overlapping SP500 + R3000 sources —
    # are a normal dedup case, not an identity collision. This function doesn't dedup them
    # (that's the caller's drop_duplicates job) — it just must not raise.
    df = pd.DataFrame({
        "ticker":  ["AAPL", "AAPL"],
        "market":  ["US", "US"],
        "company": ["Apple", "Apple Inc."],
    })
    result = check_no_cross_market_collision(df)
    assert len(result) == 2


def test_cross_market_collision_raises():
    # Magna International (TSX) vs Mistras Group (NYSE) — same bare symbol, different markets.
    # Confirmed live 2026-07: SEC's own company_tickers.json still maps bare 'MG' to Mistras
    # Group's CIK (a stale entry from before it went private in 2023).
    df = pd.DataFrame({
        "ticker":  ["MG", "MG"],
        "market":  ["CA", "US"],
        "company": ["Magna International", "Mistras Group"],
    })
    with pytest.raises(CrossMarketCollisionError, match="MG"):
        check_no_cross_market_collision(df)


def test_missing_market_column_defaults_to_us_no_false_positive():
    # A frame with NULL market values (rows predating the `market` column) should not
    # false-positive against rows that already carry the default market explicitly.
    df = pd.DataFrame({
        "ticker":  ["AAPL", "AAPL"],
        "market":  [None, "US"],
        "company": ["Apple", "Apple"],
    })
    result = check_no_cross_market_collision(df)
    assert len(result) == 2
    assert (result["market"] == "US").all()


def test_empty_frame_passes():
    df = pd.DataFrame(columns=["ticker", "market", "company"])
    result = check_no_cross_market_collision(df)
    assert result.empty


def test_dual_listed_same_company_merges_to_one_row():
    # Brookfield Asset Management trades as BAM on both the NYSE and TSX — confirmed real
    # S&P/TSX Composite (XIC) constituent, 2026-07. The US row (more complete — has sector)
    # should win over the freshly-scraped Canadian candidate.
    df = pd.DataFrame({
        "ticker":  ["BAM", "BAM"],
        "market":  ["US", "CA"],
        "company": ["Brookfield Asset Management Ltd.", "BROOKFIELD ASSET MANAGEMENT VOTING"],
        "sector":  ["Financials", None],
    })
    result = check_no_cross_market_collision(df)
    assert len(result) == 1
    assert result.iloc[0]["ticker"] == "BAM"
    assert result.iloc[0]["market"] == "US"


def test_partial_name_overlap_is_ambiguous_and_raises():
    # Boyd Gaming Corp (US) vs Boyd Group Services Inc (TSX) — real XIC/SEC overlap, 2026-07.
    # Two genuinely different companies that happen to share one word ("Boyd") — not enough
    # signal to safely merge, and not confidently zero-overlap either. Must still raise rather
    # than guess.
    df = pd.DataFrame({
        "ticker":  ["BYD", "BYD"],
        "market":  ["US", "CA"],
        "company": ["Boyd Gaming Corp", "Boyd Group Services Inc"],
    })
    with pytest.raises(CrossMarketCollisionError, match="BYD"):
        check_no_cross_market_collision(df)


@pytest.mark.parametrize(
    "name_a, name_b, expected",
    [
        ("Brookfield Asset Management Ltd.", "BROOKFIELD ASSET MANAGEMENT VOTING", "same"),
        ("GFL Environmental Inc.", "GFL ENVIRONMENTAL SUBORDINATE VOTI", "same"),
        ("RB GLOBAL INC.", "RB GLOBAL INC", "same"),
        ("Magna International", "Mistras Group", "different"),
        ("Aecon Group Inc", "Alexandria Real Estate Equities, Inc.", "different"),
        ("Boyd Gaming Corp", "Boyd Group Services Inc", "ambiguous"),
        # Truncation/pluralization near-miss (real XIC vs SEC data, 2026-07): still ambiguous,
        # never silently treated as the same company.
        ("Restaurant Brands International Inc.", "RESTAURANTS BRANDS INTERNATIONAL I", "ambiguous"),
    ],
)
def test_classify_company_match(name_a, name_b, expected):
    assert classify_company_match(name_a, name_b) == expected

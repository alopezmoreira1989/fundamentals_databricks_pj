"""Tests for the cross-market ticker-identity guard (fundamentals_pipeline/identity.py)."""

from __future__ import annotations

import pandas as pd
import pytest

from fundamentals_pipeline.identity import (
    CrossMarketCollisionError,
    check_no_cross_market_collision,
)


def test_no_collision_passes():
    df = pd.DataFrame({
        "ticker":  ["AAPL", "MSFT", "TSM"],
        "market":  ["US", "US", "US"],
        "company": ["Apple", "Microsoft", "TSMC"],
    })
    check_no_cross_market_collision(df)  # must not raise


def test_same_ticker_same_market_is_not_a_collision():
    # Duplicate rows for the same (ticker, market) — e.g. overlapping SP500 + R3000 sources —
    # are a normal dedup case, not an identity collision.
    df = pd.DataFrame({
        "ticker":  ["AAPL", "AAPL"],
        "market":  ["US", "US"],
        "company": ["Apple", "Apple Inc."],
    })
    check_no_cross_market_collision(df)  # must not raise


def test_cross_market_collision_raises():
    # Magna International (TSX) vs Mistras Group (NYSE) — same bare symbol, different markets.
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
    check_no_cross_market_collision(df)  # must not raise


def test_empty_frame_passes():
    df = pd.DataFrame(columns=["ticker", "market", "company"])
    check_no_cross_market_collision(df)  # must not raise

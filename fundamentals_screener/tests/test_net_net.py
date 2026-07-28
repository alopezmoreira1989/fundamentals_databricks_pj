"""Tests for the Net-Net Finder read model (CompanyListingRepository.net_net_screen).

Self-contained: an in-memory DuckDB connection with hand-written dashboard_metrics /
dashboard_prices tables, injected into CompanyListingRepository(connection=...), plus a
monkeypatched repositories.company_listing.load_meta (not data_source.get_meta — same import-
binding caveat as test_benchmark.py). No Django settings, no fixture files.
"""

from __future__ import annotations

import duckdb
import pytest
from fundamentals_screener.repositories import company_listing as company_listing_module
from fundamentals_screener.repositories.company_listing import (
    CompanyListingRepository,
    _altman_zone,
)

_META = {
    "tickers": [
        {"ticker": "GOOD1", "company": "Good One Corp", "sector": "Industrials",
         "industry": "Machinery", "country": "United States", "market": "US"},
        {"ticker": "NOMATCH1", "company": "No Match One Corp", "sector": "Industrials",
         "industry": "Machinery", "country": "United States", "market": "US"},
        {"ticker": "MODTICK", "company": "Mod Tick Corp", "sector": "Industrials",
         "industry": "Machinery", "country": "United States", "market": "US"},
        {"ticker": "JOINBUG1", "company": "Join Bug One Corp", "sector": "Industrials",
         "industry": "Machinery", "country": "United States", "market": "US"},
    ]
}

# ticker, metric, fiscal_year, value, period_type
_METRIC_ROWS = [
    # GOOD1: everything live/current — single latest-value rows, no anchoring needed anymore.
    ("GOOD1", "NCAV Ratio (Live)", 2024, 0.5, "FY"),
    ("GOOD1", "NCAV / Share (Live)", 2024, 12.0, "FY"),
    ("GOOD1", "Piotroski F-Score", 2024, 6.0, "FY"),
    ("GOOD1", "Altman Z-Score", 2024, 2.5, "FY"),
    ("GOOD1", "Market Cap (Live)", 2024, 1_000_000.0, "FY"),

    # NOMATCH1: has NCAV / Share (Live) but never a non-null NCAV Ratio (Live) — must never
    # appear (the eligibility gate).
    ("NOMATCH1", "NCAV / Share (Live)", 2024, 7.0, "FY"),

    # MODTICK: both "relaxed" and "moderate" ratios non-null, with DIFFERENT NCAV/Share (Live)
    # values per level — proves `level` selects the right gate/value.
    ("MODTICK", "NCAV Ratio (Live)", 2024, 0.5, "FY"),
    ("MODTICK", "NCAV / Share (Live)", 2024, 12.0, "FY"),
    ("MODTICK", "NCAV (Moderate) Ratio (Live)", 2024, 0.6, "FY"),
    ("MODTICK", "NCAV (Moderate) / Share (Live)", 2024, 8.0, "FY"),
    ("MODTICK", "Market Cap (Live)", 2024, 2_000_000.0, "FY"),

    # JOINBUG1: regression test for a real bug (2026-07) — Market Cap (Live) has NO meaningful
    # fiscal_year relationship to the other per-metric rows (it's a single current snapshot, not
    # tied to any particular FY), while NCAV Ratio (Live)/F-Score/Z-Score are deliberately
    # stamped with DIFFERENT fiscal years from each other and from Market Cap (Live)'s own. An
    # earlier version of this query joined Market Cap through a shared-fiscal-year CTE and
    # silently returned NULL whenever the years didn't match — confirmed against real published
    # data as affecting 271 tickers. This fixture proves the fix (Market Cap fetched via a
    # separate, ticker-only query, independent of any fiscal_year).
    ("JOINBUG1", "NCAV Ratio (Live)", 2019, 0.3, "FY"),
    ("JOINBUG1", "NCAV / Share (Live)", 2019, 5.0, "FY"),
    ("JOINBUG1", "Piotroski F-Score", 2022, 4.0, "FY"),
    ("JOINBUG1", "Altman Z-Score", 2023, 1.9, "FY"),
    ("JOINBUG1", "Market Cap (Live)", 9999, 3_000_000.0, "FY"),
]

# ticker, date, close
_PRICE_ROWS = [
    ("GOOD1", "2026-07-01", 5.0),
    ("GOOD1", "2026-07-10", 6.0),  # latest — must be the one picked, not the earlier row
    ("MODTICK", "2026-07-10", 4.0),
    ("JOINBUG1", "2026-07-10", 2.0),
    # NOMATCH1 has no price row at all — must degrade to price=None, not crash.
]


@pytest.fixture
def con():
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE dashboard_metrics ("
        " ticker VARCHAR, metric VARCHAR, fiscal_year INTEGER, value DOUBLE, period_type VARCHAR)"
    )
    conn.executemany("INSERT INTO dashboard_metrics VALUES (?,?,?,?,?)", _METRIC_ROWS)
    conn.execute("CREATE TABLE dashboard_prices (ticker VARCHAR, date DATE, close DOUBLE)")
    conn.executemany("INSERT INTO dashboard_prices VALUES (?,?,?)", _PRICE_ROWS)
    yield conn
    conn.close()


@pytest.fixture
def repo(con, monkeypatch):
    monkeypatch.setattr(company_listing_module, "load_meta", lambda: _META)
    return CompanyListingRepository(connection=con)


def test_ticker_with_no_ratio_never_appears(repo):
    rows = {r.ticker: r for r in repo.net_net_screen(level="relaxed")}
    assert "NOMATCH1" not in rows


def test_level_selects_the_matching_ratio_and_value(repo):
    relaxed = {r.ticker: r for r in repo.net_net_screen(level="relaxed")}
    moderate = {r.ticker: r for r in repo.net_net_screen(level="moderate")}
    assert relaxed["MODTICK"].ncav_per_share_relaxed == 12.0
    assert moderate["MODTICK"].ncav_per_share_moderate == 8.0


def test_unrecognized_level_falls_back_to_relaxed(repo):
    fallback = {r.ticker: r for r in repo.net_net_screen(level="not-a-real-level")}
    relaxed = {r.ticker: r for r in repo.net_net_screen(level="relaxed")}
    assert set(fallback) == set(relaxed)


def test_price_picks_the_latest_date(repo):
    rows = {r.ticker: r for r in repo.net_net_screen(level="relaxed")}
    assert rows["GOOD1"].price == 6.0


def test_missing_price_degrades_to_none_not_a_crash(repo):
    rows = repo.net_net_screen(level="relaxed")
    assert isinstance(rows, tuple)


def test_market_cap_live_fetched_independently_of_fiscal_year(repo):
    """Regression test (2026-07): Market Cap (Live) must not go NULL just because a ticker's
    other Net-Net metrics are stamped with a different fiscal_year than its own Market Cap
    (Live) row — confirmed as a real bug affecting 271 tickers before this fix."""
    rows = {r.ticker: r for r in repo.net_net_screen(level="relaxed")}
    assert rows["JOINBUG1"].market_cap == 3_000_000.0


@pytest.mark.parametrize(
    "z_score, expected",
    [
        (None, None),
        (3.5, "safe"),
        (3.0, "grey"),      # boundary: gate is strictly > 3.0, so exactly 3.0 is grey
        (2.0, "grey"),
        (1.8, "grey"),      # boundary: gate is strictly < 1.8, so exactly 1.8 is grey
        (1.5, "distress"),
    ],
)
def test_altman_zone_thresholds(z_score, expected):
    assert _altman_zone(z_score) == expected

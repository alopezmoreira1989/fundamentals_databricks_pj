"""Tests for CompanyRepository.net_net_snapshot — the Valuation page's Net-Net card read model
(issue #262). Self-contained: in-memory DuckDB (dashboard_metrics + dashboard_prices), injected
connection, monkeypatched repositories.companies.load_meta (not data_source.get_meta — same
import-binding caveat as test_benchmark.py/test_net_net.py).
"""

from __future__ import annotations

import duckdb
import pytest
from fundamentals_screener.repositories import companies as companies_module
from fundamentals_screener.repositories.companies import CompanyRepository

_META = {
    "tickers": [
        {"ticker": "GOOD1", "company": "Good One Corp", "sector": "Industrials",
         "industry": "Machinery", "country": "United States", "market": "US"},
        {"ticker": "NEG1", "company": "Negative One Corp", "sector": "Technology",
         "industry": "Software", "country": "United States", "market": "US"},
        {"ticker": "NOPRICE1", "company": "No Price One Corp", "sector": "Industrials",
         "industry": "Machinery", "country": "United States", "market": "US"},
        {"ticker": "MISMATCH1", "company": "Mismatch One Corp", "sector": "Industrials",
         "industry": "Machinery", "country": "United States", "market": "US"},
    ]
}

# ticker, metric, unit, fiscal_year, value, category, subcategory, sort_order, period_type, period_end
_METRIC_ROWS = [
    ("GOOD1", "NCAV / Share", "usd", 2024, 10.0, "Valuation", "Net-Net", 1.0, "FY", "2024-12-31"),
    ("GOOD1", "NCAV (Moderate) / Share", "usd", 2024, 8.0, "Valuation", "Net-Net", 1.0, "FY", "2024-12-31"),
    ("GOOD1", "NCAV (Strict) / Share", "usd", 2024, 6.0, "Valuation", "Net-Net", 1.0, "FY", "2024-12-31"),
    ("GOOD1", "Piotroski F-Score", "ratio", 2024, 7.0, "Quality", None, 1.0, "FY", "2024-12-31"),
    ("GOOD1", "Altman Z-Score", "ratio", 2024, 3.5, "Quality", None, 1.0, "FY", "2024-12-31"),
    ("GOOD1", "Market Cap (Live)", "usd", 2024, 5_000_000.0, None, None, 1.0, "FY", "2024-12-31"),

    ("NEG1", "NCAV / Share", "usd", 2024, -9.17, "Valuation", "Net-Net", 1.0, "FY", "2024-12-31"),
    ("NEG1", "Altman Z-Score", "ratio", 2024, 12.0, "Quality", None, 1.0, "FY", "2024-12-31"),

    ("NOPRICE1", "NCAV / Share", "usd", 2024, 4.0, "Valuation", "Net-Net", 1.0, "FY", "2024-12-31"),

    # MISMATCH1: mirrors the real BMI bug (issue: NCAV/Share vs. Ratio anchoring disagreement).
    # "NCAV / Share" has a NEWER value (FY2024=20.0) than "NCAV Ratio" (only present at
    # FY2023=0.5 — e.g. market_cap wasn't available for FY2024, so the ratio couldn't be
    # computed that year even though NCAV/Share itself could). net_net_snapshot must anchor to
    # FY2023 (15.0), matching net_net_screen's own rule, NOT the metric's raw latest FY (20.0).
    ("MISMATCH1", "NCAV / Share", "usd", 2023, 15.0, "Valuation", "Net-Net", 1.0, "FY", "2023-12-31"),
    ("MISMATCH1", "NCAV / Share", "usd", 2024, 20.0, "Valuation", "Net-Net", 1.0, "FY", "2024-12-31"),
    ("MISMATCH1", "NCAV Ratio", "ratio", 2023, 0.5, None, None, 1.0, "FY", "2023-12-31"),
]

# ticker, date, close
_PRICE_ROWS = [
    ("GOOD1", "2026-07-01", 4.0),
    ("GOOD1", "2026-07-10", 3.0),  # latest — must win over the earlier row
    ("NEG1", "2026-07-10", 200.0),
    # NOPRICE1 deliberately has no price row — snapshot.price must degrade to None, not crash.
]


@pytest.fixture
def con():
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE dashboard_metrics ("
        " ticker VARCHAR, metric VARCHAR, unit VARCHAR, fiscal_year INTEGER, value DOUBLE,"
        " category VARCHAR, subcategory VARCHAR, sort_order DOUBLE, period_type VARCHAR,"
        " period_end DATE)"
    )
    conn.executemany("INSERT INTO dashboard_metrics VALUES (?,?,?,?,?,?,?,?,?,?)", _METRIC_ROWS)
    conn.execute("CREATE TABLE dashboard_prices (ticker VARCHAR, date DATE, close DOUBLE)")
    conn.executemany("INSERT INTO dashboard_prices VALUES (?,?,?)", _PRICE_ROWS)
    yield conn
    conn.close()


@pytest.fixture
def repo(con, monkeypatch):
    monkeypatch.setattr(companies_module, "load_meta", lambda: _META)
    return CompanyRepository(connection=con)


def test_snapshot_has_all_fields_and_own_price(repo):
    s = repo.net_net_snapshot("GOOD1")
    assert s.ticker == "GOOD1"
    assert s.price == 3.0  # latest date wins, not the earlier row
    assert s.market_cap == 5_000_000.0
    assert s.ncav_per_share_relaxed == 10.0
    assert s.ncav_per_share_moderate == 8.0
    assert s.ncav_per_share_strict == 6.0
    assert s.f_score == 7.0
    assert s.z_score == 3.5
    assert s.z_score_zone == "safe"  # reuses the same _altman_zone thresholds as net_net_screen


def test_unknown_ticker_returns_none(repo):
    assert repo.net_net_snapshot("NOPE") is None


def test_negative_ncav_still_returns_a_snapshot(repo):
    # The Valuation page always shows a company's own numbers, unlike net_net_screen's
    # eligibility-gated listing — a negative NCAV/share is real data, not an error.
    s = repo.net_net_snapshot("NEG1")
    assert s.ncav_per_share_relaxed == -9.17
    assert s.price == 200.0
    assert s.z_score_zone == "safe"


def test_missing_price_degrades_to_none(repo):
    s = repo.net_net_snapshot("NOPRICE1")
    assert s.ncav_per_share_relaxed == 4.0
    assert s.price is None


def test_ncav_per_share_anchors_to_the_ratios_own_latest_fy_not_the_metrics_own(repo):
    """Real bug (2026-07): a ticker whose 'NCAV / Share' metric has a newer FY value than its
    'NCAV Ratio' (e.g. market_cap missing for the newer year) must show the RATIO-anchored
    value, matching CompanyListingRepository.net_net_screen's own rule — not the metric's raw
    latest-FY value, which could come from a different, unanchored year."""
    s = repo.net_net_snapshot("MISMATCH1")
    assert s.ncav_per_share_relaxed == 15.0  # FY2023 (the Ratio's own latest year), not 20.0


def test_ncav_per_share_falls_back_to_the_metrics_own_latest_fy_when_no_ratio_ever_exists(repo):
    """GOOD1/NEG1/NOPRICE1 have no 'NCAV Ratio' row at all in this fixture — the anchored
    lookup finds nothing, so net_net_snapshot must fall back to the metric's own latest-FY
    value (today's pre-fix behavior), preserving the "show a company's own numbers, even
    negative/absent ones" intent for tickers that are never ratio-eligible."""
    assert repo.net_net_snapshot("GOOD1").ncav_per_share_relaxed == 10.0
    assert repo.net_net_snapshot("NEG1").ncav_per_share_relaxed == -9.17

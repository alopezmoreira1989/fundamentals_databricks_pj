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
    ]
}

# ticker, metric, unit, fiscal_year, value, category, subcategory, sort_order, period_type, period_end
# NCAV/Share (Live) and Market Cap (Live) are single-current-row-per-ticker metrics
# (category/subcategory NULL, matching how 51__export_dashboard_data.py actually publishes
# them) — no anchoring/fallback logic needed anymore (see net_net_snapshot's own docstring):
# each is just read directly.
_METRIC_ROWS = [
    ("GOOD1", "NCAV / Share (Live)", "usd", 2024, 10.0, None, None, 1.0, "FY", "2024-12-31"),
    ("GOOD1", "NCAV (Moderate) / Share (Live)", "usd", 2024, 8.0, None, None, 1.0, "FY", "2024-12-31"),
    ("GOOD1", "NCAV (Strict) / Share (Live)", "usd", 2024, 6.0, None, None, 1.0, "FY", "2024-12-31"),
    ("GOOD1", "Piotroski F-Score", "ratio", 2024, 7.0, "Quality", None, 1.0, "FY", "2024-12-31"),
    ("GOOD1", "Altman Z-Score", "ratio", 2024, 3.5, "Quality", None, 1.0, "FY", "2024-12-31"),
    ("GOOD1", "Market Cap (Live)", "usd", 2024, 5_000_000.0, None, None, 1.0, "FY", "2024-12-31"),

    ("NEG1", "NCAV / Share (Live)", "usd", 2024, -9.17, None, None, 1.0, "FY", "2024-12-31"),
    ("NEG1", "Altman Z-Score", "ratio", 2024, 12.0, "Quality", None, 1.0, "FY", "2024-12-31"),

    ("NOPRICE1", "NCAV / Share (Live)", "usd", 2024, 4.0, None, None, 1.0, "FY", "2024-12-31"),
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


def test_missing_live_ncav_degrades_to_none_not_a_crash(repo):
    # NEG1/NOPRICE1 have no "NCAV (Moderate)/(Strict) / Share (Live)" rows at all in this
    # fixture — must degrade to None per level independently, never raise.
    assert repo.net_net_snapshot("NEG1").ncav_per_share_moderate is None
    assert repo.net_net_snapshot("NEG1").ncav_per_share_strict is None

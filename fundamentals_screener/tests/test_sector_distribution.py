"""Tests for the Sector Distribution panel's data source: `CompanyListingRepository.screen_table
()`'s `sector_distribution` field — the sector breakdown of the FULL filtered screener universe.

Self-contained: an in-memory DuckDB connection with a hand-written dashboard_metrics table,
injected into CompanyListingRepository(connection=...), plus a monkeypatched
repositories.company_listing.load_meta -- same pattern as test_currency_lens.py/
test_investor_presets.py. The point of these tests is to prove sector_distribution is computed
from the EXACT SAME scoped/where-filtered query as `rows`/`total` (descriptive filters AND
metric filters), never a second, independently-filtered query.
"""

from __future__ import annotations

import duckdb
import pytest
from fundamentals_screener.repositories import company_listing as company_listing_module
from fundamentals_screener.repositories.company_listing import CompanyListingRepository, MetricFilter

_META = {
    "tickers": [
        {"ticker": "AAA", "company": "Alpha Corp", "sector": "Information Technology",
         "industry": "Software", "country": "United States", "market": "US"},
        {"ticker": "BBB", "company": "Beta Inc", "sector": "Information Technology",
         "industry": "Semiconductors", "country": "United States", "market": "US"},
        {"ticker": "CCC", "company": "Gamma LLC", "sector": "Financials",
         "industry": "Banks", "country": "United States", "market": "US"},
        {"ticker": "DDD", "company": "Delta Ltd", "sector": "Financials",
         "industry": "Insurance", "country": "Canada", "market": "CA"},
        {"ticker": "EEE", "company": "Epsilon Co", "sector": "Industrials",
         "industry": "Machinery", "country": "United States", "market": "US"},
        # No sector on record at all -- must group as "Unknown", not vanish or crash.
        {"ticker": "FFF", "company": "No-Sector Co", "sector": None,
         "industry": None, "country": "United States", "market": "US"},
    ]
}

# ticker, metric, unit, fiscal_year, value, period_type, period_end
_METRIC_ROWS = [
    ("AAA", "P/E", "ratio", 2024, 20.0, "FY", "2024-12-31"),
    ("BBB", "P/E", "ratio", 2024, 30.0, "FY", "2024-12-31"),
    ("CCC", "P/E", "ratio", 2024, 8.0, "FY", "2024-12-31"),
    ("DDD", "P/E", "ratio", 2024, 12.0, "FY", "2024-12-31"),
    ("EEE", "P/E", "ratio", 2024, 15.0, "FY", "2024-12-31"),
    ("FFF", "P/E", "ratio", 2024, 5.0, "FY", "2024-12-31"),
]


@pytest.fixture
def con():
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE dashboard_metrics ("
        " ticker VARCHAR, metric VARCHAR, unit VARCHAR, fiscal_year INTEGER, value DOUBLE,"
        " period_type VARCHAR, period_end DATE)"
    )
    conn.executemany("INSERT INTO dashboard_metrics VALUES (?,?,?,?,?,?,?)", _METRIC_ROWS)
    yield conn
    conn.close()


@pytest.fixture
def repo(con, monkeypatch):
    monkeypatch.setattr(company_listing_module, "load_meta", lambda: _META)
    return CompanyListingRepository(connection=con)


def _by_sector(result):
    return {sc.sector: sc.count for sc in result.sector_distribution}


def test_full_universe_counts_and_descending_order(repo):
    result = repo.screen_table(page_size=10)
    assert _by_sector(result) == {
        "Information Technology": 2, "Financials": 2, "Industrials": 1, "Unknown": 1,
    }
    # count(*) DESC, IT/Financials tie at 2 -- both must lead over the 1-counts either way.
    counts = [sc.count for sc in result.sector_distribution]
    assert counts == sorted(counts, reverse=True)


def test_sector_filter_collapses_distribution_to_that_one_sector(repo):
    result = repo.screen_table(sector="Financials", page_size=10)
    assert _by_sector(result) == {"Financials": 2}
    assert result.total == 2


def test_country_filter_recalculates_distribution(repo):
    result = repo.screen_table(country="Canada", page_size=10)
    assert _by_sector(result) == {"Financials": 1}


def test_market_filter_recalculates_distribution(repo):
    result = repo.screen_table(market="US", page_size=10)
    assert _by_sector(result) == {
        "Information Technology": 2, "Financials": 1, "Industrials": 1, "Unknown": 1,
    }


def test_search_filter_recalculates_distribution(repo):
    result = repo.screen_table(search="Alpha", page_size=10)
    assert _by_sector(result) == {"Information Technology": 1}


def test_metric_filter_recalculates_distribution_proving_shared_where_clause(repo):
    # P/E <= 15 (inclusive) matches CCC (8.0, Financials), DDD (12.0, Financials),
    # EEE (15.0, Industrials), FFF (5.0, Unknown) -- excludes AAA (20.0)/BBB (30.0). Proves
    # sector_distribution shares the metric-filter WHERE clause with the table query, not a
    # separate implementation.
    result = repo.screen_table(
        filters=[MetricFilter(metric="P/E", max_value=15.0)], page_size=10,
    )
    assert _by_sector(result) == {"Financials": 2, "Industrials": 1, "Unknown": 1}
    assert result.total == 4


def test_null_sector_grouped_as_unknown_not_dropped(repo):
    result = repo.screen_table(search="No-Sector", page_size=10)
    assert _by_sector(result) == {"Unknown": 1}


def test_empty_scope_returns_empty_distribution_no_crash(repo):
    result = repo.screen_table(search="NoSuchTickerAtAll", page_size=10)
    assert result.sector_distribution == ()
    assert result.total == 0

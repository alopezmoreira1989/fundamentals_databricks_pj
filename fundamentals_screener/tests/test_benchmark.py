"""Tests for the Derived-metrics tab's benchmark switch (industry / sector / compare).

Self-contained: an in-memory DuckDB connection with a hand-written dashboard_metrics table,
injected into CompanyRepository(connection=...), plus a monkeypatched
repositories.companies.load_meta. No Django settings, no pytest-django, no dependency on the
gitignored Streamlit fixture parquet files — CompanyRepository never touches the ORM or
FUNDAMENTALS_DATA_PATH once a connection is injected and load_meta is monkeypatched.

Must monkeypatch ``fundamentals_screener.repositories.companies.load_meta``, not
``data_source.get_meta`` — companies.py imports it as ``from ..data_source import get_meta as
load_meta``, so patching the origin wouldn't touch the name already bound locally there.
"""

from __future__ import annotations

import duckdb
import pytest
from fundamentals_screener.dtos import MetricSeries, PeerBenchmark
from fundamentals_screener.repositories import companies as companies_module
from fundamentals_screener.repositories.companies import CompanyRepository
from fundamentals_screener.services import _merge_peer_medians

_META = {
    "tickers": [
        {"ticker": "AAA", "company": "Alpha Corp", "sector": "Manufacturing", "industry": "Widgets"},
        {"ticker": "BBB", "company": "Beta Inc", "sector": "Manufacturing", "industry": "Widgets"},
        {"ticker": "CCC", "company": "Gamma LLC", "sector": "Manufacturing", "industry": "Widgets"},
        {"ticker": "DDD", "company": "Delta Ltd", "sector": "Manufacturing", "industry": "Widgets"},
        {"ticker": "KKK", "company": "Kilo Corp", "sector": "Manufacturing", "industry": "Gadgets"},
        {"ticker": "EEE", "company": "Epsilon Co", "sector": "Materials", "industry": "Thin"},
        {"ticker": "FFF", "company": "Foxtrot Co", "sector": "Materials", "industry": "Thin"},
        {"ticker": "GGG", "company": "Golf Co", "sector": "Materials", "industry": "Other"},
        {"ticker": "HHH", "company": "Hotel Co", "sector": "Materials", "industry": "Other"},
        {"ticker": "III", "company": "India Solo", "sector": "Nobody", "industry": "Solo"},
    ]
}
# ticker, metric, unit, fiscal_year, value, category, subcategory, sort_order, period_type
_ROWS = [
    ("AAA", "ROE", "percent", 2024, 22.0, "Profitability", None, 1.0, "FY"),
    ("BBB", "ROE", "percent", 2024, 18.0, "Profitability", None, 1.0, "FY"),
    ("CCC", "ROE", "percent", 2024, 14.0, "Profitability", None, 1.0, "FY"),
    ("DDD", "ROE", "percent", 2024, 10.0, "Profitability", None, 1.0, "FY"),
    ("EEE", "ROE", "percent", 2024, 30.0, "Profitability", None, 1.0, "FY"),
    ("FFF", "ROE", "percent", 2024, 26.0, "Profitability", None, 1.0, "FY"),
    ("GGG", "ROE", "percent", 2024, 12.0, "Profitability", None, 1.0, "FY"),
    ("HHH", "ROE", "percent", 2024, 8.0, "Profitability", None, 1.0, "FY"),
]


@pytest.fixture
def con():
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE dashboard_metrics ("
        " ticker VARCHAR, metric VARCHAR, unit VARCHAR, fiscal_year INTEGER, value DOUBLE,"
        " category VARCHAR, subcategory VARCHAR, sort_order DOUBLE, period_type VARCHAR)"
    )
    conn.executemany("INSERT INTO dashboard_metrics VALUES (?,?,?,?,?,?,?,?,?)", _ROWS)
    yield conn
    conn.close()


@pytest.fixture
def repo(con, monkeypatch):
    monkeypatch.setattr(companies_module, "load_meta", lambda: _META)
    return CompanyRepository(connection=con)


# ── auto cascade (unchanged today-behavior) ─────────────────────────────────────────────
def test_auto_prefers_industry_when_enough_peers(repo):
    benchmarks, basis, count, peers = repo.industry_benchmark("AAA", "Widgets", "Manufacturing")
    assert basis == "industry" and count == 3
    assert {p.ticker for p in peers} == {"BBB", "CCC", "DDD"}
    roe = next(b for b in benchmarks if b.metric == "ROE")
    assert roe.peer_median == 14.0 and roe.peer_count == 3


def test_auto_falls_back_to_sector_when_industry_too_thin(repo):
    # EEE's industry "Thin" has only 1 other ticker (FFF) — below min_peers=3, so auto falls
    # back to sector "Materials" (FFF, GGG, HHH).
    benchmarks, basis, count, peers = repo.industry_benchmark("EEE", "Thin", "Materials")
    assert basis == "sector" and count == 3
    assert peers == ()  # sector never returns a chip roster
    roe = next(b for b in benchmarks if b.metric == "ROE")
    assert roe.peer_median == 12.0  # median(26, 12, 8)


# ── forced basis: no cascade, "no peers" state must not crash ───────────────────────────
def test_forced_industry_returns_thin_peer_set_without_fallback(repo):
    # Forced "industry" never falls back to sector, even when below min_peers.
    benchmarks, basis, count, peers = repo.industry_benchmark(
        "EEE", "Thin", "Materials", basis="industry",
    )
    assert basis == "industry" and count == 1
    assert [p.ticker for p in peers] == ["FFF"]


def test_forced_basis_with_zero_peers_degrades_gracefully(repo):
    assert repo.industry_benchmark("III", "Solo", "Nobody", basis="industry") == ((), None, 0, ())
    assert repo.industry_benchmark("III", "Solo", "Nobody", basis="sector") == ((), None, 0, ())


# ── compare-to-company ───────────────────────────────────────────────────────────────────
def test_compare_to_known_ticker(repo):
    benchmarks, compare = repo.compare_benchmark("AAA", "BBB")
    assert compare == companies_module.PeerCompany(ticker="BBB", name="Beta Inc")
    roe = next(b for b in benchmarks if b.metric == "ROE")
    assert roe.peer_median == 18.0 and roe.peer_count == 1


@pytest.mark.parametrize("candidate", ["ZZZ", "", "AAA"])  # unknown / blank / self
def test_compare_degrades_gracefully(repo, candidate):
    assert repo.compare_benchmark("AAA", candidate) == ((), None)


# ── peer counts (both pills, independent of active basis) ──────────────────────────────
def test_peer_counts_are_independent_of_active_basis(repo):
    # AAA: 3 other Widgets tickers (industry) vs 4 other Manufacturing tickers (sector, incl. KKK).
    assert repo.peer_counts("AAA", "Widgets", "Manufacturing") == (3, 4)


# ── datalist source ──────────────────────────────────────────────────────────────────────
def test_all_companies_sorted_by_ticker(repo):
    companies = repo.all_companies()
    assert [c.ticker for c in companies][:3] == ["AAA", "BBB", "CCC"]
    assert len(companies) == len(_META["tickers"])


# ── services._merge_peer_medians (pure function, no DB/connection needed) ──────────────
def test_merge_peer_medians_fills_matching_metric_only():
    series = (
        MetricSeries(ticker="AAA", metric="ROE", unit="percent", category="Profitability",
                     subcategory=None, sort_order=1.0, fiscal_years=(2024,), values=(22.0,)),
        MetricSeries(ticker="AAA", metric="Debt/Equity", unit="ratio", category="Leverage",
                     subcategory=None, sort_order=2.0, fiscal_years=(2024,), values=(0.5,)),
    )
    benchmarks = (PeerBenchmark(metric="ROE", peer_median=14.0, peer_count=3),)
    merged = _merge_peer_medians(series, benchmarks)
    assert merged[0].peer_median == 14.0 and merged[0].peer_count == 3
    assert merged[1].peer_median is None and merged[1].peer_count == 0  # untouched default

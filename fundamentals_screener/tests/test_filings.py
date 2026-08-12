"""CompanyRepository.get_filings — the company page's Filings tab (issue #318). Sourced from
the dashboard_filings artifact (written by the pipeline's 15__fetch_sec_filings.py), not a
live SEC call of our own. Self-contained: in-memory DuckDB, injected connection, same pattern
as test_net_net_snapshot.py.

fiscal_year/period_type are joined in from dashboard_data (matched on report_date ==
period_end for the same ticker) rather than derived from the date — the fixture below
includes a dashboard_data table so that join has something real to match against, plus one
filing (MSFT's) with no matching period to exercise the LEFT JOIN's NULL fallback.
"""

from __future__ import annotations

import duckdb
import pytest
from fundamentals_screener.dtos import FilingRow
from fundamentals_screener.repositories.companies import CompanyRepository

# ticker, form, filing_date, report_date, description, url
_FILING_ROWS = [
    ("AAPL", "10-K", "2026-03-17", "2025-12-31", "Annual report", "https://sec.gov/AAPL/10k-2026.htm"),
    ("AAPL", "10-Q", "2025-11-10", "2025-09-30", "Quarterly report", "https://sec.gov/AAPL/10q-2025q3.htm"),
    ("MSFT", "10-K", "2026-01-15", "2025-12-31", "Annual report", "https://sec.gov/MSFT/10k-2026.htm"),
]

# ticker, period_end, fiscal_year, period_type — only AAPL's periods are present, so MSFT's
# filing above has nothing to join against.
_PERIOD_ROWS = [
    ("AAPL", "2025-12-31", 2025, "FY"),
    ("AAPL", "2025-09-30", 2025, "Q3"),
]


@pytest.fixture
def con():
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE dashboard_filings ("
        " ticker VARCHAR, form VARCHAR, filing_date VARCHAR, report_date VARCHAR,"
        " description VARCHAR, url VARCHAR)"
    )
    conn.executemany("INSERT INTO dashboard_filings VALUES (?,?,?,?,?,?)", _FILING_ROWS)
    conn.execute(
        "CREATE TABLE dashboard_data ("
        " ticker VARCHAR, period_end VARCHAR, fiscal_year INTEGER, period_type VARCHAR)"
    )
    conn.executemany("INSERT INTO dashboard_data VALUES (?,?,?,?)", _PERIOD_ROWS)
    yield conn
    conn.close()


@pytest.fixture
def repo(con):
    return CompanyRepository(connection=con)


def test_get_filings_returns_only_this_tickers_rows_newest_first(repo):
    rows = repo.get_filings("AAPL")
    assert len(rows) == 2
    assert all(isinstance(r, FilingRow) for r in rows)
    assert [r.filing_date for r in rows] == ["2026-03-17", "2025-11-10"]  # newest first


def test_get_filings_row_shape(repo):
    row = repo.get_filings("AAPL")[0]
    assert row.form == "10-K"
    assert row.report_date == "2025-12-31"
    assert row.description == "Annual report"
    assert row.url == "https://sec.gov/AAPL/10k-2026.htm"


def test_get_filings_joins_fiscal_year_and_period_type(repo):
    rows = repo.get_filings("AAPL")
    by_form = {r.form: r for r in rows}
    assert by_form["10-K"].fiscal_year == 2025
    assert by_form["10-K"].period_type == "FY"
    assert by_form["10-Q"].fiscal_year == 2025
    assert by_form["10-Q"].period_type == "Q3"


def test_get_filings_no_matching_period_leaves_fiscal_year_none(repo):
    row = repo.get_filings("MSFT")[0]
    assert row.fiscal_year is None
    assert row.period_type is None


def test_get_filings_unknown_ticker_returns_empty(repo):
    assert repo.get_filings("NOPE") == ()


def test_get_filings_missing_view_degrades_to_empty():
    """No ``dashboard_filings`` view registered at all (artifact not yet published by a
    pipeline run — the 2026-07-30 production incident) → ``()``, never raise."""
    empty_con = duckdb.connect(":memory:")
    try:
        repo = CompanyRepository(connection=empty_con)
        assert repo.get_filings("AAPL") == ()
    finally:
        empty_con.close()

"""CompanyRepository.get_filings — the company page's Filings tab (issue #318). Sourced from
the dashboard_filings artifact (written by the pipeline's 15__fetch_sec_filings.py), not a
live SEC call of our own. Self-contained: in-memory DuckDB, injected connection, same pattern
as test_net_net_snapshot.py.
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


@pytest.fixture
def con():
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE dashboard_filings ("
        " ticker VARCHAR, form VARCHAR, filing_date VARCHAR, report_date VARCHAR,"
        " description VARCHAR, url VARCHAR)"
    )
    conn.executemany("INSERT INTO dashboard_filings VALUES (?,?,?,?,?,?)", _FILING_ROWS)
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


def test_get_filings_unknown_ticker_returns_empty(repo):
    assert repo.get_filings("NOPE") == ()

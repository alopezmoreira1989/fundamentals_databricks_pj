"""CompanyRepository.get_filings — the company page's Filings tab. Sourced from the
``filings`` view (backed by the ``dashboard_filings`` artifact, written by the pipeline's own
``15__fetch_sec_filings.py``), not a live SEC call of our own (retired
``infrastructure/filings.py``'s per-request fetch). Self-contained: in-memory DuckDB, injected
connection — no fixtures/network needed.
"""

from __future__ import annotations

import duckdb
import pytest
from repositories.companies import CompanyRepository
from repositories.dtos import FilingRow

# ticker, form, filing_date, report_date, description, url
_FILING_ROWS = [
    ("AAPL", "10-K", "2025-11-01", "2025-09-30", "10-K", "https://www.sec.gov/Archives/edgar/data/320193/x/aapl-10k.htm"),
    ("AAPL", "10-Q", "2025-08-01", "2025-06-30", "10-Q", "https://www.sec.gov/Archives/edgar/data/320193/x/aapl-10q.htm"),
    ("MSFT", "10-K", "2026-01-15", "2025-12-31", "Annual report", "https://www.sec.gov/Archives/edgar/data/789019/x/msft-10k.htm"),
]


@pytest.fixture
def con():
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE filings ("
        " ticker VARCHAR, form VARCHAR, filing_date VARCHAR, report_date VARCHAR,"
        " description VARCHAR, url VARCHAR)"
    )
    conn.executemany("INSERT INTO filings VALUES (?,?,?,?,?,?)", _FILING_ROWS)
    yield conn
    conn.close()


@pytest.fixture
def repo(con):
    return CompanyRepository(connection=con)


def test_get_filings_returns_only_this_tickers_rows_newest_first(repo):
    rows = repo.get_filings("AAPL")
    assert len(rows) == 2
    assert all(isinstance(r, FilingRow) for r in rows)
    assert [r.filing_date for r in rows] == ["2025-11-01", "2025-08-01"]  # newest first


def test_get_filings_row_shape(repo):
    row = repo.get_filings("AAPL")[0]
    assert row.form == "10-K"
    assert row.report_date == "2025-09-30"
    assert row.description == "10-K"
    assert row.url == "https://www.sec.gov/Archives/edgar/data/320193/x/aapl-10k.htm"


def test_get_filings_unknown_ticker_returns_empty(repo):
    assert repo.get_filings("NOPE") == ()


def test_get_filings_missing_view_degrades_to_empty():
    """No ``filings`` view registered at all (optional artifact absent) → ``()``, never raise."""
    empty_con = duckdb.connect(":memory:")
    try:
        repo = CompanyRepository(connection=empty_con)
        assert repo.get_filings("AAPL") == ()
    finally:
        empty_con.close()

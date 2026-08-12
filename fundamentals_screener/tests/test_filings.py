"""CompanyRepository.get_filings — the company page's Filings tab (issue #318). Sourced from
the dashboard_filings artifact (written by the pipeline's 15__fetch_sec_filings.py), not a
live SEC call of our own. Self-contained: in-memory DuckDB, injected connection, same pattern
as test_net_net_snapshot.py.

fiscal_year/period_type are matched via an ASOF JOIN against dashboard_data's
period_type='FY' period_end dates (see companies.py's own comment on _FILINGS_SQL for why FY
anchors + ASOF, not a direct join against the short-retention Q1-Q4 rows, and not a
between-two-anchors bracket) — the fixture below models two consecutive *closed* fiscal
years' worth of FY anchors, a mix of 10-K/10-Q filings, PLUS three 10-Qs from a still-*open*
fiscal year (FY2026, whose own 10-K hasn't been filed yet -- confirmed live for AAPL 2026-08-12,
the regression the between-two-anchors version of this query introduced), one filing older
than the earliest known anchor (NULL fallback), and one ticker (MSFT) with no dashboard_data
rows at all.
"""

from __future__ import annotations

import duckdb
import pytest
from fundamentals_screener.dtos import FilingRow
from fundamentals_screener.repositories.companies import CompanyRepository

# ticker, form, filing_date, report_date, description, url
_FILING_ROWS = [
    # FY2026 -- open fiscal year: 3 10-Qs already filed, no 10-K yet.
    ("AAPL", "10-Q", "2026-07-31", "2026-06-27", "Q3 FY26", "https://sec.gov/AAPL/10q-2026q3.htm"),
    ("AAPL", "10-Q", "2026-05-01", "2026-03-28", "Q2 FY26", "https://sec.gov/AAPL/10q-2026q2.htm"),
    ("AAPL", "10-Q", "2026-01-30", "2025-12-27", "Q1 FY26", "https://sec.gov/AAPL/10q-2026q1.htm"),
    ("AAPL", "10-K", "2025-10-31", "2025-09-27", "Annual report FY25", "https://sec.gov/AAPL/10k-2025.htm"),
    ("AAPL", "10-Q", "2025-08-01", "2025-06-28", "Q3 FY25", "https://sec.gov/AAPL/10q-2025q3.htm"),
    ("AAPL", "10-Q", "2025-05-02", "2025-03-29", "Q2 FY25", "https://sec.gov/AAPL/10q-2025q2.htm"),
    ("AAPL", "10-Q", "2025-01-31", "2024-12-28", "Q1 FY25", "https://sec.gov/AAPL/10q-2025q1.htm"),
    ("AAPL", "10-K", "2024-11-01", "2024-09-28", "Annual report FY24", "https://sec.gov/AAPL/10k-2024.htm"),
    # Older than the earliest known FY anchor (FY2024) below -- no preceding anchor to match.
    ("AAPL", "10-Q", "2023-08-04", "2023-07-01", "Old 10-Q", "https://sec.gov/AAPL/10q-old.htm"),
    ("MSFT", "10-K", "2026-01-15", "2025-12-31", "Annual report", "https://sec.gov/MSFT/10k-2026.htm"),
]

# ticker, period_end, fiscal_year, period_type — FY-only anchors (the long-retention bucket).
# MSFT has none, so its filing above has nothing to bracket against.
_PERIOD_ROWS = [
    ("AAPL", "2024-09-28", 2024, "FY"),
    ("AAPL", "2025-09-27", 2025, "FY"),
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
    assert len(rows) == 9
    assert all(isinstance(r, FilingRow) for r in rows)
    assert [r.filing_date for r in rows][:3] == ["2026-07-31", "2026-05-01", "2026-01-30"]  # newest first


def test_get_filings_row_shape(repo):
    row = repo.get_filings("AAPL")[0]
    assert row.form == "10-Q"
    assert row.report_date == "2026-06-27"
    assert row.description == "Q3 FY26"
    assert row.url == "https://sec.gov/AAPL/10q-2026q3.htm"


def test_get_filings_open_fiscal_year_10qs_resolve_without_a_closing_anchor(repo):
    """FY2026 hasn't closed yet (no 10-K filed), so there's no upper-bound anchor -- only the
    prior year's (FY2025). Must still resolve via the nearest-preceding-anchor ASOF match,
    not come back blank the way the older between-two-anchors version regressed to."""
    rows = {r.report_date: r for r in repo.get_filings("AAPL")}
    assert rows["2025-12-27"].fiscal_year == 2026
    assert rows["2025-12-27"].period_type == "Q1"
    assert rows["2026-03-28"].fiscal_year == 2026
    assert rows["2026-03-28"].period_type == "Q2"
    assert rows["2026-06-27"].fiscal_year == 2026
    assert rows["2026-06-27"].period_type == "Q3"


def test_get_filings_10k_matches_fy_anchor_exactly(repo):
    rows = {r.report_date: r for r in repo.get_filings("AAPL")}
    assert rows["2025-09-27"].fiscal_year == 2025
    assert rows["2025-09-27"].period_type == "FY"
    assert rows["2024-09-28"].fiscal_year == 2024
    assert rows["2024-09-28"].period_type == "FY"


def test_get_filings_10q_brackets_into_fy_with_ordinal_quarter(repo):
    rows = {r.report_date: r for r in repo.get_filings("AAPL")}
    assert rows["2024-12-28"].fiscal_year == 2025
    assert rows["2024-12-28"].period_type == "Q1"
    assert rows["2025-03-29"].fiscal_year == 2025
    assert rows["2025-03-29"].period_type == "Q2"
    assert rows["2025-06-28"].fiscal_year == 2025
    assert rows["2025-06-28"].period_type == "Q3"


def test_get_filings_no_matching_period_leaves_fiscal_year_none(repo):
    row = repo.get_filings("MSFT")[0]
    assert row.fiscal_year is None
    assert row.period_type is None


def test_get_filings_10q_older_than_earliest_anchor_leaves_fiscal_year_none(repo):
    rows = {r.report_date: r for r in repo.get_filings("AAPL")}
    assert rows["2023-07-01"].fiscal_year is None
    assert rows["2023-07-01"].period_type is None


def test_get_filings_never_duplicates_rows_for_a_filing(repo):
    """A 10-K's period_end can also carry a same-dated Q4 row in dashboard_data within the
    short quarterly-retention window -- confirmed in production 2026-08-12 to fan out into
    two rows per 10-K under a naive period_end-equality join. Bracketing against FY-only
    anchors must never do that: exactly one row per input filing."""
    aapl_filings = sum(1 for row in _FILING_ROWS if row[0] == "AAPL")
    assert len(repo.get_filings("AAPL")) == aapl_filings


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

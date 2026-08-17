"""CompanyRepository.get_quarterly — the company page's "Periods" tab (Phase 6.2; the tab was
named "Quarterly" before this phase, see docs/phase6-2-multi-market-periods-reports.md).
Self-contained: in-memory DuckDB, injected connection, same pattern as test_filings.py/
test_net_net_snapshot.py.

No test existed for this repository method before Phase 6.2 — a real, confirmed gap (the
Quarterly/Periods tab had zero dedicated test coverage). `_QUARTERLY_SQL`'s own filter is
`period_type <> 'FY'` (repositories/companies.py) -- genuinely period-shape-neutral, not a
`period_type IN ('Q1','Q2','Q3','Q4')` enum -- these fixtures exercise that directly: a real
US-shaped quarterly fixture, and a real EU/CA-shaped FY-only fixture (matching the live,
verified fact that AQN and all 8 currently-admitted European tickers have ONLY `period_type =
'FY'` rows in the real published dashboard_data as of this phase -- confirmed by downloading
and inspecting the real GitHub Release artifact, not assumed).
"""

from __future__ import annotations

import duckdb
import pytest
from fundamentals_screener.dtos import QuarterGrid, StatementLine
from fundamentals_screener.repositories.companies import CompanyRepository

# ticker, stmt, section, group, concept, display_name, sort_order, period_type, period_end, fiscal_year, value
_US_ROWS = [
    ("AAPL", "Income Statement", "Revenue", None, "Revenue", "Revenue", 1, "Q1", "2025-12-27", 2026, 100.0),
    ("AAPL", "Income Statement", "Revenue", None, "Revenue", "Revenue", 1, "Q2", "2026-03-28", 2026, 110.0),
    ("AAPL", "Income Statement", "Revenue", None, "Revenue", "Revenue", 1, "Q3", "2026-06-27", 2026, 120.0),
    ("AAPL", "Income Statement", "Revenue", None, "Revenue", "Revenue", 1, "FY", "2025-09-27", 2025, 400.0),
]

# A real EU/CA-shaped fixture: every row is period_type='FY' -- no interim period exists at
# all, matching the live published data for every one of the 8 admitted European tickers and
# for AQN (Canada). This is the real, common case _QUARTERLY_SQL's `period_type <> 'FY'` filter
# excludes entirely -- not a hypothetical edge case.
_EU_ROWS = [
    ("FCC", "Income Statement", "Revenue", None, "Revenue", "Revenue", 1, "FY", "2024-12-31", 2024, 9071416000.0),
    ("FCC", "Income Statement", "Revenue", None, "Revenue", "Revenue", 1, "FY", "2023-12-31", 2023, 9026016000.0),
]

_ALL_ROWS = _US_ROWS + _EU_ROWS


@pytest.fixture
def con():
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE dashboard_data ("
        " ticker VARCHAR, stmt VARCHAR, section VARCHAR, \"group\" VARCHAR, concept VARCHAR,"
        " display_name VARCHAR, sort_order INTEGER, period_type VARCHAR, period_end VARCHAR,"
        " fiscal_year INTEGER, value DOUBLE)"
    )
    conn.executemany(
        "INSERT INTO dashboard_data VALUES (?,?,?,?,?,?,?,?,?,?,?)", _ALL_ROWS
    )
    yield conn
    conn.close()


@pytest.fixture
def repo(con):
    return CompanyRepository(connection=con)


def test_get_quarterly_us_ticker_returns_real_quarter_columns(repo):
    grid = repo.get_quarterly("AAPL")
    assert isinstance(grid, QuarterGrid)
    assert grid.name == "Income Statement"
    # Newest period_end first.
    assert grid.columns == ("Q3 2026", "Q2 2026", "Q1 2026")
    assert len(grid.lines) == 1
    assert isinstance(grid.lines[0], StatementLine)
    assert grid.lines[0].values == (120.0, 110.0, 100.0)


def test_get_quarterly_fy_only_ticker_returns_empty_grid():
    """The real, common case: an issuer (European ESEF or Canadian MJDS/40-F filer) whose only
    published period_type is 'FY'. Must be genuinely empty -- not a grid with fabricated
    quarter columns, and not an error -- so the template's existing `{% if quarterly.lines %}`
    guard correctly omits the Periods tab entirely for these tickers (verified live this phase
    against AQN/FCC/ALO/IBE/SGO -- see docs/phase6-2-multi-market-periods-reports.md)."""
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE dashboard_data ("
        " ticker VARCHAR, stmt VARCHAR, section VARCHAR, \"group\" VARCHAR, concept VARCHAR,"
        " display_name VARCHAR, sort_order INTEGER, period_type VARCHAR, period_end VARCHAR,"
        " fiscal_year INTEGER, value DOUBLE)"
    )
    conn.executemany("INSERT INTO dashboard_data VALUES (?,?,?,?,?,?,?,?,?,?,?)", _EU_ROWS)
    repo = CompanyRepository(connection=conn)

    grid = repo.get_quarterly("FCC")
    assert grid == QuarterGrid(name="Income Statement", columns=(), lines=())
    assert grid.columns == ()
    assert grid.lines == ()
    conn.close()


def test_get_quarterly_unknown_ticker_returns_empty_grid(repo):
    grid = repo.get_quarterly("NOPE")
    assert grid.columns == ()
    assert grid.lines == ()


def test_get_quarterly_only_returns_requested_tickers_rows(repo):
    """A real, adjacent regression this fixture also proves: an FY-only ticker (FCC) sharing
    the same in-memory table as a real quarterly ticker (AAPL) must not leak AAPL's quarters
    into FCC's grid, or vice versa."""
    us_grid = repo.get_quarterly("AAPL")
    eu_grid = repo.get_quarterly("FCC")
    assert us_grid.lines != ()
    assert eu_grid.lines == ()

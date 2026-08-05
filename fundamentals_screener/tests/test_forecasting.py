"""ForecastRepository — the Forecasting tab's 10-year cross-sectional ML scenario forecasts
plus PV-discounted forward P/E / FCF Yield. Sourced from the ``dashboard_forecast`` artifact
(written by the pipeline's ``24__forecasting.py``, issues #332/#334). Self-contained:
in-memory DuckDB, injected connection, same pattern as ``test_filings.py``.
"""

from __future__ import annotations

import duckdb
import pytest
from fundamentals_screener.dtos import ForecastPoint, ForecastSeries
from fundamentals_screener.repositories.forecasting import ForecastRepository

# ticker, fiscal_year, horizon, metric, quantile_level, forecast_value
_FORECAST_ROWS = [
    ("AAPL", 2025, 1, "revenue", 0.10, 400.0),
    ("AAPL", 2025, 2, "revenue", 0.10, 410.0),
    ("AAPL", 2025, 1, "revenue", 0.90, 460.0),
    ("AAPL", 2025, 2, "revenue", 0.90, 490.0),
    ("AAPL", 2025, 1, "net_income", 0.50, 90.0),
    ("AAPL", 2025, 2, "net_income", 0.50, 95.0),
    # Forward multiples — must never leak into forecast_points()/forecast_series().
    ("AAPL", 2025, 1, "forward_pe", 0.50, 27.4),
    ("AAPL", 2025, 2, "forward_pe", 0.50, 25.1),
    ("AAPL", 2025, 1, "forward_fcf_yield", 0.50, 0.031),
    ("AAPL", 2025, 1, "forward_pe", 0.10, 15.0),  # non-mid scenario — excluded from forward_multiples()
    ("MSFT", 2025, 1, "revenue", 0.50, 250.0),
]


@pytest.fixture
def con():
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE dashboard_forecast ("
        " ticker VARCHAR, fiscal_year INTEGER, horizon INTEGER, metric VARCHAR,"
        " quantile_level DOUBLE, forecast_value DOUBLE)"
    )
    conn.executemany("INSERT INTO dashboard_forecast VALUES (?,?,?,?,?,?)", _FORECAST_ROWS)
    yield conn
    conn.close()


@pytest.fixture
def repo(con):
    return ForecastRepository(connection=con)


def test_forecast_points_returns_only_target_metric_rows(repo):
    points = repo.forecast_points("AAPL")
    assert len(points) == 6
    assert all(isinstance(p, ForecastPoint) for p in points)
    assert all(p.metric in ("revenue", "net_income", "free_cash_flow") for p in points)


def test_forecast_points_unknown_ticker_returns_empty(repo):
    assert repo.forecast_points("NOPE") == ()


def test_forecast_points_missing_view_degrades_to_empty():
    """No ``dashboard_forecast`` view registered at all (optional artifact absent) → ``()``,
    never raise."""
    empty_con = duckdb.connect(":memory:")
    try:
        repo = ForecastRepository(connection=empty_con)
        assert repo.forecast_points("AAPL") == ()
    finally:
        empty_con.close()


def test_forecast_series_groups_by_metric_and_quantile_level(repo):
    series = repo.forecast_series("AAPL")
    assert len(series) == 3  # revenue@0.10, revenue@0.90, net_income@0.50
    assert all(isinstance(s, ForecastSeries) for s in series)


def test_forecast_series_orders_points_by_horizon(repo):
    series = repo.forecast_series("AAPL")
    revenue_bear = next(s for s in series if s.metric == "revenue" and s.quantile_level == 0.10)
    assert revenue_bear.horizons == (1, 2)
    assert revenue_bear.values == (400.0, 410.0)


def test_forecast_series_orders_metric_then_quantile_level(repo):
    series = repo.forecast_series("AAPL")
    keys = [(s.metric, s.quantile_level) for s in series]
    # revenue (TARGET_METRICS' own order) before net_income, ascending quantile within a metric.
    assert keys == [("revenue", 0.10), ("revenue", 0.90), ("net_income", 0.50)]


def test_forecast_series_unknown_ticker_returns_empty(repo):
    assert repo.forecast_series("NOPE") == ()


def test_forecast_series_only_returns_this_tickers_series(repo):
    series = repo.forecast_series("MSFT")
    assert len(series) == 1
    assert series[0].metric == "revenue"
    assert series[0].values == (250.0,)


def test_forward_multiples_returns_only_mid_scenario_rows(repo):
    rows = repo.forward_multiples("AAPL")
    assert len(rows) == 3
    assert all(r.quantile_level == 0.50 for r in rows)
    assert all(r.metric in ("forward_pe", "forward_fcf_yield") for r in rows)


def test_forward_multiples_ordered_by_metric_then_horizon(repo):
    rows = repo.forward_multiples("AAPL")
    keys = [(r.metric, r.horizon) for r in rows]
    assert keys == [("forward_fcf_yield", 1), ("forward_pe", 1), ("forward_pe", 2)]


def test_forward_multiples_unknown_ticker_returns_empty(repo):
    assert repo.forward_multiples("NOPE") == ()


def test_forward_multiples_missing_view_degrades_to_empty():
    empty_con = duckdb.connect(":memory:")
    try:
        repo = ForecastRepository(connection=empty_con)
        assert repo.forward_multiples("AAPL") == ()
    finally:
        empty_con.close()

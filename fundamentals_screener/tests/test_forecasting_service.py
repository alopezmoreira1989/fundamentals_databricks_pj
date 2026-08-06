"""Tests for services.get_forecast_chart — composes CompanyRepository (historical statements +
Free Cash Flow metric history) with ForecastRepository (published forecast series + forward
multiples) into one ForecastChart DTO. Exercised via monkeypatched fake repositories (same
pattern as test_net_net_service.py) so no real DuckDB/meta dependency is needed.
"""

from __future__ import annotations

from fundamentals_screener.dtos import (
    CompanyStatements,
    CompanySummary,
    ForecastPoint,
    ForecastSeries,
    MetricSeries,
    Statement,
    StatementLine,
)
from fundamentals_screener.services import get_forecast_chart

from fundamentals_screener import services as services_module

_SUMMARY = CompanySummary(ticker="AAPL", name="Apple Inc.")

# newest-first, as CompanyRepository.get_statements/metric_history actually return.
_STATEMENTS = CompanyStatements(statements=(
    Statement(
        name="Income Statement",
        years=(2025, 2024, 2023),
        lines=(
            StatementLine(display_name="Revenue", section="Revenue", values=(400.0, 380.0, 350.0)),
            StatementLine(display_name="Net Income", section="Net Income", values=(90.0, 85.0, 80.0)),
            StatementLine(display_name="Cost of Revenue", section="Revenue", values=(200.0, 190.0, 175.0)),
        ),
    ),
))

_FCF_SERIES = MetricSeries(
    ticker="AAPL", metric="Free Cash Flow", unit="usd", category="Cash Flow", subcategory=None,
    sort_order=1.0, fiscal_years=(2025, 2024, 2023), values=(108.0, 99.0, 92.0),
)

_FORECAST_SERIES = (
    ForecastSeries(metric="revenue", quantile_level=0.10, horizons=(1, 2), values=(390.0, 380.0)),
    ForecastSeries(metric="revenue", quantile_level=0.90, horizons=(1, 2), values=(420.0, 445.0)),
)

_FORWARD_MULTIPLES = (
    ForecastPoint(metric="forward_pe", horizon=1, quantile_level=0.50, forecast_value=27.4),
    ForecastPoint(metric="forward_fcf_yield", horizon=1, quantile_level=0.50, forecast_value=0.031),
)


class _FakeCompanyRepo:
    def __init__(self, *, summary=_SUMMARY, statements=_STATEMENTS, fcf_series=(_FCF_SERIES,)):
        self._summary = summary
        self._statements = statements
        self._fcf_series = fcf_series

    def get_summary(self, ticker):
        return self._summary

    def get_statements(self, ticker, *, max_years=11):
        return self._statements

    def metric_history(self, ticker, *, years=11):
        return self._fcf_series


class _FakeForecastRepo:
    def __init__(self, *, series=_FORECAST_SERIES, forward_multiples=_FORWARD_MULTIPLES):
        self._series = series
        self._forward_multiples = forward_multiples

    def forecast_series(self, ticker):
        return self._series

    def forward_multiples(self, ticker):
        return self._forward_multiples


def _patch(monkeypatch, *, company_repo=None, forecast_repo=None):
    monkeypatch.setattr(services_module, "CompanyRepository", lambda: company_repo or _FakeCompanyRepo())
    monkeypatch.setattr(services_module, "ForecastRepository", lambda: forecast_repo or _FakeForecastRepo())


def test_unknown_ticker_returns_none(monkeypatch):
    _patch(monkeypatch, company_repo=_FakeCompanyRepo(summary=None))
    assert get_forecast_chart("NOPE") is None


def test_historical_reversed_to_chronological_ending_at_fy0(monkeypatch):
    _patch(monkeypatch)
    chart = get_forecast_chart("AAPL")
    revenue = next(m for m in chart.metrics if m.metric == "revenue")
    assert [h.fiscal_year for h in revenue.historical] == [2023, 2024, 2025]
    assert [h.value for h in revenue.historical] == [350.0, 380.0, 400.0]


def test_scenarios_anchored_to_fy0_historical_value(monkeypatch):
    _patch(monkeypatch)
    chart = get_forecast_chart("AAPL")
    revenue = next(m for m in chart.metrics if m.metric == "revenue")
    bear = next(s for s in revenue.scenarios if s.quantile_level == 0.10)
    assert bear.horizons == (0, 1, 2)
    assert bear.values == (400.0, 390.0, 380.0)  # FY0 anchor (400.0) prepended


def test_free_cash_flow_uses_metric_history_and_its_own_unit(monkeypatch):
    _patch(monkeypatch)
    chart = get_forecast_chart("AAPL")
    fcf = next(m for m in chart.metrics if m.metric == "free_cash_flow")
    assert fcf.unit == "usd"
    assert [h.fiscal_year for h in fcf.historical] == [2023, 2024, 2025]
    assert [h.value for h in fcf.historical] == [92.0, 99.0, 108.0]


def test_metric_with_no_history_and_no_forecast_is_omitted(monkeypatch):
    # A statements set with no "Net Income" line and no fcf history -- net_income/
    # free_cash_flow have neither historical data nor a forecast series, so both are omitted;
    # revenue (has both) still comes through.
    statements = CompanyStatements(statements=(
        Statement(
            name="Income Statement", years=(2025,),
            lines=(StatementLine(display_name="Revenue", section="Revenue", values=(400.0,)),),
        ),
    ))
    _patch(monkeypatch, company_repo=_FakeCompanyRepo(statements=statements, fcf_series=()))
    chart = get_forecast_chart("AAPL")
    metrics = [m.metric for m in chart.metrics]
    assert "revenue" in metrics
    assert "net_income" not in metrics
    assert "free_cash_flow" not in metrics


def test_forward_multiples_passed_through(monkeypatch):
    _patch(monkeypatch)
    chart = get_forecast_chart("AAPL")
    assert len(chart.forward_multiples) == 2
    pe = next(r for r in chart.forward_multiples if r.metric == "forward_pe")
    assert pe.horizon == 1
    assert pe.value == 27.4


def test_known_ticker_with_no_published_forecast_returns_empty_chart_not_none(monkeypatch):
    _patch(
        monkeypatch,
        company_repo=_FakeCompanyRepo(statements=CompanyStatements(statements=()), fcf_series=()),
        forecast_repo=_FakeForecastRepo(series=(), forward_multiples=()),
    )
    chart = get_forecast_chart("AAPL")
    assert chart is not None
    assert chart.metrics == ()
    assert chart.forward_multiples == ()

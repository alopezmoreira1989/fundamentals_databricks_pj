"""Read-model tests: repositories → services → views, offline against fixture artifacts.

No database (no ORM here); the ``artifacts_from_fixtures`` fixture points storage at the
committed Streamlit fixtures and DuckDB queries the parquet directly.
"""

from __future__ import annotations

import dataclasses

import pytest
from apps.companies import services as company_services
from repositories.companies import CompanyRepository
from repositories.dtos import CompanySummary, MetricPoint, ScreenRow
from repositories.screener import ScreenerRepository
from repositories.valuation import ValuationRepository

TICKER = "AAPL"


def _a_metric_of(ticker: str) -> str:
    """A metric name the ticker actually has a non-null latest-FY value for."""
    for m in CompanyRepository().latest_metrics(ticker):
        if m.value is not None:
            return m.metric
    raise AssertionError(f"no non-null metric found for {ticker} in fixtures")


# ── repositories ─────────────────────────────────────────────────────────────────────
def test_company_summary(artifacts_from_fixtures):
    summary = CompanyRepository().get_summary(TICKER)
    assert isinstance(summary, CompanySummary)
    assert summary.ticker == TICKER
    assert summary.name  # non-empty company name
    assert CompanyRepository().get_summary("NOTAREALTICKER") is None


def test_latest_metrics_are_single_year_dtos(artifacts_from_fixtures):
    metrics = CompanyRepository().latest_metrics(TICKER, limit=25)
    assert metrics and all(isinstance(m, MetricPoint) for m in metrics)
    assert len(metrics) <= 25
    # "latest FY" ⇒ every row shares the one most-recent fiscal year
    assert len({m.fiscal_year for m in metrics}) == 1


def test_dtos_are_immutable(artifacts_from_fixtures):
    summary = CompanyRepository().get_summary(TICKER)
    assert summary is not None
    with pytest.raises(dataclasses.FrozenInstanceError):
        summary.name = "mutated"


def test_screener_respects_bounds_ordering_and_limit(artifacts_from_fixtures):
    metric = _a_metric_of(TICKER)
    rows = ScreenerRepository().screen(metric=metric, limit=5)
    assert rows and all(isinstance(r, ScreenRow) for r in rows)
    assert len(rows) <= 5
    values = [r.value for r in rows if r.value is not None]
    assert values == sorted(values, reverse=True)  # ordered by value desc

    # A min bound only returns rows at/above it.
    floor = values[-1]
    bounded = ScreenerRepository().screen(metric=metric, min_value=floor, limit=50)
    assert all(r.value is not None and r.value >= floor for r in bounded)


def test_valuation_returns_only_mos_metrics(artifacts_from_fixtures):
    points = ValuationRepository().margin_of_safety(TICKER)
    assert points  # AAPL has MoS metrics in the fixtures
    assert all(p.metric.startswith("MoS ") for p in points)


# ── service ──────────────────────────────────────────────────────────────────────────
def test_company_service_composes_detail(artifacts_from_fixtures):
    detail = company_services.get_company_detail(TICKER)
    assert detail is not None
    assert detail.summary.ticker == TICKER
    assert detail.metrics  # non-empty
    assert company_services.get_company_detail("NOTAREALTICKER") is None


# ── views (JSON) ─────────────────────────────────────────────────────────────────────
def test_company_view_200_and_404(artifacts_from_fixtures, client):
    resp = client.get(f"/companies/{TICKER.lower()}/")  # lower-case → view upper-cases it
    assert resp.status_code == 200
    body = resp.json()
    assert body["ticker"] == TICKER
    assert isinstance(body["metrics"], list) and body["metrics"]

    assert client.get("/companies/notareal/").status_code == 404


def test_screener_view_requires_metric(artifacts_from_fixtures, client):
    assert client.get("/screener/").status_code == 400
    assert client.get("/screener/?metric=X&min=abc").status_code == 400


def test_screener_view_returns_results(artifacts_from_fixtures, client):
    metric = _a_metric_of(TICKER)
    resp = client.get("/screener/", {"metric": metric, "limit": 5})
    assert resp.status_code == 200
    body = resp.json()
    assert body["metric"] == metric
    assert body["count"] == len(body["results"]) <= 5


def test_valuation_view_200(artifacts_from_fixtures, client):
    resp = client.get(f"/valuation/{TICKER}/")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ticker"] == TICKER
    assert all(row["metric"].startswith("MoS ") for row in body["margin_of_safety"])

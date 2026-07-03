"""DRF API tests: the read-only viewsets over companies / screener / valuation, offline against
the fixture artifacts (no database — the read model is artifact-backed, not ORM-backed)."""

from __future__ import annotations

from repositories.companies import CompanyRepository

TICKER = "AAPL"


def _a_metric_of(ticker: str) -> str:
    """A metric name the ticker actually has a non-null latest-FY value for."""
    for m in CompanyRepository().latest_metrics(ticker):
        if m.value is not None:
            return m.metric
    raise AssertionError(f"no non-null metric found for {ticker} in fixtures")


# ── companies ────────────────────────────────────────────────────────────────────────
def test_company_list_is_paginated_universe(artifacts_from_fixtures, client):
    resp = client.get("/api/companies/", {"page_size": 25})
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] > 100 and body["page"] == 1 and body["page_size"] == 25
    assert len(body["results"]) == 25
    row = body["results"][0]
    assert set(row) == {"ticker", "name", "sector", "industry", "metric_value", "fiscal_year"}
    assert row["metric_value"] is None  # no metric filter ⇒ descriptive-only rows
    tickers = [r["ticker"] for r in body["results"]]
    assert tickers == sorted(tickers)  # ticker-ordered for stable paging


def test_company_list_search_narrows(artifacts_from_fixtures, client):
    full = client.get("/api/companies/", {"page_size": 1}).json()["count"]
    hit = client.get("/api/companies/", {"q": "aapl", "page_size": 1}).json()["count"]
    assert 0 < hit < full


def test_company_list_metric_filter_orders_and_bounds(artifacts_from_fixtures, client):
    metric = _a_metric_of(TICKER)
    body = client.get("/api/companies/", {"metric": metric, "page_size": 50}).json()
    values = [r["metric_value"] for r in body["results"]]
    assert values and all(v is not None for v in values)
    assert values == sorted(values, reverse=True)


def test_company_list_rejects_bad_bounds(artifacts_from_fixtures, client):
    assert client.get("/api/companies/", {"min": "abc"}).status_code == 400


def test_company_retrieve_200_and_404(artifacts_from_fixtures, client):
    resp = client.get(f"/api/companies/{TICKER.lower()}/")  # lower-case → viewset upper-cases
    assert resp.status_code == 200
    body = resp.json()
    assert body["summary"]["ticker"] == TICKER and body["summary"]["name"]
    assert isinstance(body["metrics"], list) and body["metrics"]
    assert {"metric", "unit", "fiscal_year", "value", "category"} <= set(body["metrics"][0])

    assert client.get("/api/companies/notareal/").status_code == 404


# ── screener ─────────────────────────────────────────────────────────────────────────
def test_screener_requires_metric(artifacts_from_fixtures, client):
    assert client.get("/api/screener/").status_code == 400


def test_screener_rejects_bad_bounds(artifacts_from_fixtures, client):
    metric = _a_metric_of(TICKER)
    assert client.get("/api/screener/", {"metric": metric, "min": "abc"}).status_code == 400


def test_screener_returns_ordered_bounded_results(artifacts_from_fixtures, client):
    metric = _a_metric_of(TICKER)
    body = client.get("/api/screener/", {"metric": metric, "limit": 5}).json()
    assert body["metric"] == metric
    assert body["count"] == len(body["results"]) <= 5
    values = [r["value"] for r in body["results"] if r["value"] is not None]
    assert values == sorted(values, reverse=True)


# ── valuation ────────────────────────────────────────────────────────────────────────
def test_valuation_retrieve_200_and_404(artifacts_from_fixtures, client):
    resp = client.get(f"/api/valuation/{TICKER.lower()}/")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ticker"] == TICKER
    assert body["margin_of_safety"] and all(
        row["metric"].startswith("MoS ") for row in body["margin_of_safety"]
    )
    field = body["football_field"]
    assert field["price"] is not None and field["price"] > 0
    assert field["bars"] and all(
        b["bear"] <= b["mid"] <= b["bull"] for b in field["bars"]
    )

    assert client.get("/api/valuation/notareal/").status_code == 404

"""DRF API tests: the versioned, read-only viewsets over companies / screener / valuation,
offline against the fixture artifacts (no database — the read model is artifact-backed).

Covers routing/behaviour, the single error envelope, and contract tests asserting the response
keys match the read-model DTOs field-for-field.
"""

from __future__ import annotations

import dataclasses

from repositories.companies import CompanyRepository
from repositories.dtos import CompanyListRow, CompanySummary, FootballBar, MetricPoint, ScreenRow

TICKER = "AAPL"
V1 = "/api/v1"


def _a_metric_of(ticker: str) -> str:
    """A metric name the ticker actually has a non-null latest-FY value for."""
    for m in CompanyRepository().latest_metrics(ticker):
        if m.value is not None:
            return m.metric
    raise AssertionError(f"no non-null metric found for {ticker} in fixtures")


def _fields(dto: type) -> set[str]:
    return {f.name for f in dataclasses.fields(dto)}


# ── versioning ───────────────────────────────────────────────────────────────────────
def test_api_root_lists_versioned_endpoints(artifacts_from_fixtures, client):
    resp = client.get(f"{V1}/")  # DRF router root, reversed under the version
    assert resp.status_code == 200
    routes = resp.json()
    # The root lists list-able collections; valuation is retrieve-only (per-ticker) so it's
    # not a root entry, but its /valuation/<ticker>/ route still resolves (tested below).
    assert set(routes) == {"companies", "screener"}
    assert all(url.startswith(f"http://testserver{V1}/") for url in routes.values())


def test_unversioned_and_bad_version_do_not_resolve(artifacts_from_fixtures, client):
    assert client.get("/api/companies/").status_code == 404  # version segment is required
    resp = client.get("/api/v2/companies/")  # v2 not in ALLOWED_VERSIONS
    assert resp.status_code == 404
    assert set(resp.json()["error"]) == {"status", "message"}  # still the envelope


# ── companies ────────────────────────────────────────────────────────────────────────
def test_company_list_is_paginated_universe(artifacts_from_fixtures, client):
    resp = client.get(f"{V1}/companies/", {"page_size": 25})
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] > 100 and body["page"] == 1 and body["page_size"] == 25
    assert len(body["results"]) == 25
    tickers = [r["ticker"] for r in body["results"]]
    assert tickers == sorted(tickers)  # ticker-ordered for stable paging


def test_company_list_row_contract_matches_dto(artifacts_from_fixtures, client):
    row = client.get(f"{V1}/companies/", {"page_size": 1}).json()["results"][0]
    assert set(row) == _fields(CompanyListRow)
    assert row["metric_value"] is None  # no metric filter ⇒ descriptive-only rows


def test_company_list_search_narrows(artifacts_from_fixtures, client):
    full = client.get(f"{V1}/companies/", {"page_size": 1}).json()["count"]
    hit = client.get(f"{V1}/companies/", {"q": "aapl", "page_size": 1}).json()["count"]
    assert 0 < hit < full


def test_company_list_metric_filter_orders(artifacts_from_fixtures, client):
    metric = _a_metric_of(TICKER)
    body = client.get(f"{V1}/companies/", {"metric": metric, "page_size": 50}).json()
    values = [r["metric_value"] for r in body["results"]]
    assert values and all(v is not None for v in values)
    assert values == sorted(values, reverse=True)


def test_company_retrieve_contract_and_404(artifacts_from_fixtures, client):
    resp = client.get(f"{V1}/companies/{TICKER.lower()}/")  # lower-case → viewset upper-cases
    assert resp.status_code == 200
    body = resp.json()
    assert set(body) == {"summary", "metrics"}
    assert set(body["summary"]) == _fields(CompanySummary)
    assert body["summary"]["ticker"] == TICKER and body["summary"]["name"]
    assert body["metrics"] and set(body["metrics"][0]) == _fields(MetricPoint)

    assert client.get(f"{V1}/companies/notareal/").status_code == 404


# ── screener ─────────────────────────────────────────────────────────────────────────
def test_screener_returns_ordered_results_contract(artifacts_from_fixtures, client):
    metric = _a_metric_of(TICKER)
    body = client.get(f"{V1}/screener/", {"metric": metric, "limit": 5}).json()
    assert body["metric"] == metric
    assert body["count"] == len(body["results"]) <= 5
    assert set(body["results"][0]) == _fields(ScreenRow)
    values = [r["value"] for r in body["results"] if r["value"] is not None]
    assert values == sorted(values, reverse=True)


# ── valuation ────────────────────────────────────────────────────────────────────────
def test_valuation_retrieve_contract_and_404(artifacts_from_fixtures, client):
    resp = client.get(f"{V1}/valuation/{TICKER.lower()}/")
    assert resp.status_code == 200
    body = resp.json()
    assert set(body) == {"ticker", "margin_of_safety", "football_field"}
    assert body["ticker"] == TICKER
    assert body["margin_of_safety"] and all(
        row["metric"].startswith("MoS ") for row in body["margin_of_safety"]
    )
    field = body["football_field"]
    assert set(field) == {"bars", "price"}
    assert field["price"] is not None and field["price"] > 0
    assert field["bars"] and set(field["bars"][0]) == _fields(FootballBar)
    assert all(b["bear"] <= b["mid"] <= b["bull"] for b in field["bars"])

    assert client.get(f"{V1}/valuation/notareal/").status_code == 404


# ── error envelope ───────────────────────────────────────────────────────────────────
def test_error_envelope_shape_on_validation_and_not_found(artifacts_from_fixtures, client):
    # 400: metric required
    resp = client.get(f"{V1}/screener/")
    assert resp.status_code == 400
    err = resp.json()["error"]
    assert err == {"status": 400, "message": "query parameter 'metric' is required."}

    # 400: non-numeric bounds / limit
    assert client.get(f"{V1}/companies/", {"min": "abc"}).json()["error"]["status"] == 400
    assert (
        client.get(f"{V1}/companies/", {"page": "abc"}).json()["error"]["message"]
        == "'page' must be an integer."
    )
    metric = _a_metric_of(TICKER)
    assert client.get(f"{V1}/screener/", {"metric": metric, "max": "x"}).status_code == 400

    # 404: consistent envelope, not DRF's default {"detail": ...}
    nf = client.get(f"{V1}/companies/notareal/").json()
    assert set(nf) == {"error"} and set(nf["error"]) == {"status", "message"}
    assert nf["error"]["status"] == 404

"""SEC EDGAR filings list tests — offline (SEC network calls are monkeypatched)."""

from __future__ import annotations

import pytest
from infrastructure import filings

_TICKER_MAP_JSON = {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}}

_SUBMISSIONS_JSON = {
    "filings": {
        "recent": {
            "form": ["10-K", "8-K", "10-Q"],
            "filingDate": ["2025-11-01", "2025-10-15", "2025-08-01"],
            "reportDate": ["2025-09-30", "", "2025-06-30"],
            "accessionNumber": ["0000320193-25-000100", "0000320193-25-000090", "0000320193-25-000080"],
            "primaryDocument": ["aapl-10k.htm", "aapl-8k.htm", "aapl-10q.htm"],
            "primaryDocDescription": ["10-K", "8-K", "10-Q"],
        }
    }
}


class _Resp:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        pass

    def json(self) -> dict:
        return self._payload


def test_fetch_filings_filters_to_10k_10q_and_builds_urls(monkeypatch):
    from django.core.cache import cache

    cache.clear()

    def _get(url, **kw):
        if "company_tickers" in url:
            return _Resp(_TICKER_MAP_JSON)
        return _Resp(_SUBMISSIONS_JSON)

    monkeypatch.setattr(filings.requests, "get", _get)
    result = filings.fetch_filings("AAPL")
    assert [f.form for f in result] == ["10-K", "10-Q"]  # 8-K dropped
    assert result[0].url == "https://www.sec.gov/Archives/edgar/data/320193/000032019325000100/aapl-10k.htm"
    assert result[0].filing_date == "2025-11-01"


def test_fetch_filings_caches_and_degrades(monkeypatch):
    from django.core.cache import cache

    cache.clear()
    calls: list[str] = []

    def _get(url, **kw):
        calls.append(url)
        if "company_tickers" in url:
            return _Resp(_TICKER_MAP_JSON)
        return _Resp(_SUBMISSIONS_JSON)

    monkeypatch.setattr(filings.requests, "get", _get)
    first = filings.fetch_filings("AAPL")
    second = filings.fetch_filings("AAPL")  # served from cache → no repeat submissions call
    assert first == second
    assert calls.count("https://data.sec.gov/submissions/CIK0000320193.json") == 1

    cache.clear()

    def _boom(url, **kw):
        raise filings.requests.RequestException("SEC down")

    monkeypatch.setattr(filings.requests, "get", _boom)
    assert filings.fetch_filings("MSFT") == ()  # any error → empty, never raises


def test_fetch_filings_unknown_ticker_returns_empty(monkeypatch):
    from django.core.cache import cache

    cache.clear()
    monkeypatch.setattr(filings.requests, "get", lambda url, **kw: _Resp(_TICKER_MAP_JSON))
    assert filings.fetch_filings("NOPE") == ()


@pytest.mark.django_db
def test_company_filings_endpoint(client, monkeypatch):
    from apps.companies import services

    monkeypatch.setattr(
        services,
        "get_company_filings",
        lambda t: (
            filings.Filing(
                form="10-K",
                filing_date="2025-11-01",
                report_date="2025-09-30",
                description="10-K",
                url="https://www.sec.gov/Archives/edgar/data/320193/x/aapl-10k.htm",
            ),
        ),
    )
    resp = client.get("/companies/AAPL/filings/")
    assert resp.status_code == 200
    payload = resp.json()["filings"]
    assert payload == [
        {
            "form": "10-K",
            "filing_date": "2025-11-01",
            "report_date": "2025-09-30",
            "description": "10-K",
            "url": "https://www.sec.gov/Archives/edgar/data/320193/x/aapl-10k.htm",
        }
    ]

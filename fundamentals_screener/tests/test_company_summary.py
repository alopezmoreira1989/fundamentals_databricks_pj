"""CompanyRepository.get_summary's cik wiring (issue #318) — cik comes from a SEPARATE cached
source (data_source.get_cik_map), not the meta artifact itself. Offline: both sources are
monkeypatched directly on the repositories.companies module (same import-binding caveat noted
in test_net_net.py/test_benchmark.py — patch the name as imported there, not data_source's
own).
"""

from __future__ import annotations

from fundamentals_screener.repositories import companies as companies_module
from fundamentals_screener.repositories.companies import CompanyRepository

_META = {
    "tickers": [
        {"ticker": "AAPL", "company": "Apple Inc."},
        {"ticker": "NOCIK", "company": "No Cik Corp"},
    ]
}


def test_get_summary_includes_cik_when_known(monkeypatch):
    monkeypatch.setattr(companies_module, "load_meta", lambda: _META)
    monkeypatch.setattr(companies_module, "get_cik_map", lambda: {"AAPL": "0000320193"})
    summary = CompanyRepository().get_summary("AAPL")
    assert summary is not None
    assert summary.cik == "0000320193"


def test_get_summary_cik_none_when_not_in_map(monkeypatch):
    monkeypatch.setattr(companies_module, "load_meta", lambda: _META)
    monkeypatch.setattr(companies_module, "get_cik_map", lambda: {"AAPL": "0000320193"})
    summary = CompanyRepository().get_summary("NOCIK")
    assert summary is not None
    assert summary.cik is None

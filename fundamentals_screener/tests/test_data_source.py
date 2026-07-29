"""data_source._sync_cik_map / get_cik_map tests — issue #318's Filings tab link-out.

Offline throughout: the SEC request is monkeypatched (same style as web/'s own
tests/test_filings.py), and `data_dir`/`settings`/`list_tickers` are monkeypatched too so this
never touches real Django settings — this package's test suite configures Django nowhere at
all (no conftest.py, no pytest-django), and `data_source.py`'s real `data_dir()` reads
`django.conf.settings.FUNDAMENTALS_DATA_PATH`, which would raise ImproperlyConfigured here.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
import requests

from fundamentals_screener import data_source


class _Resp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


# A handful of SEC's own (ticker, cik_str, title) shape — NOTOURS isn't in our universe and
# must be filtered out; cik_str is a plain int, needing zero-padding to 10 digits.
_SEC_TICKER_JSON = {
    "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
    "1": {"cik_str": 1018724, "ticker": "AMZN", "title": "Amazon.com Inc."},
    "2": {"cik_str": 1, "ticker": "NOTOURS", "title": "Not In Our Universe Inc."},
}


@pytest.fixture
def isolated(monkeypatch, tmp_path):
    """Redirects data_dir()/settings/list_tickers so no real Django config is ever touched."""
    monkeypatch.setattr(data_source, "data_dir", lambda: tmp_path)
    monkeypatch.setattr(data_source, "settings", SimpleNamespace())
    monkeypatch.setattr(data_source, "list_tickers", lambda: [{"ticker": "AAPL"}, {"ticker": "AMZN"}])
    return tmp_path


def test_sync_cik_map_filters_to_our_universe_and_zero_pads(isolated, monkeypatch):
    monkeypatch.setattr(data_source.requests, "get", lambda url, **kw: _Resp(_SEC_TICKER_JSON))
    updated = data_source._sync_cik_map()
    assert updated is True
    assert data_source.get_cik_map() == {"AAPL": "0000320193", "AMZN": "0001018724"}


def test_sync_cik_map_sends_a_user_agent_header(isolated, monkeypatch):
    captured = {}

    def _get(url, headers=None, **kw):
        captured["headers"] = headers
        return _Resp(_SEC_TICKER_JSON)

    monkeypatch.setattr(data_source.requests, "get", _get)
    data_source._sync_cik_map()
    assert captured["headers"]["User-Agent"]  # non-empty, whatever the configured/placeholder value


def test_sync_cik_map_skips_when_already_cached(isolated, monkeypatch):
    (isolated / data_source.CIK_MAP_FILE).write_text(json.dumps({"AAPL": "0000320193"}), encoding="utf-8")
    calls = []
    monkeypatch.setattr(
        data_source.requests, "get",
        lambda url, **kw: (calls.append(url), _Resp(_SEC_TICKER_JSON))[1],
    )
    updated = data_source._sync_cik_map()
    assert updated is False
    assert calls == []


def test_sync_cik_map_force_refetches_even_when_cached(isolated, monkeypatch):
    (isolated / data_source.CIK_MAP_FILE).write_text(json.dumps({"STALE": "0000000001"}), encoding="utf-8")
    monkeypatch.setattr(data_source.requests, "get", lambda url, **kw: _Resp(_SEC_TICKER_JSON))
    updated = data_source._sync_cik_map(force=True)
    assert updated is True
    assert data_source.get_cik_map() == {"AAPL": "0000320193", "AMZN": "0001018724"}


def test_get_cik_map_empty_when_never_synced(isolated):
    assert data_source.get_cik_map() == {}


class _FailingResp:
    """A response whose raise_for_status() raises — mirrors SEC's real 403 when
    SEC_USER_AGENT isn't configured."""

    def raise_for_status(self):
        raise requests.exceptions.HTTPError("403 Client Error: Forbidden")


def test_sync_cik_map_degrades_gracefully_on_http_error(isolated, monkeypatch, capsys):
    # Regression (2026-07-29 production incident): this used to raise and propagate all the
    # way up through sync_fundamentals_data --force, killing the WHOLE deploy script (via its
    # `set -e`) before it reached the media-file copy or health check — even though the
    # essential parquet/meta artifacts had already synced successfully earlier in the same
    # call. Must never raise; must print something an operator can find.
    monkeypatch.setattr(data_source.requests, "get", lambda url, **kw: _FailingResp())
    updated = data_source._sync_cik_map()
    assert updated is False
    assert data_source.get_cik_map() == {}
    assert "Could not refresh the SEC ticker" in capsys.readouterr().out


def test_sync_cik_map_degrades_gracefully_on_connection_error(isolated, monkeypatch):
    def _boom(url, **kw):
        raise requests.exceptions.ConnectionError("no route to host")

    monkeypatch.setattr(data_source.requests, "get", _boom)
    updated = data_source._sync_cik_map()  # must not raise
    assert updated is False
    assert data_source.get_cik_map() == {}

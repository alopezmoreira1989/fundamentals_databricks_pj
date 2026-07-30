"""data_source.sync()/_download() — the cron-driven artifact fetch.

Covers the 404-tolerance fix (2026-07-30 production incident): `dashboard_filings` was added
to `ARTIFACT_NAMES` in this package a run before the pipeline actually published it to the
GitHub Release, and an unconditional `raise_for_status()` on that one missing artifact crashed
the whole `sync_fundamentals_data --force` cron job via a bare exception, which killed the
consumer's deploy script (`set -e`) after code was already installed but before the site's
media/health-check steps ran. `_download` now treats a 404 as "not published yet" (skip, don't
raise) — any other HTTP/network error still raises.

Offline throughout, same isolation style as the (now-removed) test_data_source.py: no real
Django settings are touched (this package's suite runs with no Django configuration at all).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from fundamentals_screener import data_source


class _Resp:
    def __init__(self, status_code=200, content=b""):
        self.status_code = status_code
        self.content = content

    def raise_for_status(self):
        if self.status_code >= 400:
            raise data_source.requests.exceptions.HTTPError(f"{self.status_code} error")


@pytest.fixture
def isolated(monkeypatch, tmp_path):
    monkeypatch.setattr(data_source, "data_dir", lambda: tmp_path)
    monkeypatch.setattr(data_source, "settings", SimpleNamespace())
    return tmp_path


def test_download_returns_false_and_leaves_dest_untouched_on_404(isolated, monkeypatch):
    monkeypatch.setattr(data_source.requests, "get", lambda url, **kw: _Resp(status_code=404))
    dest = isolated / "dashboard_filings.parquet"
    assert data_source._download("dashboard_filings.parquet", dest) is False
    assert not dest.exists()


def test_download_writes_bytes_and_returns_true_on_success(isolated, monkeypatch):
    monkeypatch.setattr(data_source.requests, "get", lambda url, **kw: _Resp(status_code=200, content=b"data"))
    dest = isolated / "dashboard_data.parquet"
    assert data_source._download("dashboard_data.parquet", dest) is True
    assert dest.read_bytes() == b"data"


def test_download_raises_on_non_404_error(isolated, monkeypatch):
    monkeypatch.setattr(data_source.requests, "get", lambda url, **kw: _Resp(status_code=500))
    with pytest.raises(data_source.requests.exceptions.HTTPError):
        data_source._download("dashboard_data.parquet", isolated / "dashboard_data.parquet")


def test_sync_skips_a_missing_artifact_but_still_downloads_the_rest(isolated, monkeypatch):
    def _get(url, **kw):
        if "dashboard_filings" in url:
            return _Resp(status_code=404)
        return _Resp(status_code=200, content=b"ok")

    monkeypatch.setattr(data_source.requests, "get", _get)
    updated = data_source.sync(force=True)
    assert "dashboard_filings.parquet" not in updated
    assert "dashboard_data.parquet" in updated
    assert not (isolated / "dashboard_filings.parquet").exists()
    assert (isolated / "dashboard_data.parquet").exists()

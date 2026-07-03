"""Artifact-cache tests: stale-while-revalidate behaviour, hit/miss metrics, and the warm command.

Fully offline — no fixtures, no network. ``_download`` is monkeypatched to write a placeholder
file and record the URLs it was asked to fetch, and the cache points at a tmp dir. Background
refresh runs inline (``ARTIFACTS_REFRESH_ASYNC=False``) so assertions are deterministic.
"""

from __future__ import annotations

import os
import time
from io import StringIO
from pathlib import Path

import pytest
import requests
from django.core.management import call_command
from infrastructure.storage import artifacts

BASE = "https://example.invalid/latest"


@pytest.fixture
def cache_env(settings, tmp_path, monkeypatch):
    """Point storage at a tmp cache with a stubbed downloader; yields the list of fetched URLs."""
    settings.ARTIFACTS_LOCAL_DIR = ""  # use the cache path, not the fixtures dir
    settings.ARTIFACTS_CACHE_DIR = tmp_path
    settings.ARTIFACTS_BASE_URL = BASE
    settings.ARTIFACTS_TTL = 600
    settings.ARTIFACTS_REFRESH_ASYNC = False  # refresh inline for deterministic assertions
    artifacts.METRICS.reset()

    fetched: list[str] = []

    def fake_download(url: str, dest: Path) -> None:
        fetched.append(url)
        Path(dest).write_bytes(b"ARTIFACT-BYTES")

    monkeypatch.setattr(artifacts, "_download", fake_download)
    return fetched


def _make_stale(path: Path) -> None:
    old = time.time() - 10_000
    os.utime(path, (old, old))


# ── stale-while-revalidate ───────────────────────────────────────────────────────────
def test_cold_miss_blocks_and_downloads(cache_env):
    path = artifacts.parquet_path("dashboard_data")
    assert path.exists()
    assert cache_env == [f"{BASE}/dashboard_data.parquet"]
    assert artifacts.METRICS.snapshot()["misses"] == 1


def test_fresh_copy_is_a_hit_with_no_io(cache_env):
    artifacts.parquet_path("dashboard_data")  # cold miss creates it
    cache_env.clear()
    artifacts.METRICS.reset()
    artifacts.parquet_path("dashboard_data")  # now fresh
    assert cache_env == []  # no download
    assert artifacts.METRICS.snapshot() == {**_zero(), "hits": 1}


def test_stale_serves_immediately_then_revalidates(cache_env):
    path = artifacts.parquet_path("dashboard_data")
    _make_stale(path)
    cache_env.clear()
    artifacts.METRICS.reset()

    returned = artifacts.parquet_path("dashboard_data")
    assert returned == path  # the stale copy was handed back
    # ...and (inline) revalidation re-downloaded it
    assert cache_env == [f"{BASE}/dashboard_data.parquet"]
    assert artifacts.METRICS.snapshot() == {**_zero(), "stale_hits": 1, "refreshes": 1}


def test_stale_still_served_when_refresh_fails(cache_env, monkeypatch):
    path = artifacts.parquet_path("dashboard_data")
    _make_stale(path)
    artifacts.METRICS.reset()

    def boom(url: str, dest: Path) -> None:
        raise requests.RequestException("network down")

    monkeypatch.setattr(artifacts, "_download", boom)
    returned = artifacts.parquet_path("dashboard_data")  # no raise: stale copy is returned
    assert returned == path
    assert artifacts.METRICS.snapshot() == {**_zero(), "stale_hits": 1, "errors": 1}


def test_cold_miss_download_failure_raises(cache_env, monkeypatch):
    def boom(url: str, dest: Path) -> None:
        raise requests.RequestException("network down")

    monkeypatch.setattr(artifacts, "_download", boom)
    with pytest.raises(artifacts.ArtifactError):
        artifacts.parquet_path("dashboard_data")
    assert artifacts.METRICS.snapshot()["errors"] == 1


# ── warm ─────────────────────────────────────────────────────────────────────────────
def test_warm_force_downloads_every_artifact(cache_env):
    report = artifacts.warm(force=True, validate=False)
    assert set(report) == {*artifacts.PARQUET_FILES, "meta"}
    assert all(status == "downloaded" for status in report.values())
    assert len(cache_env) == len(artifacts.PARQUET_FILES) + 1  # every parquet + meta
    assert artifacts.METRICS.snapshot()["refreshes"] == len(cache_env)


def test_warm_respect_ttl_skips_fresh(cache_env):
    artifacts.warm(force=True, validate=False)  # everything now fresh
    cache_env.clear()
    report = artifacts.warm(force=False, validate=False)
    assert cache_env == []  # nothing re-fetched
    assert all(status == "fresh" for status in report.values())


# ── management commands ──────────────────────────────────────────────────────────────
def test_warm_command_runs(cache_env):
    out = StringIO()
    call_command("warm_artifact_cache", "--no-validate", stdout=out)
    assert "artifact cache warmed" in out.getvalue()
    assert len(cache_env) == len(artifacts.PARQUET_FILES) + 1


def test_stats_command_reports_counters(cache_env):
    artifacts.parquet_path("dashboard_data")  # one miss
    out = StringIO()
    call_command("artifact_cache_stats", stdout=out)
    assert "misses: 1" in out.getvalue()


def _zero() -> dict[str, int]:
    return {"hits": 0, "stale_hits": 0, "misses": 0, "refreshes": 0, "errors": 0}

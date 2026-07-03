"""Retrieval, validation, and local caching of the published Release artifacts.

The web layer's analytical source is the exact same set of artifacts the Streamlit app
reads — five files published to the GitHub Release ``latest`` by ``50__publish/51+52``:

    dashboard_data.parquet      dashboard_metrics.parquet   dashboard_prices.parquet
    dashboard_backtest.parquet  dashboard_meta.json

This module ensures a *fresh local copy* of each artifact exists on disk and returns its
path; ``services/duckdb`` then queries the parquet files directly (no per-request pandas
load of the multi-million-row frames). Freshness is TTL-based (``settings.ARTIFACTS_TTL``):
a cached file older than the TTL is re-downloaded. Downloads are validated against the
shared contract in :mod:`fundamentals_pipeline.schemas`, so a drifted publish fails loudly
here rather than rendering a broken page downstream.

No Databricks dependency and no financial logic — this is read-only plumbing.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

import requests
from django.conf import settings

from fundamentals_pipeline.schemas import (
    ARTIFACT_NAMES,
    SchemaError,
    validate_artifact,
    validate_meta,
)

logger = logging.getLogger(__name__)

# artifact key (matches fundamentals_pipeline.schemas.ARTIFACT_NAMES) → published filename.
PARQUET_FILES: dict[str, str] = {
    "dashboard_data": "dashboard_data.parquet",
    "dashboard_metrics": "dashboard_metrics.parquet",
    "dashboard_prices": "dashboard_prices.parquet",
    "dashboard_backtest": "dashboard_backtest.parquet",
}
META_FILE = "dashboard_meta.json"

# Core artifacts must satisfy the contract (hard-fail); prices/backtest degrade gracefully
# — mirrors the Streamlit app, where a bad price frame never blocks the rest of the page.
_HARD_ARTIFACTS = frozenset({"dashboard_data", "dashboard_metrics"})


class ArtifactError(RuntimeError):
    """A published artifact could not be fetched (network/404) or read."""


class _CacheMetrics:
    """Process-local artifact-cache counters (thread-safe).

    In-memory and per-process — with multiple gunicorn workers each keeps its own tally; a
    cross-process metrics backend is the observability task (#157). Enough to answer "are reads
    mostly cache hits?" and to alarm on a spike in misses/errors.

    - ``hits``       fresh cached copy served with no I/O
    - ``stale_hits`` stale copy served immediately while a background refresh runs (SWR)
    - ``misses``     no cached copy → blocking download on the request path (cold start)
    - ``refreshes``  a background/warm re-download completed
    - ``errors``     a download failed
    """

    _KIND_TO_KEY = {
        "hit": "hits",
        "stale": "stale_hits",
        "miss": "misses",
        "refresh": "refreshes",
        "error": "errors",
    }

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts: dict[str, int] = dict.fromkeys(self._KIND_TO_KEY.values(), 0)

    def record(self, kind: str) -> None:
        key = self._KIND_TO_KEY[kind]
        with self._lock:
            self._counts[key] += 1

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counts)

    def reset(self) -> None:
        with self._lock:
            self._counts = dict.fromkeys(self._KIND_TO_KEY.values(), 0)


METRICS = _CacheMetrics()

# In-flight background refreshes, so a burst of readers triggers at most one download per file.
_inflight_lock = threading.Lock()
_inflight: set[str] = set()


def _refresh_in_background(filename: str, url: str, dest: Path) -> None:
    """Re-download ``filename`` off the request path (stale-while-revalidate).

    De-duped via ``_inflight``. Runs on a daemon thread unless ``ARTIFACTS_REFRESH_ASYNC`` is
    False (tests set it False for deterministic assertions). A failed refresh is logged, never
    raised — the caller has already been handed the stale copy.
    """
    with _inflight_lock:
        if filename in _inflight:
            return
        _inflight.add(filename)

    def _work() -> None:
        try:
            _download(url, dest)
            METRICS.record("refresh")
            logger.info("artifact cache refreshed: %s", filename)
        except requests.RequestException as exc:
            METRICS.record("error")
            logger.warning("artifact cache refresh failed for %s: %s", filename, exc)
        finally:
            with _inflight_lock:
                _inflight.discard(filename)

    if getattr(settings, "ARTIFACTS_REFRESH_ASYNC", True):
        threading.Thread(target=_work, name=f"artifact-refresh-{filename}", daemon=True).start()
    else:
        _work()


def _cache_dir() -> Path:
    d = Path(settings.ARTIFACTS_CACHE_DIR)
    d.mkdir(parents=True, exist_ok=True)
    return d


def _local_dir() -> Path | None:
    raw = getattr(settings, "ARTIFACTS_LOCAL_DIR", "") or ""
    return Path(raw) if raw else None


def _is_fresh(path: Path) -> bool:
    """True if ``path`` exists and is younger than the configured TTL."""
    if not path.exists():
        return False
    age = time.time() - path.stat().st_mtime
    return age < settings.ARTIFACTS_TTL


def _download(url: str, dest: Path) -> None:
    """Fetch ``url`` to ``dest`` atomically (temp file + rename), so a concurrent reader
    never observes a half-written artifact."""
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    fd, tmp_name = tempfile.mkstemp(dir=str(dest.parent), suffix=".part")
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(resp.content)
        os.replace(tmp, dest)  # atomic on the same filesystem
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)


def parquet_path(name: str) -> Path:
    """Return a filesystem path to a fresh local copy of parquet artifact ``name``.

    ``name`` is one of :data:`fundamentals_pipeline.schemas.ARTIFACT_NAMES`. With
    ``ARTIFACTS_LOCAL_DIR`` set, the file is served straight from that directory (offline
    dev / fixtures). Otherwise the GitHub Release ``latest`` copy is downloaded when the
    cached copy is missing or older than ``ARTIFACTS_TTL``.

    Raises :class:`ArtifactError` if the file cannot be produced.
    """
    if name not in PARQUET_FILES:
        raise ValueError(f"unknown artifact {name!r}; expected one of {tuple(PARQUET_FILES)}")
    filename = PARQUET_FILES[name]

    local = _local_dir()
    if local is not None:
        path = local / filename
        if not path.exists():
            raise ArtifactError(f"local artifact {path} not found (ARTIFACTS_LOCAL_DIR={local})")
        return path

    dest = _cache_dir() / filename
    url = f"{settings.ARTIFACTS_BASE_URL}/{filename}"
    if dest.exists():
        if _is_fresh(dest):
            METRICS.record("hit")
        else:
            # Stale-while-revalidate: hand back the stale copy now, refresh off the request path.
            METRICS.record("stale")
            _refresh_in_background(filename, url, dest)
        return dest
    # Cold miss: nothing cached, so this request must block on the download.
    METRICS.record("miss")
    try:
        _download(url, dest)
    except requests.RequestException as exc:
        METRICS.record("error")
        raise ArtifactError(f"could not fetch {url}: {exc}") from exc
    return dest


def meta() -> dict[str, Any]:
    """Return the parsed, schema-validated ``dashboard_meta.json`` (tickers, fy_ranges, …).

    Cached to disk with the same TTL as the parquet artifacts. Raises
    :class:`ArtifactError` on a fetch failure and :class:`SchemaError` on contract drift.
    """
    local = _local_dir()
    if local is not None:
        path = local / META_FILE
        if not path.exists():
            raise ArtifactError(f"local artifact {path} not found (ARTIFACTS_LOCAL_DIR={local})")
        data = json.loads(path.read_text(encoding="utf-8"))
    else:
        dest = _cache_dir() / META_FILE
        url = f"{settings.ARTIFACTS_BASE_URL}/{META_FILE}"
        if dest.exists():
            if _is_fresh(dest):
                METRICS.record("hit")
            else:
                METRICS.record("stale")
                _refresh_in_background(META_FILE, url, dest)
        else:
            METRICS.record("miss")
            try:
                _download(url, dest)
            except requests.RequestException as exc:
                METRICS.record("error")
                raise ArtifactError(f"could not fetch {url}: {exc}") from exc
        data = json.loads(dest.read_text(encoding="utf-8"))

    violations = validate_meta(data)
    if violations:
        raise SchemaError("dashboard_meta failed schema validation:\n  - " + "\n  - ".join(violations))
    return data


def validate_cached(name: str) -> list[str]:
    """Read a small slice of parquet artifact ``name`` and check it against the contract.

    Returns the list of violations (empty ⇒ valid). Reads only the column headers +
    a head sample via pandas, so it is cheap even for the multi-million-row frames.
    """
    import pandas as pd  # local import: pandas is only needed for validation, not for queries

    path = parquet_path(name)
    # nrows-limited read to keep validation O(1) w.r.t. table size.
    df = pd.read_parquet(path).head(256)
    return validate_artifact(name, df)


def ensure_valid(names: tuple[str, ...] = ARTIFACT_NAMES) -> None:
    """Validate the named artifacts, hard-failing only on the core data/metrics frames.

    Prices/backtest violations are returned as a no-op here (they degrade gracefully at the
    query layer, exactly like the Streamlit app). Raises :class:`SchemaError` if a *hard*
    artifact (``dashboard_data`` / ``dashboard_metrics``) is missing/invalid.
    """
    problems: list[str] = []
    for name in names:
        if name not in PARQUET_FILES:
            continue
        try:
            violations = validate_cached(name)
        except ArtifactError:
            if name in _HARD_ARTIFACTS:
                raise
            continue
        if violations and name in _HARD_ARTIFACTS:
            problems.extend(violations)
    if problems:
        raise SchemaError("Published artifacts are incompatible:\n  - " + "\n  - ".join(problems))


def _warm_one(filename: str) -> str:
    """Download ``filename`` into the cache (blocking). Returns a per-file status string."""
    dest = _cache_dir() / filename
    url = f"{settings.ARTIFACTS_BASE_URL}/{filename}"
    try:
        _download(url, dest)
        METRICS.record("refresh")
        return "downloaded"
    except requests.RequestException as exc:
        METRICS.record("error")
        return f"error: {exc}"


def warm(*, force: bool = True, validate: bool = True) -> dict[str, str]:
    """Fetch a fresh local copy of every published artifact (blocking) — pre-warm on deploy and
    on the publish cadence, so request-path reads are cache hits, not cold downloads.

    ``force`` (default) re-downloads even within TTL, to align the cache with a just-published
    Release; ``force=False`` skips artifacts still fresh. ``validate`` schema-checks the core
    frames afterwards (raises :class:`SchemaError` on drift). With ``ARTIFACTS_LOCAL_DIR`` set
    there is nothing to fetch. Returns a per-artifact status report.
    """
    if _local_dir() is not None:
        report = {name: "local" for name in (*PARQUET_FILES, "meta")}
    else:
        report = {}
        for name, filename in PARQUET_FILES.items():
            report[name] = "fresh" if (not force and _is_fresh(_cache_dir() / filename)) else _warm_one(filename)
        report["meta"] = (
            "fresh" if (not force and _is_fresh(_cache_dir() / META_FILE)) else _warm_one(META_FILE)
        )
    if validate:
        ensure_valid()
    logger.info("artifact cache warm complete: %s", report)
    return report

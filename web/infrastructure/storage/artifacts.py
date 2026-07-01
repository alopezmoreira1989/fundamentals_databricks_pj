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
import os
import tempfile
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
    if not _is_fresh(dest):
        url = f"{settings.ARTIFACTS_BASE_URL}/{filename}"
        try:
            _download(url, dest)
        except requests.RequestException as exc:
            if dest.exists():
                # Serve the stale cached copy rather than failing if the network blips.
                return dest
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
        if not _is_fresh(dest):
            url = f"{settings.ARTIFACTS_BASE_URL}/{META_FILE}"
            try:
                _download(url, dest)
            except requests.RequestException as exc:
                if not dest.exists():
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

"""Retrieval, validation, and local caching of the published Release artifacts.

Public API:
    ``parquet_path(name)`` — fresh local path to a parquet artifact (stale-while-revalidate).
    ``meta()``             — parsed + validated ``dashboard_meta.json``.
    ``warm(force=…)``      — pre-warm/refresh the whole cache (deploy + scheduled).
    ``METRICS``            — process-local cache hit/miss counters.
    ``validate_cached``/``ensure_valid`` — contract checks vs ``fundamentals_pipeline.schemas``.
"""

from .artifacts import (
    META_FILE,
    METRICS,
    PARQUET_FILES,
    ArtifactError,
    ensure_valid,
    meta,
    parquet_path,
    validate_cached,
    warm,
)

__all__ = [
    "METRICS",
    "META_FILE",
    "PARQUET_FILES",
    "ArtifactError",
    "ensure_valid",
    "meta",
    "parquet_path",
    "validate_cached",
    "warm",
]

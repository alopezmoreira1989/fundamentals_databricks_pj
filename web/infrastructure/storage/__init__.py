"""Retrieval, validation, and local caching of the published Release artifacts (Phase 2).

Public API:
    ``parquet_path(name)`` — fresh local path to a parquet artifact (download-on-stale).
    ``meta()``             — parsed + validated ``dashboard_meta.json``.
    ``validate_cached``/``ensure_valid`` — contract checks vs ``fundamentals_pipeline.schemas``.
"""

from .artifacts import (
    META_FILE,
    PARQUET_FILES,
    ArtifactError,
    ensure_valid,
    meta,
    parquet_path,
    validate_cached,
)

__all__ = [
    "ArtifactError",
    "META_FILE",
    "PARQUET_FILES",
    "ensure_valid",
    "meta",
    "parquet_path",
    "validate_cached",
]

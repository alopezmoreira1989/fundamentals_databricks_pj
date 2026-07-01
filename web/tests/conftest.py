"""Shared fixtures for the web-layer tests.

The services tests run fully offline against the Streamlit fixture artifacts (the same
published-artifact shape, committed under ``60__frontends/61__streamlit/fixtures/`` but
gitignored). If the fixtures aren't present locally, the artifact-backed tests skip —
mirroring the root suite's fixture-gated tests.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# web/ → repo root → the Streamlit fixtures directory.
_REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = _REPO_ROOT / "fundamentals_pipeline" / "60__frontends" / "61__streamlit" / "fixtures"
_REQUIRED = ("dashboard_data.parquet", "dashboard_metrics.parquet", "dashboard_meta.json")


def _fixtures_available() -> bool:
    return all((FIXTURE_DIR / f).exists() for f in _REQUIRED)


@pytest.fixture
def artifacts_from_fixtures(settings):
    """Point the storage layer at the local fixture directory (no network)."""
    if not _fixtures_available():
        pytest.skip(f"Streamlit fixtures not present at {FIXTURE_DIR}")
    settings.ARTIFACTS_LOCAL_DIR = str(FIXTURE_DIR)
    return FIXTURE_DIR

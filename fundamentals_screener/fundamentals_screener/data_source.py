"""Fetches and caches the daily fundamentals data export from fundamentals_databricks_pj.

That pipeline publishes 6 parquet artifacts + a meta JSON to a GitHub Release tagged
"latest" (a moving tag, republished daily). This module downloads and caches them locally
under ``FUNDAMENTALS_DATA_PATH``, validated against the ``fundamentals_pipeline`` schema
contract — it never queries Databricks or the pipeline repo directly.

Deliberately cron-driven, not lazy-fetch-on-read: the target hosting for this package's
original consumer is plain CGI (mod_cgi, no persistent process between requests), so a
background-thread refresh — the pattern the pipeline's own ``web/`` Django app uses — would
never survive from one request to the next. Call :func:`sync` from a scheduled job (a cron
running ``manage.py sync_fundamentals_data``, see ``management/commands/``); the repository
layer then only ever reads what's already on disk, no network on the request path at all.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import requests
from django.conf import settings

# The lightweight, pandas-free sibling of `schemas` — sync()/get_meta()/list_tickers() (the
# request-path functions) never need pandas, only the plain ARTIFACT_NAMES string tuple.
# validate() below defers importing the full `schemas` module (which does need pandas, for its
# dtype-level DataFrame checks) since that function only ever runs from the cron-driven
# sync_fundamentals_data management command, never on a real request. See artifacts.py's own
# docstring for the full split rationale — this was costing every request ~400ms (pandas's
# cold-import time) under this package's no-persistent-process (CGI) deployment, just to read
# a constant that never needed pandas at all.
from fundamentals_pipeline.artifacts import ARTIFACT_NAMES

RELEASE_BASE_URL = (
    "https://github.com/alopezmoreira1989/fundamentals_databricks_pj/releases/download/latest"
)
META_FILE = "dashboard_meta.json"


def data_dir() -> Path:
    path = Path(settings.FUNDAMENTALS_DATA_PATH)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _download(filename: str, dest: Path) -> None:
    response = requests.get(f"{RELEASE_BASE_URL}/{filename}", timeout=60)
    response.raise_for_status()
    dest.write_bytes(response.content)


def sync(force: bool = False) -> list[str]:
    """Download the latest artifacts to ``FUNDAMENTALS_DATA_PATH``. Returns the filenames
    updated (empty list ⇒ everything was already cached and ``force`` was not set).

    ``dashboard_filings.parquet`` (the Filings tab's SEC 10-K/10-Q list — see
    ``fundamentals_pipeline/artifacts.py``) downloads here like every other artifact, since
    it's in ``ARTIFACT_NAMES`` — no separate SEC fetch of our own needed. That's a deliberate
    simplification: an earlier version of this function also made its own small SEC request
    here for a ticker→CIK map, which needed its own ``SEC_USER_AGENT`` setting on every
    consumer host and caused a real production incident when one host's wasn't configured
    (2026-07-29). Moving the whole SEC-filing fetch into the pipeline
    (``15__fetch_sec_filings.py``) removed that dependency entirely.
    """
    directory = data_dir()
    filenames = [f"{name}.parquet" for name in ARTIFACT_NAMES] + [META_FILE]
    updated = []
    for filename in filenames:
        dest = directory / filename
        if force or not dest.exists():
            _download(filename, dest)
            updated.append(filename)
    return updated


def validate() -> list[str]:
    """Validate cached artifacts against the schema contract. Empty list means all valid."""
    import duckdb

    # Deferred: the only caller of this function is the cron-driven sync_fundamentals_data
    # management command, never a real request — see this module's own top-of-file comment on
    # why ARTIFACT_NAMES is imported from the lightweight `artifacts` module instead.
    from fundamentals_pipeline import schemas

    directory = data_dir()
    violations = []
    for name in ARTIFACT_NAMES:
        path = directory / f"{name}.parquet"
        if not path.exists():
            violations.append(f"{name}: not cached yet")
            continue
        df = duckdb.sql("SELECT * FROM read_parquet(?)", params=[str(path)]).df()
        violations.extend(schemas.validate_artifact(name, df))

    meta_path = directory / META_FILE
    if not meta_path.exists():
        violations.append("meta: not cached yet")
    else:
        violations.extend(schemas.validate_meta(get_meta()))
    return violations


def get_meta() -> dict[str, Any]:
    meta_path = data_dir() / META_FILE
    return json.loads(meta_path.read_text(encoding="utf-8"))


def list_tickers() -> list[dict[str, Any]]:
    return get_meta().get("tickers", [])

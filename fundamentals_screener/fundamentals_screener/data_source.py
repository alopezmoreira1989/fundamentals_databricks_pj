"""Fetches and caches the daily fundamentals data export from fundamentals_databricks_pj.

That pipeline publishes 5 parquet artifacts + a meta JSON to a GitHub Release tagged
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

from fundamentals_pipeline import schemas

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
    updated (empty list ⇒ everything was already cached and ``force`` was not set)."""
    directory = data_dir()
    filenames = [f"{name}.parquet" for name in schemas.ARTIFACT_NAMES] + [META_FILE]
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

    directory = data_dir()
    violations = []
    for name in schemas.ARTIFACT_NAMES:
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

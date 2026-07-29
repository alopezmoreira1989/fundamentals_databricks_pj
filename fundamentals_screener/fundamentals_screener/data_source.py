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

# Filings tab (issue #318): a ticker->CIK cache for linking straight out to SEC EDGAR's own
# filing-browse page, NOT a per-filing list — this app's CGI/no-persistent-process hosting has
# no request-path network at all (see this module's own docstring), so `web/`'s richer,
# live-per-request Filings tab (infrastructure/filings.py, calling SEC's submissions API on
# every page load) isn't an option here. A single small ticker-map fetch, done here in the
# cron-driven sync, is.
SEC_TICKER_INDEX_URL = "https://www.sec.gov/files/company_tickers.json"
CIK_MAP_FILE = "cik_map.json"

# SEC blocks requests without a real, identifying User-Agent (org + contact email) — same class
# of gotcha as the root pipeline's 01__tickers.py placeholder (see CLAUDE.md). Ships inert; the
# host MUST set a real one via the SEC_USER_AGENT Django setting (see README) or SEC will simply
# reject the request (403) — logged as a warning by _sync_cik_map below, not raised (see its
# own docstring for why this one step is deliberately lenient, unlike _download).
_SEC_USER_AGENT_PLACEHOLDER = "fundamentals_screener (configure SEC_USER_AGENT in settings.py)"


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
    filenames = [f"{name}.parquet" for name in ARTIFACT_NAMES] + [META_FILE]
    updated = []
    for filename in filenames:
        dest = directory / filename
        if force or not dest.exists():
            _download(filename, dest)
            updated.append(filename)
    # Runs AFTER the loop above: needs list_tickers() (backed by META_FILE), which the loop
    # just ensured is on disk (whether freshly downloaded this run or already cached).
    if _sync_cik_map(force=force):
        updated.append(CIK_MAP_FILE)
    return updated


def _sync_cik_map(force: bool = False) -> bool:
    """Cache ``{ticker: 10-digit CIK}`` for this app's own ticker universe (Filings tab
    link-out — see this module's own top-of-file comment). Skips if already cached and not
    ``force``. Returns whether it fetched.

    Deliberately NEVER raises — unlike ``_download`` above, whose targets are essential (the
    whole app is non-functional without them), the CIK map is optional, best-effort data for
    one tab's link-out. ``sync()`` runs directly inside the production deploy script (not just
    an independent cron), so a failure in this one non-essential step must never take down the
    rest of that run — including the essential parquet/meta refresh already completed earlier
    in the SAME call. Confirmed as a real incident (2026-07-29): an unconfigured
    ``SEC_USER_AGENT`` produced a 403 here, which (before this fix) propagated and killed the
    whole deploy script via its ``set -e``, before it ever reached the media-file copy or
    health check. Prints a clear, visible warning instead of raising — an operator diagnosing
    "why is the Filings tab empty" needs this logged somewhere findable, just not fatal.
    """
    dest = data_dir() / CIK_MAP_FILE
    if dest.exists() and not force:
        return False
    headers = {"User-Agent": getattr(settings, "SEC_USER_AGENT", _SEC_USER_AGENT_PLACEHOLDER)}
    try:
        response = requests.get(SEC_TICKER_INDEX_URL, headers=headers, timeout=30)
        response.raise_for_status()
        idx = response.json()
        sec_map = {entry["ticker"].upper(): str(entry["cik_str"]).zfill(10) for entry in idx.values()}
    except Exception as exc:
        print(
            f"⚠ Could not refresh the SEC ticker→CIK map ({exc}) — the Filings "
            f"tab will show \"not available\" until this succeeds. If this is a 403, set a "
            f"real SEC_USER_AGENT (see the README)."
        )
        return False
    our_tickers = {t["ticker"] for t in list_tickers()}
    cik_map = {ticker: cik for ticker, cik in sec_map.items() if ticker in our_tickers}
    dest.write_text(json.dumps(cik_map), encoding="utf-8")
    return True


def get_cik_map() -> dict[str, str]:
    """``{ticker: 10-digit CIK}``, or ``{}`` on ANY failure to load it — never synced yet, SEC
    was unreachable on a prior sync, a corrupt cache file, or (the case that matters for
    callers outside a fully-configured Django project, e.g. this package's own test suite,
    which configures Django nowhere at all) ``FUNDAMENTALS_DATA_PATH`` itself not being set up.
    `get_summary()` calls this unconditionally for every ticker, so a CIK-cache problem must
    never take down the rest of a company page over one optional field — same "degrade, don't
    crash" convention as this file's own ``usd_fx_rate``."""
    try:
        path = data_dir() / CIK_MAP_FILE
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


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

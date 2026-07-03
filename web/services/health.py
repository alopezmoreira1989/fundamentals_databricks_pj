"""Health / readiness aggregation — a storage-agnostic use-case over the checkable dependencies.

Two orthogonal signals, following the Kubernetes-style split:

- **Liveness** (``/healthz``): is the process itself up and serving? No dependency checks — a
  failing liveness probe means *restart me*, so it must not flap on a transient DB/network blip.
- **Readiness** (``/readyz``): can this instance serve real traffic *right now*? It checks the
  things a request needs — the application database (auth/user data) and a locally cached copy
  of the core analytical artifacts — and returns not-ready (503) if any hard dependency is down,
  so a load balancer drains this instance instead of serving errors.

The view layer calls :func:`readiness` and maps ``ready`` → HTTP 200/503; it never touches the
database or the filesystem directly. Dependency probes live below this tier (Django's DB
connection; :mod:`infrastructure.storage.artifacts`), keeping the strict
``views → services → infrastructure`` layering intact.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from django.db import connections
from django.db.utils import Error as DatabaseError
from infrastructure.storage import artifacts

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Check:
    """Outcome of a single dependency probe."""

    name: str
    ok: bool
    detail: str = ""


@dataclass(frozen=True)
class Readiness:
    """Aggregate readiness: ``ready`` is true only if every hard-required check passed."""

    ready: bool
    checks: tuple[Check, ...] = field(default_factory=tuple)
    cache_metrics: dict[str, int] = field(default_factory=dict)


def _check_database() -> Check:
    """Confirm the default database answers a trivial query — a request needing auth/user data
    is dead in the water without it."""
    try:
        with connections["default"].cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
    except DatabaseError as exc:  # pragma: no cover - exercised via a patched connection in tests
        logger.warning("readiness: database check failed: %s", exc)
        return Check(name="database", ok=False, detail=str(exc))
    return Check(name="database", ok=True)


def _check_artifacts() -> Check:
    """Confirm the core analytical artifacts are present locally (network-free). Missing files
    mean every analytical page would cold-download on the request path — not ready to serve."""
    status = artifacts.cached_status()
    missing = sorted(name for name, s in status.items() if not s["present"])
    if missing:
        return Check(name="artifacts", ok=False, detail=f"not cached: {', '.join(missing)}")
    stale = sorted(name for name, s in status.items() if not s["fresh"])
    # Stale-but-present is still servable (stale-while-revalidate), so it does not gate readiness.
    detail = f"stale (serving while revalidating): {', '.join(stale)}" if stale else ""
    return Check(name="artifacts", ok=True, detail=detail)


def readiness() -> Readiness:
    """Run all readiness probes and aggregate them into a single ready/not-ready verdict."""
    checks = (_check_database(), _check_artifacts())
    ready = all(c.ok for c in checks)
    return Readiness(ready=ready, checks=checks, cache_metrics=artifacts.METRICS.snapshot())

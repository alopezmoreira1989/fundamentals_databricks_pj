"""Health probe views — HTTP only. Liveness is a constant; readiness delegates to the service.

Both are unauthenticated, uncached, and cheap so a load balancer / uptime monitor can hit them
on a tight interval. They live at the site root (``/healthz``, ``/readyz``) rather than under
``/api/`` so they are independent of API versioning and DRF content negotiation.
"""

from __future__ import annotations

from dataclasses import asdict

from django.http import HttpRequest, JsonResponse
from django.views.decorators.cache import never_cache
from services import health


@never_cache
def livez(request: HttpRequest) -> JsonResponse:
    """Liveness: the process is up and can serve a request. No dependency checks — a 200 here
    means *do not restart me*."""
    return JsonResponse({"status": "ok"})


@never_cache
def readyz(request: HttpRequest) -> JsonResponse:
    """Readiness: can this instance serve real traffic now? 200 when every hard dependency
    (database, cached core artifacts) is healthy, 503 otherwise so the balancer drains it."""
    result = health.readiness()
    body = {
        "status": "ready" if result.ready else "not_ready",
        "checks": [asdict(check) for check in result.checks],
        "cache_metrics": result.cache_metrics,
    }
    return JsonResponse(body, status=200 if result.ready else 503)

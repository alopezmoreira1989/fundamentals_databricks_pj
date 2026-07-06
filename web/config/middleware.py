"""Request-scoped observability + admin-hardening middleware.

:class:`RequestLogMiddleware` assigns (or honours an upstream) request id, times each request,
emits one structured access-log record per request, and echoes the id back as ``X-Request-ID``
so a client/proxy can correlate a response with its server-side log line. The id is published on
the :data:`config.log.request_id_var` context var, so *every* log record emitted while the
request is in flight — not just this access log — carries the same ``request_id``.

:class:`AdminIPAllowlistMiddleware` restricts ``ADMIN_URL_PATH`` to an operator-configured IP
allowlist (see GitHub issue #181, item 3 — admin exposure).
"""

from __future__ import annotations

import ipaddress
import logging
import time
import uuid
from collections.abc import Callable

from django.conf import settings
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden

from config.log import request_id_var

logger = logging.getLogger("web.request")

_REQUEST_ID_HEADER = "HTTP_X_REQUEST_ID"
# Liveness/readiness probes fire on a tight interval; logging each one buries real traffic.
_QUIET_PATHS = frozenset({"/healthz", "/readyz"})


class RequestLogMiddleware:
    def __init__(self, get_response: Callable[[HttpRequest], HttpResponse]) -> None:
        self.get_response = get_response

    def __call__(self, request: HttpRequest) -> HttpResponse:
        # Honour an id set by an upstream proxy (distributed tracing); otherwise mint one.
        incoming = request.META.get(_REQUEST_ID_HEADER, "")
        rid = incoming.strip()[:64] or uuid.uuid4().hex
        token = request_id_var.set(rid)
        start = time.monotonic()
        try:
            response = self.get_response(request)
        finally:
            request_id_var.reset(token)
        response["X-Request-ID"] = rid

        if request.path not in _QUIET_PATHS:
            self._log_access(request, response, start, rid)
        return response

    @staticmethod
    def _log_access(request: HttpRequest, response: HttpResponse, start: float, rid: str) -> None:
        # Access logging must never turn a good response into a 500. Resolving request.user
        # lazily loads the session user from the DB, so if the database is down this raises —
        # exactly when a /readyz 503 must still get through. Guard the whole block.
        try:
            duration_ms = round((time.monotonic() - start) * 1000, 1)
            user = getattr(request, "user", None)
            try:
                authed = user is not None and user.is_authenticated
            except Exception:
                authed = False
            user_id = str(user.pk) if authed and user is not None else "anon"
            logger.info(
                "%s %s %s",
                request.method,
                request.path,
                response.status_code,
                extra={
                    "request_id": rid,
                    "method": request.method,
                    "path": request.path,
                    "status": response.status_code,
                    "duration_ms": duration_ms,
                    "user": user_id,
                },
            )
        except Exception:  # pragma: no cover - defensive; logging must not break the request
            logger.warning("access-log emission failed", exc_info=True)


def _client_ip(request: HttpRequest) -> str:
    # Behind Render/Fly's single reverse-proxy hop, the proxy APPENDS the real client IP to
    # X-Forwarded-For rather than replacing it, so trusting only the entry NUM_PROXIES hops from
    # the right (default 1) means a client can't spoof an earlier hop to bypass the allowlist.
    xff = request.META.get("HTTP_X_FORWARDED_FOR", "")
    hops = [h.strip() for h in xff.split(",") if h.strip()]
    if hops:
        num_proxies = getattr(settings, "NUM_PROXIES", 1) or 1
        return hops[max(len(hops) - num_proxies, 0)]
    return request.META.get("REMOTE_ADDR", "")


class AdminIPAllowlistMiddleware:
    """403s a request under ``ADMIN_URL_PATH`` whose client IP isn't in ``ADMIN_IP_ALLOWLIST``.

    No-ops (allows everything through) when the allowlist is empty — the default, so local dev
    and a fresh prod deploy (before a static admin-access IP is known) are never locked out by
    accident; enforcement is opt-in. Both settings are read fresh per request rather than cached
    at ``__init__`` — Django builds the middleware chain once at process start, so caching here
    would go stale under ``override_settings``/pytest-django's ``settings`` fixture.
    """

    def __init__(self, get_response: Callable[[HttpRequest], HttpResponse]) -> None:
        self.get_response = get_response

    def __call__(self, request: HttpRequest) -> HttpResponse:
        allowlist: list[str] = settings.ADMIN_IP_ALLOWLIST
        if allowlist and request.path.startswith(f"/{settings.ADMIN_URL_PATH}"):
            try:
                addr = ipaddress.ip_address(_client_ip(request))
            except ValueError:
                return HttpResponseForbidden("Forbidden")
            networks = [ipaddress.ip_network(cidr, strict=False) for cidr in allowlist]
            if not any(addr in net for net in networks):
                return HttpResponseForbidden("Forbidden")
        return self.get_response(request)

"""Request-scoped observability middleware.

:class:`RequestLogMiddleware` assigns (or honours an upstream) request id, times each request,
emits one structured access-log record per request, and echoes the id back as ``X-Request-ID``
so a client/proxy can correlate a response with its server-side log line. The id is published on
the :data:`config.log.request_id_var` context var, so *every* log record emitted while the
request is in flight — not just this access log — carries the same ``request_id``.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable

from django.http import HttpRequest, HttpResponse

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
            duration_ms = round((time.monotonic() - start) * 1000, 1)
            user = getattr(request, "user", None)
            user_id = str(user.pk) if user is not None and user.is_authenticated else "anon"
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
        return response

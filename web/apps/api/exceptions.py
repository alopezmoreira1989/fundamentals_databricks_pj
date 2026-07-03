"""Consistent error envelope for the REST API.

Every handled API error is normalised to a single shape so clients parse one thing::

    {"error": {"status": 404, "message": "unknown ticker 'ZZZZ'"}}

DRF's default handler already turns known exceptions (``ValidationError``, ``NotFound``,
``MethodNotAllowed``, …) into a response; we only reshape its body. Unhandled exceptions
(handler returns ``None``) are left to Django's 500 path — we don't leak internals as 200s.
"""

from __future__ import annotations

from typing import Any

from rest_framework.response import Response
from rest_framework.views import exception_handler as drf_exception_handler


def _flatten_message(detail: Any) -> str:
    """Reduce DRF's error detail (str | list | dict, nested) to one human-readable line."""
    if isinstance(detail, dict):
        # Prefer a top-level "detail"; otherwise join "field: message" for each entry.
        if "detail" in detail:
            return _flatten_message(detail["detail"])
        return "; ".join(f"{field}: {_flatten_message(msg)}" for field, msg in detail.items())
    if isinstance(detail, (list, tuple)):
        return "; ".join(_flatten_message(item) for item in detail)
    return str(detail)


def api_exception_handler(exc: Exception, context: dict[str, Any]) -> Response | None:
    """Wrap DRF's handled responses in the ``{"error": {status, message}}`` envelope."""
    response = drf_exception_handler(exc, context)
    if response is None:
        return None
    response.data = {
        "error": {
            "status": response.status_code,
            "message": _flatten_message(response.data),
        }
    }
    return response

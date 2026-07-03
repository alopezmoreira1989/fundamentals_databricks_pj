"""Structured logging primitives — a stdlib-only JSON formatter and a per-request id.

Kept dependency-free on purpose (no ``python-json-logger``): the formatter emits one JSON
object per log record so the platform's log collector can index fields (level, logger,
request_id, status, duration_ms) instead of grepping free text. The request id is a
:class:`contextvars.ContextVar` so it survives across the middleware/view call stack without
being threaded through every function, and lands on every record emitted while a request is
in flight (see :class:`config.middleware.RequestLogMiddleware`).
"""

from __future__ import annotations

import contextvars
import datetime as _dt
import json
import logging

# Set at the start of each request, cleared at the end. Empty string outside a request
# (management commands, startup) so the field is always present and always a string.
request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="")

# LogRecord attributes that are framework noise or already surfaced as top-level JSON keys;
# everything else attached via ``logger.info(..., extra={...})`` is copied into the output.
_RESERVED = frozenset(
    logging.makeLogRecord({}).__dict__.keys()
    | {"message", "asctime", "taskName"}
)


class JSONFormatter(logging.Formatter):
    """Render a log record as a single line of JSON with a stable core schema plus any extras."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": _dt.datetime.fromtimestamp(record.created, tz=_dt.timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        rid = request_id_var.get()
        if rid:
            payload["request_id"] = rid
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        # Structured context passed via ``extra=`` (status, duration_ms, method, path, user, …).
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        return json.dumps(payload, default=str)

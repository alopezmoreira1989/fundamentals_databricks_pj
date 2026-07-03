"""Error-tracking wiring (Sentry) — gated on configuration, inert without it.

:func:`init_sentry` is a no-op unless a ``SENTRY_DSN`` is supplied, so the same code path runs
safely in dev/CI (no DSN, no ``sentry-sdk`` import) and in production (DSN present, errors
captured with release + environment tags for regression attribution). ``sentry_sdk`` is imported
lazily *inside* the DSN-present branch, keeping it a production-only dependency that neither dev
nor the test suite needs installed.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def init_sentry(
    *,
    dsn: str,
    environment: str,
    release: str = "",
    traces_sample_rate: float = 0.0,
) -> bool:
    """Initialise Sentry when ``dsn`` is set; return whether it was initialised.

    ``release`` (e.g. the deployed git SHA) tags every event so a spike can be pinned to the
    deploy that introduced it. PII is not sent by default. Returns ``False`` (and does nothing)
    when ``dsn`` is empty — the normal dev/CI case — so callers need no conditional of their own.
    """
    if not dsn:
        return False
    try:
        import sentry_sdk
        from sentry_sdk.integrations.django import DjangoIntegration
    except ImportError:  # pragma: no cover - prod-only dependency
        logger.warning("SENTRY_DSN is set but sentry-sdk is not installed; error tracking disabled")
        return False

    sentry_sdk.init(
        dsn=dsn,
        environment=environment,
        release=release or None,
        integrations=[DjangoIntegration()],
        traces_sample_rate=traces_sample_rate,
        send_default_pii=False,
    )
    return True

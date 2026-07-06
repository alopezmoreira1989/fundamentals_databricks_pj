"""Production settings — hardened, secrets from the environment only.

Inherits everything from ``base`` and locks it down: DEBUG off, no insecure fallbacks for the
secret/hosts/database (a missing one fails loud at boot rather than running unsafely), HTTPS
enforcement (SSL redirect, HSTS, secure cookies), and WhiteNoise static serving. Verified by
``manage.py check --deploy`` (see tests/test_prod_settings.py, which runs it with --fail-level
WARNING). Nothing here is environment-specific beyond what the env provides.
"""

from config.observability import init_sentry

from .base import *  # noqa: F403
from .base import MIDDLEWARE, REST_FRAMEWORK, env

DEBUG = False

# No insecure defaults in prod: each of these must come from the environment / secret store.
SECRET_KEY = env("DJANGO_SECRET_KEY")
ALLOWED_HOSTS = env.list("DJANGO_ALLOWED_HOSTS")
DATABASES = {"default": env.db("DATABASE_URL")}
# Reuse DB connections across requests (seconds) instead of reconnecting each time.
DATABASES["default"]["CONN_MAX_AGE"] = env.int("DJANGO_CONN_MAX_AGE", default=60)

# ── HTTPS / transport security ────────────────────────────────────────────────────────
# Behind a TLS-terminating proxy (nginx / load balancer) that sets X-Forwarded-Proto.
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
SECURE_SSL_REDIRECT = True
# The platform's health checks hit the internal port over plain HTTP; without this they'd get a
# 301 to https and be marked unhealthy. Exempt the probe paths only (matched without the leading
# slash) — all real traffic is still force-redirected to HTTPS.
SECURE_REDIRECT_EXEMPT = [r"^healthz$", r"^readyz$"]
SECURE_HSTS_SECONDS = 31_536_000  # 1 year
SECURE_HSTS_INCLUDE_SUBDOMAINS = True
SECURE_HSTS_PRELOAD = True
SECURE_CONTENT_TYPE_NOSNIFF = True

# Behind a TLS-terminating platform proxy (Render, Fly, ...) on a custom/https domain, Django 4+
# requires the site's HTTPS origin(s) to be trusted or the login/signup POST forms fail CSRF
# origin verification (403). Supply the scheme-qualified origins (e.g.
# https://fundamentals-web.onrender.com, https://fundamentals.example.com).
CSRF_TRUSTED_ORIGINS = env.list("DJANGO_CSRF_TRUSTED_ORIGINS", default=[])

# Proxy hop count so REMOTE_ADDR-derived client IP (used by the API rate throttle and
# config.middleware.AdminIPAllowlistMiddleware) is read from the correct X-Forwarded-For entry.
# Render/Fly each add a single proxy hop; override if that changes.
#
# Exposed BOTH as a bare setting (AdminIPAllowlistMiddleware reads settings.NUM_PROXIES directly)
# AND injected into REST_FRAMEWORK below — DRF's AnonRateThrottle (rest_framework.throttling.
# SimpleRateThrottle.get_ident) only ever reads NUM_PROXIES via api_settings, which resolves from
# settings.REST_FRAMEWORK, NOT from a bare top-level Django setting. Without this, DRF silently
# falls back to trusting the entire raw X-Forwarded-For value as the client identity — for a
# single real proxy hop that happens to still work, but it means a client can bypass the anon
# rate limit by sending an arbitrary fake X-Forwarded-For prefix per request (the proxy appends,
# not replaces, so "fake-id, real-ip" changes identity every time the fake prefix changes).
NUM_PROXIES = env.int("DJANGO_NUM_PROXIES", default=1)
REST_FRAMEWORK = {**REST_FRAMEWORK, "NUM_PROXIES": NUM_PROXIES}

# ── cookies ───────────────────────────────────────────────────────────────────────────
SESSION_COOKIE_SECURE = True
CSRF_COOKIE_SECURE = True
SESSION_COOKIE_HTTPONLY = True
X_FRAME_OPTIONS = "DENY"

# ── static files (WhiteNoise) ─────────────────────────────────────────────────────────
# Insert WhiteNoise directly after SecurityMiddleware so it serves collected static files
# (compressed + hashed manifest) without a separate web server. Locate SecurityMiddleware by
# name rather than position, so prepending observability middleware in base doesn't misplace it.
_security = MIDDLEWARE.index("django.middleware.security.SecurityMiddleware")
MIDDLEWARE = [
    *MIDDLEWARE[: _security + 1],
    "whitenoise.middleware.WhiteNoiseMiddleware",
    *MIDDLEWARE[_security + 1 :],
]
STORAGES = {
    "default": {"BACKEND": "django.core.files.storage.FileSystemStorage"},
    "staticfiles": {"BACKEND": "whitenoise.storage.CompressedManifestStaticFilesStorage"},
}

# Emails over SMTP in production (host/credentials from the environment).
EMAIL_BACKEND = "django.core.mail.backends.smtp.EmailBackend"

# ── logging ───────────────────────────────────────────────────────────────────────────
# Structured JSON to stdout so the platform's log collector indexes fields (level, logger,
# request_id, status, duration_ms) instead of grepping free text. Each request emits one access
# log via ``config.middleware.RequestLogMiddleware``; every record carries the request id.
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"json": {"()": "config.log.JSONFormatter"}},
    "handlers": {"console": {"class": "logging.StreamHandler", "formatter": "json"}},
    "root": {"handlers": ["console"], "level": "INFO"},
    "loggers": {
        "django.request": {"handlers": ["console"], "level": "WARNING", "propagate": False},
        "web.request": {"handlers": ["console"], "level": "INFO", "propagate": False},
    },
}

# ── error tracking (Sentry) ───────────────────────────────────────────────────────────
# Inert unless SENTRY_DSN is provided. APP_RELEASE (the deployed git SHA) tags events so a
# regression can be pinned to the deploy that shipped it.
init_sentry(
    dsn=env("SENTRY_DSN", default=""),
    environment=env("SENTRY_ENVIRONMENT", default="production"),
    release=env("APP_RELEASE", default=""),
    traces_sample_rate=env.float("SENTRY_TRACES_SAMPLE_RATE", default=0.0),
)

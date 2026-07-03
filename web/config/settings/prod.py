"""Production settings — hardened, secrets from the environment only.

Inherits everything from ``base`` and locks it down: DEBUG off, no insecure fallbacks for the
secret/hosts/database (a missing one fails loud at boot rather than running unsafely), HTTPS
enforcement (SSL redirect, HSTS, secure cookies), and WhiteNoise static serving. Verified by
``manage.py check --deploy`` (see tests/test_prod_settings.py, which runs it with --fail-level
WARNING). Nothing here is environment-specific beyond what the env provides.
"""

from .base import *  # noqa: F403
from .base import MIDDLEWARE, env

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
SECURE_HSTS_SECONDS = 31_536_000  # 1 year
SECURE_HSTS_INCLUDE_SUBDOMAINS = True
SECURE_HSTS_PRELOAD = True
SECURE_CONTENT_TYPE_NOSNIFF = True

# ── cookies ───────────────────────────────────────────────────────────────────────────
SESSION_COOKIE_SECURE = True
CSRF_COOKIE_SECURE = True
SESSION_COOKIE_HTTPONLY = True
X_FRAME_OPTIONS = "DENY"

# ── static files (WhiteNoise) ─────────────────────────────────────────────────────────
# Insert WhiteNoise directly after SecurityMiddleware so it serves collected static files
# (compressed + hashed manifest) without a separate web server.
MIDDLEWARE = [MIDDLEWARE[0], "whitenoise.middleware.WhiteNoiseMiddleware", *MIDDLEWARE[1:]]
STORAGES = {
    "default": {"BACKEND": "django.core.files.storage.FileSystemStorage"},
    "staticfiles": {"BACKEND": "whitenoise.storage.CompressedManifestStaticFilesStorage"},
}

# Emails over SMTP in production (host/credentials from the environment).
EMAIL_BACKEND = "django.core.mail.backends.smtp.EmailBackend"

# ── logging ───────────────────────────────────────────────────────────────────────────
# Minimal, stdout-only so the platform's log collector captures it. Richer error tracking
# (Sentry etc.) is a separate observability task.
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"simple": {"format": "%(asctime)s %(levelname)s %(name)s %(message)s"}},
    "handlers": {"console": {"class": "logging.StreamHandler", "formatter": "simple"}},
    "root": {"handlers": ["console"], "level": "INFO"},
    "loggers": {"django.request": {"handlers": ["console"], "level": "WARNING", "propagate": False}},
}

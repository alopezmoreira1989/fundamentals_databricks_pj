"""Base Django settings shared across environments.

Secrets and environment-specific values come from the environment (a `web/.env` file in
local dev, injected variables in Docker/prod), read via ``django-environ`` — nothing
sensitive is hardcoded. Environment-specific overrides live in ``dev.py`` / (later) ``prod.py``.
"""
from pathlib import Path

import environ

# web/ — the directory that contains manage.py, config/, apps/, services/, templates/.
BASE_DIR = Path(__file__).resolve().parents[2]

env = environ.Env()
# Load web/.env if present (local dev). In Docker/prod the variables are injected directly,
# so a missing file is fine.
environ.Env.read_env(BASE_DIR / ".env")

SECRET_KEY = env("DJANGO_SECRET_KEY", default="dev-insecure-change-me")
DEBUG = env.bool("DJANGO_DEBUG", default=False)
ALLOWED_HOSTS = env.list("DJANGO_ALLOWED_HOSTS", default=[])

DJANGO_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
]
THIRD_PARTY_APPS = [
    "rest_framework",
    "drf_spectacular",
]
# Presentation + user-domain apps only. All financial logic lives in the installed
# `fundamentals_pipeline` package; nothing here computes ratios, valuations, or metrics.
LOCAL_APPS = [
    "apps.users",
    "apps.companies",
    "apps.screener",
    "apps.valuation",
    "apps.watchlists",
    "apps.favorites",
    "apps.history",
    "apps.api",
    "apps.artifacts",
    "apps.health",
]
INSTALLED_APPS = DJANGO_APPS + THIRD_PARTY_APPS + LOCAL_APPS

MIDDLEWARE = [
    # Outermost: assign the request id + emit the structured access log around everything else.
    "config.middleware.RequestLogMiddleware",
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "config.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "config.wsgi.application"

DATABASES = {
    "default": env.db(
        "DATABASE_URL",
        default="postgres://postgres:postgres@db:5432/fundamentals",
    ),
}

# Custom user model, set BEFORE the first migration (no migration ran in Phase 1/2), so the
# project never touches Django's default User. `users` is the app *label* (the app's name is
# `apps.users`; Django derives the label from the last component). See apps/users/models.py.
AUTH_USER_MODEL = "users.User"

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

# Session auth flow (mounted at /accounts/ by config.urls). LOGIN_URL is the named login
# route so @login_required redirects there; both post-login/logout land on the home page.
LOGIN_URL = "users:login"
LOGIN_REDIRECT_URL = "home"
LOGOUT_REDIRECT_URL = "home"

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"
# Project-level source assets (the editorial theme); collected into STATIC_ROOT for prod.
STATICFILES_DIRS = [BASE_DIR / "static"]
# Collected static root (populated by `collectstatic`); WhiteNoise serves it in prod.
STATIC_ROOT = BASE_DIR / "staticfiles"
MEDIA_URL = "media/"
MEDIA_ROOT = BASE_DIR / "media"

# Framework tables (auth/admin/sessions/contenttypes) keep BigAutoField. OUR application
# models must instead declare an explicit UUID primary key (see docs/architecture.md → UUID
# primary keys) — never AutoField/BigAutoField for application entities.
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# ── Published data artifacts (read-only analytical source) ───────────────────────────
# The web layer reads the same GitHub Release `latest` parquet/JSON artifacts as the
# Streamlit app (published by 50__publish/51+52). It NEVER queries Databricks at request
# time. `services/storage` fetches + caches these locally; `services/duckdb` queries the
# cached files. The schema is validated against `fundamentals_pipeline.schemas`.
ARTIFACTS_BASE_URL = env(
    "ARTIFACTS_BASE_URL",
    default="https://github.com/alopezmoreira1989/fundamentals_databricks_pj/releases/download/latest",
)
# Where fetched artifacts are cached on disk (gitignored). One copy shared by all workers.
ARTIFACTS_CACHE_DIR = Path(env("ARTIFACTS_CACHE_DIR", default=str(BASE_DIR / ".artifact_cache")))
# Re-download an artifact once its cached copy is older than this many seconds.
ARTIFACTS_TTL = env.int("ARTIFACTS_TTL", default=600)
# Dev/offline override: if set, artifacts are read straight from this local directory
# (e.g. the Streamlit `fixtures/`) and the network is never touched. Empty ⇒ use the Release.
ARTIFACTS_LOCAL_DIR = env("ARTIFACTS_LOCAL_DIR", default="")
# Stale-while-revalidate: refresh a stale cached artifact on a background daemon thread so no
# request pays the download latency. False forces the refresh inline (deterministic tests).
ARTIFACTS_REFRESH_ASYNC = env.bool("ARTIFACTS_REFRESH_ASYNC", default=True)

# ── REST API (apps/api) ──────────────────────────────────────────────────────────────
# The API exposes the same public, read-only analytical read model the HTML pages render —
# no user data, no writes. So: JSON only (no browsable API / templates), open access, and no
# authentication/session coupling (nothing to protect, and it keeps the endpoints CSRF-free).
REST_FRAMEWORK = {
    "DEFAULT_RENDERER_CLASSES": ["rest_framework.renderers.JSONRenderer"],
    "DEFAULT_AUTHENTICATION_CLASSES": [],
    "DEFAULT_PERMISSION_CLASSES": ["rest_framework.permissions.AllowAny"],
    "UNAUTHENTICATED_USER": None,
    # Rate-limit the public API: every call runs a DuckDB query over the parquet artifacts, so
    # unbounded anonymous traffic is an abuse/DoS vector. All requests are anonymous (no auth
    # classes), so AnonRateThrottle keys them by client IP. Behind a proxy the real IP comes
    # from X-Forwarded-For — set NUM_PROXIES to the proxy hop count (see prod.py) or every
    # client collapses onto the proxy's IP and shares one bucket. Rate is env-tunable.
    "DEFAULT_THROTTLE_CLASSES": ["rest_framework.throttling.AnonRateThrottle"],
    "DEFAULT_THROTTLE_RATES": {"anon": env("API_THROTTLE_ANON", default="120/min")},
    # URL-path versioning: routes live under /api/<version>/ (only v1 today). request.version
    # is populated, so a future v2 is an additive route + serializer, not a breaking move.
    "DEFAULT_VERSIONING_CLASS": "rest_framework.versioning.URLPathVersioning",
    "DEFAULT_VERSION": "v1",
    "ALLOWED_VERSIONS": ["v1"],
    # One error envelope for the whole API: {"error": {"status", "message"}}.
    "EXCEPTION_HANDLER": "apps.api.exceptions.api_exception_handler",
    # OpenAPI 3 schema generation (drf-spectacular).
    "DEFAULT_SCHEMA_CLASS": "drf_spectacular.openapi.AutoSchema",
}

# OpenAPI schema + Swagger/Redoc docs (drf-spectacular). The schema is generated from the
# viewsets/serializers and pinned to v1; the committed web/api-schema.yml is drift-checked
# by tests (regenerated in-process and compared).
SPECTACULAR_SETTINGS = {
    "TITLE": "Fundamentals API",
    "DESCRIPTION": (
        "Read-only REST API over the published fundamentals read model — company summaries "
        "and derived metrics, the single-metric screener, and intrinsic-value data. All data "
        "is public and precomputed by the pipeline; there are no write endpoints."
    ),
    "VERSION": "1.0.0",
    # The schema view serves only the endpoints; the Swagger/Redoc HTML is served separately.
    "SERVE_INCLUDE_SCHEMA": False,
}

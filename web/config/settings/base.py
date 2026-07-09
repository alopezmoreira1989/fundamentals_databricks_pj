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

# ── Admin hardening (#181 item 3: admin exposure) ────────────────────────────────────
# Env-configurable path so an operator can move it off the well-known default in prod without a
# code change; unset ⇒ unchanged "admin/" (local-dev default). Normalized to a single trailing
# slash regardless of how the env var is supplied.
ADMIN_URL_PATH = env("ADMIN_URL_PATH", default="admin/").strip("/") + "/"
# IP/CIDR allowlist enforced by config.middleware.AdminIPAllowlistMiddleware. Empty (default) ⇒
# the middleware no-ops (allow all) — see that class's docstring for why.
ADMIN_IP_ALLOWLIST = env.list("ADMIN_IP_ALLOWLIST", default=[])

DJANGO_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django.contrib.humanize",
]
THIRD_PARTY_APPS = [
    "rest_framework",
    "drf_spectacular",
    "axes",
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
    # Early (before Session/Csrf/Auth) so a blocked /admin/ hit is cheap and still logged by
    # RequestLogMiddleware above.
    "config.middleware.AdminIPAllowlistMiddleware",
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    # Last, per django-axes' own requirement — it needs to run after AuthenticationMiddleware
    # and every other middleware has had a chance to run first (#181 item 2).
    "axes.middleware.AxesMiddleware",
]

# AxesBackend MUST be first — it wraps authenticate() to enforce lockout (raising before
# falling through to ModelBackend's real credential check) rather than after (#181 item 2).
# Django's built-in LoginView (apps/users/urls.py) calls authenticate() under the hood, so it
# gets this for free; apps.users.views.signup's direct login() call is unaffected — it never
# calls authenticate(), see the backend= kwarg there for why a second backend needs it anyway.
AUTHENTICATION_BACKENDS = [
    "axes.backends.AxesBackend",
    "django.contrib.auth.backends.ModelBackend",
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

# PostgreSQL (Neon in production, or the docker-compose `db` service) when DATABASE_URL is set;
# otherwise a zero-setup local SQLite file — no Docker/Postgres server needed to just run the
# app. `prod.py` overrides this with `env.db("DATABASE_URL")` and NO default, so production
# fails loudly at boot if the variable is missing rather than silently falling back here.
_database_url = env("DATABASE_URL", default="")
if _database_url:
    DATABASES = {"default": env.db("DATABASE_URL")}
else:
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": BASE_DIR / "db.sqlite3",
        }
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

# ── Login brute-force protection (#181 item 2) ────────────────────────────────────────────
# Locks the specific (username, ip_address) PAIR after AXES_FAILURE_LIMIT failed
# /accounts/login/ attempts — not the IP or username alone, so one bad actor can't
# collateral-lock every other user behind a shared IP (office/NAT network), and an attacker
# can't lock a real account out from every IP by spraying failures from many addresses.
AXES_FAILURE_LIMIT = env.int("AXES_FAILURE_LIMIT", default=5)
AXES_LOCKOUT_PARAMETERS = [["username", "ip_address"]]
AXES_COOLOFF_TIME = 1  # hour — lockout expires on its own; no admin unlock needed
AXES_RESET_ON_SUCCESS = True  # a real login clears the counter, so stray typos don't add up

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

# Logo.dev publishable key for company logos (hotlinked from their CDN). Empty ⇒ fall back to
# an editorial monogram everywhere. The key is publishable (safe in the client-side image URL).
LOGO_DEV_KEY = env("LOGO_DEV_KEY", default="")

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

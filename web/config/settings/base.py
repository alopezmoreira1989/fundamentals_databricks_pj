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
]
INSTALLED_APPS = DJANGO_APPS + LOCAL_APPS

MIDDLEWARE = [
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

# WSGI_APPLICATION is intentionally unset in Phase 1 (runserver uses the default); the
# wsgi.py entry point is added with Gunicorn in the deployment phase.

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

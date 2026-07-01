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

# NOTE (Phase 4): a custom user model — AUTH_USER_MODEL = "apps.users.User" — will be
# introduced in the users app BEFORE the first `migrate`. No migration runs in Phase 1,
# so this can still be set without the painful post-migrate swap.

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"
MEDIA_URL = "media/"
MEDIA_ROOT = BASE_DIR / "media"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

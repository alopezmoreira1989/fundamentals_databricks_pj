"""Minimal Django settings for fundamentals_screener's own test suite.

Every OTHER test in this suite runs with zero Django configuration (in-memory DuckDB fixtures
injected directly into repository constructors — see their own docstrings). Only the Update
model/views/admin/feed (tests/test_updates.py) need a real Django app registry and database, so
this file exists solely to let pytest-django run those — it is not shipped to consumers
(excluded from the built package) and is not a template for a host project's own settings.
"""

from __future__ import annotations

SECRET_KEY = "test-only-not-for-production"  # noqa: S105 -- test-only settings module, never deployed
DEBUG = True
ALLOWED_HOSTS = ["testserver"]
USE_TZ = True

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django.contrib.humanize",
    "fundamentals_screener",
]

MIDDLEWARE = [
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.common.CommonMiddleware",
]

ROOT_URLCONF = "tests.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}

STATIC_URL = "/static/"

# Required by data_source.py/repository.py — unused by the Update-model tests, but the app's
# system checks (checks.py) and other modules read it at import/config time.
FUNDAMENTALS_DATA_PATH = "/tmp/fundamentals-screener-test-data"

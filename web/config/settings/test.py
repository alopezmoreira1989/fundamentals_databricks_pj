"""Test settings.

SQLite in-memory so the suite runs anywhere without a PostgreSQL server (CI, local dev via
the venv). The ORM features exercised here — a UUID pk, a unique-email constraint, password
hashing, authentication — behave identically on SQLite, so this keeps the tests fast and
dependency-free. Postgres-specific behaviour, if any is ever added, gets its own marked tests.
"""
from .dev import *  # noqa: F401,F403
from .dev import REST_FRAMEWORK

# Disable API throttling by default so the functional API tests aren't rate-limited by
# accumulated per-IP request history; the dedicated throttle test re-enables it via
# override_settings with a tiny rate.
REST_FRAMEWORK = {**REST_FRAMEWORK, "DEFAULT_THROTTLE_RATES": {"anon": None}}

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}

# Cheap, fast hashing for tests (never used outside the test settings).
PASSWORD_HASHERS = ["django.contrib.auth.hashers.MD5PasswordHasher"]

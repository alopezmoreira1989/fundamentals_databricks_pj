"""Production-settings guardrail: ``manage.py check --deploy`` must pass clean under
``config.settings.prod``.

Run in a subprocess (a fresh interpreter with the prod settings module + the env vars prod
requires), so the assertion exercises the real boot path without disturbing the in-process test
settings. ``--fail-level WARNING`` makes any deploy warning (weak secret, DEBUG on, missing
HSTS/secure-cookie, open ALLOWED_HOSTS, …) a non-zero exit.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

WEB_DIR = Path(__file__).resolve().parents[1]


def test_check_deploy_passes_under_prod_settings():
    env = {
        **os.environ,
        "DJANGO_SETTINGS_MODULE": "config.settings.prod",
        # A throwaway key that satisfies W009 (>=50 chars, >=5 unique, no insecure prefix).
        "DJANGO_SECRET_KEY": "test-secret-key-with-plenty-of-unique-characters-0123456789-abcdef",
        "DJANGO_ALLOWED_HOSTS": "fundamentals.example.com",
        "DATABASE_URL": "postgres://u:p@localhost:5432/db",  # check doesn't connect
    }
    result = subprocess.run(
        [sys.executable, "manage.py", "check", "--deploy", "--fail-level", "WARNING"],
        cwd=WEB_DIR,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"check --deploy failed:\n{result.stdout}\n{result.stderr}"


def test_prod_num_proxies_reaches_drf_api_settings():
    """Regression test: DRF's AnonRateThrottle reads NUM_PROXIES via api_settings, which only
    resolves from settings.REST_FRAMEWORK — NOT a bare top-level Django setting. prod.py must
    inject NUM_PROXIES into REST_FRAMEWORK, or the anon rate limit silently trusts a
    client-spoofable X-Forwarded-For value as the throttle identity.
    """
    env = {
        **os.environ,
        "DJANGO_SETTINGS_MODULE": "config.settings.prod",
        "DJANGO_SECRET_KEY": "test-secret-key-with-plenty-of-unique-characters-0123456789-abcdef",
        "DJANGO_ALLOWED_HOSTS": "fundamentals.example.com",
        "DATABASE_URL": "postgres://u:p@localhost:5432/db",
        "DJANGO_NUM_PROXIES": "2",
    }
    script = (
        "import django; django.setup()\n"
        "from django.conf import settings\n"
        "from rest_framework.settings import api_settings\n"
        "assert settings.REST_FRAMEWORK['NUM_PROXIES'] == 2, settings.REST_FRAMEWORK\n"
        "assert api_settings.NUM_PROXIES == 2, api_settings.NUM_PROXIES\n"
        "print('OK')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script], cwd=WEB_DIR, env=env, capture_output=True, text=True
    )
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    assert "OK" in result.stdout

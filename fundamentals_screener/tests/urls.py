"""Minimal root URLconf for the test-only Django settings (tests/settings.py) — mounts the
package under the same path the README tells a real host project to use."""

from __future__ import annotations

from django.urls import include, path

urlpatterns = [
    path("apps/screener/", include("fundamentals_screener.urls")),
]

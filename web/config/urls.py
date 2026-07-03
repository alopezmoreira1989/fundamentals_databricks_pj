"""Root URL configuration.

The home page is a static landing template (``TemplateView``) so no app needs view logic
in Phase 1. App URLconfs are mounted in their dedicated phases.
"""
from django.contrib import admin
from django.urls import include, path
from django.views.generic import TemplateView
from drf_spectacular.views import (
    SpectacularAPIView,
    SpectacularRedocView,
    SpectacularSwaggerView,
)

urlpatterns = [
    path("admin/", admin.site.urls),
    # Liveness/readiness probes at the site root (before content routes), for the platform
    # health check and uptime monitor.
    path("", include("apps.health.urls")),
    path("", TemplateView.as_view(template_name="home.html"), name="home"),
    # User-facing usage guide (static content).
    path("help/", TemplateView.as_view(template_name="help.html"), name="help"),
    # Session auth (login / logout / signup).
    path("accounts/", include("apps.users.urls")),
    # User-owned data (login-required).
    path("watchlist/", include("apps.watchlists.urls")),
    path("favorites/", include("apps.favorites.urls")),
    path("history/", include("apps.history.urls")),
    # Read-only analytical pages + JSON, served from the published artifacts via DuckDB.
    path("companies/", include("apps.companies.urls")),
    path("screener/", include("apps.screener.urls")),
    path("valuation/", include("apps.valuation.urls")),
    # OpenAPI 3 schema (pinned to v1) + interactive docs (Swagger UI / Redoc). These exact
    # paths must precede the versioned API include below, whose greedy "<version>/" segment
    # would otherwise swallow "schema"/"docs"/"redoc" as an (invalid) version.
    path("api/schema/", SpectacularAPIView.as_view(api_version="v1"), name="schema"),
    path("api/docs/", SpectacularSwaggerView.as_view(url_name="schema"), name="swagger-ui"),
    path("api/redoc/", SpectacularRedocView.as_view(url_name="schema"), name="redoc"),
    # REST API (DRF) over the same read model, for a decoupled frontend / third parties.
    path("api/", include("apps.api.urls")),
]

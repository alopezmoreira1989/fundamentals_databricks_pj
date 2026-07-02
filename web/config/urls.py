"""Root URL configuration.

The home page is a static landing template (``TemplateView``) so no app needs view logic
in Phase 1. App URLconfs are mounted in their dedicated phases.
"""
from django.contrib import admin
from django.urls import include, path
from django.views.generic import TemplateView

urlpatterns = [
    path("admin/", admin.site.urls),
    path("", TemplateView.as_view(template_name="home.html"), name="home"),
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
]

"""URLconf for the ``fundamentals_screener`` app.

Namespaced (``app_name``), so the host project mounts it anywhere with
``path("apps/screener/", include("fundamentals_screener.urls"))`` and reverses with
``{% url 'fundamentals_screener:screen' %}`` etc.
"""

from __future__ import annotations

from django.urls import path

from . import views
from .feeds import UpdatesFeed

app_name = "fundamentals_screener"

urlpatterns = [
    path("", views.screen, name="screen"),
    path("data/", views.screen_data, name="screen_data"),
    path("about/", views.about, name="about"),
    # Updates (development journal) routes must come BEFORE the <str:ticker>/ catch-all below —
    # "updates" would otherwise resolve as a (nonexistent) ticker instead of this section.
    path("updates/", views.updates_list, name="updates_list"),
    path("updates/feed/", UpdatesFeed(), name="updates_feed"),
    path("updates/<slug:slug>/", views.update_detail, name="update_detail"),
    path("<str:ticker>/", views.company_detail, name="company_detail"),
    path("<str:ticker>/data/", views.company_data, name="company_data"),
    path("<str:ticker>/news/", views.company_news, name="company_news"),
]

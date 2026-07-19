"""URLconf for the ``fundamentals_screener`` app.

Namespaced (``app_name``), so the host project mounts it anywhere with
``path("apps/screener/", include("fundamentals_screener.urls"))`` and reverses with
``{% url 'fundamentals_screener:screen' %}`` etc.
"""

from __future__ import annotations

from django.urls import path

from . import views

app_name = "fundamentals_screener"

urlpatterns = [
    path("", views.screen, name="screen"),
    path("data/", views.screen_data, name="screen_data"),
    path("<str:ticker>/", views.company_detail, name="company_detail"),
    path("<str:ticker>/data/", views.company_data, name="company_data"),
    path("<str:ticker>/news/", views.company_news, name="company_news"),
    path("<str:ticker>/valuation/", views.valuation, name="valuation"),
    path("<str:ticker>/valuation/data/", views.valuation_data, name="valuation_data"),
]

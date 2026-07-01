"""Root URL configuration.

The home page is a static landing template (``TemplateView``) so no app needs view logic
in Phase 1. App URLconfs are mounted in their dedicated phases.
"""
from django.contrib import admin
from django.urls import path
from django.views.generic import TemplateView

urlpatterns = [
    path("admin/", admin.site.urls),
    path("", TemplateView.as_view(template_name="home.html"), name="home"),
]

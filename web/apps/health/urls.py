"""Health probe URLconf — mounted at the site root in ``config.urls``.

Exposes ``/healthz`` (liveness) and ``/readyz`` (readiness). The ``z`` suffix is the
conventional health-endpoint spelling and avoids colliding with any real content path.
"""

from django.urls import path

from . import views

urlpatterns = [
    path("healthz", views.livez, name="livez"),
    path("readyz", views.readyz, name="readyz"),
]

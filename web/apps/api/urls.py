"""REST API URLconf — a DRF ``DefaultRouter`` over the read-only viewsets, URL-path versioned.

Mounted at ``/api/`` (see ``config.urls``); the ``<version>`` segment makes the live routes:
- ``/api/v1/companies/`` (list) and ``/api/v1/companies/<ticker>/`` (retrieve)
- ``/api/v1/screener/`` (list; ``?metric=`` required)
- ``/api/v1/valuation/<ticker>/`` (retrieve)

Only ``v1`` is allowed (``ALLOWED_VERSIONS`` in settings); any other version segment 404s
through the API error envelope. No app namespace, so DRF's router root/reverse resolve plainly.
"""

from django.urls import include, path
from rest_framework.routers import DefaultRouter

from . import views

router = DefaultRouter()
router.register("companies", views.CompanyViewSet, basename="company")
router.register("screener", views.ScreenerViewSet, basename="screener")
router.register("valuation", views.ValuationViewSet, basename="valuation")

urlpatterns = [
    path("<version>/", include(router.urls)),
]

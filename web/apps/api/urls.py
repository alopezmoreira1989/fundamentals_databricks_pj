"""REST API URLconf — a DRF ``DefaultRouter`` over the read-only viewsets.

Mounted at ``/api/`` (see ``config.urls``). Routes:
- ``/api/companies/`` (list) and ``/api/companies/<ticker>/`` (retrieve)
- ``/api/screener/`` (list; ``?metric=`` required)
- ``/api/valuation/<ticker>/`` (retrieve)
"""

from rest_framework.routers import DefaultRouter

from . import views

app_name = "api"

router = DefaultRouter()
router.register("companies", views.CompanyViewSet, basename="company")
router.register("screener", views.ScreenerViewSet, basename="screener")
router.register("valuation", views.ValuationViewSet, basename="valuation")

urlpatterns = router.urls

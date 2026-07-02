from django.urls import path

from . import views

app_name = "valuation"

urlpatterns = [
    path("<str:ticker>/", views.valuation_detail, name="detail"),
]

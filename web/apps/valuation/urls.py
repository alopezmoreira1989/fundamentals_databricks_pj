from django.urls import path

from . import views

app_name = "valuation"

urlpatterns = [
    path("<str:ticker>/", views.valuation_page, name="detail"),
    path("<str:ticker>/data/", views.valuation_data, name="detail_data"),
]

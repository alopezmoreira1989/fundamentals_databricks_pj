from django.urls import path

from . import views

app_name = "forecasting"

urlpatterns = [
    path("<str:ticker>/data/", views.forecast_data, name="detail_data"),
]

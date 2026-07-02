from django.urls import path

from . import views

app_name = "screener"

urlpatterns = [
    path("", views.screen_page, name="screen"),
    path("data/", views.screen_data, name="screen_data"),
]

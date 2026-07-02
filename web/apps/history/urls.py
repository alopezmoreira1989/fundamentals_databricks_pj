from django.urls import path

from . import views

app_name = "history"

urlpatterns = [
    path("", views.history_page, name="list"),
    path("clear/", views.clear, name="clear"),
]

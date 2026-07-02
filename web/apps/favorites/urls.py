from django.urls import path

from . import views

app_name = "favorites"

urlpatterns = [
    path("", views.favorites_page, name="list"),
    path("add/", views.add, name="add"),
    path("remove/", views.remove, name="remove"),
]

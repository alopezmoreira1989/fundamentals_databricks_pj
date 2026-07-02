from django.urls import path

from . import views

app_name = "watchlists"

urlpatterns = [
    path("", views.watchlist_page, name="list"),
    path("add/", views.add, name="add"),
    path("remove/", views.remove, name="remove"),
]

from django.urls import path

from . import views

app_name = "watchlists"

urlpatterns = [
    path("", views.overview, name="overview"),
    path("create/", views.create, name="create"),
    path("add/", views.add, name="add"),
    path("remove/", views.remove, name="remove"),
    path("<uuid:watchlist_id>/", views.detail, name="detail"),
    path("<uuid:watchlist_id>/rename/", views.rename, name="rename"),
    path("<uuid:watchlist_id>/delete/", views.delete, name="delete"),
]

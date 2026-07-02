from django.urls import path

from . import views

app_name = "companies"

urlpatterns = [
    path("<str:ticker>/", views.company_page, name="detail"),
    path("<str:ticker>/data/", views.company_data, name="detail_data"),
]

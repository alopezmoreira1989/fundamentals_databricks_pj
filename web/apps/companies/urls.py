from django.urls import path

from . import views

app_name = "companies"

urlpatterns = [
    path("<str:ticker>/", views.company_detail, name="detail"),
]

"""URL configuration for the SELDON service."""

from django.urls import path

from seldon import views

urlpatterns = [
    path("healthz", views.healthz, name="healthz"),
]

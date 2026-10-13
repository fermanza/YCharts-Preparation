from django.urls import path

from marketdata import views

urlpatterns = [
    path("health/", views.health),
    path("api/securities/", views.securities),
    path("api/securities/<str:symbol>/metrics/", views.metrics),
    path("api/batches/", views.batches),
]

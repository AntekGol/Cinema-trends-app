"""API URL routing."""
from django.urls import path

from . import views

app_name = "api"

urlpatterns = [
    path("trends/daily/", views.DailyTrendListView.as_view(), name="daily"),
    path("trends/weekly/", views.WeeklySummaryListView.as_view(), name="weekly"),
    path("trends/monthly/", views.MonthlyGenreListView.as_view(), name="monthly"),
    path("genres/", views.GenreListView.as_view(), name="genres"),
]

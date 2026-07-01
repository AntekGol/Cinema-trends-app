"""URL routing for the trends app."""
from django.urls import path

from . import views

app_name = "trends"

urlpatterns = [
    path("", views.DashboardView.as_view(), name="dashboard"),
    path("daily/", views.DailyTrendsView.as_view(), name="daily"),
    path("weekly/", views.WeeklyAnalysisView.as_view(), name="weekly"),
    path("monthly/", views.MonthlyReportView.as_view(), name="monthly"),
    path("movie/<int:pk>/", views.MovieDetailView.as_view(), name="movie_detail"),
    path("actor/<int:pk>/", views.ActorProfileView.as_view(), name="actor_profile"),
]

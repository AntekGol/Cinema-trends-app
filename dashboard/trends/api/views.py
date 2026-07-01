"""DRF API views for CineTrends chart data."""
from rest_framework import generics

from trends.models import DailyTrend, Genre, MonthlyGenreStat, WeeklySummary

from .serializers import (
    DailyTrendSerializer,
    GenreSerializer,
    MonthlyGenreStatSerializer,
    WeeklySummarySerializer,
)


class DailyTrendListView(generics.ListAPIView):
    serializer_class = DailyTrendSerializer

    def get_queryset(self):
        qs = DailyTrend.objects.select_related("movie").order_by("position")
        date = self.request.query_params.get("date")
        media_type = self.request.query_params.get("media_type")
        if date:
            qs = qs.filter(date=date)
        if media_type:
            qs = qs.filter(media_type=media_type)
        return qs


class WeeklySummaryListView(generics.ListAPIView):
    serializer_class = WeeklySummarySerializer

    def get_queryset(self):
        qs = WeeklySummary.objects.select_related("movie").order_by("avg_position")
        week = self.request.query_params.get("week_start")
        if week:
            qs = qs.filter(week_start=week)
        return qs


class MonthlyGenreListView(generics.ListAPIView):
    serializer_class = MonthlyGenreStatSerializer

    def get_queryset(self):
        qs = MonthlyGenreStat.objects.select_related("genre").order_by("-trending_count")
        month = self.request.query_params.get("month")
        if month:
            qs = qs.filter(month=month)
        return qs


class GenreListView(generics.ListAPIView):
    serializer_class = GenreSerializer
    queryset = Genre.objects.all()

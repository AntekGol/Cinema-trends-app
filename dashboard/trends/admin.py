"""Admin registration for CineTrends models."""
from django.contrib import admin

from .models import (
    DailyTrend,
    Genre,
    MonthlyGenreStat,
    Movie,
    MovieCast,
    MovieGenre,
    Person,
    WeeklySummary,
)


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ("genre_id", "name")


@admin.register(Movie)
class MovieAdmin(admin.ModelAdmin):
    list_display = ("movie_id", "title", "release_date", "budget", "revenue", "roi")
    search_fields = ("title",)
    list_filter = ("original_language",)


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ("person_id", "name", "known_for_department", "popularity")
    search_fields = ("name",)


@admin.register(DailyTrend)
class DailyTrendAdmin(admin.ModelAdmin):
    list_display = ("date", "position", "movie", "media_type", "popularity", "position_change")
    list_filter = ("date", "media_type")


@admin.register(WeeklySummary)
class WeeklySummaryAdmin(admin.ModelAdmin):
    list_display = ("week_start", "movie", "days_trending", "avg_position", "avg_popularity")
    list_filter = ("week_start",)


@admin.register(MonthlyGenreStat)
class MonthlyGenreStatAdmin(admin.ModelAdmin):
    list_display = ("month", "genre", "trending_count", "avg_popularity")
    list_filter = ("month",)


admin.site.register(MovieGenre)
admin.site.register(MovieCast)

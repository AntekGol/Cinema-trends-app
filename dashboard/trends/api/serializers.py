"""DRF serializers for CineTrends API."""
from rest_framework import serializers

from trends.models import DailyTrend, Genre, MonthlyGenreStat, Movie, Person, WeeklySummary


class GenreSerializer(serializers.ModelSerializer):
    class Meta:
        model = Genre
        fields = ["genre_id", "name"]


class MovieSerializer(serializers.ModelSerializer):
    genres = GenreSerializer(many=True, read_only=True)

    class Meta:
        model = Movie
        fields = [
            "movie_id", "title", "original_language", "overview",
            "release_date", "runtime", "budget", "revenue", "roi",
            "poster_url", "backdrop_url", "genres",
        ]


class DailyTrendSerializer(serializers.ModelSerializer):
    movie_title = serializers.CharField(source="movie.title", read_only=True)
    poster_url = serializers.CharField(source="movie.poster_url", read_only=True)

    class Meta:
        model = DailyTrend
        fields = [
            "id", "movie_id", "movie_title", "poster_url", "media_type",
            "date", "position", "position_change", "popularity",
            "vote_average", "vote_count",
        ]


class WeeklySummarySerializer(serializers.ModelSerializer):
    movie_title = serializers.CharField(source="movie.title", read_only=True)

    class Meta:
        model = WeeklySummary
        fields = [
            "id", "movie_id", "movie_title", "media_type", "week_start",
            "days_trending", "avg_position", "best_position",
            "avg_popularity", "popularity_change_pct",
        ]


class MonthlyGenreStatSerializer(serializers.ModelSerializer):
    genre_name = serializers.CharField(source="genre.name", read_only=True)

    class Meta:
        model = MonthlyGenreStat
        fields = [
            "id", "month", "genre_id", "genre_name", "trending_count",
            "avg_popularity", "avg_vote_average", "total_budget",
            "total_revenue", "avg_roi",
        ]

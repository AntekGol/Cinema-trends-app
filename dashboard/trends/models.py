"""
CineTrends Data Models.

Star schema design:
- Dimension tables: Genre, Movie, Person
- Bridge tables: MovieGenre, MovieCast
- Fact table: DailyTrend
- Aggregation tables: WeeklySummary, MonthlyGenreStat
"""
from django.db import models


class Genre(models.Model):
    """Dimension: Movie/TV genres from TMDB."""
    genre_id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=100)

    class Meta:
        db_table = "dim_genres"
        ordering = ["name"]

    def __str__(self):
        return self.name


class Movie(models.Model):
    """Dimension: Movies and TV shows."""
    movie_id = models.IntegerField(primary_key=True)
    title = models.CharField(max_length=500)
    original_language = models.CharField(max_length=10, blank=True, default="")
    overview = models.TextField(blank=True, default="")
    release_date = models.DateField(null=True, blank=True)
    runtime = models.IntegerField(null=True, blank=True)
    budget = models.BigIntegerField(default=0)
    revenue = models.BigIntegerField(default=0)
    roi = models.FloatField(null=True, blank=True)
    poster_url = models.URLField(max_length=500, blank=True, default="")
    backdrop_url = models.URLField(max_length=500, blank=True, default="")
    genres = models.ManyToManyField(Genre, through="MovieGenre", blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "dim_movies"
        ordering = ["-updated_at"]

    def __str__(self):
        return self.title

    @property
    def budget_display(self):
        """Format budget for display: $160M."""
        if self.budget >= 1_000_000:
            return f"${self.budget / 1_000_000:.0f}M"
        elif self.budget > 0:
            return f"${self.budget:,}"
        return "N/A"

    @property
    def revenue_display(self):
        """Format revenue for display."""
        if self.revenue >= 1_000_000:
            return f"${self.revenue / 1_000_000:.0f}M"
        elif self.revenue > 0:
            return f"${self.revenue:,}"
        return "N/A"


class Person(models.Model):
    """Dimension: Actors, directors, and crew."""
    person_id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=255)
    profile_url = models.URLField(max_length=500, blank=True, default="")
    known_for_department = models.CharField(max_length=50, blank=True, default="")
    popularity = models.FloatField(default=0)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "dim_people"
        ordering = ["-popularity"]

    def __str__(self):
        return self.name


class MovieGenre(models.Model):
    """Bridge: Movie ↔ Genre (many-to-many)."""
    movie = models.ForeignKey(Movie, on_delete=models.CASCADE)
    genre = models.ForeignKey(Genre, on_delete=models.CASCADE)

    class Meta:
        db_table = "bridge_movie_genres"
        unique_together = ("movie", "genre")


class MovieCast(models.Model):
    """Bridge: Movie ↔ Person with role details."""
    movie = models.ForeignKey(Movie, on_delete=models.CASCADE, related_name="cast_members")
    person = models.ForeignKey(Person, on_delete=models.CASCADE, related_name="filmography")
    character_name = models.CharField(max_length=500, blank=True, default="")
    cast_order = models.IntegerField(default=0)

    class Meta:
        db_table = "bridge_movie_cast"
        unique_together = ("movie", "person")
        ordering = ["cast_order"]


class DailyTrend(models.Model):
    """Fact: Daily trending position records."""
    movie = models.ForeignKey(Movie, on_delete=models.CASCADE, related_name="daily_trends")
    media_type = models.CharField(max_length=10, default="movie")
    date = models.DateField()
    position = models.IntegerField()
    position_change = models.IntegerField(default=0)
    popularity = models.FloatField(default=0)
    vote_average = models.FloatField(default=0)
    vote_count = models.IntegerField(default=0)

    class Meta:
        db_table = "fact_daily_trends"
        unique_together = ("movie", "date", "media_type")
        ordering = ["date", "position"]

    def __str__(self):
        arrow = "↑" if self.position_change > 0 else "↓" if self.position_change < 0 else "→"
        return f"#{self.position} {arrow} {self.movie.title} ({self.date})"


class WeeklySummary(models.Model):
    """Aggregation: Weekly trend summary per movie."""
    movie = models.ForeignKey(Movie, on_delete=models.CASCADE, related_name="weekly_summaries")
    media_type = models.CharField(max_length=10, default="movie")
    week_start = models.DateField()
    days_trending = models.IntegerField(default=0)
    avg_position = models.FloatField(default=0)
    best_position = models.IntegerField(default=0)
    avg_popularity = models.FloatField(default=0)
    popularity_change_pct = models.FloatField(default=0)

    class Meta:
        db_table = "agg_weekly_summary"
        ordering = ["-week_start", "avg_position"]


class MonthlyGenreStat(models.Model):
    """Aggregation: Monthly genre-level statistics."""
    month = models.DateField()
    genre = models.ForeignKey(Genre, on_delete=models.CASCADE)
    trending_count = models.IntegerField(default=0)
    avg_popularity = models.FloatField(default=0)
    avg_vote_average = models.FloatField(default=0)
    top_movie = models.ForeignKey(Movie, on_delete=models.SET_NULL, null=True, blank=True)
    total_budget = models.BigIntegerField(default=0)
    total_revenue = models.BigIntegerField(default=0)
    avg_roi = models.FloatField(null=True, blank=True)

    class Meta:
        db_table = "agg_monthly_genres"
        ordering = ["-month", "-trending_count"]

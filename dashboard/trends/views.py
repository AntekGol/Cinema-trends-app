"""
Dashboard views for CineTrends.

Each view queries the database and prepares rich context for templates,
including JSON-serialized chart data for Plotly.js.
"""
import json
from datetime import date, timedelta

from django.db.models import Avg, Count, Max, Min, Q, Sum
from django.views.generic import DetailView, TemplateView

from .models import (
    DailyTrend,
    Genre,
    MonthlyGenreStat,
    Movie,
    MovieCast,
    Person,
    WeeklySummary,
)


class DashboardView(TemplateView):
    """Landing page with KPIs, genre breakdown, and trending overview."""

    template_name = "trends/dashboard.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        today = date.today()

        # KPIs
        ctx["total_movies"] = Movie.objects.count()
        ctx["total_genres"] = Genre.objects.count()
        latest_trend = DailyTrend.objects.order_by("-date").first()
        ctx["latest_date"] = latest_trend.date if latest_trend else today
        ctx["trending_today"] = DailyTrend.objects.filter(
            date=ctx["latest_date"]
        ).count()

        avg_rating = DailyTrend.objects.filter(date=ctx["latest_date"]).aggregate(
            avg=Avg("vote_average")
        )
        ctx["avg_rating"] = round(avg_rating["avg"] or 0, 1)

        # Top 5 trending today
        ctx["top_trending"] = (
            DailyTrend.objects.filter(date=ctx["latest_date"], media_type="movie")
            .select_related("movie")
            .order_by("position")[:5]
        )

        # Genre distribution (for donut chart)
        genre_counts = (
            DailyTrend.objects.filter(date=ctx["latest_date"])
            .values("movie__genres__name")
            .annotate(count=Count("id"))
            .exclude(movie__genres__name__isnull=True)
            .order_by("-count")[:10]
        )
        ctx["genre_chart_data"] = json.dumps(
            {
                "labels": [g["movie__genres__name"] for g in genre_counts],
                "values": [g["count"] for g in genre_counts],
            }
        )

        # Popularity timeline (last 7 days, top 5 movies)
        week_ago = ctx["latest_date"] - timedelta(days=6)
        top_movie_ids = (
            DailyTrend.objects.filter(
                date=ctx["latest_date"], media_type="movie"
            )
            .order_by("position")
            .values_list("movie_id", flat=True)[:5]
        )

        timeline_data = {"dates": [], "series": []}
        if top_movie_ids:
            dates = (
                DailyTrend.objects.filter(
                    movie_id__in=top_movie_ids, date__gte=week_ago
                )
                .values_list("date", flat=True)
                .distinct()
                .order_by("date")
            )
            timeline_data["dates"] = [d.isoformat() for d in dates]

            for movie_id in top_movie_ids:
                movie = Movie.objects.filter(pk=movie_id).first()
                if movie:
                    pops = (
                        DailyTrend.objects.filter(
                            movie_id=movie_id, date__gte=week_ago
                        )
                        .order_by("date")
                        .values_list("popularity", flat=True)
                    )
                    timeline_data["series"].append(
                        {"name": movie.title[:25], "values": list(pops)}
                    )

        ctx["timeline_chart_data"] = json.dumps(timeline_data)

        return ctx


class DailyTrendsView(TemplateView):
    """Daily trending movies/TV with filterable cards."""

    template_name = "trends/daily.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)

        # Date from query param or latest
        date_str = self.request.GET.get("date")
        media_type = self.request.GET.get("media_type", "all")

        latest_trend = DailyTrend.objects.order_by("-date").first()
        if date_str:
            try:
                selected_date = date.fromisoformat(date_str)
            except ValueError:
                selected_date = latest_trend.date if latest_trend else date.today()
        else:
            selected_date = latest_trend.date if latest_trend else date.today()

        trends = DailyTrend.objects.filter(date=selected_date).select_related("movie")
        if media_type != "all":
            trends = trends.filter(media_type=media_type)
        trends = trends.order_by("position")

        ctx["trends"] = trends
        ctx["selected_date"] = selected_date
        ctx["media_type"] = media_type
        ctx["available_dates"] = (
            DailyTrend.objects.values_list("date", flat=True)
            .distinct()
            .order_by("-date")[:30]
        )

        # HTMX partial rendering
        if self.request.htmx:
            self.template_name = "trends/partials/_trend_table.html"

        return ctx


class WeeklyAnalysisView(TemplateView):
    """Weekly summary with position change charts."""

    template_name = "trends/weekly.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)

        latest = WeeklySummary.objects.order_by("-week_start").first()
        selected_week = latest.week_start if latest else date.today()

        summaries = (
            WeeklySummary.objects.filter(week_start=selected_week)
            .select_related("movie")
            .order_by("avg_position")[:20]
        )
        ctx["summaries"] = summaries
        ctx["selected_week"] = selected_week

        # Position chart data
        position_data = {"movies": [], "positions": [], "changes": []}
        for s in summaries[:10]:
            position_data["movies"].append(s.movie.title[:20])
            position_data["positions"].append(s.avg_position)
            position_data["changes"].append(s.popularity_change_pct)

        ctx["position_chart_data"] = json.dumps(position_data)

        ctx["available_weeks"] = (
            WeeklySummary.objects.values_list("week_start", flat=True)
            .distinct()
            .order_by("-week_start")[:12]
        )

        # Top movers (separate queries to avoid reorder-after-slice)
        ctx["top_risers"] = (
            WeeklySummary.objects.filter(week_start=selected_week)
            .select_related("movie")
            .order_by("-popularity_change_pct")[:5]
        )
        ctx["top_fallers"] = (
            WeeklySummary.objects.filter(week_start=selected_week)
            .select_related("movie")
            .order_by("popularity_change_pct")[:5]
        )

        return ctx


class MonthlyReportView(TemplateView):
    """Monthly genre report with heatmap and budget analysis."""

    template_name = "trends/monthly.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)

        latest = MonthlyGenreStat.objects.order_by("-month").first()
        selected_month = latest.month if latest else date.today().replace(day=1)

        stats = (
            MonthlyGenreStat.objects.filter(month=selected_month)
            .select_related("genre", "top_movie")
            .order_by("-trending_count")
        )
        ctx["genre_stats"] = stats
        ctx["selected_month"] = selected_month

        # Genre bar chart data
        genre_chart = {"genres": [], "counts": [], "popularity": []}
        for s in stats[:12]:
            genre_chart["genres"].append(s.genre.name)
            genre_chart["counts"].append(s.trending_count)
            genre_chart["popularity"].append(s.avg_popularity)

        ctx["genre_chart_data"] = json.dumps(genre_chart)

        # Budget vs Revenue scatter
        movies_with_budget = Movie.objects.filter(budget__gt=0, revenue__gt=0)[:50]
        scatter_data = {
            "titles": [m.title[:20] for m in movies_with_budget],
            "budgets": [m.budget / 1_000_000 for m in movies_with_budget],
            "revenues": [m.revenue / 1_000_000 for m in movies_with_budget],
            "roi": [m.roi or 0 for m in movies_with_budget],
        }
        ctx["scatter_chart_data"] = json.dumps(scatter_data)

        ctx["available_months"] = (
            MonthlyGenreStat.objects.values_list("month", flat=True)
            .distinct()
            .order_by("-month")[:12]
        )

        return ctx


class MovieDetailView(DetailView):
    """Movie deep dive with popularity timeline and cast."""

    model = Movie
    template_name = "trends/movie_detail.html"
    context_object_name = "movie"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        movie = self.object

        # Popularity timeline
        trends = (
            DailyTrend.objects.filter(movie=movie)
            .order_by("date")
            .values("date", "popularity", "position")
        )
        timeline = {
            "dates": [t["date"].isoformat() for t in trends],
            "popularity": [t["popularity"] for t in trends],
            "positions": [t["position"] for t in trends],
        }
        ctx["timeline_data"] = json.dumps(timeline)

        # Cast
        ctx["cast"] = (
            MovieCast.objects.filter(movie=movie)
            .select_related("person")
            .order_by("cast_order")[:15]
        )

        # Genres
        ctx["genres"] = movie.genres.all()

        return ctx


class ActorProfileView(DetailView):
    """Actor profile with filmography."""

    model = Person
    template_name = "trends/actor_profile.html"
    context_object_name = "actor"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        actor = self.object

        ctx["filmography"] = (
            MovieCast.objects.filter(person=actor)
            .select_related("movie")
            .order_by("-movie__release_date")
        )

        return ctx

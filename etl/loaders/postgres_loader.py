"""
PostgreSQL loader - upserts processed data into Neon/Postgres.
All writes use INSERT ON CONFLICT so they're safe to re-run.
"""
from __future__ import annotations

import logging
import os
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

load_dotenv()
logger = logging.getLogger(__name__)


class PostgresLoader:
    """Loads data into PostgreSQL with upsert (idempotent writes)."""

    def __init__(self, database_url: str | None = None) -> None:
        self.database_url = database_url or os.environ.get("DATABASE_URL", "")
        if not self.database_url:
            raise ValueError("DATABASE_URL required.")

        self.engine: Engine = create_engine(
            self.database_url, pool_size=5, max_overflow=10, pool_pre_ping=True,
        )
        logger.info("PostgresLoader ready")

    def create_tables(self) -> None:
        """Create all tables if they don't exist (idempotent)."""
        ddl = """
        CREATE TABLE IF NOT EXISTS dim_genres (
            genre_id INTEGER PRIMARY KEY,
            name VARCHAR(100) NOT NULL
        );
        CREATE TABLE IF NOT EXISTS dim_movies (
            movie_id INTEGER PRIMARY KEY,
            title VARCHAR(500) NOT NULL,
            original_language VARCHAR(10) DEFAULT '',
            overview TEXT DEFAULT '',
            release_date DATE,
            runtime INTEGER,
            budget BIGINT DEFAULT 0,
            revenue BIGINT DEFAULT 0,
            roi FLOAT,
            poster_url TEXT DEFAULT '',
            backdrop_url TEXT DEFAULT '',
            updated_at TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS dim_people (
            person_id INTEGER PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            profile_url TEXT DEFAULT '',
            known_for_department VARCHAR(50) DEFAULT '',
            popularity FLOAT DEFAULT 0,
            updated_at TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS bridge_movie_genres (
            movie_id INTEGER REFERENCES dim_movies(movie_id) ON DELETE CASCADE,
            genre_id INTEGER REFERENCES dim_genres(genre_id) ON DELETE CASCADE,
            PRIMARY KEY (movie_id, genre_id)
        );
        CREATE TABLE IF NOT EXISTS bridge_movie_cast (
            movie_id INTEGER REFERENCES dim_movies(movie_id) ON DELETE CASCADE,
            person_id INTEGER REFERENCES dim_people(person_id) ON DELETE CASCADE,
            character_name VARCHAR(500) DEFAULT '',
            cast_order INTEGER DEFAULT 0,
            PRIMARY KEY (movie_id, person_id)
        );
        CREATE TABLE IF NOT EXISTS fact_daily_trends (
            id SERIAL PRIMARY KEY,
            movie_id INTEGER REFERENCES dim_movies(movie_id) ON DELETE CASCADE,
            media_type VARCHAR(10) NOT NULL,
            date DATE NOT NULL,
            position INTEGER NOT NULL,
            position_change INTEGER DEFAULT 0,
            popularity FLOAT DEFAULT 0,
            vote_average FLOAT DEFAULT 0,
            vote_count INTEGER DEFAULT 0,
            UNIQUE(movie_id, date, media_type)
        );
        CREATE TABLE IF NOT EXISTS agg_weekly_summary (
            id SERIAL PRIMARY KEY,
            movie_id INTEGER REFERENCES dim_movies(movie_id) ON DELETE CASCADE,
            media_type VARCHAR(10) DEFAULT '',
            week_start DATE NOT NULL,
            days_trending INTEGER DEFAULT 0,
            avg_position FLOAT DEFAULT 0,
            best_position INTEGER DEFAULT 0,
            avg_popularity FLOAT DEFAULT 0,
            popularity_change_pct FLOAT DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS agg_monthly_genres (
            id SERIAL PRIMARY KEY,
            month DATE NOT NULL,
            genre_id INTEGER REFERENCES dim_genres(genre_id) ON DELETE CASCADE,
            trending_count INTEGER DEFAULT 0,
            avg_popularity FLOAT DEFAULT 0,
            avg_vote_average FLOAT DEFAULT 0,
            top_movie_id INTEGER REFERENCES dim_movies(movie_id) ON DELETE SET NULL,
            total_budget BIGINT DEFAULT 0,
            total_revenue BIGINT DEFAULT 0,
            avg_roi FLOAT
        );
        CREATE INDEX IF NOT EXISTS idx_daily_trends_date ON fact_daily_trends(date);
        CREATE INDEX IF NOT EXISTS idx_daily_trends_movie ON fact_daily_trends(movie_id);
        CREATE INDEX IF NOT EXISTS idx_weekly_summary_week ON agg_weekly_summary(week_start);
        CREATE INDEX IF NOT EXISTS idx_monthly_genres_month ON agg_monthly_genres(month)
        """
        with self.engine.begin() as conn:
            for stmt in ddl.split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.execute(text(stmt))
        logger.info("Tables created/verified")

    def upsert_movies(self, movies: list[dict[str, Any]]) -> int:
        """Insert or update movies. Returns count."""
        if not movies:
            return 0

        sql = text("""
            INSERT INTO dim_movies (movie_id, title, original_language, overview, release_date,
                runtime, budget, revenue, roi, poster_url, backdrop_url, updated_at)
            VALUES (:movie_id, :title, :original_language, :overview, :release_date,
                :runtime, :budget, :revenue, :roi, :poster_url, :backdrop_url, NOW())
            ON CONFLICT (movie_id) DO UPDATE SET
                title = EXCLUDED.title, overview = EXCLUDED.overview,
                release_date = EXCLUDED.release_date, runtime = EXCLUDED.runtime,
                budget = EXCLUDED.budget, revenue = EXCLUDED.revenue, roi = EXCLUDED.roi,
                poster_url = EXCLUDED.poster_url, backdrop_url = EXCLUDED.backdrop_url,
                updated_at = NOW()
        """)

        count = 0
        with self.engine.begin() as conn:
            for movie in movies:
                budget = movie.get("budget", 0) or 0
                revenue = movie.get("revenue", 0) or 0
                roi = ((revenue - budget) / budget * 100) if budget > 0 else None
                conn.execute(sql, {
                    "movie_id": movie["id"],
                    "title": movie.get("title", movie.get("name", "")),
                    "original_language": movie.get("original_language", ""),
                    "overview": movie.get("overview", ""),
                    "release_date": movie.get("release_date") or None,
                    "runtime": movie.get("runtime"),
                    "budget": budget, "revenue": revenue, "roi": roi,
                    "poster_url": movie.get("poster_url", ""),
                    "backdrop_url": movie.get("backdrop_url", ""),
                })
                count += 1
        logger.info(f"Upserted {count} movies")
        return count

    def upsert_genres(self, genres: list[dict[str, Any]]) -> int:
        if not genres:
            return 0
        sql = text("INSERT INTO dim_genres (genre_id, name) VALUES (:genre_id, :name) ON CONFLICT (genre_id) DO UPDATE SET name = EXCLUDED.name")
        with self.engine.begin() as conn:
            for g in genres:
                conn.execute(sql, {"genre_id": g["id"], "name": g["name"]})
        logger.info(f"Upserted {len(genres)} genres")
        return len(genres)

    def upsert_people(self, people: list[dict[str, Any]]) -> int:
        if not people:
            return 0
        sql = text("""
            INSERT INTO dim_people (person_id, name, profile_url, known_for_department, popularity, updated_at)
            VALUES (:person_id, :name, :profile_url, :known_for_department, :popularity, NOW())
            ON CONFLICT (person_id) DO UPDATE SET
                name = EXCLUDED.name, profile_url = EXCLUDED.profile_url,
                popularity = EXCLUDED.popularity, updated_at = NOW()
        """)
        with self.engine.begin() as conn:
            for p in people:
                conn.execute(sql, {
                    "person_id": p["id"], "name": p.get("name", ""),
                    "profile_url": p.get("profile_url", ""),
                    "known_for_department": p.get("known_for_department", ""),
                    "popularity": p.get("popularity", 0),
                })
        logger.info(f"Upserted {len(people)} people")
        return len(people)

    def insert_daily_trends(self, trends: list[dict[str, Any]]) -> int:
        """Upsert daily trend records."""
        if not trends:
            return 0
        sql = text("""
            INSERT INTO fact_daily_trends (movie_id, media_type, date, position,
                position_change, popularity, vote_average, vote_count)
            VALUES (:movie_id, :media_type, :date, :position,
                :position_change, :popularity, :vote_average, :vote_count)
            ON CONFLICT (movie_id, date, media_type) DO UPDATE SET
                position = EXCLUDED.position, position_change = EXCLUDED.position_change,
                popularity = EXCLUDED.popularity, vote_average = EXCLUDED.vote_average,
                vote_count = EXCLUDED.vote_count
        """)
        with self.engine.begin() as conn:
            for t in trends:
                conn.execute(sql, {
                    "movie_id": t["movie_id"], "media_type": t.get("media_type", "movie"),
                    "date": t["date"], "position": t["position"],
                    "position_change": t.get("position_change", 0),
                    "popularity": t.get("popularity", 0),
                    "vote_average": t.get("vote_average", 0),
                    "vote_count": t.get("vote_count", 0),
                })
        logger.info(f"Inserted {len(trends)} daily trends")
        return len(trends)

    def upsert_weekly_summary(self, summaries: list[dict[str, Any]]) -> int:
        """Replace weekly summaries (delete + insert for the same week/movie)."""
        if not summaries:
            return 0
        with self.engine.begin() as conn:
            for s in summaries:
                conn.execute(text("DELETE FROM agg_weekly_summary WHERE movie_id = :movie_id AND week_start = :week_start"),
                    {"movie_id": s["movie_id"], "week_start": s["week_start"]})
                conn.execute(text("""
                    INSERT INTO agg_weekly_summary (movie_id, media_type, week_start, days_trending,
                        avg_position, best_position, avg_popularity, popularity_change_pct)
                    VALUES (:movie_id, :media_type, :week_start, :days_trending,
                        :avg_position, :best_position, :avg_popularity, :popularity_change_pct)
                """), s)
        logger.info(f"Upserted {len(summaries)} weekly summaries")
        return len(summaries)

    def upsert_monthly_genres(self, genre_stats: list[dict[str, Any]]) -> int:
        """Replace monthly genre stats."""
        if not genre_stats:
            return 0
        with self.engine.begin() as conn:
            for s in genre_stats:
                conn.execute(text("DELETE FROM agg_monthly_genres WHERE month = :month AND genre_id = :genre_id"),
                    {"month": s["month"], "genre_id": s["genre_id"]})
                conn.execute(text("""
                    INSERT INTO agg_monthly_genres (month, genre_id, trending_count, avg_popularity,
                        avg_vote_average, top_movie_id, total_budget, total_revenue, avg_roi)
                    VALUES (:month, :genre_id, :trending_count, :avg_popularity,
                        :avg_vote_average, :top_movie_id, :total_budget, :total_revenue, :avg_roi)
                """), s)
        logger.info(f"Upserted {len(genre_stats)} monthly genre stats")
        return len(genre_stats)

    def link_movie_genres(self, movie_id: int, genre_ids: list[int]) -> None:
        sql = text("INSERT INTO bridge_movie_genres (movie_id, genre_id) VALUES (:movie_id, :genre_id) ON CONFLICT DO NOTHING")
        with self.engine.begin() as conn:
            for gid in genre_ids:
                conn.execute(sql, {"movie_id": movie_id, "genre_id": gid})

    def link_movie_cast(self, movie_id: int, cast: list[dict[str, Any]]) -> None:
        sql = text("INSERT INTO bridge_movie_cast (movie_id, person_id, character_name, cast_order) VALUES (:movie_id, :person_id, :character_name, :cast_order) ON CONFLICT DO NOTHING")
        with self.engine.begin() as conn:
            for m in cast:
                conn.execute(sql, {"movie_id": movie_id, "person_id": m["id"],
                    "character_name": m.get("character", ""), "cast_order": m.get("order", 0)})

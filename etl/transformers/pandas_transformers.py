"""
Pandas-based transformations for local/lightweight processing.
Used when Spark/Databricks isn't available or the dataset is small enough.
"""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


class PandasTransformer:
    """Data cleaning, trend calculations, and aggregations using Pandas."""

    @staticmethod
    def clean_movies(df: pd.DataFrame) -> pd.DataFrame:
        """Drop nulls, cast types, calculate ROI, deduplicate."""
        df = df.copy()
        df = df.dropna(subset=["id"])
        df["id"] = df["id"].astype(int)

        if "release_date" in df.columns:
            df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce").dt.date

        for col in ["popularity", "vote_average", "vote_count", "budget", "revenue"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # ROI = (revenue - budget) / budget * 100
        if "budget" in df.columns and "revenue" in df.columns:
            df["roi"] = df.apply(
                lambda r: (r["revenue"] - r["budget"]) / r["budget"] * 100 if r["budget"] > 0 else None,
                axis=1,
            )

        for col in ["title", "overview", "original_language"]:
            if col in df.columns:
                df[col] = df[col].fillna("")

        df = df.drop_duplicates(subset=["id"], keep="last")
        logger.info(f"Cleaned {len(df)} movies")
        return df

    @staticmethod
    def clean_tv_shows(df: pd.DataFrame) -> pd.DataFrame:
        """Same as clean_movies but for TV show fields."""
        df = df.copy()
        df = df.dropna(subset=["id"])
        df["id"] = df["id"].astype(int)

        if "first_air_date" in df.columns:
            df["first_air_date"] = pd.to_datetime(df["first_air_date"], errors="coerce").dt.date

        for col in ["popularity", "vote_average", "vote_count"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        for col in ["name", "overview", "original_language"]:
            if col in df.columns:
                df[col] = df[col].fillna("")

        df = df.drop_duplicates(subset=["id"], keep="last")
        logger.info(f"Cleaned {len(df)} TV shows")
        return df

    @staticmethod
    def calculate_daily_trends(current_df: pd.DataFrame, previous_df: pd.DataFrame | None = None) -> pd.DataFrame:
        """
        Build daily trend records with position changes.
        Compares current positions to previous day's positions.
        """
        trends = current_df[["id", "_position", "_media_type", "popularity", "vote_average", "vote_count"]].copy()
        trends = trends.rename(columns={"id": "movie_id", "_position": "position"})

        if previous_df is not None and not previous_df.empty:
            prev = previous_df[["id", "_position"]].rename(columns={"id": "movie_id", "_position": "prev_position"})
            trends = trends.merge(prev, on="movie_id", how="left")
            # positive change = moved up in ranking
            trends["position_change"] = (trends["prev_position"] - trends["position"]).fillna(0).astype(int)
        else:
            trends["position_change"] = 0

        if "prev_position" in trends.columns:
            trends = trends.drop(columns=["prev_position"])

        return trends

    @staticmethod
    def aggregate_weekly(daily_df: pd.DataFrame) -> pd.DataFrame:
        """Roll up daily trends into weekly stats per movie."""
        if daily_df.empty:
            return pd.DataFrame()

        daily_df = daily_df.copy()
        daily_df["date"] = pd.to_datetime(daily_df["date"])
        # week_start = Monday of that week
        daily_df["week_start"] = (daily_df["date"] - pd.to_timedelta(daily_df["date"].dt.weekday, unit="d")).dt.date

        weekly = (
            daily_df.groupby(["movie_id", "media_type", "week_start"])
            .agg(days_trending=("position", "count"), avg_position=("position", "mean"),
                 best_position=("position", "min"), avg_popularity=("popularity", "mean"))
            .reset_index()
        )
        weekly["avg_position"] = weekly["avg_position"].round(1)
        weekly["avg_popularity"] = weekly["avg_popularity"].round(1)

        # week-over-week popularity change
        weekly = weekly.sort_values(["movie_id", "week_start"])
        weekly["prev_popularity"] = weekly.groupby("movie_id")["avg_popularity"].shift(1)
        weekly["popularity_change_pct"] = (
            (weekly["avg_popularity"] - weekly["prev_popularity"]) / weekly["prev_popularity"] * 100
        ).fillna(0).round(1)
        weekly = weekly.drop(columns=["prev_popularity"])

        logger.info(f"Aggregated {len(weekly)} weekly summaries")
        return weekly

    @staticmethod
    def aggregate_monthly_genres(daily_df: pd.DataFrame, movie_genres_df: pd.DataFrame) -> pd.DataFrame:
        """Count trending movies per genre per month."""
        if daily_df.empty or movie_genres_df.empty:
            return pd.DataFrame()

        daily_df = daily_df.copy()
        daily_df["date"] = pd.to_datetime(daily_df["date"])
        daily_df["month"] = daily_df["date"].dt.to_period("M").dt.to_timestamp().dt.date

        merged = daily_df.merge(movie_genres_df, on="movie_id", how="left")

        monthly = (
            merged.groupby(["month", "genre_id"])
            .agg(trending_count=("movie_id", "nunique"), avg_popularity=("popularity", "mean"),
                 avg_vote_average=("vote_average", "mean"))
            .reset_index()
        )
        monthly["avg_popularity"] = monthly["avg_popularity"].round(1)
        monthly["avg_vote_average"] = monthly["avg_vote_average"].round(2)

        logger.info(f"Aggregated {len(monthly)} monthly genre stats")
        return monthly

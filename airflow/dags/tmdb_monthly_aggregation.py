"""
TMDB Monthly Aggregation DAG - CineTrends Pipeline.

Computes monthly genre trend statistics and career trajectory data.

Schedule: 1st of each month at 12:00 UTC
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")
logger = logging.getLogger(__name__)

default_args = {
    "owner": "cinetrends",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def compute_monthly_genres(**context) -> str:
    """Compute monthly genre aggregation statistics."""
    import pandas as pd
    from sqlalchemy import create_engine, text

    from etl.loaders.postgres_loader import PostgresLoader
    from etl.transformers.pandas_transformers import PandasTransformer

    database_url = os.environ.get("DATABASE_URL", "")
    engine = create_engine(database_url)

    # Get last month's data
    today = datetime.utcnow().date()
    first_of_month = today.replace(day=1)
    last_month_start = (first_of_month - timedelta(days=1)).replace(day=1)

    daily_df = pd.read_sql(
        text("""
            SELECT * FROM fact_daily_trends
            WHERE date >= :start AND date < :end
        """),
        engine,
        params={"start": last_month_start, "end": first_of_month},
    )

    genres_df = pd.read_sql(
        text("""
            SELECT mg.movie_id, mg.genre_id
            FROM bridge_movie_genres mg
        """),
        engine,
    )

    if daily_df.empty:
        logger.info("No daily trend data for last month.")
        return "No data"

    transformer = PandasTransformer()
    monthly_df = transformer.aggregate_monthly_genres(daily_df, genres_df)

    if not monthly_df.empty:
        # Add top movie per genre
        for idx, row in monthly_df.iterrows():
            genre_movies = daily_df[
                daily_df["movie_id"].isin(
                    genres_df[genres_df["genre_id"] == row["genre_id"]]["movie_id"]
                )
            ]
            if not genre_movies.empty:
                top_movie = genre_movies.loc[
                    genre_movies["popularity"].idxmax(), "movie_id"
                ]
                monthly_df.at[idx, "top_movie_id"] = int(top_movie)

        loader = PostgresLoader()
        loader.upsert_monthly_genres(monthly_df.to_dict("records"))

    return f"Created {len(monthly_df)} monthly genre stats"


with DAG(
    dag_id="tmdb_monthly_aggregation",
    default_args=default_args,
    description="Monthly genre trend aggregation",
    schedule="0 12 1 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["tmdb", "monthly", "aggregation", "cinetrends"],
) as dag:

    t_monthly = PythonOperator(
        task_id="compute_monthly_genres",
        python_callable=compute_monthly_genres,
    )

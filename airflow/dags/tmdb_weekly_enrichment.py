"""
TMDB Weekly Enrichment DAG - CineTrends Pipeline.

Enriches movie data with detailed information (budget, revenue, credits)
and computes weekly aggregations.

Schedule: Every Monday at 10:00 UTC
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


def enrich_movie_details(**context) -> str:
    """Fetch detailed info for movies that trended this week."""
    from sqlalchemy import create_engine, text

    from etl.extractors.tmdb_extractor import TMDBExtractor
    from etl.loaders.postgres_loader import PostgresLoader

    database_url = os.environ.get("DATABASE_URL", "")
    engine = create_engine(database_url)
    week_ago = (datetime.utcnow() - timedelta(days=7)).date()

    # Find movies that trended this week but lack budget data
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT DISTINCT f.movie_id
                FROM fact_daily_trends f
                JOIN dim_movies m ON f.movie_id = m.movie_id
                WHERE f.date >= :week_ago AND m.budget = 0
                LIMIT 50
            """),
            {"week_ago": week_ago},
        )
        movie_ids = [row[0] for row in result]

    if not movie_ids:
        logger.info("No movies need enrichment.")
        return "No enrichment needed"

    extractor = TMDBExtractor()
    loader = PostgresLoader()

    enriched = 0
    for movie_id in movie_ids:
        try:
            details = extractor.get_movie_details(movie_id)
            loader.upsert_movies([details])

            # Link cast
            credits = details.get("credits", {})
            cast = credits.get("cast", [])[:10]  # Top 10 billed
            if cast:
                loader.upsert_people(cast)
                loader.link_movie_cast(movie_id, cast)

            enriched += 1
        except Exception as e:
            logger.error(f"Failed to enrich movie {movie_id}: {e}")

    logger.info(f"Enriched {enriched}/{len(movie_ids)} movies.")
    return f"Enriched {enriched} movies"


def compute_weekly_aggregation(**context) -> str:
    """Compute weekly summary aggregations."""
    import pandas as pd
    from sqlalchemy import create_engine, text

    from etl.loaders.postgres_loader import PostgresLoader
    from etl.transformers.pandas_transformers import PandasTransformer

    database_url = os.environ.get("DATABASE_URL", "")
    engine = create_engine(database_url)

    week_ago = (datetime.utcnow() - timedelta(days=7)).date()

    daily_df = pd.read_sql(
        text("SELECT * FROM fact_daily_trends WHERE date >= :week_ago"),
        engine,
        params={"week_ago": week_ago},
    )

    if daily_df.empty:
        logger.info("No daily trend data for this week.")
        return "No data"

    transformer = PandasTransformer()
    weekly_df = transformer.aggregate_weekly(daily_df)

    loader = PostgresLoader()
    loader.upsert_weekly_summary(weekly_df.to_dict("records"))

    return f"Created {len(weekly_df)} weekly summaries"


with DAG(
    dag_id="tmdb_weekly_enrichment",
    default_args=default_args,
    description="Weekly enrichment of movie details and aggregation",
    schedule="0 10 * * 1",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["tmdb", "weekly", "enrichment", "cinetrends"],
) as dag:

    t_enrich = PythonOperator(
        task_id="enrich_movie_details",
        python_callable=enrich_movie_details,
    )

    t_weekly_agg = PythonOperator(
        task_id="compute_weekly_aggregation",
        python_callable=compute_weekly_aggregation,
    )

    t_enrich >> t_weekly_agg

"""
TMDB Daily Extract DAG - CineTrends Pipeline.

Orchestrates the daily extraction of trending movies, TV shows, and people
from the TMDB API, uploads raw data to Azure Blob Storage, transforms it,
and loads the results into PostgreSQL.

Schedule: Daily at 08:00 UTC
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Add project root to path for imports
sys.path.insert(0, "/opt/airflow")

logger = logging.getLogger(__name__)

# Default DAG args
default_args = {
    "owner": "cinetrends",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


# Task Functions
def extract_trending_movies(**context) -> str:
    """Extract trending movies from TMDB API."""
    from etl.extractors.tmdb_extractor import TMDBExtractor

    extractor = TMDBExtractor()
    movies = extractor.get_trending_movies("day")
    logger.info(f"Extracted {len(movies)} trending movies.")

    # Push to XCom
    context["ti"].xcom_push(key="trending_movies", value=movies)
    return f"Extracted {len(movies)} movies"


def extract_trending_tv(**context) -> str:
    """Extract trending TV shows from TMDB API."""
    from etl.extractors.tmdb_extractor import TMDBExtractor

    extractor = TMDBExtractor()
    tv_shows = extractor.get_trending_tv("day")
    logger.info(f"Extracted {len(tv_shows)} trending TV shows.")

    context["ti"].xcom_push(key="trending_tv", value=tv_shows)
    return f"Extracted {len(tv_shows)} TV shows"


def extract_trending_people(**context) -> str:
    """Extract trending people from TMDB API."""
    from etl.extractors.tmdb_extractor import TMDBExtractor

    extractor = TMDBExtractor()
    people = extractor.get_trending_people("day")
    logger.info(f"Extracted {len(people)} trending people.")

    context["ti"].xcom_push(key="trending_people", value=people)
    return f"Extracted {len(people)} people"


def extract_genres(**context) -> str:
    """Extract genre list from TMDB API."""
    from etl.extractors.tmdb_extractor import TMDBExtractor

    extractor = TMDBExtractor()
    genres = extractor.get_genres()
    logger.info(f"Extracted {len(genres)} genres.")

    context["ti"].xcom_push(key="genres", value=genres)
    return f"Extracted {len(genres)} genres"


def upload_to_blob(**context) -> str:
    """Upload raw extracted data to Azure Blob Storage (Bronze layer)."""
    ti = context["ti"]
    today = datetime.utcnow()

    movies = ti.xcom_pull(task_ids="extract.extract_trending_movies", key="trending_movies") or []
    tv_shows = ti.xcom_pull(task_ids="extract.extract_trending_tv", key="trending_tv") or []
    people = ti.xcom_pull(task_ids="extract.extract_trending_people", key="trending_people") or []

    try:
        from etl.loaders.azure_blob_loader import AzureBlobLoader

        loader = AzureBlobLoader()
        loader.upload_daily_extract(movies, "bronze", "trending_movies", today)
        loader.upload_daily_extract(tv_shows, "bronze", "trending_tv", today)
        loader.upload_daily_extract(people, "bronze", "trending_people", today)
        logger.info("Uploaded all data to Azure Blob Storage.")
    except (ImportError, ValueError) as e:
        logger.warning(f"Azure Blob not configured, skipping upload: {e}")

    return "Upload complete"


def transform_and_load(**context) -> str:
    """Transform extracted data and load to PostgreSQL."""
    import pandas as pd

    from etl.loaders.postgres_loader import PostgresLoader
    from etl.transformers.pandas_transformers import PandasTransformer

    ti = context["ti"]
    today = datetime.utcnow().date()

    # Pull data from XCom
    movies = ti.xcom_pull(task_ids="extract.extract_trending_movies", key="trending_movies") or []
    tv_shows = ti.xcom_pull(task_ids="extract.extract_trending_tv", key="trending_tv") or []
    people = ti.xcom_pull(task_ids="extract.extract_trending_people", key="trending_people") or []
    genres = ti.xcom_pull(task_ids="extract.extract_genres", key="genres") or []

    transformer = PandasTransformer()
    loader = PostgresLoader()

    # Ensure tables exist
    loader.create_tables()

    # Load genres
    loader.upsert_genres(genres)

    # Transform & load movies
    if movies:
        movies_df = pd.DataFrame(movies)
        clean_movies = transformer.clean_movies(movies_df)
        loader.upsert_movies(clean_movies.to_dict("records"))

        # Build daily trends
        for i, movie in enumerate(movies, 1):
            movie["_position"] = i
            movie["_media_type"] = "movie"

        trends_df = transformer.calculate_daily_trends(pd.DataFrame(movies))
        trends_df["date"] = today
        loader.insert_daily_trends(trends_df.to_dict("records"))

        # Link genres
        for movie in movies:
            genre_ids = movie.get("genre_ids", [])
            if genre_ids:
                loader.link_movie_genres(movie["id"], genre_ids)

    # Transform & load TV shows
    if tv_shows:
        tv_df = pd.DataFrame(tv_shows)
        clean_tv = transformer.clean_tv_shows(tv_df)
        # Store TV shows in same movies table (they share structure in our model)
        for show in clean_tv.to_dict("records"):
            show["title"] = show.get("name", show.get("title", ""))
            show["release_date"] = show.get("first_air_date")
        loader.upsert_movies(clean_tv.to_dict("records"))

        for i, show in enumerate(tv_shows, 1):
            show["_position"] = i
            show["_media_type"] = "tv"

        tv_trends = transformer.calculate_daily_trends(pd.DataFrame(tv_shows))
        tv_trends["date"] = today
        loader.insert_daily_trends(tv_trends.to_dict("records"))

    # Load people
    if people:
        loader.upsert_people(people)

    total = len(movies) + len(tv_shows) + len(people)
    logger.info(f"Transform & load complete: {total} records processed.")
    return f"Processed {total} records"


def data_quality_check(**context) -> str:
    """Validate that data was loaded correctly."""
    from sqlalchemy import create_engine, text

    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        logger.warning("DATABASE_URL not set, skipping DQ check.")
        return "Skipped"

    engine = create_engine(database_url)
    today = datetime.utcnow().date()

    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM fact_daily_trends WHERE date = :date"),
            {"date": today},
        )
        trend_count = result.scalar()

        result = conn.execute(text("SELECT COUNT(*) FROM dim_movies"))
        movie_count = result.scalar()

        result = conn.execute(text("SELECT COUNT(*) FROM dim_genres"))
        genre_count = result.scalar()

    logger.info(
        f"DQ Check: {trend_count} trends today, "
        f"{movie_count} total movies, {genre_count} genres."
    )

    if trend_count == 0:
        raise ValueError("Data quality check failed: no trends loaded for today!")

    return f"DQ passed: {trend_count} trends, {movie_count} movies"


# DAG Definition
with DAG(
    dag_id="tmdb_daily_pipeline",
    default_args=default_args,
    description="Daily extraction of trending movies, TV shows, and people from TMDB API",
    schedule="0 8 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["tmdb", "daily", "extract", "cinetrends"],
    doc_md="""
    ## TMDB Daily Pipeline

    Extracts trending content from The Movie Database API daily:
    1. **Extract**: Trending movies, TV shows, people, and genres
    2. **Upload**: Raw JSON to Azure Blob Storage (Bronze layer)
    3. **Transform**: Clean, type-cast, calculate position changes
    4. **Load**: Upsert to PostgreSQL (Gold layer)
    5. **Validate**: Data quality checks

    ### Schedule
    Runs daily at 08:00 UTC.

    ### Dependencies
    - TMDB API access token (`TMDB_ACCESS_TOKEN`)
    - Azure Blob Storage (`AZURE_STORAGE_CONNECTION_STRING`) - optional
    - PostgreSQL (`DATABASE_URL`)
    """,
) as dag:

    # Extract Task Group
    with TaskGroup("extract", tooltip="Extract data from TMDB API") as extract_group:
        t_movies = PythonOperator(
            task_id="extract_trending_movies",
            python_callable=extract_trending_movies,
        )
        t_tv = PythonOperator(
            task_id="extract_trending_tv",
            python_callable=extract_trending_tv,
        )
        t_people = PythonOperator(
            task_id="extract_trending_people",
            python_callable=extract_trending_people,
        )
        t_genres = PythonOperator(
            task_id="extract_genres",
            python_callable=extract_genres,
        )

    # Transform & Load Task Group
    with TaskGroup("transform_load", tooltip="Transform and load to PostgreSQL") as tl_group:
        t_upload = PythonOperator(
            task_id="upload_to_blob",
            python_callable=upload_to_blob,
        )
        t_transform = PythonOperator(
            task_id="transform_and_load",
            python_callable=transform_and_load,
        )

    # Validate
    t_validate = PythonOperator(
        task_id="data_quality_check",
        python_callable=data_quality_check,
    )

    # Task Dependencies
    extract_group >> tl_group >> t_validate
    # Within transform_load group: upload first, then transform
    t_upload >> t_transform

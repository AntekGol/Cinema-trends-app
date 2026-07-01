"""Custom Airflow Hook for TMDB API integration."""
from __future__ import annotations

from airflow.hooks.base import BaseHook

from etl.extractors.tmdb_extractor import TMDBExtractor


class TMDBHook(BaseHook):
    """
    Airflow Hook wrapping TMDBExtractor for use in DAGs.

    Can use Airflow connections for token management, or fall back
    to environment variables.

    Usage:
        hook = TMDBHook()
        extractor = hook.get_conn()
        movies = extractor.get_trending_movies()
    """

    conn_name_attr = "tmdb_conn_id"
    default_conn_name = "tmdb_default"
    conn_type = "http"
    hook_name = "TMDB API"

    def __init__(self, tmdb_conn_id: str = default_conn_name, **kwargs) -> None:
        super().__init__(**kwargs)
        self.tmdb_conn_id = tmdb_conn_id
        self._extractor: TMDBExtractor | None = None

    def get_conn(self) -> TMDBExtractor:
        """
        Get a configured TMDBExtractor instance.

        Tries to use Airflow connection first, falls back to env var.
        """
        if self._extractor is not None:
            return self._extractor

        access_token = None
        try:
            conn = self.get_connection(self.tmdb_conn_id)
            access_token = conn.password or conn.extra_dejson.get("access_token")
        except Exception:
            self.log.info("No Airflow connection found, using env var.")

        self._extractor = TMDBExtractor(access_token=access_token)
        return self._extractor

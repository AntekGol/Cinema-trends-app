"""
TMDB API client for extracting trending movie/TV data.
Uses Bearer token auth and handles rate limiting automatically.
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from etl.extractors.base_extractor import BaseExtractor

load_dotenv()
logger = logging.getLogger(__name__)


class TMDBAPIError(Exception):
    """Generic TMDB API error."""
    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        self.message = message
        super().__init__(f"TMDB API {status_code}: {message}")


class TMDBRateLimitError(TMDBAPIError):
    """HTTP 429 - too many requests."""


class TMDBExtractor(BaseExtractor):
    """
    Pulls trending data from TMDB API v3.
    Handles rate limiting (40 req/10s) and retries on failure.
    """

    BASE_URL = "https://api.themoviedb.org/3"
    IMAGE_BASE_URL = "https://image.tmdb.org/t/p"
    MAX_REQUESTS_PER_WINDOW = 40
    RATE_LIMIT_WINDOW = 10

    def __init__(self, access_token: str | None = None) -> None:
        self.access_token = access_token or os.environ.get("TMDB_ACCESS_TOKEN", "")
        if not self.access_token:
            raise ValueError(
                "TMDB access token required. Set TMDB_ACCESS_TOKEN env var."
            )

        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        self._request_timestamps: list[float] = []
        logger.info("TMDBExtractor ready")

    def _rate_limit(self) -> None:
        """Simple sliding window rate limiter."""
        now = time.time()
        self._request_timestamps = [
            ts for ts in self._request_timestamps
            if now - ts < self.RATE_LIMIT_WINDOW
        ]
        if len(self._request_timestamps) >= self.MAX_REQUESTS_PER_WINDOW:
            oldest = self._request_timestamps[0]
            sleep_time = self.RATE_LIMIT_WINDOW - (now - oldest) + 0.1
            if sleep_time > 0:
                logger.info(f"Rate limit hit, sleeping {sleep_time:.1f}s")
                time.sleep(sleep_time)
        self._request_timestamps.append(time.time())

    @retry(
        retry=retry_if_exception_type((TMDBRateLimitError, requests.ConnectionError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
    )
    def _make_request(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Make a rate-limited GET request to TMDB."""
        self._rate_limit()
        url = f"{self.BASE_URL}{endpoint}"
        response = self.session.get(url, params=params, timeout=30)

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 5))
            logger.warning(f"429 rate limited, waiting {retry_after}s")
            time.sleep(retry_after)
            raise TMDBRateLimitError(429, "Rate limit exceeded")

        if response.status_code != 200:
            raise TMDBAPIError(response.status_code, response.text)

        return response.json()

    def get_trending_movies(self, time_window: str = "day") -> list[dict[str, Any]]:
        """Get today's trending movies. time_window: 'day' or 'week'."""
        data = self._make_request(f"/trending/movie/{time_window}")
        results = data.get("results", [])
        for movie in results:
            movie["poster_url"] = self._build_image_url(movie.get("poster_path"), "w500")
            movie["backdrop_url"] = self._build_image_url(movie.get("backdrop_path"), "w1280")
        logger.info(f"Got {len(results)} trending movies")
        return results

    def get_trending_tv(self, time_window: str = "day") -> list[dict[str, Any]]:
        """Get trending TV shows."""
        data = self._make_request(f"/trending/tv/{time_window}")
        results = data.get("results", [])
        for show in results:
            show["poster_url"] = self._build_image_url(show.get("poster_path"), "w500")
            show["backdrop_url"] = self._build_image_url(show.get("backdrop_path"), "w1280")
        logger.info(f"Got {len(results)} trending TV shows")
        return results

    def get_trending_people(self, time_window: str = "day") -> list[dict[str, Any]]:
        """Get trending actors/directors."""
        data = self._make_request(f"/trending/person/{time_window}")
        results = data.get("results", [])
        for person in results:
            person["profile_url"] = self._build_image_url(person.get("profile_path"), "w185")
        logger.info(f"Got {len(results)} trending people")
        return results

    def get_movie_details(self, movie_id: int) -> dict[str, Any]:
        """Get full movie info including cast (credits)."""
        data = self._make_request(
            f"/movie/{movie_id}",
            params={"append_to_response": "credits"},
        )
        data["poster_url"] = self._build_image_url(data.get("poster_path"), "w500")
        data["backdrop_url"] = self._build_image_url(data.get("backdrop_path"), "w1280")
        return data

    def get_genres(self) -> list[dict[str, Any]]:
        """Get all movie genres from TMDB."""
        data = self._make_request("/genre/movie/list")
        return data.get("genres", [])

    def get_discover_movies(
        self, sort_by: str = "popularity.desc", page: int = 1,
        max_pages: int = 5, **filters: Any,
    ) -> list[dict[str, Any]]:
        """Discover movies with filters. Paginates up to max_pages."""
        all_results: list[dict[str, Any]] = []
        params = {"sort_by": sort_by, **filters}

        for current_page in range(page, page + max_pages):
            params["page"] = current_page
            data = self._make_request("/discover/movie", params=params)
            results = data.get("results", [])
            total_pages = data.get("total_pages", 1)
            all_results.extend(results)
            if current_page >= total_pages:
                break

        logger.info(f"Discovered {len(all_results)} movies")
        return all_results

    # -- BaseExtractor interface --

    def extract(self) -> list[dict[str, Any]]:
        """Run full daily extraction - movies + TV + people."""
        extracted_at = datetime.utcnow().isoformat()
        all_data: list[dict[str, Any]] = []

        movies = self.get_trending_movies("day")
        for i, movie in enumerate(movies, 1):
            movie["_source"] = "trending_movies_day"
            movie["_position"] = i
            movie["_extracted_at"] = extracted_at
            movie["_media_type"] = "movie"
        all_data.extend(movies)

        tv_shows = self.get_trending_tv("day")
        for i, show in enumerate(tv_shows, 1):
            show["_source"] = "trending_tv_day"
            show["_position"] = i
            show["_extracted_at"] = extracted_at
            show["_media_type"] = "tv"
        all_data.extend(tv_shows)

        people = self.get_trending_people("day")
        for i, person in enumerate(people, 1):
            person["_source"] = "trending_people_day"
            person["_position"] = i
            person["_extracted_at"] = extracted_at
            person["_media_type"] = "person"
        all_data.extend(people)

        logger.info(f"Extracted {len(movies)} movies, {len(tv_shows)} TV, {len(people)} people")
        return all_data

    def validate(self, data: list[dict[str, Any]]) -> bool:
        """Check that extracted data has required fields."""
        if not data:
            logger.error("Validation failed: empty dataset")
            return False

        required = {"id", "_source", "_extracted_at"}
        for i, record in enumerate(data):
            missing = required - set(record.keys())
            if missing:
                logger.error(f"Record {i} missing: {missing}")
                return False

        logger.info(f"Validated {len(data)} records OK")
        return True

    def _build_image_url(self, path: str | None, size: str = "w500") -> str:
        if not path:
            return ""
        return f"{self.IMAGE_BASE_URL}/{size}{path}"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    try:
        extractor = TMDBExtractor()
        genres = extractor.get_genres()
        print(f"\nGenres ({len(genres)}):")
        for g in genres[:5]:
            print(f"  - {g['name']} (ID: {g['id']})")

        movies = extractor.get_trending_movies("day")
        print(f"\nTrending Movies ({len(movies)}):")
        for i, m in enumerate(movies[:5], 1):
            print(f"  {i}. {m['title']} (pop: {m['popularity']:.1f})")

        all_data = extractor.extract()
        valid = extractor.validate(all_data)
        print(f"\nFull extract: {len(all_data)} records, valid={valid}")
    except ValueError as e:
        print(f"Config error: {e}")
    except TMDBAPIError as e:
        print(f"API error: {e}")

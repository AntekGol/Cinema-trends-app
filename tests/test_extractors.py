"""Tests for TMDB Extractor."""
from unittest.mock import MagicMock, patch

import pytest

from etl.extractors.tmdb_extractor import TMDBExtractor, TMDBAPIError


class TestTMDBExtractorInit:
    """Tests for TMDBExtractor initialization."""

    def test_init_with_token(self):
        extractor = TMDBExtractor(access_token="test_token")
        assert extractor.access_token == "test_token"

    def test_init_without_token_raises(self):
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(ValueError, match="access token is required"):
                TMDBExtractor()

    @patch.dict("os.environ", {"TMDB_ACCESS_TOKEN": "env_token"})
    def test_init_from_env(self):
        extractor = TMDBExtractor()
        assert extractor.access_token == "env_token"


class TestTMDBExtractorAPI:
    """Tests for API methods with mocked HTTP."""

    @pytest.fixture
    def extractor(self):
        return TMDBExtractor(access_token="test_token")

    @patch("etl.extractors.tmdb_extractor.requests.Session.get")
    def test_get_genres(self, mock_get, extractor):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "genres": [{"id": 28, "name": "Action"}, {"id": 18, "name": "Drama"}]
        }
        mock_get.return_value = mock_response

        genres = extractor.get_genres()
        assert len(genres) == 2
        assert genres[0]["name"] == "Action"

    @patch("etl.extractors.tmdb_extractor.requests.Session.get")
    def test_get_trending_movies(self, mock_get, extractor):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                {"id": 27205, "title": "Inception", "poster_path": "/test.jpg",
                 "backdrop_path": "/back.jpg", "popularity": 120.5},
            ]
        }
        mock_get.return_value = mock_response

        movies = extractor.get_trending_movies("day")
        assert len(movies) == 1
        assert movies[0]["title"] == "Inception"
        assert "poster_url" in movies[0]

    def test_validate_empty_data(self, extractor):
        assert extractor.validate([]) is False

    def test_validate_valid_data(self, extractor):
        data = [
            {"id": 1, "_source": "test", "_extracted_at": "2026-01-01"},
            {"id": 2, "_source": "test", "_extracted_at": "2026-01-01"},
        ]
        assert extractor.validate(data) is True

    def test_validate_missing_fields(self, extractor):
        data = [{"id": 1, "_source": "test"}]  # missing _extracted_at
        assert extractor.validate(data) is False

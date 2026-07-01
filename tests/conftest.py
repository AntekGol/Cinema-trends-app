"""Pytest fixtures for CineTrends tests."""
import pytest


@pytest.fixture
def sample_trending_movies():
    """Sample TMDB trending movies API response."""
    return [
        {
            "id": 27205,
            "title": "Inception",
            "original_language": "en",
            "overview": "A thief who steals corporate secrets...",
            "release_date": "2010-07-16",
            "popularity": 120.5,
            "vote_average": 8.4,
            "vote_count": 35000,
            "genre_ids": [28, 878, 12],
            "poster_path": "/edv5CZvWj09upOsy2Y6IwDhK8bt.jpg",
            "backdrop_path": "/s3TBrRGB1iav7gFOCNx3H31MoES.jpg",
            "_position": 1,
            "_media_type": "movie",
            "_source": "trending_movies_day",
            "_extracted_at": "2026-06-29T08:00:00",
        },
        {
            "id": 155,
            "title": "The Dark Knight",
            "original_language": "en",
            "overview": "Batman raises the stakes...",
            "release_date": "2008-07-18",
            "popularity": 95.2,
            "vote_average": 9.0,
            "vote_count": 31000,
            "genre_ids": [28, 80, 18],
            "poster_path": "/qJ2tW6WMUDux911BTUgMe1tKVIP.jpg",
            "backdrop_path": "/nMKdUUepR0i5zn0y1T4CsSB5ez9.jpg",
            "_position": 2,
            "_media_type": "movie",
            "_source": "trending_movies_day",
            "_extracted_at": "2026-06-29T08:00:00",
        },
        {
            "id": 872585,
            "title": "Oppenheimer",
            "original_language": "en",
            "overview": "The story of J. Robert Oppenheimer...",
            "release_date": "2023-07-21",
            "popularity": 88.7,
            "vote_average": 8.1,
            "vote_count": 12000,
            "genre_ids": [18, 36],
            "poster_path": "/8Gxv8gSFCU0XGDykEGv7zR1n2ua.jpg",
            "backdrop_path": "/fm6KqXpk3M2HVveHwCrBSSBaO0V.jpg",
            "_position": 3,
            "_media_type": "movie",
            "_source": "trending_movies_day",
            "_extracted_at": "2026-06-29T08:00:00",
        },
    ]


@pytest.fixture
def sample_genres():
    """Sample TMDB genres."""
    return [
        {"id": 28, "name": "Action"},
        {"id": 12, "name": "Adventure"},
        {"id": 878, "name": "Science Fiction"},
        {"id": 18, "name": "Drama"},
        {"id": 80, "name": "Crime"},
        {"id": 36, "name": "History"},
    ]

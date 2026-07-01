"""Tests for Pandas Transformer."""
import pandas as pd
import pytest

from etl.transformers.pandas_transformers import PandasTransformer


class TestCleanMovies:
    def test_clean_basic(self, sample_trending_movies):
        df = pd.DataFrame(sample_trending_movies)
        result = PandasTransformer.clean_movies(df)
        assert len(result) == 3
        assert result["id"].dtype == int

    def test_clean_drops_null_ids(self):
        df = pd.DataFrame([{"id": None, "title": "Test"}, {"id": 1, "title": "Good"}])
        result = PandasTransformer.clean_movies(df)
        assert len(result) == 1

    def test_clean_deduplicates(self):
        df = pd.DataFrame([
            {"id": 1, "title": "First"}, {"id": 1, "title": "Duplicate"}
        ])
        result = PandasTransformer.clean_movies(df)
        assert len(result) == 1

    def test_roi_calculation(self):
        df = pd.DataFrame([{"id": 1, "budget": 100000000, "revenue": 500000000}])
        result = PandasTransformer.clean_movies(df)
        assert result.iloc[0]["roi"] == pytest.approx(400.0)

    def test_roi_zero_budget(self):
        df = pd.DataFrame([{"id": 1, "budget": 0, "revenue": 100}])
        result = PandasTransformer.clean_movies(df)
        assert result.iloc[0]["roi"] is None


class TestDailyTrends:
    def test_calculate_without_previous(self, sample_trending_movies):
        df = pd.DataFrame(sample_trending_movies)
        result = PandasTransformer.calculate_daily_trends(df)
        assert len(result) == 3
        assert all(result["position_change"] == 0)

    def test_calculate_with_previous(self, sample_trending_movies):
        current = pd.DataFrame(sample_trending_movies)
        previous = pd.DataFrame([
            {"id": 27205, "_position": 3, "_media_type": "movie"},
            {"id": 155, "_position": 1, "_media_type": "movie"},
        ])
        result = PandasTransformer.calculate_daily_trends(current, previous)
        # Inception moved from 3 to 1 = +2
        inception = result[result["movie_id"] == 27205].iloc[0]
        assert inception["position_change"] == 2  # 3 - 1 = 2


class TestAggregations:
    def test_aggregate_weekly_empty(self):
        result = PandasTransformer.aggregate_weekly(pd.DataFrame())
        assert len(result) == 0

    def test_aggregate_weekly(self):
        data = {
            "movie_id": [1, 1, 1, 2, 2],
            "media_type": ["movie"] * 5,
            "date": pd.date_range("2026-06-23", periods=5),
            "position": [1, 2, 1, 5, 3],
            "popularity": [100, 95, 110, 50, 55],
        }
        df = pd.DataFrame(data)
        result = PandasTransformer.aggregate_weekly(df)
        assert len(result) >= 1

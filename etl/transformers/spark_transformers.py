"""
PySpark transformations for Databricks processing.
Mostly window functions (LAG, RANK, etc.) and rolling aggregations.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

try:
    from pyspark.sql import DataFrame, SparkSession, Window
    from pyspark.sql import functions as F
    from pyspark.sql.types import DateType, FloatType, IntegerType, LongType, StringType, StructField, StructType
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    logger.info("PySpark not installed - SparkTransformer won't work")


class SparkTransformer:
    """
    Heavier transformations using PySpark. Meant to run in Databricks
    but also works locally with pyspark installed.
    """

    def __init__(self, spark: Any | None = None) -> None:
        if not SPARK_AVAILABLE:
            raise ImportError("pyspark required: pip install pyspark")

        if spark is not None:
            self.spark = spark
        else:
            self.spark = SparkSession.builder.appName("CineTrends").master("local[*]").getOrCreate()

    def deduplicate(self, df: DataFrame, keys: list[str], order_col: str = "_ingested_at", keep: str = "last") -> DataFrame:
        """Remove dupes by keys, keeping most recent (or earliest) by order_col."""
        ascending = keep == "first"
        order = F.col(order_col).asc() if ascending else F.col(order_col).desc()
        window = Window.partitionBy(*keys).orderBy(order)
        return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")

    def validate_data_quality(self, df: DataFrame, checks: dict[str, Any]) -> bool:
        """Run min/max checks on columns. Returns True if all pass."""
        ok = True
        for col, rules in checks.items():
            if col not in df.columns:
                continue
            if "min" in rules and df.filter(F.col(col) < rules["min"]).count() > 0:
                logger.error(f"DQ fail: {col} has values below {rules['min']}")
                ok = False
            if "max" in rules and df.filter(F.col(col) > rules["max"]).count() > 0:
                logger.error(f"DQ fail: {col} has values above {rules['max']}")
                ok = False
        if ok:
            logger.info("DQ checks passed")
        return ok

    def calculate_position_changes(self, df: DataFrame) -> DataFrame:
        """Use LAG to get yesterday's position and compute the change."""
        w = Window.partitionBy("movie_id", "media_type").orderBy("date")
        return (
            df.withColumn("prev_position", F.lag("position", 1).over(w))
              .withColumn("position_change",
                  F.when(F.col("prev_position").isNotNull(), F.col("prev_position") - F.col("position")).otherwise(0))
        )

    def calculate_weekly_rankings(self, df: DataFrame) -> DataFrame:
        """Aggregate to weekly level and rank by popularity using DENSE_RANK."""
        # week_start = Monday
        df_w = df.withColumn("week_start", F.date_sub(F.col("date"), (F.dayofweek(F.col("date")) + 5) % 7))

        weekly = df_w.groupBy("movie_id", "media_type", "week_start").agg(
            F.count("*").alias("days_trending"),
            F.round(F.avg("position"), 1).alias("avg_position"),
            F.min("position").alias("best_position"),
            F.round(F.avg("popularity"), 1).alias("avg_popularity"),
        )

        # rank within each week
        w_rank = Window.partitionBy("week_start").orderBy(F.col("avg_popularity").desc())
        ranked = weekly.withColumn("weekly_rank", F.dense_rank().over(w_rank))

        # week-over-week change
        w_movie = Window.partitionBy("movie_id").orderBy("week_start")
        ranked = (
            ranked.withColumn("prev_pop", F.lag("avg_popularity", 1).over(w_movie))
                  .withColumn("popularity_change_pct",
                      F.when((F.col("prev_pop").isNotNull()) & (F.col("prev_pop") > 0),
                          F.round((F.col("avg_popularity") - F.col("prev_pop")) / F.col("prev_pop") * 100, 1))
                      .otherwise(0.0))
                  .drop("prev_pop")
        )
        return ranked

    def calculate_genre_trends(self, df: DataFrame, genres_df: DataFrame) -> DataFrame:
        """Daily genre stats with a 30-day rolling average on popularity."""
        joined = df.join(genres_df, on="movie_id", how="inner")

        daily = joined.groupBy("date", "genre_id", "genre_name").agg(
            F.count("*").alias("trending_count"),
            F.round(F.avg("popularity"), 1).alias("avg_popularity"),
            F.round(F.avg("vote_average"), 2).alias("avg_vote_average"),
        )

        # 30-day rolling avg
        w_roll = Window.partitionBy("genre_id").orderBy(F.col("date").cast("long")).rowsBetween(-29, 0)
        return daily.withColumn("avg_popularity_30d", F.round(F.avg("avg_popularity").over(w_roll), 1))

    def calculate_roi(self, df: DataFrame) -> DataFrame:
        """Add ROI column: (revenue - budget) / budget * 100."""
        return df.withColumn("roi",
            F.when(F.col("budget") > 0,
                F.round((F.col("revenue") - F.col("budget")) / F.col("budget") * 100, 1))
            .otherwise(F.lit(None)))

    def calculate_monthly_aggregation(self, df: DataFrame) -> DataFrame:
        """Monthly rollup by genre - counts, averages, totals."""
        df_m = df.withColumn("month", F.date_trunc("month", F.col("date")))

        monthly = df_m.groupBy("month", "genre_id").agg(
            F.countDistinct("movie_id").alias("trending_count"),
            F.round(F.avg("popularity"), 1).alias("avg_popularity"),
            F.round(F.avg("vote_average"), 2).alias("avg_vote_average"),
            F.sum("budget").alias("total_budget"),
            F.sum("revenue").alias("total_revenue"),
        )
        return monthly.withColumn("avg_roi",
            F.when(F.col("total_budget") > 0,
                F.round((F.col("total_revenue") - F.col("total_budget")) / F.col("total_budget") * 100, 1))
            .otherwise(F.lit(None)))

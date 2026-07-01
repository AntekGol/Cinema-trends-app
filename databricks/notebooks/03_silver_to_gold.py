
# #  CineTrends Pipeline - Stage 3: Silver → Gold
#
# **Purpose:** Build analytics-ready Gold tables from curated Silver data using advanced
# PySpark transformations - window functions, rolling aggregates, and multi-dimensional
# analytics.
#
# **Gold Tables Produced:**
# | Table | Description |


from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Final

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType


# ## 1. Configuration


# -- Widget parameters --
dbutils.widgets.text("storage_account", "", "Azure Storage Account Name")
dbutils.widgets.text("container", "cinetrends-datalake", "Blob Container Name")
dbutils.widgets.text("date", datetime.now(timezone.utc).strftime("%Y-%m-%d"), "Extraction Date (YYYY-MM-DD)")
dbutils.widgets.text("log_level", "INFO", "Logging Level")

STORAGE_ACCOUNT: str = dbutils.widgets.get("storage_account")
CONTAINER: str = dbutils.widgets.get("container")
EXTRACTION_DATE: str = dbutils.widgets.get("date")
LOG_LEVEL: str = dbutils.widgets.get("log_level")

# Logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("CineTrends.SilverToGold")

# Storage paths
WASBS_BASE: str = f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net"
SILVER_BASE_PATH: str = f"{WASBS_BASE}/silver"
GOLD_BASE_PATH: str = f"{WASBS_BASE}/gold"

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net",
    dbutils.secrets.get(scope="cinetrends-kv", key="storage-account-key"),
)

logger.info("Silver→Gold pipeline started - date=%s", EXTRACTION_DATE)


# ## 2. Read Silver Data


# -- Load Silver Delta tables --
df_movies: DataFrame = spark.read.format("delta").load(f"{SILVER_BASE_PATH}/trending_movies/")
df_tv: DataFrame = spark.read.format("delta").load(f"{SILVER_BASE_PATH}/trending_tv/")

logger.info("Silver movies: %d records", df_movies.count())
logger.info("Silver TV: %d records", df_tv.count())

# Combine movies and TV for unified content analysis
# Standardise column names where they differ
df_tv_aligned = (
    df_tv
    .withColumnRenamed("name", "title")
    .withColumnRenamed("first_air_date", "release_date")
    .withColumn("media_type", F.lit("tv"))
)

df_movies_aligned = df_movies.withColumn("media_type", F.lit("movie"))

# Select common columns for the unified dataset
COMMON_COLUMNS: Final[list[str]] = [
    "id", "title", "popularity", "vote_average", "vote_count",
    "release_date", "genres", "extraction_date", "media_type",
    "_ingested_at", "_source", "_batch_id",
]

# Only select columns that exist in both DataFrames
available_cols_movies = [c for c in COMMON_COLUMNS if c in df_movies_aligned.columns]
available_cols_tv = [c for c in COMMON_COLUMNS if c in df_tv_aligned.columns]
common_available = list(set(available_cols_movies) & set(available_cols_tv))

df_content = df_movies_aligned.select(*common_available).unionByName(
    df_tv_aligned.select(*common_available),
    allowMissingColumns=True,
)

logger.info("Unified content dataset: %d records", df_content.count())


# ## 3. Gold Table 1: Daily Position Changes
#
# Uses `LAG()` window functions to compute day-over-day position and popularity changes.
# Detects new entries that appeared for the first time.


def build_daily_trends(df: DataFrame) -> DataFrame:
    """
    Compute daily position changes and popularity deltas using LAG window functions.

    For each piece of content, ordered by extraction_date, this function calculates:
    - position: DENSE_RANK within each extraction_date by descending popularity
    - prev_position: LAG(position, 1) - the previous day's rank
    - position_change: prev_position - position (positive = moved up)
    - prev_popularity: LAG(popularity, 1)
    - popularity_delta: popularity - prev_popularity
    - is_new_entry: True if there is no previous position (first appearance)

    Parameters
    df : DataFrame
        Unified content DataFrame with id, popularity, extraction_date columns.

    Returns
    DataFrame
        DataFrame enriched with daily trend metrics.
    """
    # Step 1: Assign daily position (rank) based on popularity within each date
    daily_rank_window = Window.partitionBy("extraction_date").orderBy(F.col("popularity").desc())

    df_ranked = df.withColumn(
        "position", F.dense_rank().over(daily_rank_window)
    )

    # Step 2: Compute LAG-based metrics - look back 1 day per content item
    content_time_window = Window.partitionBy("id").orderBy("extraction_date")

    df_with_lags = (
        df_ranked
        # Previous day's position for this content item
        .withColumn("prev_position", F.lag("position", 1).over(content_time_window))
        # Previous day's popularity
        .withColumn("prev_popularity", F.lag("popularity", 1).over(content_time_window))
        # Position change: positive means the item moved UP in ranking
        .withColumn(
            "position_change",
            F.when(
                F.col("prev_position").isNotNull(),
                F.col("prev_position") - F.col("position"),
            ).otherwise(F.lit(None)),
        )
        # Absolute popularity delta
        .withColumn(
            "popularity_delta",
            F.when(
                F.col("prev_popularity").isNotNull(),
                F.col("popularity") - F.col("prev_popularity"),
            ).otherwise(F.lit(None)),
        )
        # Flag new entries (no previous position means first appearance)
        .withColumn(
            "is_new_entry",
            F.when(F.col("prev_position").isNull(), F.lit(True)).otherwise(F.lit(False)),
        )
        # Days on chart (cumulative count of appearances)
        .withColumn(
            "days_on_chart",
            F.count("*").over(
                content_time_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
    )

    logger.info("Daily trends computed - %d records.", df_with_lags.count())
    return df_with_lags


df_daily_trends = build_daily_trends(df_content)
display(df_daily_trends.filter(F.col("extraction_date") == EXTRACTION_DATE).orderBy("position").limit(20))


# ## 4. Gold Table 2: Weekly Rankings
#
# Aggregate to weekly granularity using `weekofyear`, then apply `DENSE_RANK`
# to produce top-20 rankings per week.


def build_weekly_rankings(df: DataFrame, top_n: int = 20) -> DataFrame:
    """
    Compute weekly rankings by average popularity.

    Groups content by id and ISO year-week, calculates aggregate metrics,
    then applies DENSE_RANK to select the top N items per week.

    Parameters
    df : DataFrame
        Unified content DataFrame.
    top_n : int
        Number of top-ranked items to retain per week.

    Returns
    DataFrame
        Weekly top-N rankings with aggregate statistics.
    """
    # Build year_week identifier (e.g. "2026-W26")
    df_with_week = (
        df
        .withColumn("week_number", F.weekofyear(F.col("extraction_date")))
        .withColumn("year", F.year(F.col("extraction_date")))
        .withColumn(
            "year_week",
            F.concat(
                F.col("year").cast(StringType()),
                F.lit("-W"),
                F.lpad(F.col("week_number").cast(StringType()), 2, "0"),
            ),
        )
    )

    # Aggregate per content item per week
    df_weekly_agg = (
        df_with_week
        .groupBy("id", "title", "media_type", "year_week", "year", "week_number")
        .agg(
            F.avg("popularity").alias("avg_popularity"),
            F.max("popularity").alias("peak_popularity"),
            F.avg("vote_average").alias("avg_vote_average"),
            F.sum("vote_count").alias("total_vote_count"),
            F.count("*").alias("days_tracked"),
            F.first("genres").alias("genres"),
        )
    )

    # Apply DENSE_RANK within each year_week by avg_popularity descending
    weekly_rank_window = Window.partitionBy("year_week").orderBy(F.col("avg_popularity").desc())

    df_ranked = (
        df_weekly_agg
        .withColumn("weekly_rank", F.dense_rank().over(weekly_rank_window))
        .filter(F.col("weekly_rank") <= top_n)
        .orderBy("year_week", "weekly_rank")
    )

    logger.info("Weekly rankings computed - %d entries across all weeks.", df_ranked.count())
    return df_ranked


df_weekly_rankings = build_weekly_rankings(df_content, top_n=20)
display(df_weekly_rankings.limit(40))


# ## 5. Gold Table 3: Monthly Genre Aggregations
#
# Explode genres and aggregate at the month×genre level for trend analysis.


def build_monthly_genre_stats(df: DataFrame) -> DataFrame:
    """
    Compute monthly genre-level statistics by exploding the genres array.

    For each genre in each month, calculates:
    - title_count: number of unique titles
    - avg_popularity: mean popularity score
    - avg_vote_average: mean vote average
    - total_vote_count: sum of vote counts
    - top_title: the most popular title in that genre for the month

    Parameters
    df : DataFrame
        Unified content DataFrame with a `genres` array column.

    Returns
    DataFrame
        Monthly genre aggregation table.
    """
    # Build year_month column
    df_with_month = df.withColumn(
        "year_month",
        F.date_format(F.col("extraction_date"), "yyyy-MM"),
    )

    # Explode genres - each record produces one row per genre
    df_exploded = df_with_month.withColumn(
        "genre", F.explode_outer(F.col("genres"))
    )

    # Filter out null genres (records with no genre information)
    df_genred = df_exploded.filter(F.col("genre").isNotNull())

    # Find the top title per genre-month (most popular single record)
    top_title_window = Window.partitionBy("genre", "year_month").orderBy(F.col("popularity").desc())
    df_with_top = df_genred.withColumn(
        "_genre_rank", F.row_number().over(top_title_window)
    )

    # Aggregate at genre × month level
    df_monthly_genres = (
        df_genred
        .groupBy("genre", "year_month")
        .agg(
            F.countDistinct("id").alias("title_count"),
            F.round(F.avg("popularity"), 2).alias("avg_popularity"),
            F.round(F.avg("vote_average"), 2).alias("avg_vote_average"),
            F.sum("vote_count").alias("total_vote_count"),
            F.round(F.max("popularity"), 2).alias("peak_popularity"),
        )
    )

    # Add top title per genre-month via a join
    df_top_titles = (
        df_with_top
        .filter(F.col("_genre_rank") == 1)
        .select(
            F.col("genre").alias("_genre"),
            F.col("year_month").alias("_year_month"),
            F.col("title").alias("top_title"),
        )
    )

    df_result = df_monthly_genres.join(
        df_top_titles,
        (df_monthly_genres["genre"] == df_top_titles["_genre"])
        & (df_monthly_genres["year_month"] == df_top_titles["_year_month"]),
        how="left",
    ).drop("_genre", "_year_month")

    logger.info("Monthly genre stats computed - %d genre-month rows.", df_result.count())
    return df_result


df_monthly_genres = build_monthly_genre_stats(df_content)
display(df_monthly_genres.orderBy(F.col("year_month").desc(), F.col("avg_popularity").desc()).limit(30))


# ## 6. Gold Table 4: ROI Calculations
#
# For movies with budget data, calculate Return on Investment and rank by ROI.
# Includes rolling average ROI per genre.


def build_roi_analysis(df: DataFrame) -> DataFrame:
    """
    Calculate ROI metrics for movies with valid budget and revenue data.

    ROI = (revenue - budget) / budget

    Applies:
    - Filter: budget > 0 to avoid division by zero
    - RANK by ROI within each genre (exploded)
    - Rolling average ROI over the last 10 movies per genre (by release_date)

    Parameters
    df : DataFrame
        Movie DataFrame. If budget/revenue columns are missing, they are
        initialised as nulls (graceful degradation).

    Returns
    DataFrame
        ROI analysis table with per-genre rankings and rolling averages.
    """
    # Ensure budget and revenue columns exist (they may not in trending data)
    if "budget" not in df.columns:
        df = df.withColumn("budget", F.lit(None).cast(DoubleType()))
    if "revenue" not in df.columns:
        df = df.withColumn("revenue", F.lit(None).cast(DoubleType()))

    # Filter to records with valid budget > 0
    df_with_financials = (
        df
        .filter(
            (F.col("budget").isNotNull())
            & (F.col("budget") > 0)
            & (F.col("revenue").isNotNull())
        )
        .withColumn("budget", F.col("budget").cast(DoubleType()))
        .withColumn("revenue", F.col("revenue").cast(DoubleType()))
    )

    # Calculate ROI
    df_roi = df_with_financials.withColumn(
        "roi",
        F.round((F.col("revenue") - F.col("budget")) / F.col("budget"), 4),
    )

    # Profit margin percentage
    df_roi = df_roi.withColumn(
        "profit_margin_pct",
        F.round(
            ((F.col("revenue") - F.col("budget")) / F.col("revenue")) * 100,
            2,
        ),
    )

    # Explode genres for per-genre ranking
    df_roi_exploded = df_roi.withColumn("genre", F.explode_outer(F.col("genres")))

    # Rank by ROI within each genre
    genre_roi_window = Window.partitionBy("genre").orderBy(F.col("roi").desc())
    df_ranked = df_roi_exploded.withColumn(
        "roi_genre_rank", F.rank().over(genre_roi_window)
    )

    # Rolling average ROI: last 10 movies per genre ordered by release_date
    genre_release_window = (
        Window.partitionBy("genre")
        .orderBy("release_date")
        .rowsBetween(-9, Window.currentRow)
    )

    df_with_rolling = df_ranked.withColumn(
        "rolling_avg_roi",
        F.round(F.avg("roi").over(genre_release_window), 4),
    )

    logger.info("ROI analysis computed - %d records.", df_with_rolling.count())
    return df_with_rolling


df_roi = build_roi_analysis(df_movies_aligned)

if df_roi.count() > 0:
    display(df_roi.orderBy(F.col("roi").desc()).limit(20))
else:
    logger.warning("No ROI data available - budget/revenue columns may be empty in trending data.")


# ## 7. Gold Table 5: Rolling Averages
#
# Compute time-series smoothing with:
# - **7-day rolling average** of popularity (row-based window)
# - **30-day rolling average** of vote_average (range-based window using days-since-epoch)


def build_rolling_averages(df: DataFrame) -> DataFrame:
    """
    Compute 7-day and 30-day rolling averages for popularity and vote metrics.

    Uses two window strategies:
    1. Row-based window (rowsBetween): 7-day rolling avg popularity over the
       last 6 rows + current row per content item, ordered by extraction_date.
    2. Range-based window (rangeBetween): 30-day rolling avg vote_average using
       a days-since-epoch column for true calendar-day ranges, covering the
       preceding 29 days + current day.

    Parameters
    df : DataFrame
        Unified content DataFrame with extraction_date, popularity, vote_average.

    Returns
    DataFrame
        DataFrame enriched with rolling average columns.
    """
    # -- 7-day rolling average popularity (row-based) --
    # Assumes one record per content item per day; the last 7 rows ≈ last 7 days.
    popularity_window = (
        Window.partitionBy("id")
        .orderBy("extraction_date")
        .rowsBetween(-6, Window.currentRow)  # current row + 6 preceding = 7 days
    )

    df_rolling = df.withColumn(
        "rolling_7d_avg_popularity",
        F.round(F.avg("popularity").over(popularity_window), 4),
    )

    # Count of data points in the rolling window (for confidence assessment)
    df_rolling = df_rolling.withColumn(
        "rolling_7d_data_points",
        F.count("popularity").over(popularity_window),
    )

    # -- 30-day rolling average vote_average (range-based) --
    # Convert extraction_date to days since epoch for rangeBetween
    df_rolling = df_rolling.withColumn(
        "_days_since_epoch",
        F.datediff(F.col("extraction_date"), F.lit("1970-01-01")),
    )

    vote_avg_window = (
        Window.partitionBy("id")
        .orderBy("_days_since_epoch")
        .rangeBetween(-29, Window.currentRow)  # 30-day calendar range
    )

    df_rolling = df_rolling.withColumn(
        "rolling_30d_avg_vote_average",
        F.round(F.avg("vote_average").over(vote_avg_window), 4),
    )

    df_rolling = df_rolling.withColumn(
        "rolling_30d_data_points",
        F.count("vote_average").over(vote_avg_window),
    )

    # -- 7-day rolling average vote_count (row-based) for engagement trends --
    vote_count_window = (
        Window.partitionBy("id")
        .orderBy("extraction_date")
        .rowsBetween(-6, Window.currentRow)
    )

    df_rolling = df_rolling.withColumn(
        "rolling_7d_avg_vote_count",
        F.round(F.avg("vote_count").over(vote_count_window), 2),
    )

    # Clean up helper column
    df_rolling = df_rolling.drop("_days_since_epoch")

    logger.info("Rolling averages computed - %d records.", df_rolling.count())
    return df_rolling


df_rolling = build_rolling_averages(df_content)
display(df_rolling.filter(F.col("extraction_date") == EXTRACTION_DATE).orderBy(F.col("popularity").desc()).limit(20))


# ## 8. Write All Gold Tables to Delta


def write_gold_table(df: DataFrame, table_name: str, partition_cols: list[str] | None = None) -> int:
    """
    Write a Gold DataFrame to Delta format.

    Parameters
    df : DataFrame
        The Gold table DataFrame.
    table_name : str
        Name of the gold table (used for path).
    partition_cols : list[str] | None
        Optional partition columns.

    Returns
    int
        Number of records written.
    """
    gold_path = f"{GOLD_BASE_PATH}/{table_name}/"
    logger.info("Writing gold table '%s' to: %s", table_name, gold_path)

    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.save(gold_path)

    record_count = df.count()
    logger.info("Wrote %d records to gold/%s", record_count, table_name)
    return record_count


# -- Write all gold tables --
gold_summary: dict[str, int] = {}

gold_summary["gold_daily_trends"] = write_gold_table(
    df_daily_trends, "gold_daily_trends", partition_cols=["extraction_date"],
)

gold_summary["gold_weekly_rankings"] = write_gold_table(
    df_weekly_rankings, "gold_weekly_rankings", partition_cols=["year_week"],
)

gold_summary["gold_monthly_genre_stats"] = write_gold_table(
    df_monthly_genres, "gold_monthly_genre_stats", partition_cols=["year_month"],
)

gold_summary["gold_roi_analysis"] = write_gold_table(
    df_roi, "gold_roi_analysis",
)

gold_summary["gold_rolling_averages"] = write_gold_table(
    df_rolling, "gold_rolling_averages", partition_cols=["extraction_date"],
)

logger.info("All gold tables written. Summary: %s", json.dumps(gold_summary, indent=2))


# ##  Gold Layer Summary


from pyspark.sql import Row

summary_rows = [
    Row(table_name=name, record_count=count)
    for name, count in gold_summary.items()
]

df_gold_summary = spark.createDataFrame(summary_rows)
display(df_gold_summary)

# Exit with status
dbutils.notebook.exit(json.dumps({
    "status": "success",
    "gold_tables": gold_summary,
    "extraction_date": EXTRACTION_DATE,
}))

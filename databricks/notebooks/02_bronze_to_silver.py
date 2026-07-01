
# #  CineTrends Pipeline - Stage 2: Bronze → Silver
#
# **Purpose:** Cleanse, deduplicate, and standardise Bronze data into a curated Silver layer
# ready for analytics consumption.
#
# **Transformations:**
# - Deduplication via `ROW_NUMBER` windowed by `id`, ordered by `_ingested_at DESC`
# - Type casting for numeric and date fields
# - Genre normalisation by exploding `genre_ids` and joining a lookup table
# - Data quality assertions (no null IDs, vote_average in [0, 10])
# - Null-safe default filling for optional fields
#
# **Output:** Silver Delta tables partitioned by `extraction_date`


from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Final

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
)


# ## 1. Configuration & Widgets


# -- Widget parameters --
dbutils.widgets.text("storage_account", "", "Azure Storage Account Name")
dbutils.widgets.text("container", "cinetrends-datalake", "Blob Container Name")
dbutils.widgets.text("date", datetime.now(timezone.utc).strftime("%Y-%m-%d"), "Extraction Date (YYYY-MM-DD)")
dbutils.widgets.text("log_level", "INFO", "Logging Level")

STORAGE_ACCOUNT: str = dbutils.widgets.get("storage_account")
CONTAINER: str = dbutils.widgets.get("container")
EXTRACTION_DATE: str = dbutils.widgets.get("date")
LOG_LEVEL: str = dbutils.widgets.get("log_level")

# Logging setup
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("CineTrends.BronzeToSilver")

# Storage paths
WASBS_BASE: str = f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net"
BRONZE_BASE_PATH: str = f"{WASBS_BASE}/bronze"
SILVER_BASE_PATH: str = f"{WASBS_BASE}/silver"

# Azure storage access
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net",
    dbutils.secrets.get(scope="cinetrends-kv", key="storage-account-key"),
)

MEDIA_TYPES: Final[list[str]] = ["trending_movies", "trending_tv", "trending_people"]

logger.info("Bronze→Silver pipeline started - date=%s", EXTRACTION_DATE)


# ## 2. Genre Lookup Table
#
# TMDB uses integer genre IDs. We maintain a static lookup for human-readable names.


# -- TMDB genre ID → name mapping (movies & TV combined) --
GENRE_LOOKUP: dict[int, str] = {
    28: "Action", 12: "Adventure", 16: "Animation", 35: "Comedy",
    80: "Crime", 99: "Documentary", 18: "Drama", 10751: "Family",
    14: "Fantasy", 36: "History", 27: "Horror", 10402: "Music",
    9648: "Mystery", 10749: "Romance", 878: "Science Fiction",
    10770: "TV Movie", 53: "Thriller", 10752: "War", 37: "Western",
    # TV-specific genres
    10759: "Action & Adventure", 10762: "Kids", 10763: "News",
    10764: "Reality", 10765: "Sci-Fi & Fantasy", 10766: "Soap",
    10767: "Talk", 10768: "War & Politics",
}

# Create a Spark DataFrame for the genre lookup to enable broadcast joins
genre_rows = [{"genre_id": gid, "genre_name": gname} for gid, gname in GENRE_LOOKUP.items()]
df_genre_lookup = spark.createDataFrame(genre_rows)

logger.info("Genre lookup table created with %d entries.", len(GENRE_LOOKUP))


# ## 3. Deduplication
#
# Use `ROW_NUMBER()` partitioned by entity ID, ordered by ingestion timestamp descending,
# to keep only the most recent record per entity.


def deduplicate(df: DataFrame, id_column: str = "id") -> DataFrame:
    """
    Remove duplicate records, keeping only the latest ingestion per entity.

    Uses a ROW_NUMBER window function partitioned by the entity's unique ID
    and ordered by _ingested_at descending. Only row_num == 1 is retained.

    Parameters
    df : DataFrame
        Input DataFrame with potential duplicates.
    id_column : str
        Column name for the entity's unique identifier.

    Returns
    DataFrame
        Deduplicated DataFrame with the `_row_num` column dropped.
    """
    dedup_window = Window.partitionBy(id_column).orderBy(F.col("_ingested_at").desc())

    df_deduped = (
        df
        .withColumn("_row_num", F.row_number().over(dedup_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    original_count = df.count()
    deduped_count = df_deduped.count()
    duplicates_removed = original_count - deduped_count

    logger.info(
        "Deduplication: %d → %d records (%d duplicates removed).",
        original_count, deduped_count, duplicates_removed,
    )

    return df_deduped


# ## 4. Type Casting & Normalisation


def cast_types_movies(df: DataFrame) -> DataFrame:
    """
    Cast columns to their correct types for movie data.

    Converts vote_average and popularity to Double, vote_count to Integer,
    and release_date to Date type. Adds an extraction_date column.

    Parameters
    df : DataFrame
        Raw-typed DataFrame from Bronze.

    Returns
    DataFrame
        DataFrame with properly typed columns.
    """
    return (
        df
        .withColumn("vote_average", F.col("vote_average").cast(DoubleType()))
        .withColumn("vote_count", F.col("vote_count").cast(IntegerType()))
        .withColumn("popularity", F.col("popularity").cast(DoubleType()))
        .withColumn("release_date", F.to_date(F.col("release_date"), "yyyy-MM-dd"))
        .withColumn("extraction_date", F.col("_extraction_date"))
    )


def cast_types_tv(df: DataFrame) -> DataFrame:
    """
    Cast columns to their correct types for TV show data.

    Similar to movie casting but uses first_air_date instead of release_date.

    Parameters
    df : DataFrame
        Raw-typed DataFrame from Bronze.

    Returns
    DataFrame
        DataFrame with properly typed columns.
    """
    return (
        df
        .withColumn("vote_average", F.col("vote_average").cast(DoubleType()))
        .withColumn("vote_count", F.col("vote_count").cast(IntegerType()))
        .withColumn("popularity", F.col("popularity").cast(DoubleType()))
        .withColumn("first_air_date", F.to_date(F.col("first_air_date"), "yyyy-MM-dd"))
        .withColumn("extraction_date", F.col("_extraction_date"))
    )


def cast_types_people(df: DataFrame) -> DataFrame:
    """
    Cast columns to their correct types for people data.

    People records have fewer numeric fields; primarily popularity.

    Parameters
    df : DataFrame
        Raw-typed DataFrame from Bronze.

    Returns
    DataFrame
        DataFrame with properly typed columns.
    """
    return (
        df
        .withColumn("popularity", F.col("popularity").cast(DoubleType()))
        .withColumn("extraction_date", F.col("_extraction_date"))
    )


# Dispatcher for type casting by media type
TYPE_CASTERS: dict[str, callable] = {
    "trending_movies": cast_types_movies,
    "trending_tv": cast_types_tv,
    "trending_people": cast_types_people,
}


# ## 5. Genre Normalisation
#
# Explode `genre_ids` arrays and join with the genre lookup to produce
# human-readable genre names alongside each record.


def normalize_genres(df: DataFrame, genre_lookup_df: DataFrame) -> DataFrame:
    """
    Explode genre_ids and join with the genre lookup table.

    For each record, the genre_ids array is exploded into individual rows,
    then left-joined with the genre lookup to add genre_name. Records
    without genre_ids retain a single row with null genre fields.

    After joining, genres are re-aggregated into an array column `genres`
    containing the resolved genre names.

    Parameters
    df : DataFrame
        DataFrame with a `genre_ids` array column.
    genre_lookup_df : DataFrame
        Lookup DataFrame with `genre_id` and `genre_name` columns.

    Returns
    DataFrame
        DataFrame with an added `genres` column (array of genre name strings).
    """
    if "genre_ids" not in df.columns:
        logger.info("No genre_ids column found - skipping genre normalisation.")
        return df.withColumn("genres", F.array().cast("array<string>"))

    # Preserve all original columns for re-aggregation
    original_columns = df.columns

    # Explode genre_ids - use outer to preserve records with empty/null arrays
    df_exploded = df.withColumn("_genre_id_single", F.explode_outer(F.col("genre_ids")))

    # Broadcast join with genre lookup (small table)
    df_with_genres = df_exploded.join(
        F.broadcast(genre_lookup_df),
        df_exploded["_genre_id_single"] == genre_lookup_df["genre_id"],
        how="left",
    )

    # Re-aggregate genres back into an array per record
    group_cols = [c for c in original_columns if c != "genre_ids"]
    df_normalized = (
        df_with_genres
        .groupBy(*group_cols)
        .agg(
            F.collect_set("genre_name").alias("genres"),
            F.first("genre_ids").alias("genre_ids"),
        )
    )

    logger.info("Genre normalisation complete.")
    return df_normalized


# ## 6. Data Quality Assertions


def assert_data_quality(df: DataFrame, media_type: str) -> DataFrame:
    """
    Run data quality checks and raise on critical violations.

    Checks performed:
    1. No null values in the `id` column (critical - raises AssertionError).
    2. vote_average is within [0, 10] range where applicable.
    3. Logs warning for any records failing soft checks.

    Parameters
    df : DataFrame
        DataFrame to validate.
    media_type : str
        Media type for contextual logging.

    Returns
    DataFrame
        The validated DataFrame (rows failing soft checks are flagged, not removed).
    """
    total_count = df.count()

    # -- Critical: No null IDs --
    null_id_count = df.filter(F.col("id").isNull()).count()
    assert null_id_count == 0, (
        f"CRITICAL: {null_id_count}/{total_count} records in '{media_type}' have NULL id. "
        f"This indicates upstream data corruption."
    )
    logger.info(" DQ Check PASSED: No null IDs in '%s'.", media_type)

    # -- Soft: vote_average range [0, 10] --
    if "vote_average" in df.columns:
        out_of_range = df.filter(
            (F.col("vote_average") < 0) | (F.col("vote_average") > 10)
        ).count()

        if out_of_range > 0:
            logger.warning(
                "⚠️ DQ Warning: %d/%d records in '%s' have vote_average outside [0, 10].",
                out_of_range, total_count, media_type,
            )
            # Flag but don't remove - add a quality flag column
            df = df.withColumn(
                "_dq_vote_avg_valid",
                F.when(
                    (F.col("vote_average") >= 0) & (F.col("vote_average") <= 10), True
                ).otherwise(False),
            )
        else:
            logger.info(" DQ Check PASSED: All vote_average values in [0, 10] for '%s'.", media_type)
            df = df.withColumn("_dq_vote_avg_valid", F.lit(True))

    # -- Soft: Popularity is non-negative --
    if "popularity" in df.columns:
        negative_pop = df.filter(F.col("popularity") < 0).count()
        if negative_pop > 0:
            logger.warning(
                "⚠️ DQ Warning: %d records with negative popularity in '%s'.",
                negative_pop, media_type,
            )

    logger.info("Data quality assertions complete for '%s'.", media_type)
    return df


# ## 7. Fill Null Defaults


def fill_null_defaults(df: DataFrame, media_type: str) -> DataFrame:
    """
    Fill null values with sensible defaults to prevent downstream failures.

    Defaults applied:
    - overview → "No overview available"
    - vote_average → 0.0
    - vote_count → 0
    - popularity → 0.0
    - poster_path → "" (empty string)
    - genres → empty array

    Parameters
    df : DataFrame
        DataFrame with potential null values.
    media_type : str
        Media type for conditional defaults.

    Returns
    DataFrame
        DataFrame with nulls replaced by defaults.
    """
    # Common defaults for all media types
    common_defaults: dict[str, object] = {
        "popularity": 0.0,
    }

    # Media-specific defaults
    content_defaults: dict[str, object] = {
        "overview": "No overview available",
        "vote_average": 0.0,
        "vote_count": 0,
        "poster_path": "",
    }

    # Apply common defaults
    for col_name, default_val in common_defaults.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(default_val)))

    # Apply content defaults (movies and TV only)
    if media_type in ("trending_movies", "trending_tv"):
        for col_name, default_val in content_defaults.items():
            if col_name in df.columns:
                df = df.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(default_val)))

    # Fill empty genres array
    if "genres" in df.columns:
        df = df.withColumn(
            "genres",
            F.when(F.col("genres").isNull(), F.array().cast("array<string>"))
            .otherwise(F.col("genres")),
        )

    logger.info("Null defaults applied for '%s'.", media_type)
    return df


# ## 8. Write to Silver Delta Layer


def write_to_silver(df: DataFrame, media_type: str) -> int:
    """
    Write the cleansed DataFrame to the Silver Delta layer.

    Partitioned by extraction_date for efficient time-based queries.

    Parameters
    df : DataFrame
        Cleansed and validated DataFrame.
    media_type : str
        Media type for the output path.

    Returns
    int
        Number of records written.
    """
    silver_path = f"{SILVER_BASE_PATH}/{media_type}/"
    logger.info("Writing Silver Delta to: %s", silver_path)

    # Drop internal Bronze metadata columns before writing
    columns_to_drop = ["_file_path", "_row_num", "_corrupt_record", "year", "month"]
    for col_name in columns_to_drop:
        if col_name in df.columns:
            df = df.drop(col_name)

    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy("extraction_date")
        .option("mergeSchema", "true")
        .save(silver_path)
    )

    record_count = df.count()
    logger.info("Wrote %d records to silver/%s", record_count, media_type)
    return record_count


# ## 9. Execute Pipeline


# -- Main execution loop --
silver_summary: dict[str, dict[str, int]] = {}

for media_type in MEDIA_TYPES:
    logger.info("=" * 60)
    logger.info("Processing: %s", media_type)
    logger.info("=" * 60)

    # Step 1: Read from Bronze Delta
    bronze_path = f"{BRONZE_BASE_PATH}/{media_type}/"
    try:
        df_bronze = spark.read.format("delta").load(bronze_path)
    except Exception as exc:
        logger.error("Failed to read bronze/%s: %s", media_type, exc)
        silver_summary[media_type] = {"bronze_count": 0, "silver_count": 0, "status": "FAILED"}
        continue

    bronze_count = df_bronze.count()
    logger.info("Read %d records from bronze/%s", bronze_count, media_type)

    # Step 2: Deduplicate
    df_deduped = deduplicate(df_bronze, id_column="id")

    # Step 3: Cast types
    caster = TYPE_CASTERS.get(media_type)
    if caster is None:
        logger.error("No type caster defined for '%s'", media_type)
        continue
    df_typed = caster(df_deduped)

    # Step 4: Normalize genres (movies & TV only)
    if media_type in ("trending_movies", "trending_tv"):
        df_genred = normalize_genres(df_typed, df_genre_lookup)
    else:
        df_genred = df_typed

    # Step 5: Data quality assertions
    df_validated = assert_data_quality(df_genred, media_type)

    # Step 6: Fill null defaults
    df_clean = fill_null_defaults(df_validated, media_type)

    # Step 7: Write to Silver
    silver_count = write_to_silver(df_clean, media_type)

    silver_summary[media_type] = {
        "bronze_count": bronze_count,
        "silver_count": silver_count,
        "duplicates_removed": bronze_count - silver_count,
        "status": "SUCCESS",
    }

logger.info("Bronze→Silver pipeline complete.")


# ##  Processing Summary


# -- Display processing summary --
from pyspark.sql import Row

summary_rows = [
    Row(
        media_type=mt,
        bronze_records=info.get("bronze_count", 0),
        silver_records=info.get("silver_count", 0),
        duplicates_removed=info.get("duplicates_removed", 0),
        status=info.get("status", "UNKNOWN"),
    )
    for mt, info in silver_summary.items()
]

df_summary = spark.createDataFrame(summary_rows)
display(df_summary)

# Exit with status
dbutils.notebook.exit(json.dumps({
    "status": "success",
    "summary": silver_summary,
}))


# #  CineTrends Pipeline - Stage 1: Raw → Bronze
#
# **Purpose:** Ingest raw JSON data from Azure Blob Storage (TMDB API extractions)
# into the Bronze Delta layer with full metadata lineage.
#
# **Input:** Raw JSON files from `raw/trending_movies/`, `raw/trending_tv/`, `raw/trending_people/`
# **Output:** Bronze Delta tables partitioned by `year/month` under `bronze/`
#
# | 1 | Configure Azure storage access |
# | 2 | Read raw JSON files per media type |
# | 3 | Enrich with ingestion metadata |
# | 4 | Validate schema constraints |
# | 5 | Write to Bronze Delta with partitioning |


from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# ## 1. Configuration & Widget Parameters


# -- Widget parameters for flexible notebook execution --
dbutils.widgets.text("storage_account", "", "Azure Storage Account Name")
dbutils.widgets.text("container", "cinetrends-datalake", "Blob Container Name")
dbutils.widgets.text("date", datetime.now(timezone.utc).strftime("%Y-%m-%d"), "Extraction Date (YYYY-MM-DD)")
dbutils.widgets.text("log_level", "INFO", "Logging Level")

# Retrieve widget values
STORAGE_ACCOUNT: str = dbutils.widgets.get("storage_account")
CONTAINER: str = dbutils.widgets.get("container")
EXTRACTION_DATE: str = dbutils.widgets.get("date")
LOG_LEVEL: str = dbutils.widgets.get("log_level")

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("CineTrends.RawToBronze")

logger.info("Pipeline parameters - account=%s, container=%s, date=%s", STORAGE_ACCOUNT, CONTAINER, EXTRACTION_DATE)


# ## 2. Azure Storage Configuration


# -- Azure Blob Storage access via WASBS protocol --
WASBS_BASE: str = f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net"

# Set storage account key from Databricks secrets
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net",
    dbutils.secrets.get(scope="cinetrends-kv", key="storage-account-key"),
)

# Build source (raw) and target (bronze) paths
RAW_BASE_PATH: str = f"{WASBS_BASE}/raw"
BRONZE_BASE_PATH: str = f"{WASBS_BASE}/bronze"

# Media types to ingest
MEDIA_TYPES: list[str] = ["trending_movies", "trending_tv", "trending_people"]

# Generate a unique batch ID for this run
BATCH_ID: str = str(uuid.uuid4())
logger.info("Batch ID: %s", BATCH_ID)


# ## 3. Schema Definitions
#
# Explicit schemas prevent silent data corruption from upstream changes.


# -- Expected schemas per media type (used for validation, not enforcement at read) --
REQUIRED_COLUMNS: dict[str, list[str]] = {
    "trending_movies": ["id", "title", "popularity", "vote_average", "vote_count", "release_date", "genre_ids"],
    "trending_tv": ["id", "name", "popularity", "vote_average", "vote_count", "first_air_date", "genre_ids"],
    "trending_people": ["id", "name", "popularity", "known_for_department"],
}


# ## 4. Read Raw JSON & Enrich with Metadata


def read_raw_json(spark: SparkSession, media_type: str, date: str) -> DataFrame:
    """
    Read raw JSON files for a given media type and extraction date.

    The function reads from the date-partitioned raw directory, adds
    ingestion metadata columns for full lineage tracking, and returns
    the enriched DataFrame.

    Parameters
    spark : SparkSession
        Active Spark session.
    media_type : str
        One of 'trending_movies', 'trending_tv', 'trending_people'.
    date : str
        Extraction date in YYYY-MM-DD format.

    Returns
    DataFrame
        Raw data enriched with metadata columns.
    """
    raw_path = f"{RAW_BASE_PATH}/{media_type}/{date}/"
    logger.info("Reading raw JSON from: %s", raw_path)

    try:
        df = (
            spark.read
            .option("multiline", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .json(raw_path)
        )
    except Exception as exc:
        logger.error("Failed to read raw data for %s on %s: %s", media_type, date, exc)
        raise

    record_count = df.count()
    logger.info("Read %d records for media_type=%s", record_count, media_type)

    if record_count == 0:
        logger.warning("No records found for %s on %s - skipping.", media_type, date)
        return df

    # -- Enrich with ingestion metadata --
    df_enriched = (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit("tmdb_api"))
        .withColumn("_batch_id", F.lit(BATCH_ID))
        .withColumn("_file_path", F.input_file_name())
        .withColumn("_media_type", F.lit(media_type))
        .withColumn("_extraction_date", F.lit(date).cast("date"))
    )

    return df_enriched


# ## 5. Schema Validation


def validate_schema(df: DataFrame, media_type: str) -> None:
    """
    Assert that the DataFrame contains all required columns for the media type.

    Raises AssertionError with a descriptive message if any columns are missing.

    Parameters
    df : DataFrame
        The DataFrame to validate.
    media_type : str
        The media type key for looking up required columns.
    """
    required = set(REQUIRED_COLUMNS.get(media_type, []))
    actual = set(df.columns)
    missing = required - actual

    assert not missing, (
        f"Schema validation FAILED for '{media_type}'. "
        f"Missing columns: {sorted(missing)}. "
        f"Available columns: {sorted(actual)}"
    )
    logger.info("Schema validation PASSED for '%s' - all %d required columns present.", media_type, len(required))


# ## 6. Write to Bronze Delta Lake


def write_to_bronze(df: DataFrame, media_type: str) -> int:
    """
    Write the enriched DataFrame to the Bronze Delta layer.

    Data is partitioned by year and month derived from the extraction date
    for efficient downstream queries.

    Parameters
    df : DataFrame
        Enriched DataFrame with metadata columns.
    media_type : str
        Media type used for the output path.

    Returns
    int
        Number of records written.
    """
    # Add partitioning columns
    df_partitioned = (
        df
        .withColumn("year", F.year(F.col("_extraction_date")))
        .withColumn("month", F.month(F.col("_extraction_date")))
    )

    bronze_path = f"{BRONZE_BASE_PATH}/{media_type}/"
    logger.info("Writing Bronze Delta to: %s", bronze_path)

    (
        df_partitioned.write
        .format("delta")
        .mode("append")
        .partitionBy("year", "month")
        .option("mergeSchema", "true")
        .save(bronze_path)
    )

    record_count = df_partitioned.count()
    logger.info("Wrote %d records to bronze/%s", record_count, media_type)
    return record_count


# ## 7. Execute Pipeline


# -- Main execution: iterate over all media types --
ingestion_summary: dict[str, int] = {}

for media_type in MEDIA_TYPES:
    logger.info("=" * 60)
    logger.info("Processing media type: %s", media_type)
    logger.info("=" * 60)

    # Step 1: Read raw JSON
    df_raw = read_raw_json(spark, media_type, EXTRACTION_DATE)

    if df_raw.count() == 0:
        ingestion_summary[media_type] = 0
        continue

    # Step 2: Validate schema
    validate_schema(df_raw, media_type)

    # Step 3: Write to Bronze
    count = write_to_bronze(df_raw, media_type)
    ingestion_summary[media_type] = count

logger.info("Pipeline complete. Summary: %s", json.dumps(ingestion_summary, indent=2))


# ##  Ingestion Summary
#
# The following table summarises records ingested into the Bronze layer during this run.


# -- Display ingestion summary as a DataFrame for notebook rendering --
from pyspark.sql import Row

summary_rows = [
    Row(
        media_type=media_type,
        record_count=count,
        batch_id=BATCH_ID,
        extraction_date=EXTRACTION_DATE,
        status=" Success" if count > 0 else "⚠️ Empty",
    )
    for media_type, count in ingestion_summary.items()
]

df_summary = spark.createDataFrame(summary_rows)
display(df_summary)

# -- Log total records for pipeline monitoring --
total_records = sum(ingestion_summary.values())
logger.info("Total records ingested to Bronze: %d", total_records)

# Exit with status for orchestrator
dbutils.notebook.exit(json.dumps({
    "status": "success",
    "batch_id": BATCH_ID,
    "total_records": total_records,
    "summary": ingestion_summary,
}))

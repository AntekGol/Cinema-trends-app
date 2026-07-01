
# #  CineTrends Pipeline - Stage 4: Gold → PostgreSQL
#
# **Purpose:** Export Gold Delta tables to PostgreSQL for downstream API consumption
# and dashboard integration. Uses JDBC with temp-table upsert strategy for
# idempotent, production-safe writes.
#
# **Strategy:**
# 1. Write to temporary staging tables via JDBC `overwrite`
# 2. Execute SQL `MERGE` (via `INSERT ... ON CONFLICT UPDATE`) from staging → main tables
# 3. Validate record counts by reading back from PostgreSQL
#
# **Tables Exported:**
# | Gold Table | PostgreSQL Target |
# | `gold_daily_trends` | `public.daily_trends` |
# | `gold_weekly_rankings` | `public.weekly_rankings` |
# | `gold_monthly_genre_stats` | `public.monthly_genre_stats` |
# | `gold_roi_analysis` | `public.roi_analysis` |
# | `gold_rolling_averages` | `public.rolling_averages` |


from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Final

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# ## 1. Configuration & JDBC Parameters


# -- Widget parameters --
dbutils.widgets.text("storage_account", "", "Azure Storage Account Name")
dbutils.widgets.text("container", "cinetrends-datalake", "Blob Container Name")
dbutils.widgets.text("pg_host", "", "PostgreSQL Host")
dbutils.widgets.text("pg_port", "5432", "PostgreSQL Port")
dbutils.widgets.text("pg_database", "cinetrends", "PostgreSQL Database")
dbutils.widgets.text("pg_schema", "public", "PostgreSQL Schema")
dbutils.widgets.text("log_level", "INFO", "Logging Level")

STORAGE_ACCOUNT: str = dbutils.widgets.get("storage_account")
CONTAINER: str = dbutils.widgets.get("container")
PG_HOST: str = dbutils.widgets.get("pg_host")
PG_PORT: str = dbutils.widgets.get("pg_port")
PG_DATABASE: str = dbutils.widgets.get("pg_database")
PG_SCHEMA: str = dbutils.widgets.get("pg_schema")
LOG_LEVEL: str = dbutils.widgets.get("log_level")

# Logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("CineTrends.GoldToPostgres")

# -- Retrieve secrets for database credentials --
PG_USER: str = dbutils.secrets.get(scope="cinetrends-kv", key="pg-username")
PG_PASSWORD: str = dbutils.secrets.get(scope="cinetrends-kv", key="pg-password")

# -- Azure storage config --
WASBS_BASE: str = f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net"
GOLD_BASE_PATH: str = f"{WASBS_BASE}/gold"

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net",
    dbutils.secrets.get(scope="cinetrends-kv", key="storage-account-key"),
)

logger.info("Gold→PostgreSQL pipeline started. Target: %s:%s/%s", PG_HOST, PG_PORT, PG_DATABASE)


# ## 2. JDBC Connection Properties


# -- Build JDBC URL and connection properties --
JDBC_URL: Final[str] = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

JDBC_PROPERTIES: Final[dict[str, str]] = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
    # Connection pool tuning
    "batchsize": "5000",
    "isolationLevel": "READ_COMMITTED",
    # SSL (enable for production)
    "ssl": "true",
    "sslmode": "require",
}

logger.info("JDBC URL: %s (user=%s)", JDBC_URL, PG_USER)


# ## 3. Table Mapping Configuration
#
# Define the mapping between Gold Delta tables and PostgreSQL target tables,
# including the conflict resolution keys for upsert operations.


# -- Table configuration: gold_name → (pg_table, conflict_keys, select_columns) --
TABLE_CONFIGS: Final[dict[str, dict]] = {
    "gold_daily_trends": {
        "pg_table": f"{PG_SCHEMA}.daily_trends",
        "pg_staging_table": f"{PG_SCHEMA}._staging_daily_trends",
        "conflict_keys": ["id", "extraction_date"],
        "select_columns": [
            "id", "title", "media_type", "popularity", "vote_average", "vote_count",
            "position", "prev_position", "position_change", "popularity_delta",
            "is_new_entry", "days_on_chart", "extraction_date",
        ],
    },
    "gold_weekly_rankings": {
        "pg_table": f"{PG_SCHEMA}.weekly_rankings",
        "pg_staging_table": f"{PG_SCHEMA}._staging_weekly_rankings",
        "conflict_keys": ["id", "year_week"],
        "select_columns": [
            "id", "title", "media_type", "year_week", "weekly_rank",
            "avg_popularity", "peak_popularity", "avg_vote_average",
            "total_vote_count", "days_tracked",
        ],
    },
    "gold_monthly_genre_stats": {
        "pg_table": f"{PG_SCHEMA}.monthly_genre_stats",
        "pg_staging_table": f"{PG_SCHEMA}._staging_monthly_genre_stats",
        "conflict_keys": ["genre", "year_month"],
        "select_columns": [
            "genre", "year_month", "title_count", "avg_popularity",
            "avg_vote_average", "total_vote_count", "peak_popularity", "top_title",
        ],
    },
    "gold_roi_analysis": {
        "pg_table": f"{PG_SCHEMA}.roi_analysis",
        "pg_staging_table": f"{PG_SCHEMA}._staging_roi_analysis",
        "conflict_keys": ["id", "genre"],
        "select_columns": [
            "id", "title", "genre", "budget", "revenue", "roi",
            "profit_margin_pct", "roi_genre_rank", "rolling_avg_roi",
            "release_date",
        ],
    },
    "gold_rolling_averages": {
        "pg_table": f"{PG_SCHEMA}.rolling_averages",
        "pg_staging_table": f"{PG_SCHEMA}._staging_rolling_averages",
        "conflict_keys": ["id", "extraction_date"],
        "select_columns": [
            "id", "title", "media_type", "popularity", "vote_average",
            "rolling_7d_avg_popularity", "rolling_7d_data_points",
            "rolling_30d_avg_vote_average", "rolling_30d_data_points",
            "rolling_7d_avg_vote_count", "extraction_date",
        ],
    },
}


# ## 4. JDBC Write Utilities


def write_to_staging(df: DataFrame, staging_table: str) -> int:
    """
    Write a DataFrame to a PostgreSQL staging table via JDBC.

    Uses 'overwrite' mode to fully replace the staging table contents
    on each pipeline run, ensuring idempotent behaviour.

    Parameters
    df : DataFrame
        The DataFrame to write.
    staging_table : str
        Fully-qualified PostgreSQL staging table name (schema.table).

    Returns
    int
        Number of records written.
    """
    record_count = df.count()
    logger.info("Writing %d records to staging table: %s", record_count, staging_table)

    (
        df.write
        .jdbc(
            url=JDBC_URL,
            table=staging_table,
            mode="overwrite",
            properties=JDBC_PROPERTIES,
        )
    )

    logger.info("Successfully wrote to staging: %s", staging_table)
    return record_count


def execute_upsert(staging_table: str, target_table: str, conflict_keys: list[str], columns: list[str]) -> None:
    """
    Execute an upsert (INSERT ... ON CONFLICT ... DO UPDATE) from staging to target.

    This function constructs and executes the PostgreSQL-specific upsert SQL
    using a JDBC connection. The staging table data is merged into the target
    table based on the specified conflict keys.

    Parameters
    staging_table : str
        Source staging table name.
    target_table : str
        Target main table name.
    conflict_keys : list[str]
        Columns forming the unique constraint for conflict detection.
    columns : list[str]
        All columns to insert/update.
    """
    # Build the ON CONFLICT clause
    conflict_clause = ", ".join(conflict_keys)

    # Build the column list and values placeholder
    col_list = ", ".join(columns)
    update_set = ", ".join(
        f"{col} = EXCLUDED.{col}" for col in columns if col not in conflict_keys
    )

    upsert_sql = f"""
        INSERT INTO {target_table} ({col_list})
        SELECT {col_list} FROM {staging_table}
        ON CONFLICT ({conflict_clause})
        DO UPDATE SET {update_set}
    """

    logger.info("Executing upsert: %s → %s (conflict keys: %s)", staging_table, target_table, conflict_keys)

    # Execute via JDBC connection
    driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
    connection = driver_manager.getConnection(JDBC_URL, PG_USER, PG_PASSWORD)

    try:
        statement = connection.createStatement()
        rows_affected = statement.executeUpdate(upsert_sql)
        logger.info("Upsert complete: %d rows affected in %s", rows_affected, target_table)
        statement.close()
    except Exception as exc:
        logger.error("Upsert FAILED for %s: %s", target_table, exc)
        raise
    finally:
        connection.close()


def cleanup_staging(staging_table: str) -> None:
    """
    Drop the staging table after successful upsert.

    Parameters
    staging_table : str
        Fully-qualified staging table name to drop.
    """
    drop_sql = f"DROP TABLE IF EXISTS {staging_table}"
    logger.info("Cleaning up staging table: %s", staging_table)

    driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
    connection = driver_manager.getConnection(JDBC_URL, PG_USER, PG_PASSWORD)

    try:
        statement = connection.createStatement()
        statement.executeUpdate(drop_sql)
        statement.close()
        logger.info("Staging table dropped: %s", staging_table)
    except Exception as exc:
        logger.warning("Failed to drop staging table %s: %s", staging_table, exc)
    finally:
        connection.close()


# ## 5. Direct Append Write (for Daily Trends)
#
# For the daily trends table, we also support a simple append mode
# for incremental daily loads where upsert overhead is unnecessary.


def write_daily_append(df: DataFrame, target_table: str) -> int:
    """
    Append daily trend records directly to the target table.

    This is used for initial loads or when data is guaranteed to be
    non-overlapping (e.g., new extraction_date only).

    Parameters
    df : DataFrame
        DataFrame to append.
    target_table : str
        PostgreSQL target table.

    Returns
    int
        Number of records appended.
    """
    record_count = df.count()
    logger.info("Appending %d records to: %s", record_count, target_table)

    (
        df.write
        .jdbc(
            url=JDBC_URL,
            table=target_table,
            mode="append",
            properties=JDBC_PROPERTIES,
        )
    )

    logger.info("Append complete for %s", target_table)
    return record_count


# ## 6. Execute Full Export Pipeline


# -- Main execution: iterate over all gold tables --
export_summary: dict[str, dict] = {}

for gold_table_name, config in TABLE_CONFIGS.items():
    logger.info("=" * 60)
    logger.info("Exporting: %s → %s", gold_table_name, config["pg_table"])
    logger.info("=" * 60)

    # Step 1: Read Gold Delta table
    gold_path = f"{GOLD_BASE_PATH}/{gold_table_name}/"
    try:
        df_gold = spark.read.format("delta").load(gold_path)
    except Exception as exc:
        logger.error("Failed to read gold/%s: %s", gold_table_name, exc)
        export_summary[gold_table_name] = {"status": "FAILED_READ", "error": str(exc)}
        continue

    # Step 2: Select only the columns we want to export
    available_select_cols = [c for c in config["select_columns"] if c in df_gold.columns]
    df_export = df_gold.select(*available_select_cols)

    # Handle array columns - convert to PostgreSQL-compatible format (comma-separated string)
    for field in df_export.schema.fields:
        if str(field.dataType).startswith("ArrayType"):
            df_export = df_export.withColumn(
                field.name,
                F.concat_ws(", ", F.col(field.name)),
            )

    gold_count = df_export.count()
    logger.info("Gold records to export: %d", gold_count)

    # Step 3: Write to staging table
    try:
        staging_count = write_to_staging(df_export, config["pg_staging_table"])
    except Exception as exc:
        logger.error("Failed to write staging for %s: %s", gold_table_name, exc)
        export_summary[gold_table_name] = {"status": "FAILED_STAGING", "error": str(exc)}
        continue

    # Step 4: Upsert from staging to main table
    try:
        execute_upsert(
            staging_table=config["pg_staging_table"],
            target_table=config["pg_table"],
            conflict_keys=config["conflict_keys"],
            columns=available_select_cols,
        )
    except Exception as exc:
        logger.error("Upsert failed for %s: %s", gold_table_name, exc)
        export_summary[gold_table_name] = {"status": "FAILED_UPSERT", "error": str(exc)}
        continue

    # Step 5: Cleanup staging table
    cleanup_staging(config["pg_staging_table"])

    export_summary[gold_table_name] = {
        "status": "SUCCESS",
        "gold_records": gold_count,
        "staged_records": staging_count,
    }

logger.info("Export pipeline complete.")


# ## 7. Validation - Read Back & Compare Counts


def validate_export(table_name: str, pg_table: str, expected_count: int) -> dict:
    """
    Validate the export by reading back record counts from PostgreSQL.

    Compares the count in the PostgreSQL target table against the expected
    count from the Gold Delta table. Reports any discrepancies.

    Parameters
    table_name : str
        Gold table name (for logging).
    pg_table : str
        PostgreSQL table to query.
    expected_count : int
        Expected number of records.

    Returns
    dict
        Validation result with actual count, expected count, and match status.
    """
    try:
        df_pg = (
            spark.read
            .jdbc(
                url=JDBC_URL,
                table=pg_table,
                properties=JDBC_PROPERTIES,
            )
        )
        actual_count = df_pg.count()
        is_valid = actual_count >= expected_count  # >= because historical data may exist

        logger.info(
            "Validation %s: %s - expected>=%d, actual=%d",
            " PASSED" if is_valid else " FAILED",
            table_name,
            expected_count,
            actual_count,
        )

        return {
            "table": table_name,
            "pg_table": pg_table,
            "expected_min": expected_count,
            "actual_count": actual_count,
            "status": "PASSED" if is_valid else "FAILED",
        }
    except Exception as exc:
        logger.error("Validation read-back failed for %s: %s", pg_table, exc)
        return {
            "table": table_name,
            "pg_table": pg_table,
            "expected_min": expected_count,
            "actual_count": -1,
            "status": "ERROR",
            "error": str(exc),
        }


# -- Run validation for all successfully exported tables --
validation_results: list[dict] = []

for gold_table_name, result in export_summary.items():
    if result.get("status") == "SUCCESS":
        config = TABLE_CONFIGS[gold_table_name]
        validation = validate_export(
            table_name=gold_table_name,
            pg_table=config["pg_table"],
            expected_count=result["gold_records"],
        )
        validation_results.append(validation)


# ##  Export & Validation Summary


# -- Display export summary --
from pyspark.sql import Row

export_rows = [
    Row(
        gold_table=name,
        status=info.get("status", "UNKNOWN"),
        gold_records=info.get("gold_records", 0),
        staged_records=info.get("staged_records", 0),
        error=info.get("error", ""),
    )
    for name, info in export_summary.items()
]

df_export_summary = spark.createDataFrame(export_rows)
display(df_export_summary)

# -- Display validation results --
if validation_results:
    validation_rows = [
        Row(
            gold_table=v["table"],
            pg_table=v["pg_table"],
            expected_min=v["expected_min"],
            actual_count=v["actual_count"],
            validation_status=v["status"],
        )
        for v in validation_results
    ]
    df_validation = spark.createDataFrame(validation_rows)
    display(df_validation)


# ##  Pipeline Complete


# -- Final status for orchestrator --
all_success = all(r.get("status") == "SUCCESS" for r in export_summary.values())
all_valid = all(v["status"] == "PASSED" for v in validation_results) if validation_results else True

final_status = "success" if (all_success and all_valid) else "partial_failure"

logger.info("Gold→PostgreSQL pipeline finished with status: %s", final_status)

dbutils.notebook.exit(json.dumps({
    "status": final_status,
    "export_summary": export_summary,
    "validation_results": validation_results,
}))

"""
Common DataFrame utilities: hashing, metadata enrichment, standardization,
broadcast hints, and dead-letter queue writers.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional, Sequence

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window

from config.constants import (
    META_BATCH_ID,
    META_EVENT_TIME,
    META_INGESTION_TIME,
    META_LOAD_ID,
    META_RECORD_HASH,
    META_SOURCE_FILE,
)
from config.paths import PATHS


def generate_run_id() -> str:
    return str(uuid.uuid4())


def generate_load_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"LOAD_{ts}_{uuid.uuid4().hex[:8]}"


def sha256_expr(columns: Sequence[str]) -> Column:
    """Build a SHA-256 hash expression over the given columns."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in columns]
    return F.sha2(F.concat_ws("||", *parts), 256)


def add_bronze_metadata(
    df: DataFrame,
    load_id: str,
    batch_id: str,
    hash_columns: Optional[Sequence[str]] = None,
    event_time_column: Optional[str] = None,
) -> DataFrame:
    """
    Enrich raw DataFrame with lineage metadata columns required by bronze.
    """
    cols = df.columns
    result = df

    if "_metadata" in cols:
        result = result.withColumn(META_SOURCE_FILE, F.col("_metadata.file_path"))
    elif META_SOURCE_FILE not in cols:
        result = result.withColumn(META_SOURCE_FILE, F.lit("unknown"))

    result = (
        result.withColumn(META_INGESTION_TIME, F.current_timestamp())
        .withColumn(META_LOAD_ID, F.lit(load_id))
        .withColumn(META_BATCH_ID, F.lit(batch_id))
    )

    if event_time_column and event_time_column in result.columns:
        result = result.withColumn(
            META_EVENT_TIME, F.to_timestamp(F.col(event_time_column))
        )
    elif META_EVENT_TIME not in result.columns:
        result = result.withColumn(META_EVENT_TIME, F.col(META_INGESTION_TIME))

    business_cols = [
        c for c in result.columns if not c.startswith("_") and c not in ("_metadata",)
    ]
    hash_cols = list(hash_columns) if hash_columns else business_cols
    if hash_cols:
        result = result.withColumn(META_RECORD_HASH, sha256_expr(hash_cols))
    else:
        result = result.withColumn(META_RECORD_HASH, F.lit(None).cast("string"))

    if "_metadata" in result.columns:
        result = result.drop("_metadata")

    return result


def standardize_string_columns(df: DataFrame, columns: Sequence[str]) -> DataFrame:
    result = df
    for c in columns:
        if c in result.columns:
            result = result.withColumn(c, F.trim(F.col(c)))
    return result


def standardize_email(col_name: str) -> Column:
    return F.lower(F.trim(F.col(col_name)))


def cast_columns(df: DataFrame, casts: dict[str, str]) -> DataFrame:
    result = df
    for col_name, dtype in casts.items():
        if col_name in result.columns:
            result = result.withColumn(col_name, F.col(col_name).cast(dtype))
    return result


def drop_exact_duplicates(df: DataFrame, key_columns: Sequence[str]) -> DataFrame:
    return df.dropDuplicates(list(key_columns))


def dedupe_keep_latest(
    df: DataFrame,
    key_columns: Sequence[str],
    order_column: str,
) -> DataFrame:
    window = Window.partitionBy(*key_columns).orderBy(F.col(order_column).desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def write_to_dlq(
    spark: SparkSession,
    df: DataFrame,
    entity: str,
    error_message: str,
) -> None:
    """Route failed records to the dead-letter queue Delta path."""
    from src.utilities.delta_helpers import write_delta

    enriched = (
        df.withColumn("_dlq_error", F.lit(error_message))
        .withColumn("_dlq_time", F.current_timestamp())
        .withColumn("_dlq_entity", F.lit(entity))
    )
    write_delta(enriched, PATHS.dlq_path(entity), mode="append", merge_schema=True)


def broadcast_hint(df: DataFrame) -> DataFrame:
    return F.broadcast(df)


DLQ_META_SCHEMA = StructType(
    [
        StructField("_dlq_error", StringType(), True),
        StructField("_dlq_time", TimestampType(), True),
        StructField("_dlq_entity", StringType(), True),
    ]
)

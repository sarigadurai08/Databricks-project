"""
Common DataFrame utilities: hashing, metadata enrichment, standardization,
broadcast hints, and dead-letter queue writers.
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from typing import Iterable, Optional, Sequence

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window

from config.constants import (
    META_BATCH_ID,
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
) -> DataFrame:
    """
    Enrich raw DataFrame with lineage metadata columns required by bronze.

    Auto Loader typically provides `_metadata.file_path`; we normalize to `_source_file`.
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

    business_cols = [
        c
        for c in result.columns
        if not c.startswith("_") and c not in ("_metadata",)
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


def standardize_phone(col_name: str) -> Column:
    """Normalize US-style phone numbers to digits-only where possible."""
    return F.regexp_replace(F.col(col_name), r"[^0-9+]", "")


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


def null_safe_fill(df: DataFrame, fill_map: dict[str, object]) -> DataFrame:
    return df.fillna(fill_map)


def broadcast_join(
    left: DataFrame,
    right: DataFrame,
    on: str | list[str],
    how: str = "left",
) -> DataFrame:
    return left.join(F.broadcast(right), on=on, how=how)


def cache_df(df: DataFrame, storage_level: str = "MEMORY_AND_DISK") -> DataFrame:
    """Persist DataFrame when supported; no-op on Serverless if persist fails."""
    try:
        from pyspark import StorageLevel

        levels = {
            "MEMORY_ONLY": StorageLevel.MEMORY_ONLY,
            "MEMORY_AND_DISK": StorageLevel.MEMORY_AND_DISK,
            "DISK_ONLY": StorageLevel.DISK_ONLY,
        }
        return df.persist(levels.get(storage_level, StorageLevel.MEMORY_AND_DISK))
    except Exception:
        return df


DLQ_SCHEMA = StructType(
    [
        StructField("DLQ_ID", StringType(), False),
        StructField("Entity", StringType(), False),
        StructField("RunID", StringType(), True),
        StructField("ErrorType", StringType(), True),
        StructField("ErrorMessage", StringType(), True),
        StructField("Payload", StringType(), True),
        StructField("CreatedAt", TimestampType(), False),
    ]
)


def write_to_dlq(
    spark: SparkSession,
    entity: str,
    run_id: str,
    error_type: str,
    error_message: str,
    payload_df: Optional[DataFrame] = None,
    payload_json: Optional[str] = None,
) -> None:
    """Append failed records / error payloads to the dead-letter queue Delta table."""
    if payload_df is not None:
        payload_json = payload_df.select(
            F.to_json(F.struct(*[F.col(c) for c in payload_df.columns])).alias("j")
        ).collect()[0]["j"] if payload_df.take(1) else None

    row = {
        "DLQ_ID": str(uuid.uuid4()),
        "Entity": entity,
        "RunID": run_id,
        "ErrorType": error_type,
        "ErrorMessage": error_message,
        "Payload": payload_json,
        "CreatedAt": datetime.now(timezone.utc).replace(tzinfo=None),
    }
    df = spark.createDataFrame([row], schema=DLQ_SCHEMA)
    from src.utilities.delta_helpers import write_delta

    write_delta(df, PATHS.dlq_path(entity), mode="append", merge_schema=True)


def dataframe_to_json_column(df: DataFrame) -> DataFrame:
    return df.select(F.to_json(F.struct(*[F.col(c) for c in df.columns])).alias("payload"))

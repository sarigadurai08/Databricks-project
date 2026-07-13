"""
SCD Type 1 and Type 2 implementations using Delta MERGE.

SCD1: overwrite changed attribute columns in place.
SCD2: maintain history with EffectiveStartDate, EffectiveEndDate, IsCurrent, VersionNumber.
"""

from __future__ import annotations

from typing import Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.constants import (
    SCD2_EFFECTIVE_END,
    SCD2_EFFECTIVE_START,
    SCD2_IS_CURRENT,
    SCD2_VERSION,
)
from src.logging.logger import HealthcareLogger
from src.utilities.delta_helpers import merge_delta, table_exists, write_delta
from src.utilities.exceptions import TransformationError


def _dataframe_is_empty(df: DataFrame) -> bool:
    """Serverless-safe emptiness check (avoids .rdd which is restricted on some runtimes)."""
    try:
        return df.isEmpty()
    except Exception:
        return df.limit(1).count() == 0


def apply_scd_type1(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    primary_key: str | Sequence[str],
    compare_columns: Optional[Sequence[str]] = None,
    logger: Optional[HealthcareLogger] = None,
) -> DataFrame:
    """
    SCD Type 1 upsert: match on PK, update all business columns, insert new keys.
    """
    keys = [primary_key] if isinstance(primary_key, str) else list(primary_key)
    merge_condition = " AND ".join([f"t.`{k}` = s.`{k}`" for k in keys])

    if not table_exists(spark, target_path):
        write_delta(source_df, target_path, mode="overwrite")
        if logger:
            logger.info(f"SCD1 created target {target_path}", module="scd1")
        return spark.read.format("delta").load(target_path)

    # Optionally only update when tracked columns change
    if compare_columns:
        change_pred = " OR ".join(
            [
                f"NOT (t.`{c}` <=> s.`{c}`)"
                for c in compare_columns
                if c in source_df.columns
            ]
        )
        from delta.tables import DeltaTable

        delta_table = DeltaTable.forPath(spark, target_path)
        (
            delta_table.alias("t")
            .merge(source_df.alias("s"), merge_condition)
            .whenMatchedUpdateAll(condition=change_pred)
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        merge_delta(
            spark,
            source_df,
            target_path,
            merge_condition=merge_condition,
            when_matched_update_all=True,
            when_not_matched_insert_all=True,
            logger=logger,
        )

    if logger:
        logger.info(f"SCD1 MERGE complete for {target_path}", module="scd1")
    return spark.read.format("delta").load(target_path)


def apply_scd_type2(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    primary_key: str | Sequence[str],
    tracked_columns: Sequence[str],
    logger: Optional[HealthcareLogger] = None,
) -> DataFrame:
    """
    SCD Type 2: close current rows on change and insert new versions.

    Target schema must include:
        EffectiveStartDate, EffectiveEndDate, IsCurrent, VersionNumber
    """
    keys = [primary_key] if isinstance(primary_key, str) else list(primary_key)
    pk_join = " AND ".join([f"t.`{k}` = s.`{k}`" for k in keys])

    # Prepare source with SCD2 attributes for new/current versions
    staged = (
        source_df.withColumn(SCD2_EFFECTIVE_START, F.current_timestamp())
        .withColumn(SCD2_EFFECTIVE_END, F.lit(None).cast("timestamp"))
        .withColumn(SCD2_IS_CURRENT, F.lit(True))
        .withColumn(SCD2_VERSION, F.lit(1))
    )

    if not table_exists(spark, target_path):
        write_delta(staged, target_path, mode="overwrite")
        if logger:
            logger.info(f"SCD2 created target {target_path}", module="scd2")
        return spark.read.format("delta").load(target_path)

    from delta.tables import DeltaTable

    target = DeltaTable.forPath(spark, target_path)
    current = spark.read.format("delta").load(target_path).filter(F.col(SCD2_IS_CURRENT) == True)  # noqa: E712

    # Detect changed records vs current
    change_expr = " OR ".join(
        [f"NOT (c.`{c}` <=> s.`{c}`)" for c in tracked_columns if c in source_df.columns]
    )
    if not change_expr:
        raise TransformationError("SCD2 requires at least one tracked column")

    source_df.createOrReplaceTempView("_scd2_src")
    current.createOrReplaceTempView("_scd2_cur")

    changes = spark.sql(
        f"""
        SELECT s.*
        FROM _scd2_src s
        INNER JOIN _scd2_cur c
          ON {" AND ".join([f"c.`{k}` = s.`{k}`" for k in keys])}
        WHERE {change_expr}
        """
    )

    news = spark.sql(
        f"""
        SELECT s.*
        FROM _scd2_src s
        LEFT ANTI JOIN _scd2_cur c
          ON {" AND ".join([f"c.`{k}` = s.`{k}`" for k in keys])}
        """
    )

    # Expire current versions that changed
    has_changes = not _dataframe_is_empty(changes)
    has_news = not _dataframe_is_empty(news)

    if has_changes:
        changes_keys = changes.select(*keys).distinct()
        changes_keys.createOrReplaceTempView("_scd2_changed_keys")
        expire_condition = " AND ".join([f"t.`{k}` = k.`{k}`" for k in keys])

        # Merge to set IsCurrent=false for changed keys
        (
            target.alias("t")
            .merge(
                changes_keys.alias("k"),
                f"({expire_condition}) AND t.`{SCD2_IS_CURRENT}` = true",
            )
            .whenMatchedUpdate(
                set={
                    SCD2_IS_CURRENT: "false",
                    SCD2_EFFECTIVE_END: "current_timestamp()",
                }
            )
            .execute()
        )

        # Insert new versions with incremented VersionNumber
        current_versions = (
            spark.read.format("delta")
            .load(target_path)
            .groupBy(*keys)
            .agg(F.max(SCD2_VERSION).alias("max_version"))
        )
        new_versions = (
            changes.alias("s")
            .join(current_versions.alias("v"), on=keys, how="left")
            .withColumn(SCD2_VERSION, F.coalesce(F.col("max_version"), F.lit(0)) + 1)
            .withColumn(SCD2_EFFECTIVE_START, F.current_timestamp())
            .withColumn(SCD2_EFFECTIVE_END, F.lit(None).cast("timestamp"))
            .withColumn(SCD2_IS_CURRENT, F.lit(True))
            .select(*[c for c in staged.columns])
        )
        write_delta(new_versions, target_path, mode="append", merge_schema=True)

    # Insert brand-new keys
    if has_news:
        new_rows = (
            news.withColumn(SCD2_EFFECTIVE_START, F.current_timestamp())
            .withColumn(SCD2_EFFECTIVE_END, F.lit(None).cast("timestamp"))
            .withColumn(SCD2_IS_CURRENT, F.lit(True))
            .withColumn(SCD2_VERSION, F.lit(1))
        )
        # Align columns with target
        target_cols = spark.read.format("delta").load(target_path).columns
        for c in target_cols:
            if c not in new_rows.columns:
                new_rows = new_rows.withColumn(c, F.lit(None))
        new_rows = new_rows.select(*target_cols)
        write_delta(new_rows, target_path, mode="append", merge_schema=True)

    if logger:
        logger.info(
            f"SCD2 MERGE complete for {target_path}",
            module="scd2",
            details={
                "changed": changes.count() if has_changes else 0,
                "new": news.count() if has_news else 0,
            },
        )
    return spark.read.format("delta").load(target_path)

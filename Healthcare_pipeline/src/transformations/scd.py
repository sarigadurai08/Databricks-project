"""
SCD Type 1 and Type 2 implementations using Delta MERGE.

SCD1: overwrite changed attribute columns in place.
SCD2: maintain history with EffectiveStartDate, EffectiveEndDate, IsCurrent, VersionNumber.
"""

from __future__ import annotations

from typing import Optional, Sequence

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from config.constants import (
    SCD2_EFFECTIVE_END,
    SCD2_EFFECTIVE_START,
    SCD2_IS_CURRENT,
    SCD2_VERSION,
)
from src.logging.logger import HealthcareLogger
from src.utilities.delta_helpers import ensure_delta_parent, merge_delta, table_exists, write_delta
from src.utilities.exceptions import TransformationError


def _dataframe_is_empty(df: DataFrame) -> bool:
    """Serverless-safe emptiness check (avoids .rdd which is restricted on some runtimes)."""
    try:
        return df.limit(1).count() == 0
    except Exception:
        try:
            return df.isEmpty()
        except Exception:
            return True


def _is_path_not_found(exc: BaseException) -> bool:
    msg = str(exc).upper()
    return "PATH_NOT_FOUND" in msg or "DOES NOT EXIST" in msg or "FILENOTFOUND" in msg.replace(" ", "")


def is_current_expr(col_name: str = SCD2_IS_CURRENT) -> Column:
    """
    Robust IsCurrent predicate for Databricks Serverless / mixed boolean types.

    Accepts boolean true, integer 1, and string "true"/"True"/"1".
    """
    c = F.col(col_name)
    return (
        (c.cast("boolean") == F.lit(True))
        | (c.cast("string").isin("true", "True", "1", "yes", "Y"))
    )


def filter_current(df: DataFrame, col_name: str = SCD2_IS_CURRENT) -> DataFrame:
    """Filter to current SCD2 rows using a Serverless-safe predicate."""
    if col_name not in df.columns:
        return df.limit(0)
    return df.filter(is_current_expr(col_name))


def _bootstrap_delta_table(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    logger: Optional[HealthcareLogger],
    module: str,
) -> DataFrame:
    """Create a new Delta table at target_path (first load / missing path)."""
    ensure_delta_parent(spark, target_path)
    write_delta(source_df, target_path, mode="overwrite", merge_schema=True)
    if logger:
        logger.info(f"{module} created target {target_path}", module=module)
    return spark.read.format("delta").load(target_path)


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
        return _bootstrap_delta_table(spark, source_df, target_path, logger, "scd1")

    try:
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
    except Exception as exc:
        if _is_path_not_found(exc):
            return _bootstrap_delta_table(spark, source_df, target_path, logger, "scd1")
        raise

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
    tracked = [c for c in tracked_columns if c in source_df.columns]
    if not tracked:
        raise TransformationError("SCD2 requires at least one tracked column present in source")

    # Prepare source with SCD2 attributes for new/current versions
    staged = (
        source_df.withColumn(SCD2_EFFECTIVE_START, F.current_timestamp())
        .withColumn(SCD2_EFFECTIVE_END, F.lit(None).cast("timestamp"))
        .withColumn(SCD2_IS_CURRENT, F.lit(True).cast("boolean"))
        .withColumn(SCD2_VERSION, F.lit(1).cast("int"))
    )

    if not table_exists(spark, target_path):
        return _bootstrap_delta_table(spark, staged, target_path, logger, "scd2")

    from delta.tables import DeltaTable

    try:
        target = DeltaTable.forPath(spark, target_path)
        full_target = spark.read.format("delta").load(target_path)
        if SCD2_IS_CURRENT not in full_target.columns:
            # Corrupt / empty demo reset — recreate
            return _bootstrap_delta_table(spark, staged, target_path, logger, "scd2")
        current = filter_current(full_target)
    except Exception as exc:
        if _is_path_not_found(exc):
            return _bootstrap_delta_table(spark, staged, target_path, logger, "scd2")
        raise

    # If no current rows exist, treat as full reload of current snapshot
    if _dataframe_is_empty(current):
        if logger:
            logger.warning(
                f"No current SCD2 rows at {target_path}; bootstrapping fresh snapshot",
                module="scd2",
            )
        return _bootstrap_delta_table(spark, staged, target_path, logger, "scd2")

    # --- Change detection via DataFrame API (Serverless-safe, no temp-view SQL) ---
    cur_for_join = current
    for col in tracked:
        cur_for_join = cur_for_join.withColumnRenamed(col, f"__cur_{col}")

    select_cols = list(keys) + [f"__cur_{c}" for c in tracked]
    cur_for_join = cur_for_join.select(*[c for c in select_cols if c in cur_for_join.columns])

    joined = source_df.join(cur_for_join, on=keys, how="inner")
    change_cond: Optional[Column] = None
    for col in tracked:
        alias = f"__cur_{col}"
        part = ~F.col(col).eqNullSafe(F.col(alias))
        change_cond = part if change_cond is None else (change_cond | part)

    assert change_cond is not None
    # MATERIALIZE before any MERGE/append. Lazy plans that join the target path
    # would re-read the mutated Delta table after MERGE and produce wrong inserts.
    changes = joined.filter(change_cond).select(*source_df.columns)
    news = source_df.join(current.select(*keys).distinct(), on=keys, how="left_anti")

    changes_mat = changes.cache()
    news_mat = news.cache()
    change_count = changes_mat.count()
    news_count = news_mat.count()
    has_changes = change_count > 0
    has_news = news_count > 0

    try:
        if has_changes:
            changes_keys = changes_mat.select(*keys).distinct()
            expire_condition = " AND ".join([f"t.`{k}` = k.`{k}`" for k in keys])

            # Expire matched current rows — use boolean false (not string) via SQL false literal
            (
                target.alias("t")
                .merge(
                    changes_keys.alias("k"),
                    f"({expire_condition}) AND (CAST(t.`{SCD2_IS_CURRENT}` AS BOOLEAN) = true)",
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
                .agg(F.max(F.col(SCD2_VERSION).cast("int")).alias("max_version"))
            )
            new_versions = (
                changes_mat.alias("s")
                .join(current_versions.alias("v"), on=keys, how="left")
                .withColumn(SCD2_VERSION, (F.coalesce(F.col("max_version"), F.lit(0)) + 1).cast("int"))
                .withColumn(SCD2_EFFECTIVE_START, F.current_timestamp())
                .withColumn(SCD2_EFFECTIVE_END, F.lit(None).cast("timestamp"))
                .withColumn(SCD2_IS_CURRENT, F.lit(True).cast("boolean"))
                .select(*[c for c in staged.columns])
            )
            write_delta(new_versions, target_path, mode="append", merge_schema=True)

        if has_news:
            new_rows = (
                news_mat.withColumn(SCD2_EFFECTIVE_START, F.current_timestamp())
                .withColumn(SCD2_EFFECTIVE_END, F.lit(None).cast("timestamp"))
                .withColumn(SCD2_IS_CURRENT, F.lit(True).cast("boolean"))
                .withColumn(SCD2_VERSION, F.lit(1).cast("int"))
            )
            target_cols = spark.read.format("delta").load(target_path).columns
            for c in target_cols:
                if c not in new_rows.columns:
                    new_rows = new_rows.withColumn(c, F.lit(None))
            new_rows = new_rows.select(*target_cols)
            write_delta(new_rows, target_path, mode="append", merge_schema=True)
    finally:
        try:
            changes_mat.unpersist()
        except Exception:
            pass
        try:
            news_mat.unpersist()
        except Exception:
            pass

    result = spark.read.format("delta").load(target_path)
    if logger:
        logger.info(
            f"SCD2 MERGE complete for {target_path}",
            module="scd2",
            details={
                "changed": change_count,
                "new": news_count,
                "current_rows": filter_current(result).count(),
            },
        )
    return result

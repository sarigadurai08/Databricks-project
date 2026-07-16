"""
Delta Lake helpers: MERGE, OPTIMIZE, VACUUM, ZORDER, Time Travel,
constraints, generated columns, and liquid clustering where supported.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.constants import OPTIMIZE_ZORDER_COLUMNS
from src.utilities.exceptions import MergeConflictError

if TYPE_CHECKING:
    from src.logging.logger import HealthcareLogger


def _dbutils(spark: Optional[SparkSession] = None):
    spark = spark or SparkSession.getActiveSession()
    try:
        from pyspark.dbutils import DBUtils  # type: ignore

        if spark is not None:
            return DBUtils(spark)
    except Exception:
        pass
    try:
        import IPython

        return IPython.get_ipython().user_ns.get("dbutils")  # type: ignore[union-attr]
    except Exception:
        return None


def ensure_delta_parent(spark: SparkSession, path: str) -> None:
    """
    Ensure parent directories exist for Volume / DBFS Delta paths.

    Databricks Volumes can raise PATH_NOT_FOUND on first write if parents
    were never created.
    """
    normalized = path.rstrip("/")
    parent = normalized.rsplit("/", 1)[0] if "/" in normalized else ""
    if not parent:
        return

    dbutils = _dbutils(spark)
    if dbutils is not None:
        try:
            dbutils.fs.mkdirs(parent)
            return
        except Exception:
            pass

    try:
        Path(parent).mkdir(parents=True, exist_ok=True)
    except OSError:
        pass


def soft_reset_delta_path(
    spark: SparkSession,
    path: str,
    schema_df: Optional[DataFrame] = None,
) -> str:
    """
    Idempotently clear a Delta path without recursive filesystem deletes.

    Avoids ``dbutils.fs.rm`` / ``shutil.rmtree`` which require elevated approval
    or fail under Serverless / restricted workspaces.

    Strategy (first success wins):
      1. DELETE FROM delta.`path` WHERE true — clears rows, keeps table
      2. Overwrite with an empty frame matching ``schema_df`` (if provided)
      3. No-op when the path does not yet exist (caller bootstraps on write)

    Returns a short status string for logging.
    """
    ensure_delta_parent(spark, path)

    if not table_exists(spark, path):
        return "absent"

    try:
        spark.sql(f"DELETE FROM delta.`{path}` WHERE true")
        return "deleted_rows"
    except Exception:
        pass

    if schema_df is not None:
        try:
            write_delta(schema_df.limit(0), path, mode="overwrite", merge_schema=True)
            return "overwritten_empty"
        except Exception:
            pass

    # Last resort: overwrite with a single throwaway row schema then delete —
    # still no filesystem recursive remove.
    try:
        if schema_df is not None:
            write_delta(schema_df, path, mode="overwrite", merge_schema=True)
            spark.sql(f"DELETE FROM delta.`{path}` WHERE true")
            return "overwrite_then_delete"
    except Exception:
        pass

    return "uncleared"


def table_exists(spark: SparkSession, path: str) -> bool:
    """
    Return True only when a readable Delta table exists at path.

    Uses limit(1).count() so Spark Connect / Serverless cannot short-circuit
    a missing path the way limit(0) sometimes does.
    """
    try:
        spark.read.format("delta").load(path).limit(1).count()
        return True
    except Exception:
        return False


def write_delta(
    df: DataFrame,
    path: str,
    mode: str = "append",
    partition_by: Optional[Sequence[str]] = None,
    merge_schema: bool = True,
) -> None:
    spark = getattr(df, "sparkSession", None) or SparkSession.getActiveSession()
    if spark is not None:
        ensure_delta_parent(spark, path)
    writer = df.write.format("delta").mode(mode).option("mergeSchema", str(merge_schema).lower())
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def merge_delta(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    merge_condition: str,
    when_matched_update: Optional[dict[str, str]] = None,
    when_matched_update_all: bool = False,
    when_not_matched_insert_all: bool = True,
    when_matched_delete_condition: Optional[str] = None,
    logger: Optional[HealthcareLogger] = None,
) -> dict[str, int]:
    """
    Execute a Delta MERGE and return approximate operation metrics.
    """
    from delta.tables import DeltaTable

    source_df.createOrReplaceTempView("_merge_source")
    if not table_exists(spark, target_path):
        write_delta(source_df, target_path, mode="overwrite")
        count = source_df.count()
        if logger:
            logger.info(
                f"Created new Delta table at {target_path}",
                module="delta_merge",
                details={"rows": count},
            )
        return {"inserted": count, "updated": 0, "deleted": 0}

    try:
        delta_table = DeltaTable.forPath(spark, target_path)
        merge_builder = delta_table.alias("t").merge(
            source_df.alias("s"), merge_condition
        )

        if when_matched_delete_condition:
            merge_builder = merge_builder.whenMatchedDelete(
                condition=when_matched_delete_condition
            )

        if when_matched_update_all:
            merge_builder = merge_builder.whenMatchedUpdateAll()
        elif when_matched_update:
            merge_builder = merge_builder.whenMatchedUpdate(set=when_matched_update)

        if when_not_matched_insert_all:
            merge_builder = merge_builder.whenNotMatchedInsertAll()

        merge_builder.execute()
        if logger:
            logger.info(f"MERGE completed for {target_path}", module="delta_merge")
        return {"inserted": -1, "updated": -1, "deleted": -1}
    except Exception as exc:
        raise MergeConflictError(
            f"MERGE failed for {target_path}: {exc}",
            details={"condition": merge_condition},
        ) from exc


def optimize_table(
    spark: SparkSession,
    path: str,
    zorder_columns: Optional[Sequence[str]] = None,
    logger: Optional[HealthcareLogger] = None,
) -> None:
    zorder_clause = ""
    if zorder_columns:
        cols = ", ".join(zorder_columns)
        zorder_clause = f" ZORDER BY ({cols})"
    sql = f"OPTIMIZE delta.`{path}`{zorder_clause}"
    spark.sql(sql)
    if logger:
        logger.info(f"OPTIMIZE executed: {sql}", module="delta_maintenance")


def vacuum_table(
    spark: SparkSession,
    path: str,
    retention_hours: int = 168,
    logger: Optional[HealthcareLogger] = None,
) -> None:
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retention_hours} HOURS")
    if logger:
        logger.info(
            f"VACUUM completed for {path}",
            module="delta_maintenance",
            details={"retention_hours": retention_hours},
        )


def time_travel(
    spark: SparkSession,
    path: str,
    version: Optional[int] = None,
    timestamp: Optional[str] = None,
) -> DataFrame:
    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)
    if timestamp is not None:
        reader = reader.option("timestampAsOf", timestamp)
    return reader.load(path)


def history(spark: SparkSession, path: str, limit: int = 20) -> DataFrame:
    from delta.tables import DeltaTable

    return DeltaTable.forPath(spark, path).history(limit)


def add_primary_key_constraint(
    spark: SparkSession,
    table_name: str,
    columns: Sequence[str],
) -> None:
    """
    Add informational primary key constraint (Unity Catalog / Delta constraints).
    Fails softly when the runtime does not support constraints.
    """
    cols = ", ".join(columns)
    try:
        spark.sql(
            f"ALTER TABLE {table_name} ADD CONSTRAINT pk_{table_name.replace('.', '_')} "
            f"PRIMARY KEY ({cols})"
        )
    except Exception:
        # Local Spark / older runtimes may not support PK constraints
        pass


def add_check_constraint(
    spark: SparkSession,
    table_name: str,
    constraint_name: str,
    expression: str,
) -> None:
    try:
        spark.sql(
            f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} CHECK ({expression})"
        )
    except Exception:
        pass


def enable_liquid_clustering(
    spark: SparkSession,
    table_name: str,
    cluster_columns: Sequence[str],
) -> None:
    """
    Enable Liquid Clustering when available (Databricks Runtime 13.3+).
    Falls back silently on unsupported runtimes.
    """
    cols = ", ".join(cluster_columns)
    try:
        spark.sql(f"ALTER TABLE {table_name} CLUSTER BY ({cols})")
    except Exception:
        try:
            spark.sql(
                f"CREATE OR REPLACE TABLE {table_name} "
                f"CLUSTER BY ({cols}) AS SELECT * FROM {table_name}"
            )
        except Exception:
            pass


def maintain_entity(
    spark: SparkSession,
    entity: str,
    path: str,
    vacuum_hours: int = 168,
    vacuum_enabled: bool = True,
    optimize_enabled: bool = True,
    logger: Optional[HealthcareLogger] = None,
) -> None:
    if not table_exists(spark, path):
        if logger:
            logger.warning(
                f"Maintenance skipped — Delta path missing: {path}",
                module="delta_maintenance",
            )
        return
    try:
        if optimize_enabled:
            zcols = OPTIMIZE_ZORDER_COLUMNS.get(entity)
            optimize_table(spark, path, zorder_columns=zcols, logger=logger)
    except Exception as exc:
        if logger:
            logger.warning(f"OPTIMIZE skipped for {entity}: {exc}", module="delta_maintenance")
    try:
        if vacuum_enabled:
            vacuum_table(spark, path, retention_hours=vacuum_hours, logger=logger)
    except Exception as exc:
        if logger:
            logger.warning(f"VACUUM skipped for {entity}: {exc}", module="delta_maintenance")


def with_generated_ingestion_date(df: DataFrame, source_col: str = "_ingestion_time") -> DataFrame:
    """Add a generated-style partition column derived from ingestion timestamp."""
    return df.withColumn("ingestion_date", F.to_date(F.col(source_col)))

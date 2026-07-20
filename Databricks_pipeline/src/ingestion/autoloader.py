"""
Auto Loader based incremental ingestion for JSON (and optional CSV) sources.

Tries Databricks ``cloudFiles`` Auto Loader when ``prefer_autoloader`` is set and
Databricks is detected. Automatically falls back to batch JSON ingestion on any
Auto Loader failure (common on Free Edition / Serverless).
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.config import CONFIG, EcommerceConfig
from config.constants import (
    ALL_ENTITIES,
    ENTITY_EVENT_TIME_COLUMNS,
    META_RESCUED_DATA,
)
from config.paths import PATHS
from src.logging.logger import EcommerceLogger
from src.utilities.dataframe_utils import add_bronze_metadata, generate_load_id
from src.utilities.delta_helpers import table_exists, with_generated_ingestion_date, write_delta
from src.utilities.exceptions import IngestionError, with_retry


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


def _path_exists(path: str, spark: Optional[SparkSession] = None) -> bool:
    p = Path(path)
    if p.exists():
        return True
    try:
        dbutils = _dbutils(spark)
        if dbutils is not None:
            dbutils.fs.ls(path)
            return True
    except Exception:
        pass
    return False


def _mkdirs(path: str, spark: Optional[SparkSession] = None) -> None:
    if PATHS.is_cloud_storage or path.startswith("/Volumes/") or path.startswith("dbfs:"):
        try:
            dbutils = _dbutils(spark)
            if dbutils is not None:
                dbutils.fs.mkdirs(path)
                return
        except Exception:
            pass
        try:
            Path(path).mkdir(parents=True, exist_ok=True)
        except OSError:
            pass
        return
    Path(path).mkdir(parents=True, exist_ok=True)


def _detect_databricks(spark: SparkSession) -> bool:
    try:
        if spark.conf.get("spark.databricks.clusterUsageTags.clusterId", None):
            return True
    except Exception:
        pass
    import os

    return bool(
        os.environ.get("DATABRICKS_RUNTIME_VERSION")
        or os.environ.get("DB_HOME")
        or Path("/databricks").exists()
    )


class AutoLoaderIngestion:
    """
    Production-style Auto Loader wrapper with automatic batch fallback.

    Prefer Auto Loader when ``config.streaming.prefer_autoloader`` and Databricks
    are both true; otherwise (or on any Auto Loader error) use batch JSON read.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: Optional[EcommerceConfig] = None,
        logger: Optional[EcommerceLogger] = None,
        run_id: Optional[str] = None,
    ) -> None:
        self.spark = spark
        self.config = config or CONFIG
        self.logger = logger
        self.run_id = run_id or str(uuid.uuid4())
        self.load_id = generate_load_id()
        self._is_databricks = _detect_databricks(spark)

    @with_retry()
    def ingest_entity(
        self,
        entity: str,
        fmt: str = "json",
        trigger_once: bool = True,
        hash_columns: Optional[list[str]] = None,
    ) -> DataFrame:
        if entity not in ALL_ENTITIES:
            raise IngestionError(f"Unknown entity: {entity}", details={"entity": entity})

        source_path = self.config.paths.landing_path(entity, fmt)
        target_path = self.config.paths.bronze_path(entity)
        checkpoint = self.config.paths.checkpoint_path(entity, "bronze")
        schema_loc = self.config.paths.schema_location(entity)
        bad_path = self.config.paths.bad_records_path(entity)
        event_time_col = ENTITY_EVENT_TIME_COLUMNS.get(entity)

        prefer = bool(self.config.streaming.prefer_autoloader and self._is_databricks)

        if self.logger:
            self.logger.info(
                f"Starting ingestion for {entity}",
                module="ingestion",
                details={
                    "format": fmt,
                    "source": source_path,
                    "target": target_path,
                    "prefer_autoloader": prefer,
                    "databricks": self._is_databricks,
                },
            )

        if prefer:
            try:
                return self._ingest_autoloader(
                    entity=entity,
                    fmt=fmt,
                    source_path=source_path,
                    target_path=target_path,
                    checkpoint=checkpoint,
                    schema_loc=schema_loc,
                    bad_path=bad_path,
                    trigger_once=trigger_once,
                    hash_columns=hash_columns,
                    event_time_column=event_time_col,
                )
            except Exception as exc:
                if self.logger:
                    self.logger.warning(
                        f"Auto Loader failed for {entity}; falling back to batch",
                        module="ingestion",
                        details={"error": str(exc)},
                    )

        try:
            return self._ingest_batch_fallback(
                entity=entity,
                fmt=fmt,
                source_path=source_path,
                target_path=target_path,
                hash_columns=hash_columns,
                event_time_column=event_time_col,
            )
        except Exception as exc:
            if self.logger:
                self.logger.error(
                    f"Ingestion failed for {entity}",
                    module="ingestion",
                    exc=exc,
                )
            raise IngestionError(
                f"Failed to ingest {entity}: {exc}",
                details={"source": source_path, "format": fmt},
            ) from exc

    def ingest_all(
        self,
        fmt: str = "json",
        entities: Optional[Sequence[str]] = None,
        continue_on_error: Optional[bool] = None,
    ) -> dict[str, str]:
        """Ingest multiple entities; returns status map."""
        entities = list(entities) if entities is not None else list(ALL_ENTITIES)
        cont = (
            self.config.retry.continue_on_error
            if continue_on_error is None
            else continue_on_error
        )
        status: dict[str, str] = {}
        for entity in entities:
            try:
                self.ingest_entity(entity, fmt=fmt)
                status[entity] = "SUCCESS"
            except Exception as exc:
                status[entity] = f"FAILED: {exc}"
                if not cont:
                    raise
                if self.logger:
                    self.logger.warning(
                        f"Continuing after failure on {entity}",
                        module="ingestion",
                        details={"error": str(exc)},
                    )
        return status

    def _ingest_autoloader(
        self,
        entity: str,
        fmt: str,
        source_path: str,
        target_path: str,
        checkpoint: str,
        schema_loc: str,
        bad_path: str,
        trigger_once: bool,
        hash_columns: Optional[list[str]],
        event_time_column: Optional[str],
    ) -> DataFrame:
        _mkdirs(schema_loc, self.spark)
        _mkdirs(checkpoint, self.spark)
        if self.config.autoloader.bad_records_path_enabled:
            _mkdirs(bad_path, self.spark)

        options = self.config.autoloader.cloud_files_options(schema_loc, fmt=fmt)
        if self.config.autoloader.bad_records_path_enabled:
            options["badRecordsPath"] = bad_path

        stream_df = (
            self.spark.readStream.format("cloudFiles")
            .options(**options)
            .load(source_path)
        )

        batch_id = f"BATCH_{uuid.uuid4().hex[:12]}"
        load_id = self.load_id
        hash_cols = hash_columns
        evt_col = event_time_column

        def foreach_batch(batch_df: DataFrame, batch_number: int) -> None:
            try:
                if batch_df.isEmpty():
                    return
            except Exception:
                if batch_df.limit(1).count() == 0:
                    return

            enriched = add_bronze_metadata(
                batch_df,
                load_id=load_id,
                batch_id=f"{batch_id}_{batch_number}",
                hash_columns=hash_cols,
                event_time_column=evt_col,
            )
            if META_RESCUED_DATA not in enriched.columns:
                enriched = enriched.withColumn(META_RESCUED_DATA, F.lit(None).cast("string"))
            enriched = with_generated_ingestion_date(enriched)
            write_delta(
                enriched,
                target_path,
                mode="append",
                partition_by=["ingestion_date"],
                merge_schema=True,
            )

        writer = (
            stream_df.writeStream.foreachBatch(foreach_batch)
            .option("checkpointLocation", checkpoint)
            .outputMode("append")
        )
        if trigger_once:
            query = writer.trigger(once=True).start()
        else:
            query = writer.trigger(
                processingTime=self.config.autoloader.checkpoint_interval
            ).start()

        query.awaitTermination()

        if table_exists(self.spark, target_path):
            return self.spark.read.format("delta").load(target_path)
        return self.spark.createDataFrame([], schema=stream_df.schema)

    def _ingest_batch_fallback(
        self,
        entity: str,
        fmt: str,
        source_path: str,
        target_path: str,
        hash_columns: Optional[list[str]],
        event_time_column: Optional[str],
    ) -> DataFrame:
        if not _path_exists(source_path, self.spark):
            raise IngestionError(
                f"No landing files for {entity}",
                details={"landing": source_path},
            )

        read_path = source_path
        p = Path(source_path)
        if p.is_file():
            read_path = str(p.parent).replace("\\", "/")

        reader = self.spark.read
        if fmt == "csv":
            raw = (
                reader.option("header", "true")
                .option("inferSchema", "true")
                .option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", META_RESCUED_DATA)
                .csv(read_path)
            )
        elif fmt == "json":
            raw = (
                reader.option("multiLine", "true")
                .option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", META_RESCUED_DATA)
                .json(read_path)
            )
        else:
            raise IngestionError(f"Unsupported format: {fmt}")

        if META_RESCUED_DATA not in raw.columns:
            raw = raw.withColumn(META_RESCUED_DATA, F.lit(None).cast("string"))
        else:
            raw = raw.withColumn(META_RESCUED_DATA, F.col(META_RESCUED_DATA).cast("string"))

        try:
            from src.utilities.databricks_runtime import patch_input_file_name

            patch_input_file_name()
        except Exception:
            pass
        try:
            raw = raw.withColumn("_source_file", F.input_file_name())
        except Exception:
            if "_metadata" in raw.columns:
                raw = raw.withColumn("_source_file", F.col("_metadata.file_path"))
            else:
                raw = raw.withColumn("_source_file", F.lit(read_path))

        batch_id = f"BATCH_{uuid.uuid4().hex[:12]}"
        enriched = add_bronze_metadata(
            raw,
            load_id=self.load_id,
            batch_id=batch_id,
            hash_columns=hash_columns,
            event_time_column=event_time_column,
        )
        enriched = with_generated_ingestion_date(enriched)

        write_delta(
            enriched,
            target_path,
            mode="append",
            partition_by=["ingestion_date"],
            merge_schema=True,
        )

        if self.logger:
            self.logger.info(
                f"Batch fallback ingestion complete for {entity}",
                module="ingestion",
                details={"rows": enriched.count(), "target": target_path},
            )
        return self.spark.read.format("delta").load(target_path)

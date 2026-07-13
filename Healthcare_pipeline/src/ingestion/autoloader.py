"""
Auto Loader based incremental ingestion for CSV and JSON sources.

Supports schema evolution, checkpointing, rescue data, bad records,
and unknown columns via cloudFiles options.

On Databricks Free Edition / Serverless the working pattern (new_bronze.py)
uses the batch fallback with Volume landing + input_file_name patch, because
cloudFiles Auto Loader is not reliably available on all Free Edition runtimes.
"""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.config import CONFIG, HealthcareConfig
from config.constants import ALL_ENTITIES, META_RESCUED_DATA
from config.paths import PATHS
from src.logging.logger import HealthcareLogger
from src.utilities.dataframe_utils import add_bronze_metadata, generate_load_id
from src.utilities.delta_helpers import table_exists, write_delta
from src.utilities.exceptions import IngestionError, with_retry


def _dbutils(spark: Optional[SparkSession] = None):
    """Return dbutils when running inside Databricks."""
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
    """Existence check that works for local paths and FUSE-mounted Volumes."""
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


class AutoLoaderIngestion:
    """
    Production-style Auto Loader wrapper.

    On Databricks, uses `cloudFiles` when `_is_databricks` is True.
    Free Edition notebooks set `_is_databricks = False` (see new_bronze.py)
    to force the Volume-backed batch fallback that is known to execute
    successfully end-to-end.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: Optional[HealthcareConfig] = None,
        logger: Optional[HealthcareLogger] = None,
        run_id: Optional[str] = None,
    ) -> None:
        self.spark = spark
        self.config = config or CONFIG
        self.logger = logger
        self.run_id = run_id or str(uuid.uuid4())
        self.load_id = generate_load_id()
        self._is_databricks = self._detect_databricks()

    def _detect_databricks(self) -> bool:
        try:
            return bool(self.spark.conf.get("spark.databricks.clusterUsageTags.clusterId", None))
        except Exception:
            # Serverless may not expose clusterId — treat Databricks Runtime env as True
            import os

            return bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    @with_retry()
    def ingest_entity(
        self,
        entity: str,
        fmt: str = "csv",
        trigger_once: bool = True,
        hash_columns: Optional[list[str]] = None,
    ) -> DataFrame:
        """
        Ingest one entity into bronze using Auto Loader (or batch fallback).

        Returns the bronze DataFrame for the current load (streaming micro-batch
        or full batch fallback).
        """
        if entity not in ALL_ENTITIES:
            raise IngestionError(f"Unknown entity: {entity}", details={"entity": entity})

        source_path = self.config.paths.landing_path(entity, fmt)
        target_path = self.config.paths.bronze_path(entity)
        checkpoint = self.config.paths.checkpoint_path(entity, "bronze")
        schema_loc = self.config.paths.schema_location(entity)
        bad_path = self.config.paths.bad_records_path(entity)

        if self.logger:
            self.logger.info(
                f"Starting ingestion for {entity}",
                module="ingestion",
                details={
                    "format": fmt,
                    "source": source_path,
                    "target": target_path,
                    "databricks": self._is_databricks,
                },
            )

        try:
            if self._is_databricks:
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
                )
            return self._ingest_batch_fallback(
                entity=entity,
                fmt=fmt,
                source_path=source_path,
                target_path=target_path,
                hash_columns=hash_columns,
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
        fmt: str = "csv",
        entities: Optional[list[str]] = None,
        continue_on_error: bool = True,
    ) -> dict[str, str]:
        """Ingest multiple entities; returns status map."""
        entities = entities or list(ALL_ENTITIES)
        status: dict[str, str] = {}
        for entity in entities:
            try:
                self.ingest_entity(entity, fmt=fmt)
                status[entity] = "SUCCESS"
            except Exception as exc:
                status[entity] = f"FAILED: {exc}"
                if not continue_on_error:
                    raise
                if self.logger:
                    self.logger.warning(
                        f"Continuing after failure on {entity}",
                        module="ingestion",
                        details={"error": str(exc)},
                    )
        return status

    # ------------------------------------------------------------------
    # Databricks Auto Loader
    # ------------------------------------------------------------------
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
    ) -> DataFrame:
        options = self.config.autoloader.cloud_files_options(schema_loc, fmt=fmt)
        if self.config.autoloader.bad_records_path_enabled:
            options["badRecordsPath"] = bad_path

        stream_df = (
            self.spark.readStream.format("cloudFiles")
            .options(**options)
            .load(source_path)
        )

        batch_id = f"BATCH_{uuid.uuid4().hex[:12]}"

        def foreach_batch(batch_df: DataFrame, batch_number: int) -> None:
            if batch_df.isEmpty():
                return
            enriched = add_bronze_metadata(
                batch_df,
                load_id=self.load_id,
                batch_id=f"{batch_id}_{batch_number}",
                hash_columns=hash_columns,
            )
            if META_RESCUED_DATA not in enriched.columns:
                enriched = enriched.withColumn(META_RESCUED_DATA, F.lit(None).cast("string"))

            from src.utilities.delta_helpers import with_generated_ingestion_date

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

    # ------------------------------------------------------------------
    # Batch fallback (Free Edition / portfolio — matches new_bronze.py)
    # ------------------------------------------------------------------
    def _ingest_batch_fallback(
        self,
        entity: str,
        fmt: str,
        source_path: str,
        target_path: str,
        hash_columns: Optional[list[str]],
    ) -> DataFrame:
        """
        Batch read landing files and append to bronze with identical metadata.

        Mimics Auto Loader semantics for schema evolution via mergeSchema and
        writes a synthetic _rescued_data column.
        """
        read_path = source_path
        files_exist = _path_exists(source_path, self.spark)

        if files_exist:
            p = Path(source_path)
            if p.is_file():
                read_path = str(p.parent).replace("\\", "/")
        else:
            sample = (
                self.config.paths.sample_csv_path(entity)
                if fmt == "csv"
                else self.config.paths.sample_json_path(entity)
            )
            if not _path_exists(sample, self.spark):
                raise IngestionError(
                    f"No landing or sample files for {entity}",
                    details={"landing": source_path, "sample": sample},
                )
            _stage_one_sample(self.spark, entity, fmt, sample, source_path)
            read_path = source_path

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

        # Lineage — uses patched F.input_file_name on Databricks Serverless
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
        )
        from src.utilities.delta_helpers import with_generated_ingestion_date

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


def _stage_one_sample(
    spark: Optional[SparkSession],
    entity: str,
    fmt: str,
    sample: str,
    dest_dir: str,
) -> None:
    """Copy one sample dataset into the landing directory (local or Volume)."""
    _mkdirs(dest_dir, spark)

    # Prefer Spark read/write for Volume targets (writable, no Git writes)
    if spark is not None and (PATHS.is_cloud_storage or dest_dir.startswith("/Volumes/")):
        if fmt == "csv":
            (
                spark.read.option("header", "true")
                .option("inferSchema", "true")
                .csv(sample)
                .coalesce(1)
                .write.mode("overwrite")
                .option("header", "true")
                .csv(dest_dir)
            )
        else:
            (
                spark.read.option("multiLine", "true")
                .json(sample)
                .coalesce(1)
                .write.mode("overwrite")
                .json(dest_dir)
            )
        return

    # Local filesystem copy
    src = Path(sample)
    if not src.exists():
        return
    dest_path = Path(dest_dir)
    dest_path.mkdir(parents=True, exist_ok=True)
    dest = dest_path / src.name
    if not dest.exists():
        shutil.copy2(src, dest)


def stage_sample_files_to_landing(
    fmt: str = "csv",
    spark: Optional[SparkSession] = None,
) -> None:
    """
    Stage root sample datasets into Auto Loader landing directories.

    On Databricks, stages into the writable Volume landing zone (never the
    Git repository). Locally, copies into datasets/landing/.
    """
    spark = spark or SparkSession.getActiveSession()
    if not PATHS.is_cloud_storage:
        PATHS.ensure_local_directories()

    for entity in ALL_ENTITIES:
        src = (
            PATHS.sample_csv_path(entity) if fmt == "csv" else PATHS.sample_json_path(entity)
        )
        if not _path_exists(src, spark):
            continue
        dest_dir = PATHS.landing_path(entity, fmt)
        _stage_one_sample(spark, entity, fmt, src, dest_dir)

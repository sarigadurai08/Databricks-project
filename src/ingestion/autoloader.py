"""
Auto Loader based incremental ingestion for CSV and JSON sources.

Supports schema evolution, checkpointing, rescue data, bad records,
and unknown columns via cloudFiles options.
"""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path
from typing import Callable, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.config import CONFIG, HealthcareConfig
from config.constants import ALL_ENTITIES, META_RESCUED_DATA
from config.paths import PATHS
from src.logging.logger import HealthcareLogger
from src.utilities.dataframe_utils import add_bronze_metadata, generate_load_id
from src.utilities.delta_helpers import table_exists, write_delta
from src.utilities.exceptions import IngestionError, with_retry


class AutoLoaderIngestion:
    """
    Production-style Auto Loader wrapper.

    On Databricks, uses `cloudFiles`. For local/portfolio Spark runs without
    Databricks Runtime, falls back to file-based batch reads with an equivalent
    bronze metadata contract so notebooks remain executable end-to-end.
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
            return False

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
        Ingest one entity into bronze using Auto Loader (or local fallback).

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
    # Local / portfolio fallback (executable without Databricks Runtime)
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
        path = Path(source_path)
        # Prefer entity landing folder; fall back to root datasets/{entity}.csv
        files_exist = path.exists() and (
            any(path.glob(f"*.{fmt}")) if path.is_dir() else True
        )
        if not files_exist:
            sample = (
                self.config.paths.sample_csv_path(entity)
                if fmt == "csv"
                else self.config.paths.sample_json_path(entity)
            )
            sample_path = Path(sample)
            if not sample_path.exists():
                raise IngestionError(
                    f"No landing or sample files for {entity}",
                    details={"landing": source_path, "sample": sample},
                )
            path.mkdir(parents=True, exist_ok=True)
            dest = path / sample_path.name
            if not dest.exists():
                shutil.copy2(sample_path, dest)
            read_path = str(path).replace("\\", "/")
        else:
            read_path = source_path if path.is_dir() else str(path.parent).replace("\\", "/")

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

        # Attach source file path for lineage (local equivalent of _metadata.file_path)
        raw = raw.withColumn(META_RESCUED_DATA, F.lit(None).cast("string"))
        raw = raw.withColumn("_source_file", F.input_file_name())

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


def stage_sample_files_to_landing(fmt: str = "csv") -> None:
    """Copy root sample datasets into Auto Loader landing directories."""
    PATHS.ensure_local_directories()
    for entity in ALL_ENTITIES:
        src = Path(
            PATHS.sample_csv_path(entity) if fmt == "csv" else PATHS.sample_json_path(entity)
        )
        if not src.exists():
            continue
        dest_dir = Path(PATHS.landing_path(entity, fmt))
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / src.name
        shutil.copy2(src, dest)

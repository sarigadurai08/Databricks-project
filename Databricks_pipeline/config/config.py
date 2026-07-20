"""
Runtime configuration for the E-Commerce Lakehouse platform.

Loads environment-aware settings for Spark, Auto Loader, Delta maintenance,
Unity Catalog naming, streaming simulator, and pipeline behavior.
Designed for Databricks jobs and local PySpark development.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Optional

from config.constants import (
    BRONZE_SCHEMA,
    BROADCAST_THRESHOLD_BYTES,
    DEFAULT_SHUFFLE_PARTITIONS,
    GOLD_SCHEMA,
    MAX_RETRIES,
    RETRY_BACKOFF_SECONDS,
    SILVER_SCHEMA,
    SIMULATOR_INTERVAL_SECONDS,
    STREAMING_TRIGGER_INTERVAL,
    STREAMING_WATERMARK,
)
from config.paths import LakehousePaths, PATHS


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


@dataclass
class SparkConfig:
    """Spark session and AQE / performance settings."""

    app_name: str = "EcommerceLakehouse"
    master: str = field(default_factory=lambda: os.getenv("SPARK_MASTER", "local[*]"))
    shuffle_partitions: int = field(
        default_factory=lambda: _env_int("SPARK_SHUFFLE_PARTITIONS", DEFAULT_SHUFFLE_PARTITIONS)
    )
    broadcast_threshold: int = field(
        default_factory=lambda: _env_int("SPARK_BROADCAST_THRESHOLD", BROADCAST_THRESHOLD_BYTES)
    )
    enable_aqe: bool = field(default_factory=lambda: _env_bool("SPARK_ENABLE_AQE", True))
    enable_adaptive_coalesce: bool = True
    enable_adaptive_skew_join: bool = True
    timezone: str = "UTC"

    def as_spark_conf(self) -> dict[str, str]:
        return {
            "spark.sql.shuffle.partitions": str(self.shuffle_partitions),
            "spark.sql.autoBroadcastJoinThreshold": str(self.broadcast_threshold),
            "spark.sql.adaptive.enabled": str(self.enable_aqe).lower(),
            "spark.sql.adaptive.coalescePartitions.enabled": str(
                self.enable_adaptive_coalesce
            ).lower(),
            "spark.sql.adaptive.skewJoin.enabled": str(self.enable_adaptive_skew_join).lower(),
            "spark.sql.session.timeZone": self.timezone,
            "spark.databricks.delta.optimizeWrite.enabled": "true",
            "spark.databricks.delta.autoCompact.enabled": "true",
            "spark.databricks.delta.schema.autoMerge.enabled": "true",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        }


@dataclass
class AutoLoaderConfig:
    """Databricks Auto Loader (cloudFiles) configuration."""

    format: str = "cloudFiles"
    infer_column_types: bool = True
    schema_evolution_mode: str = "addNewColumns"
    rescue_data_column: str = "_rescued_data"
    max_files_per_trigger: int = 1000
    include_existing_files: bool = True
    use_notifications: bool = False
    json_multi_line: bool = True
    bad_records_path_enabled: bool = True
    checkpoint_interval: str = field(
        default_factory=lambda: os.getenv("ECOMMERCE_STREAM_TRIGGER", STREAMING_TRIGGER_INTERVAL)
    )

    def cloud_files_options(self, schema_location: str, fmt: str = "json") -> dict[str, str]:
        options: dict[str, str] = {
            "cloudFiles.format": fmt,
            "cloudFiles.schemaLocation": schema_location,
            "cloudFiles.inferColumnTypes": str(self.infer_column_types).lower(),
            "cloudFiles.schemaEvolutionMode": self.schema_evolution_mode,
            "cloudFiles.maxFilesPerTrigger": str(self.max_files_per_trigger),
            "cloudFiles.includeExistingFiles": str(self.include_existing_files).lower(),
            "rescueDataColumn": self.rescue_data_column,
        }
        if fmt == "json":
            options["multiLine"] = str(self.json_multi_line).lower()
        if self.use_notifications:
            options["cloudFiles.useNotifications"] = "true"
        return options


@dataclass
class StreamingConfig:
    """Structured Streaming + simulator settings."""

    watermark: str = field(
        default_factory=lambda: os.getenv("ECOMMERCE_STREAM_WATERMARK", STREAMING_WATERMARK)
    )
    trigger_interval: str = field(
        default_factory=lambda: os.getenv("ECOMMERCE_STREAM_TRIGGER", STREAMING_TRIGGER_INTERVAL)
    )
    simulator_interval_seconds: int = field(
        default_factory=lambda: _env_int(
            "ECOMMERCE_SIMULATOR_INTERVAL_SECONDS", SIMULATOR_INTERVAL_SECONDS
        )
    )
    simulator_events_per_tick: int = field(
        default_factory=lambda: _env_int("ECOMMERCE_SIMULATOR_EVENTS_PER_TICK", 25)
    )
    simulator_ticks: int = field(
        default_factory=lambda: _env_int("ECOMMERCE_SIMULATOR_TICKS", 3)
    )
    prefer_autoloader: bool = field(
        default_factory=lambda: _env_bool("ECOMMERCE_PREFER_AUTOLOADER", True)
    )


@dataclass
class DeltaMaintenanceConfig:
    """OPTIMIZE / VACUUM / ZORDER / Liquid Clustering settings."""

    optimize_enabled: bool = field(
        default_factory=lambda: _env_bool("ECOMMERCE_OPTIMIZE_ENABLED", False)
    )
    vacuum_enabled: bool = field(
        default_factory=lambda: _env_bool("ECOMMERCE_VACUUM_ENABLED", False)
    )
    vacuum_retention_hours: int = 168
    zorder_enabled: bool = field(
        default_factory=lambda: _env_bool("ECOMMERCE_ZORDER_ENABLED", False)
    )
    liquid_clustering_enabled: bool = field(
        default_factory=lambda: _env_bool("ECOMMERCE_LIQUID_CLUSTERING_ENABLED", False)
    )
    time_travel_enabled: bool = True


@dataclass
class UnityCatalogConfig:
    """Unity Catalog naming for registered physical tables.

    ``catalog`` defaults empty and is resolved at runtime by
    ``prepare_databricks_runtime`` / ``discover_catalog``.
    Override with ``ECOMMERCE_UC_CATALOG`` when you need a fixed catalog.
    """

    enabled: bool = field(default_factory=lambda: _env_bool("ECOMMERCE_UC_ENABLED", True))
    catalog: str = field(
        default_factory=lambda: os.getenv("ECOMMERCE_UC_CATALOG", "").strip()
    )
    bronze_schema: str = field(
        default_factory=lambda: os.getenv("ECOMMERCE_UC_BRONZE_SCHEMA", BRONZE_SCHEMA)
    )
    silver_schema: str = field(
        default_factory=lambda: os.getenv("ECOMMERCE_UC_SILVER_SCHEMA", SILVER_SCHEMA)
    )
    gold_schema: str = field(
        default_factory=lambda: os.getenv("ECOMMERCE_UC_GOLD_SCHEMA", GOLD_SCHEMA)
    )
    register_tables: bool = field(
        default_factory=lambda: _env_bool("ECOMMERCE_REGISTER_TABLES", True)
    )

    def table_fqn(self, schema: str, table: str) -> str:
        if self.enabled and self.catalog:
            return f"{self.catalog}.{schema}.{table}"
        return f"{schema}.{table}"


@dataclass
class RetryConfig:
    max_retries: int = MAX_RETRIES
    backoff_seconds: float = RETRY_BACKOFF_SECONDS
    continue_on_error: bool = True


@dataclass
class DataQualityConfig:
    fail_pipeline_on_critical: bool = False
    write_failed_records: bool = True
    quarantine_invalid_rows: bool = True


@dataclass
class EcommerceConfig:
    """Top-level application configuration."""

    environment: str = field(
        default_factory=lambda: os.getenv("ECOMMERCE_ENV", "dev")
    )
    paths: LakehousePaths = field(default_factory=lambda: PATHS)
    spark: SparkConfig = field(default_factory=SparkConfig)
    autoloader: AutoLoaderConfig = field(default_factory=AutoLoaderConfig)
    streaming: StreamingConfig = field(default_factory=StreamingConfig)
    delta_maintenance: DeltaMaintenanceConfig = field(default_factory=DeltaMaintenanceConfig)
    unity_catalog: UnityCatalogConfig = field(default_factory=UnityCatalogConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    data_quality: DataQualityConfig = field(default_factory=DataQualityConfig)

    def to_dict(self) -> dict[str, Any]:
        return {
            "environment": self.environment,
            "storage_base": self.paths.storage_base,
            "use_dbfs": self.paths.use_dbfs,
            "uc_enabled": self.unity_catalog.enabled,
            "catalog": self.unity_catalog.catalog,
            "spark_app": self.spark.app_name,
            "aqe_enabled": self.spark.enable_aqe,
            "simulator_interval_seconds": self.streaming.simulator_interval_seconds,
            "prefer_autoloader": self.streaming.prefer_autoloader,
        }


def get_config() -> EcommerceConfig:
    """Factory used by notebooks and jobs."""
    cfg = EcommerceConfig()
    cfg.paths.ensure_local_directories()
    return cfg


CONFIG = get_config()

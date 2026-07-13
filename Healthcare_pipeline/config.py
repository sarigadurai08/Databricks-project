"""
Runtime configuration for the Healthcare Lakehouse platform.

Loads environment-aware settings for Spark, Auto Loader, Delta maintenance,
Unity Catalog naming, and pipeline behavior. Designed for Databricks jobs
and local PySpark development.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Optional

from config.constants import (
    BRONZE_SCHEMA,
    BROADCAST_THRESHOLD_BYTES,
    CATALOG_NAME,
    DEFAULT_SHUFFLE_PARTITIONS,
    GOLD_SCHEMA,
    MAX_RETRIES,
    RETRY_BACKOFF_SECONDS,
    SILVER_SCHEMA,
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

    app_name: str = "HealthcareLakehouse"
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
    schema_evolution_mode: str = "addNewColumns"  # rescue | failOnNewColumns | none
    rescue_data_column: str = "_rescued_data"
    max_files_per_trigger: int = 1000
    include_existing_files: bool = True
    use_notifications: bool = False  # set True in production cloud with SQS/Event Grid
    csv_header: bool = True
    csv_infer_schema: bool = False  # prefer explicit / inferred schema location
    csv_multi_line: bool = False
    json_multi_line: bool = True
    bad_records_path_enabled: bool = True
    checkpoint_interval: str = "10 seconds"

    def cloud_files_options(self, schema_location: str, fmt: str = "csv") -> dict[str, str]:
        options: dict[str, str] = {
            "cloudFiles.format": fmt,
            "cloudFiles.schemaLocation": schema_location,
            "cloudFiles.inferColumnTypes": str(self.infer_column_types).lower(),
            "cloudFiles.schemaEvolutionMode": self.schema_evolution_mode,
            "cloudFiles.maxFilesPerTrigger": str(self.max_files_per_trigger),
            "cloudFiles.includeExistingFiles": str(self.include_existing_files).lower(),
            "rescueDataColumn": self.rescue_data_column,
        }
        if fmt == "csv":
            options["header"] = str(self.csv_header).lower()
            options["multiLine"] = str(self.csv_multi_line).lower()
        elif fmt == "json":
            options["multiLine"] = str(self.json_multi_line).lower()
        if self.use_notifications:
            options["cloudFiles.useNotifications"] = "true"
        return options


@dataclass
class DeltaMaintenanceConfig:
    """OPTIMIZE / VACUUM / ZORDER / Liquid Clustering settings."""

    # Defaults tuned for Databricks Free Edition / Serverless (safe, non-blocking)
    optimize_enabled: bool = field(default_factory=lambda: _env_bool("HC_OPTIMIZE_ENABLED", False))
    vacuum_enabled: bool = field(default_factory=lambda: _env_bool("HC_VACUUM_ENABLED", False))
    vacuum_retention_hours: int = 168  # 7 days
    zorder_enabled: bool = field(default_factory=lambda: _env_bool("HC_ZORDER_ENABLED", False))
    liquid_clustering_enabled: bool = field(
        default_factory=lambda: _env_bool("HC_LIQUID_CLUSTERING_ENABLED", False)
    )
    time_travel_enabled: bool = True


@dataclass
class UnityCatalogConfig:
    """Unity Catalog naming (no-op gracefully when UC is unavailable)."""

    enabled: bool = field(default_factory=lambda: _env_bool("HEALTHCARE_UC_ENABLED", False))
    catalog: str = field(default_factory=lambda: os.getenv("HEALTHCARE_UC_CATALOG", CATALOG_NAME))
    bronze_schema: str = BRONZE_SCHEMA
    silver_schema: str = SILVER_SCHEMA
    gold_schema: str = GOLD_SCHEMA

    def table_fqn(self, schema: str, table: str) -> str:
        if self.enabled:
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
class HealthcareConfig:
    """Top-level application configuration."""

    environment: str = field(
        default_factory=lambda: os.getenv("HEALTHCARE_ENV", "dev")
    )
    paths: LakehousePaths = field(default_factory=lambda: PATHS)
    spark: SparkConfig = field(default_factory=SparkConfig)
    autoloader: AutoLoaderConfig = field(default_factory=AutoLoaderConfig)
    delta_maintenance: DeltaMaintenanceConfig = field(default_factory=DeltaMaintenanceConfig)
    unity_catalog: UnityCatalogConfig = field(default_factory=UnityCatalogConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    data_quality: DataQualityConfig = field(default_factory=DataQualityConfig)

    # Sample data volumes (for dataset generator)
    num_patients: int = field(default_factory=lambda: _env_int("HC_NUM_PATIENTS", 500))
    num_doctors: int = field(default_factory=lambda: _env_int("HC_NUM_DOCTORS", 50))
    num_appointments: int = field(default_factory=lambda: _env_int("HC_NUM_APPOINTMENTS", 2000))
    num_claims: int = field(default_factory=lambda: _env_int("HC_NUM_CLAIMS", 1500))
    num_pharmacy: int = field(default_factory=lambda: _env_int("HC_NUM_PHARMACY", 1200))
    num_labs: int = field(default_factory=lambda: _env_int("HC_NUM_LABS", 1800))
    num_billing: int = field(default_factory=lambda: _env_int("HC_NUM_BILLING", 1800))

    def to_dict(self) -> dict[str, Any]:
        return {
            "environment": self.environment,
            "storage_base": self.paths.storage_base,
            "use_dbfs": self.paths.use_dbfs,
            "uc_enabled": self.unity_catalog.enabled,
            "catalog": self.unity_catalog.catalog,
            "spark_app": self.spark.app_name,
            "aqe_enabled": self.spark.enable_aqe,
        }


def get_config() -> HealthcareConfig:
    """Factory used by notebooks and jobs."""
    cfg = HealthcareConfig()
    # Local only — never mkdir into Volumes/DBFS or read-only Git folders
    cfg.paths.ensure_local_directories()
    return cfg


# Eager singleton for simple imports: `from config.config import CONFIG`
CONFIG = get_config()
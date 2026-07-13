"""
Databricks Free Edition / Serverless runtime helpers.

Mirrors the working pattern established in notebooks/Bronze/new_bronze.py:
- reuse the managed Spark session
- write all runtime data to a Unity Catalog Volume
- patch input_file_name for batch lineage on Databricks
"""

from __future__ import annotations

from typing import Any, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.config import HealthcareConfig, get_config
from config.paths import PATHS

DEFAULT_VOLUME_CATALOG = "workspace"
DEFAULT_VOLUME_SCHEMA = "default"
DEFAULT_VOLUME_NAME = "healthcare_lakehouse"


def resolve_notebook_spark(notebook_globals: Optional[dict[str, Any]] = None) -> SparkSession:
    """
    Resolve Spark from notebook globals, then the active Databricks session.

    Never creates a local[*] session when a managed session already exists.
    """
    g = notebook_globals or {}
    spark = g.get("spark") or SparkSession.getActiveSession()
    if spark is not None:
        return spark

    from src.utilities.spark_session import get_spark

    return get_spark("HealthcareLakehouse")


def patch_input_file_name() -> None:
    """
    Databricks Serverless / newer runtimes deprecate F.input_file_name().

    Map it to _metadata.file_path so batch bronze lineage keeps working
    (same patch as new_bronze.py).
    """
    F.input_file_name = lambda: F.col("_metadata.file_path")  # type: ignore[assignment]


def configure_writable_volume(
    spark: SparkSession,
    cfg: Optional[HealthcareConfig] = None,
    volume_catalog: str = DEFAULT_VOLUME_CATALOG,
    volume_schema: str = DEFAULT_VOLUME_SCHEMA,
    volume_name: str = DEFAULT_VOLUME_NAME,
) -> str:
    """
    Create (if needed) and bind all lakehouse paths to a writable UC Volume.

    Matches new_bronze.py:
        CREATE VOLUME IF NOT EXISTS workspace.default.healthcare_lakehouse
        storage_base = /Volumes/workspace/default/healthcare_lakehouse
    """
    cfg = cfg or get_config()
    fqn = f"{volume_catalog}.{volume_schema}.{volume_name}"
    volume_base = f"/Volumes/{volume_catalog}/{volume_schema}/{volume_name}"

    try:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {fqn}")
    except Exception:
        # Volume may already exist or catalog naming may differ; continue with path bind
        pass

    cfg.paths.bind_storage_base(volume_base, cloud=True)
    PATHS.bind_storage_base(volume_base, cloud=True)
    return volume_base


def prepare_databricks_runtime(
    spark: SparkSession,
    cfg: Optional[HealthcareConfig] = None,
) -> HealthcareConfig:
    """Full Free-Edition prep: volume storage + input_file_name patch."""
    cfg = cfg or get_config()
    configure_writable_volume(spark, cfg)
    patch_input_file_name()
    return cfg

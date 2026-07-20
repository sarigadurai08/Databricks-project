"""
Databricks runtime helpers — portable across Free Edition, Serverless, and Enterprise.

Auto-discovers:
  - Unity Catalog name (never hardcodes a single catalog)
  - Volume catalog / schema / name
  - Writable storage root under /Volumes/... with DBFS fallback
  - Spark session from notebook globals

All lakehouse writes bind to the discovered Volume so Git folders stay read-only.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.config import EcommerceConfig, get_config
from config.paths import PATHS

DEFAULT_VOLUME_NAME = "ecommerce_lakehouse"
DEFAULT_VOLUME_SCHEMA = "default"
DBFS_FALLBACK_BASE = "dbfs:/FileStore/ecommerce_lakehouse"


@dataclass
class RuntimeEnvironment:
    """Resolved, workspace-agnostic runtime coordinates."""

    catalog: str
    volume_catalog: str
    volume_schema: str
    volume_name: str
    volume_path: str
    storage_backend: str  # "unity_catalog_volume" | "dbfs" | "env_override"
    project_root: Optional[str] = None
    current_user: Optional[str] = None
    runtime_version: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "catalog": self.catalog,
            "volume_catalog": self.volume_catalog,
            "volume_schema": self.volume_schema,
            "volume_name": self.volume_name,
            "volume_path": self.volume_path,
            "storage_backend": self.storage_backend,
            "project_root": self.project_root,
            "current_user": self.current_user,
            "runtime_version": self.runtime_version,
        }


def resolve_notebook_spark(notebook_globals: Optional[dict[str, Any]] = None) -> SparkSession:
    """Resolve Spark from notebook globals, then the active Databricks session."""
    g = notebook_globals or {}
    spark = g.get("spark") or SparkSession.getActiveSession()
    if spark is not None:
        return spark

    from src.utilities.spark_session import get_spark

    return get_spark("EcommerceLakehouse")


def patch_input_file_name() -> None:
    """
    Databricks Serverless / newer runtimes deprecate F.input_file_name().

    Map it to _metadata.file_path so batch bronze lineage keeps working.
    """
    F.input_file_name = lambda: F.col("_metadata.file_path")  # type: ignore[assignment]


def _current_catalog(spark: SparkSession) -> Optional[str]:
    try:
        row = spark.sql("SELECT current_catalog() AS c").collect()[0]
        cat = str(row["c"]).strip()
        if cat and cat.lower() not in {"null", "none"}:
            return cat
    except Exception:
        pass
    return None


def _list_catalogs(spark: SparkSession) -> list[str]:
    names: list[str] = []
    try:
        for row in spark.sql("SHOW CATALOGS").collect():
            data = row.asDict()
            name = data.get("catalog") or data.get("catalogName") or next(iter(data.values()), None)
            if name:
                names.append(str(name))
    except Exception:
        pass
    return names


def discover_catalog(
    spark: SparkSession,
    preferred: Optional[str] = None,
) -> str:
    """
    Discover a usable Unity Catalog name for this workspace.

    Priority:
      1. ECOMMERCE_UC_CATALOG env
      2. Explicit preferred (from config) when it is actually usable
      3. current_catalog()
      4. First catalog from SHOW CATALOGS that accepts USE CATALOG
      5. Last-resort well-known names only if they appear in SHOW CATALOGS
    """
    env_cat = os.getenv("ECOMMERCE_UC_CATALOG", "").strip()
    available = _list_catalogs(spark)
    available_lower = {c.lower(): c for c in available}

    candidates: list[str] = []
    for c in (env_cat, preferred, _current_catalog(spark)):
        if c and c not in candidates:
            candidates.append(c)

    for c in available:
        if c not in candidates and c.lower() not in {"system", "samples", "__databricks_internal"}:
            candidates.append(c)

    for hint in ("workspace", "main", "hive_metastore", "spark_catalog"):
        if hint in available_lower and available_lower[hint] not in candidates:
            candidates.append(available_lower[hint])

    for catalog in candidates:
        try:
            spark.sql(f"USE CATALOG `{catalog}`")
            return catalog
        except Exception:
            continue

    return _current_catalog(spark) or (available[0] if available else "spark_catalog")


def discover_volume_schema(spark: SparkSession, catalog: str) -> str:
    """Prefer ECOMMERCE_UC_VOLUME_SCHEMA, else default/main, else current schema."""
    env_schema = os.getenv("ECOMMERCE_UC_VOLUME_SCHEMA", "").strip()
    if env_schema:
        return env_schema

    for schema in ("default", "main"):
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
            return schema
        except Exception:
            try:
                spark.sql(f"USE CATALOG `{catalog}`")
                spark.sql(f"USE SCHEMA `{schema}`")
                return schema
            except Exception:
                continue

    try:
        row = spark.sql("SELECT current_schema() AS s").collect()[0]
        s = str(row["s"]).strip()
        if s and s.lower() not in {"null", "none"}:
            return s
    except Exception:
        pass
    return DEFAULT_VOLUME_SCHEMA


def discover_volume_name() -> str:
    return (
        os.getenv("ECOMMERCE_UC_VOLUME_NAME", DEFAULT_VOLUME_NAME).strip() or DEFAULT_VOLUME_NAME
    )


def discover_current_user(spark: SparkSession) -> Optional[str]:
    try:
        row = spark.sql("SELECT current_user() AS u").collect()[0]
        return str(row["u"])
    except Exception:
        return os.getenv("USER") or os.getenv("USERNAME")


def discover_runtime_version() -> Optional[str]:
    return os.getenv("DATABRICKS_RUNTIME_VERSION")


def _bind_dbfs_fallback(cfg: EcommerceConfig) -> RuntimeEnvironment:
    """Fall back to DBFS FileStore when UC Volumes are unavailable."""
    base = os.getenv("ECOMMERCE_DBFS_FALLBACK", DBFS_FALLBACK_BASE).rstrip("/")
    cfg.paths.bind_storage_base(base, cloud=True)
    PATHS.bind_storage_base(base, cloud=True)
    return RuntimeEnvironment(
        catalog=cfg.unity_catalog.catalog or "spark_catalog",
        volume_catalog=cfg.unity_catalog.catalog or "spark_catalog",
        volume_schema=DEFAULT_VOLUME_SCHEMA,
        volume_name=discover_volume_name(),
        volume_path=base,
        storage_backend="dbfs",
        project_root=str(cfg.paths.project_root),
        current_user=os.getenv("USER") or os.getenv("USERNAME"),
        runtime_version=discover_runtime_version(),
    )


def configure_writable_volume(
    spark: SparkSession,
    cfg: Optional[EcommerceConfig] = None,
    volume_catalog: Optional[str] = None,
    volume_schema: Optional[str] = None,
    volume_name: Optional[str] = None,
) -> RuntimeEnvironment:
    """
    Discover (or create) a writable UC Volume and bind all lakehouse paths to it.

    Falls back to DBFS automatically when Volumes are unavailable.
    Never hardcodes a catalog.
    """
    cfg = cfg or get_config()

    env_storage = os.getenv("ECOMMERCE_STORAGE_BASE", "").strip()
    if env_storage:
        cfg.paths.bind_storage_base(env_storage, cloud=True)
        PATHS.bind_storage_base(env_storage, cloud=True)
        catalog = discover_catalog(spark, cfg.unity_catalog.catalog or None)
        cfg.unity_catalog.catalog = catalog
        return RuntimeEnvironment(
            catalog=catalog,
            volume_catalog=volume_catalog or catalog,
            volume_schema=volume_schema or DEFAULT_VOLUME_SCHEMA,
            volume_name=volume_name or discover_volume_name(),
            volume_path=env_storage.rstrip("/"),
            storage_backend="env_override",
            project_root=str(cfg.paths.project_root),
            current_user=discover_current_user(spark),
            runtime_version=discover_runtime_version(),
        )

    catalog = discover_catalog(spark, (volume_catalog or cfg.unity_catalog.catalog or None))
    cfg.unity_catalog.catalog = catalog

    v_catalog = volume_catalog or catalog
    v_schema = volume_schema or discover_volume_schema(spark, v_catalog)
    v_name = volume_name or discover_volume_name()
    fqn = f"`{v_catalog}`.`{v_schema}`.`{v_name}`"
    volume_base = f"/Volumes/{v_catalog}/{v_schema}/{v_name}"

    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{v_catalog}`.`{v_schema}`")
    except Exception:
        pass

    volume_ok = False
    try:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {fqn}")
        volume_ok = True
    except Exception:
        # Probe whether the volume path is already usable
        try:
            spark.sql(f"DESCRIBE VOLUME {fqn}")
            volume_ok = True
        except Exception:
            volume_ok = False

    if not volume_ok and os.getenv("ECOMMERCE_FORCE_VOLUME", "").lower() not in {
        "1",
        "true",
        "yes",
    }:
        return _bind_dbfs_fallback(cfg)

    cfg.paths.bind_storage_base(volume_base, cloud=True)
    PATHS.bind_storage_base(volume_base, cloud=True)

    return RuntimeEnvironment(
        catalog=catalog,
        volume_catalog=v_catalog,
        volume_schema=v_schema,
        volume_name=v_name,
        volume_path=volume_base,
        storage_backend="unity_catalog_volume",
        project_root=str(cfg.paths.project_root),
        current_user=discover_current_user(spark),
        runtime_version=discover_runtime_version(),
    )


def prepare_databricks_runtime(
    spark: SparkSession,
    cfg: Optional[EcommerceConfig] = None,
) -> EcommerceConfig:
    """
    Full portable prep: discover catalog + volume/DBFS storage + input_file_name patch.

    Safe to call from every notebook; idempotent across workspaces.
    """
    cfg = cfg or get_config()
    runtime = configure_writable_volume(spark, cfg)
    try:
        from config import config as config_mod

        if getattr(config_mod, "CONFIG", None) is not None:
            config_mod.CONFIG.paths.bind_storage_base(runtime.volume_path, cloud=True)
            config_mod.CONFIG.unity_catalog.catalog = runtime.catalog
    except Exception:
        pass
    patch_input_file_name()
    try:
        cfg._runtime = runtime  # type: ignore[attr-defined]
    except Exception:
        pass
    return cfg

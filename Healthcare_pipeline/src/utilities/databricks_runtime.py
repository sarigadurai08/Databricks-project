"""
Databricks runtime helpers — portable across Free Edition, Serverless, and Enterprise.

Auto-discovers:
  - Unity Catalog name (never hardcodes a single catalog)
  - Volume catalog / schema / name
  - Writable storage root under /Volumes/...
  - Spark session from notebook globals

All lakehouse writes bind to the discovered Volume so Git folders stay read-only.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.config import HealthcareConfig, get_config
from config.paths import PATHS

# Volume *name* is project-owned (created if missing). Catalog/schema are discovered.
DEFAULT_VOLUME_NAME = "healthcare_lakehouse"
DEFAULT_VOLUME_SCHEMA = "default"


@dataclass
class RuntimeEnvironment:
    """Resolved, workspace-agnostic runtime coordinates."""

    catalog: str
    volume_catalog: str
    volume_schema: str
    volume_name: str
    volume_path: str
    project_root: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "catalog": self.catalog,
            "volume_catalog": self.volume_catalog,
            "volume_schema": self.volume_schema,
            "volume_name": self.volume_name,
            "volume_path": self.volume_path,
            "project_root": self.project_root,
        }


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
            # Column may be catalog or catalogName depending on runtime
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
      1. HEALTHCARE_UC_CATALOG env
      2. Explicit preferred (from config) when it is actually usable
      3. current_catalog()
      4. First catalog from SHOW CATALOGS that accepts USE CATALOG
      5. Last-resort well-known names only if they appear in SHOW CATALOGS
    """
    env_cat = os.getenv("HEALTHCARE_UC_CATALOG", "").strip()
    available = _list_catalogs(spark)
    available_lower = {c.lower(): c for c in available}

    candidates: list[str] = []
    for c in (env_cat, preferred, _current_catalog(spark)):
        if c and c not in candidates:
            candidates.append(c)

    # Prefer catalogs that actually exist in this workspace
    for c in available:
        if c not in candidates and c.lower() not in {"system", "samples", "__databricks_internal"}:
            candidates.append(c)

    # Last-resort names — only if present in this workspace (never invent them)
    for hint in ("workspace", "main", "hive_metastore", "spark_catalog"):
        if hint in available_lower and available_lower[hint] not in candidates:
            candidates.append(available_lower[hint])

    for catalog in candidates:
        try:
            spark.sql(f"USE CATALOG `{catalog}`")
            return catalog
        except Exception:
            continue

    # Absolute fallback: whatever Spark reports as current, else spark_catalog
    return _current_catalog(spark) or (available[0] if available else "spark_catalog")


def discover_volume_schema(spark: SparkSession, catalog: str) -> str:
    """
    Prefer HEALTHCARE_UC_VOLUME_SCHEMA, else `default` if it exists / is creatable,
    else the current schema.
    """
    env_schema = os.getenv("HEALTHCARE_UC_VOLUME_SCHEMA", "").strip()
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
    return os.getenv("HEALTHCARE_UC_VOLUME_NAME", DEFAULT_VOLUME_NAME).strip() or DEFAULT_VOLUME_NAME


def configure_writable_volume(
    spark: SparkSession,
    cfg: Optional[HealthcareConfig] = None,
    volume_catalog: Optional[str] = None,
    volume_schema: Optional[str] = None,
    volume_name: Optional[str] = None,
) -> RuntimeEnvironment:
    """
    Discover (or create) a writable UC Volume and bind all lakehouse paths to it.

    Never hardcodes a catalog. Works across Free Edition (`workspace`),
    Enterprise (`main` / custom), and workspaces with different defaults.
    """
    cfg = cfg or get_config()

    # Explicit storage override wins (advanced / CI)
    env_storage = os.getenv("HEALTHCARE_STORAGE_BASE", "").strip()
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
            project_root=str(cfg.paths.project_root),
        )

    catalog = discover_catalog(spark, (volume_catalog or cfg.unity_catalog.catalog or None))
    cfg.unity_catalog.catalog = catalog

    v_catalog = volume_catalog or catalog
    v_schema = volume_schema or discover_volume_schema(spark, v_catalog)
    v_name = volume_name or discover_volume_name()
    fqn = f"`{v_catalog}`.`{v_schema}`.`{v_name}`"
    volume_base = f"/Volumes/{v_catalog}/{v_schema}/{v_name}"

    # Ensure schema exists (idempotent, no elevated rights beyond UC grants)
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{v_catalog}`.`{v_schema}`")
    except Exception:
        pass

    try:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {fqn}")
    except Exception:
        # Volume may already exist, or CREATE VOLUME may be restricted —
        # still bind the path; subsequent writes will surface a clear error.
        pass

    cfg.paths.bind_storage_base(volume_base, cloud=True)
    PATHS.bind_storage_base(volume_base, cloud=True)

    return RuntimeEnvironment(
        catalog=catalog,
        volume_catalog=v_catalog,
        volume_schema=v_schema,
        volume_name=v_name,
        volume_path=volume_base,
        project_root=str(cfg.paths.project_root),
    )


def prepare_databricks_runtime(
    spark: SparkSession,
    cfg: Optional[HealthcareConfig] = None,
) -> HealthcareConfig:
    """
    Full portable prep: discover catalog + volume storage + input_file_name patch.

    Safe to call from every notebook; idempotent across workspaces.
    """
    cfg = cfg or get_config()
    runtime = configure_writable_volume(spark, cfg)
    # Keep CONFIG singleton paths in sync when notebooks imported CONFIG earlier
    try:
        from config import config as config_mod

        if getattr(config_mod, "CONFIG", None) is not None:
            config_mod.CONFIG.paths.bind_storage_base(runtime.volume_path, cloud=True)
            config_mod.CONFIG.unity_catalog.catalog = runtime.catalog
    except Exception:
        pass
    patch_input_file_name()
    # Stash runtime on config for notebooks/logging
    try:
        cfg._runtime = runtime  # type: ignore[attr-defined]
    except Exception:
        pass
    return cfg

"""
Path configuration for the Healthcare Lakehouse.

Supports local / DBFS / Unity Catalog volume layouts via environment-aware
base paths. All pipeline code should resolve paths through this module.

On Databricks Free Edition / Serverless, runtime data must use a writable
Volume (see src.utilities.databricks_runtime.configure_writable_volume).
Never write lakehouse data into the Git repository.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from config.constants import (
    ALL_ENTITIES,
    BAD_RECORDS_PATH_SUFFIX,
    CHECKPOINT_SUFFIX,
    SCHEMA_LOCATION_SUFFIX,
)


def _default_project_root() -> Path:
    """Resolve project root relative to this file (project-root paths.py)."""
    return Path(__file__).resolve().parent


def _is_cloud_uri(path: str) -> bool:
    p = path.replace("\\", "/")
    return (
        p.startswith("/Volumes/")
        or p.startswith("dbfs:")
        or p.startswith("s3a:")
        or p.startswith("s3:")
        or p.startswith("abfss:")
        or p.startswith("wasbs:")
        or p.startswith("gs:")
    )


class LakehousePaths:
    """
    Resolves storage paths for landing, bronze, silver, gold, checkpoints,
    schemas, audit, and dead-letter queues.

    Environment variables (optional):
        HEALTHCARE_LAKEHOUSE_ROOT  - override project root
        HEALTHCARE_STORAGE_BASE    - override storage base (e.g. /Volumes/... or dbfs:/mnt/...)
        HEALTHCARE_USE_DBFS        - "true" to force cloud-style paths under storage_base
    """

    def __init__(
        self,
        project_root: Optional[str | Path] = None,
        storage_base: Optional[str] = None,
        use_dbfs: Optional[bool] = None,
    ) -> None:
        env_root = os.getenv("HEALTHCARE_LAKEHOUSE_ROOT")
        self.project_root = Path(project_root or env_root or _default_project_root())

        env_dbfs = os.getenv("HEALTHCARE_USE_DBFS", "false").lower() == "true"
        self.use_dbfs = use_dbfs if use_dbfs is not None else env_dbfs

        env_storage = os.getenv("HEALTHCARE_STORAGE_BASE")
        if storage_base:
            self.storage_base = storage_base.rstrip("/")
        elif env_storage:
            self.storage_base = env_storage.rstrip("/")
        elif self.use_dbfs:
            # Legacy flag — Databricks binds a UC Volume via prepare_databricks_runtime.
            self.storage_base = str(self.project_root / "data").replace("\\", "/")
            self.use_dbfs = False
        else:
            self.storage_base = str(self.project_root / "data").replace("\\", "/")

        if _is_cloud_uri(self.storage_base):
            self.use_dbfs = True

        self.datasets_dir = self.project_root / "datasets"
        self.landing_csv_dir = self.datasets_dir / "landing" / "csv"
        self.landing_json_dir = self.datasets_dir / "landing" / "json"

    def bind_storage_base(self, storage_base: str, cloud: bool = True) -> None:
        """Rebind all runtime paths to a writable storage root (Volume / DBFS)."""
        self.storage_base = storage_base.rstrip("/")
        self.use_dbfs = cloud or _is_cloud_uri(self.storage_base)

    @property
    def is_cloud_storage(self) -> bool:
        return self.use_dbfs or _is_cloud_uri(self.storage_base)

    def landing_path(self, entity: str, fmt: str = "csv") -> str:
        fmt = fmt.lower()
        if self.is_cloud_storage:
            return f"{self.storage_base}/landing/{fmt}/{entity}"
        local = self.landing_csv_dir if fmt == "csv" else self.landing_json_dir
        return str((local / entity).resolve()).replace("\\", "/")

    def sample_csv_path(self, entity: str) -> str:
        return str((self.datasets_dir / f"{entity}.csv").resolve()).replace("\\", "/")

    def sample_json_path(self, entity: str) -> str:
        return str((self.datasets_dir / f"{entity}.json").resolve()).replace("\\", "/")

    def bronze_path(self, entity: str) -> str:
        return f"{self.storage_base}/bronze/{entity}"

    def silver_path(self, entity: str) -> str:
        return f"{self.storage_base}/silver/{entity}"

    def gold_path(self, table_name: str) -> str:
        return f"{self.storage_base}/gold/{table_name}"

    def checkpoint_path(self, entity: str, layer: str = "bronze") -> str:
        return f"{self.storage_base}/{layer}/{CHECKPOINT_SUFFIX}/{entity}"

    def schema_location(self, entity: str) -> str:
        return f"{self.storage_base}/bronze/{SCHEMA_LOCATION_SUFFIX}/{entity}"

    def bad_records_path(self, entity: str) -> str:
        return f"{self.storage_base}/bronze/{BAD_RECORDS_PATH_SUFFIX}/{entity}"

    def dlq_path(self, entity: str) -> str:
        return f"{self.storage_base}/dead_letter/{entity}"

    def audit_path(self) -> str:
        return f"{self.storage_base}/audit/pipeline_audit"

    def log_path(self) -> str:
        return f"{self.storage_base}/ops_logging/pipeline_logs"

    def dq_failed_records_path(self) -> str:
        return f"{self.storage_base}/data_quality/failed_records"

    def dq_results_path(self) -> str:
        return f"{self.storage_base}/data_quality/validation_results"

    def ensure_local_directories(self) -> None:
        if self.is_cloud_storage:
            return
        try:
            for entity in ALL_ENTITIES:
                (self.landing_csv_dir / entity).mkdir(parents=True, exist_ok=True)
                (self.landing_json_dir / entity).mkdir(parents=True, exist_ok=True)
            Path(self.storage_base).mkdir(parents=True, exist_ok=True)
        except OSError:
            pass

    def all_entity_landing_paths(self, fmt: str = "csv") -> dict[str, str]:
        return {entity: self.landing_path(entity, fmt) for entity in ALL_ENTITIES}


PATHS = LakehousePaths()

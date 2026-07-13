"""
Path configuration for the Healthcare Lakehouse.

Supports local / DBFS / Unity Catalog volume layouts via environment-aware
base paths. All pipeline code should resolve paths through this module.
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
    """Resolve project root relative to this file (config/paths.py)."""
    return Path(__file__).resolve().parent.parent


class LakehousePaths:
    """
    Resolves storage paths for landing, bronze, silver, gold, checkpoints,
    schemas, audit, and dead-letter queues.

    Environment variables (optional):
        HEALTHCARE_LAKEHOUSE_ROOT  - override project root
        HEALTHCARE_STORAGE_BASE    - override storage base (e.g. dbfs:/mnt/healthcare)
        HEALTHCARE_USE_DBFS        - "true" to use DBFS-style paths
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
            self.storage_base = "dbfs:/mnt/healthcare_lakehouse"
        else:
            self.storage_base = str(self.project_root / "data").replace("\\", "/")

        # Local datasets for sample CSV/JSON generation & local Spark runs
        self.datasets_dir = self.project_root / "datasets"
        self.landing_csv_dir = self.datasets_dir / "landing" / "csv"
        self.landing_json_dir = self.datasets_dir / "landing" / "json"

    # ------------------------------------------------------------------
    # Landing (raw files for Auto Loader)
    # ------------------------------------------------------------------
    def landing_path(self, entity: str, fmt: str = "csv") -> str:
        fmt = fmt.lower()
        if self.use_dbfs:
            return f"{self.storage_base}/landing/{fmt}/{entity}"
        local = self.landing_csv_dir if fmt == "csv" else self.landing_json_dir
        return str((local / entity).resolve()).replace("\\", "/")

    def sample_csv_path(self, entity: str) -> str:
        return str((self.datasets_dir / f"{entity}.csv").resolve()).replace("\\", "/")

    def sample_json_path(self, entity: str) -> str:
        return str((self.datasets_dir / f"{entity}.json").resolve()).replace("\\", "/")

    # ------------------------------------------------------------------
    # Medallion table / path locations
    # ------------------------------------------------------------------
    def bronze_path(self, entity: str) -> str:
        return f"{self.storage_base}/bronze/{entity}"

    def silver_path(self, entity: str) -> str:
        return f"{self.storage_base}/silver/{entity}"

    def gold_path(self, table_name: str) -> str:
        return f"{self.storage_base}/gold/{table_name}"

    # ------------------------------------------------------------------
    # Auto Loader support paths
    # ------------------------------------------------------------------
    def checkpoint_path(self, entity: str, layer: str = "bronze") -> str:
        return f"{self.storage_base}/{layer}/{CHECKPOINT_SUFFIX}/{entity}"

    def schema_location(self, entity: str) -> str:
        return f"{self.storage_base}/bronze/{SCHEMA_LOCATION_SUFFIX}/{entity}"

    def bad_records_path(self, entity: str) -> str:
        return f"{self.storage_base}/bronze/{BAD_RECORDS_PATH_SUFFIX}/{entity}"

    def dlq_path(self, entity: str) -> str:
        return f"{self.storage_base}/dead_letter/{entity}"

    # ------------------------------------------------------------------
    # Ops / audit / logging
    # ------------------------------------------------------------------
    def audit_path(self) -> str:
        return f"{self.storage_base}/audit/pipeline_audit"

    def log_path(self) -> str:
        return f"{self.storage_base}/ops_logging/pipeline_logs"

    def dq_failed_records_path(self) -> str:
        return f"{self.storage_base}/data_quality/failed_records"

    def dq_results_path(self) -> str:
        return f"{self.storage_base}/data_quality/validation_results"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def ensure_local_directories(self) -> None:
        """Create local landing and data directories when not using DBFS."""
        if self.use_dbfs:
            return
        for entity in ALL_ENTITIES:
            (self.landing_csv_dir / entity).mkdir(parents=True, exist_ok=True)
            (self.landing_json_dir / entity).mkdir(parents=True, exist_ok=True)
        Path(self.storage_base).mkdir(parents=True, exist_ok=True)

    def all_entity_landing_paths(self, fmt: str = "csv") -> dict[str, str]:
        return {entity: self.landing_path(entity, fmt) for entity in ALL_ENTITIES}


# Module-level singleton for convenience imports
PATHS = LakehousePaths()

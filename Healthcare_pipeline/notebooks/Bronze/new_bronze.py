# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion — Healthcare Lakehouse (Reference Implementation)
# MAGIC
# MAGIC Project standard for Databricks Free Edition / Serverless.
# MAGIC All other notebooks follow this bootstrap, Spark, Volume, audit, and exit pattern.

# COMMAND ----------

import sys
from pathlib import Path

def _seed_project_root() -> str:
    import os
    def _is_root(p: Path) -> bool:
        return (p / "config" / "config.py").exists()
    candidates = []
    env = os.getenv("HEALTHCARE_LAKEHOUSE_ROOT")
    if env:
        candidates.append(Path(env))
    try:
        candidates.extend([Path.cwd(), *list(Path.cwd().parents)[:12]])
    except Exception:
        pass
    try:
        nb = Path(
            dbutils.notebook.entry_point.getDbutils()  # type: ignore[name-defined]
            .notebook().getContext().notebookPath().get()
        )
        ws = nb if str(nb).startswith("/Workspace") else Path("/Workspace") / str(nb).lstrip("/")
        candidates = [ws, *list(ws.parents)[:12]] + candidates
    except Exception:
        pass
    for base_name in ("/Workspace/Users", "/Workspace/Repos", "/Workspace"):
        base = Path(base_name)
        if not base.exists():
            continue
        try:
            for child in list(base.iterdir())[:80]:
                if not child.is_dir():
                    continue
                candidates.append(child)
                try:
                    for gc in list(child.iterdir())[:40]:
                        if gc.is_dir():
                            candidates.append(gc)
                except Exception:
                    pass
        except Exception:
            pass
    seen = set()
    for cand in candidates:
        key = str(cand)
        if key in seen:
            continue
        seen.add(key)
        if _is_root(cand):
            root = str(cand)
            if root in sys.path:
                sys.path.remove(root)
            sys.path.insert(0, root)
            return root
    raise FileNotFoundError(
        "Healthcare_pipeline root not found. Set HEALTHCARE_LAKEHOUSE_ROOT."
    )

_PROJECT_ROOT = _seed_project_root()

from src.utilities.bootstrap import bootstrap_notebook
_PROJECT_ROOT = str(bootstrap_notebook(dbutils=globals().get("dbutils"), reload_modules=True))


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.config import CONFIG, get_config
from config.constants import (
    ALL_ENTITIES,
    META_BATCH_ID,
    META_INGESTION_TIME,
    META_LOAD_ID,
    META_RECORD_HASH,
    META_SOURCE_FILE,
    PIPELINE_BRONZE_INGESTION,
)
from config.paths import PATHS
from src.audit.auditor import PipelineAuditor
from src.ingestion.autoloader import AutoLoaderIngestion, stage_sample_files_to_landing
from src.logging.logger import ensure_log_table, get_logger
from src.utilities.dataframe_utils import generate_run_id
from src.utilities.databricks_runtime import prepare_databricks_runtime

# COMMAND ----------

spark = globals().get("spark") or SparkSession.getActiveSession()
cfg = get_config()
cfg = prepare_databricks_runtime(spark, cfg)
run_id = generate_run_id()
logger = get_logger(spark, PIPELINE_BRONZE_INGESTION, run_id)
ensure_log_table(spark)
auditor = PipelineAuditor(spark, PIPELINE_BRONZE_INGESTION, run_id, cfg.environment, logger)

logger.info("Bronze pipeline started", module="bronze", details=cfg.to_dict())

# COMMAND ----------

try:
    stage_sample_files_to_landing(fmt="csv", spark=spark)
    logger.info("Landing zone staged with CSV samples", module="bronze")
except Exception as exc:
    logger.error("Failed to stage landing files", module="bronze", exc=exc)
    logger.flush()
    raise

# COMMAND ----------

ingestion = AutoLoaderIngestion(spark, cfg, logger, run_id)
ingestion._is_databricks = False
status_map = {}

try:
    for entity in ALL_ENTITIES:
        with auditor.track(f"bronze_{entity}") as ctx:
            bronze_df = ingestion.ingest_entity(entity, fmt="csv", trigger_once=True)
            row_count = bronze_df.count()
            ctx["rows_read"] = row_count
            ctx["rows_inserted"] = row_count
            status_map[entity] = row_count
            logger.info(
                f"Bronze loaded {entity}",
                module="bronze",
                details={"rows": row_count, "path": cfg.paths.bronze_path(entity)},
            )
            display(bronze_df.limit(5))  # noqa: F821 — Databricks builtin
except Exception as exc:
    logger.error("Bronze ingestion failed", module="bronze", exc=exc)
    logger.flush()
    raise

# COMMAND ----------

required_meta = [META_INGESTION_TIME, META_SOURCE_FILE, META_LOAD_ID, META_BATCH_ID, META_RECORD_HASH]

for entity in ALL_ENTITIES:
    df = spark.read.format("delta").load(cfg.paths.bronze_path(entity))
    missing = [c for c in required_meta if c not in df.columns]
    assert not missing, f"{entity} missing metadata columns: {missing}"
    logger.info(f"Metadata validation passed for {entity}", module="bronze")

# COMMAND ----------

patients_bronze = spark.read.format("delta").load(cfg.paths.bronze_path("patients"))
if "_rescued_data" in patients_bronze.columns:
    rescued = patients_bronze.filter(F.col("_rescued_data").isNotNull())
    logger.info("Rescued record count", module="bronze", details={"count": rescued.count()})
    display(rescued.limit(10))  # noqa: F821
else:
    logger.info("No _rescued_data column present in this runtime load", module="bronze")

# COMMAND ----------

logger.flush()
logger.info("Bronze pipeline completed", module="bronze", details=status_map)
dbutils.notebook.exit(str(status_map))  # type: ignore[name-defined]

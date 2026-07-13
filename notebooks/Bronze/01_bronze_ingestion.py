# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion — Healthcare Lakehouse
# MAGIC
# MAGIC Incremental raw ingestion using **Databricks Auto Loader** (`cloudFiles`) with:
# MAGIC - CSV / JSON support
# MAGIC - Schema evolution (`addNewColumns`)
# MAGIC - Rescue data column for unknown / malformed fields
# MAGIC - Checkpointing & bad records path
# MAGIC - Bronze lineage metadata (`_ingestion_time`, `_source_file`, `_load_id`, `_batch_id`, `_record_hash`)
# MAGIC
# MAGIC **Layer contract:** store data exactly as received + metadata. No business cleansing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup — path bootstrap for Databricks Repos / local

# COMMAND ----------

import sys
from pathlib import Path

def _bootstrap_project_root() -> None:
    candidates = [
        Path.cwd(),
        Path.cwd().parent,
        Path("/Workspace/Repos/Healthcare_Lakehouse"),
        Path("/Workspace/Healthcare_Lakehouse"),
    ]
    for cand in candidates:
        if (cand / "config" / "config.py").exists():
            root = str(cand)
            if root not in sys.path:
                sys.path.insert(0, root)
            return
    # Fallback: walk up from notebook path if available
    try:
        nb = Path(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())  # type: ignore[name-defined]
        for parent in nb.parents:
            if (parent / "config" / "config.py").exists():
                if str(parent) not in sys.path:
                    sys.path.insert(0, str(parent))
                return
    except Exception:
        pass

_bootstrap_project_root()

# COMMAND ----------

from config.config import CONFIG, get_config
from config.constants import ALL_ENTITIES, PIPELINE_BRONZE_INGESTION
from src.audit.auditor import PipelineAuditor
from src.ingestion.autoloader import AutoLoaderIngestion, stage_sample_files_to_landing
from src.logging.logger import ensure_log_table, get_logger
from src.utilities.dataframe_utils import generate_run_id
from src.utilities.spark_session import get_spark

# COMMAND ----------

spark = get_spark("BronzeIngestion")
cfg = get_config()
run_id = generate_run_id()
logger = get_logger(spark, PIPELINE_BRONZE_INGESTION, run_id)
ensure_log_table(spark)
auditor = PipelineAuditor(spark, PIPELINE_BRONZE_INGESTION, run_id, cfg.environment, logger)

logger.info("Bronze pipeline started", module="bronze", details=cfg.to_dict())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Stage sample files into Auto Loader landing zones

# COMMAND ----------

stage_sample_files_to_landing(fmt="csv")
logger.info("Landing zone staged with CSV samples", module="bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingest all entities into Bronze (Auto Loader / batch fallback)

# COMMAND ----------

ingestion = AutoLoaderIngestion(spark, cfg, logger, run_id)
status_map = {}

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate bronze metadata columns

# COMMAND ----------

from pyspark.sql import functions as F
from config.constants import (
    META_BATCH_ID,
    META_INGESTION_TIME,
    META_LOAD_ID,
    META_RECORD_HASH,
    META_SOURCE_FILE,
)

required_meta = [META_INGESTION_TIME, META_SOURCE_FILE, META_LOAD_ID, META_BATCH_ID, META_RECORD_HASH]

for entity in ALL_ENTITIES:
    df = spark.read.format("delta").load(cfg.paths.bronze_path(entity))
    missing = [c for c in required_meta if c not in df.columns]
    assert not missing, f"{entity} missing metadata columns: {missing}"
    logger.info(f"Metadata validation passed for {entity}", module="bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Schema evolution demo — inspect rescued data column

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

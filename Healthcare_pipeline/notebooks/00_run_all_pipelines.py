# Databricks notebook source
# MAGIC %md
# MAGIC # End-to-End Orchestration
# MAGIC
# MAGIC Runs Bronze → Silver → SCD Demo → Gold → Data Quality → Monitoring sequentially.
# MAGIC Suitable as a Databricks Job multi-task workflow entrypoint or single notebook driver.
# MAGIC
# MAGIC **Runtime standard:** same bootstrap / Spark / Volume / audit pattern as `new_bronze.py`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run order
# MAGIC 1. Bronze ingestion
# MAGIC 2. Silver SCD transforms
# MAGIC 3. SCD Type 1 / Type 2 demo
# MAGIC 4. Gold analytics
# MAGIC 5. Data quality
# MAGIC 6. Monitoring / maintenance

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
    users_root = Path("/Workspace/Users")
    if users_root.exists():
        for user_dir in users_root.iterdir():
            candidates.extend(
                [
                    user_dir / "Databricks-project" / "Healthcare_pipeline",
                    user_dir / "Databricks-project",
                    user_dir / "Healthcare_pipeline",
                    user_dir / "Healthcare_Lakehouse",
                ]
            )
    for cand in candidates:
        if (cand / "config" / "config.py").exists():
            root = str(cand)
            if root not in sys.path:
                sys.path.insert(0, root)
            return
    try:
        nb = Path(
            dbutils.notebook.entry_point.getDbutils()  # type: ignore[name-defined]
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        workspace_nb = Path("/Workspace") / str(nb).lstrip("/")
        for parent in list(nb.parents) + list(workspace_nb.parents):
            if (parent / "config" / "config.py").exists():
                if str(parent) not in sys.path:
                    sys.path.insert(0, str(parent))
                return
    except Exception:
        pass

_bootstrap_project_root()

# COMMAND ----------

from pyspark.sql import SparkSession

from config.config import get_config
from config.constants import PIPELINE_MAINTENANCE
from src.audit.auditor import PipelineAuditor
from src.logging.logger import ensure_log_table, get_logger
from src.utilities.dataframe_utils import generate_run_id
from src.utilities.databricks_runtime import prepare_databricks_runtime

# COMMAND ----------

spark = globals().get("spark") or SparkSession.getActiveSession()
cfg = get_config()
cfg = prepare_databricks_runtime(spark, cfg)
run_id = generate_run_id()
logger = get_logger(spark, "run_all_pipelines", run_id)
ensure_log_table(spark)
auditor = PipelineAuditor(spark, "run_all_pipelines", run_id, cfg.environment, logger)

logger.info("End-to-end orchestration started", module="orchestration", details=cfg.to_dict())

# COMMAND ----------

NOTEBOOKS = [
    ("./Bronze/01_bronze_ingestion", "bronze"),
    ("./Silver/01_silver_transformations", "silver"),
    ("./Silver/02_scd_type1_type2_demo", "scd_demo"),
    ("./Gold/01_gold_analytics", "gold"),
    ("./DataQuality/01_data_quality_framework", "data_quality"),
    ("./Monitoring/01_monitoring_maintenance", "monitoring"),
]

status_map = {}

try:
    for path, name in NOTEBOOKS:
        with auditor.track(f"orchestrate_{name}") as ctx:
            logger.info(f"Starting notebook {path}", module="orchestration")
            result = dbutils.notebook.run(path, timeout_seconds=3600)  # type: ignore[name-defined]
            ctx["rows_inserted"] = 1
            status_map[name] = result or "SUCCESS"
            logger.info(
                f"Completed notebook {path}",
                module="orchestration",
                details={"result": status_map[name]},
            )
except NameError:
    logger.warning(
        "dbutils unavailable — execute notebooks individually or run: python scripts/run_local_pipeline.py",
        module="orchestration",
    )
    status_map["error"] = "dbutils_unavailable"
except Exception as exc:
    logger.error("Orchestration failed", module="orchestration", exc=exc)
    logger.flush()
    raise

# COMMAND ----------

logger.flush()
logger.info("End-to-end orchestration completed", module="orchestration", details=status_map)
dbutils.notebook.exit(str(status_map))  # type: ignore[name-defined]

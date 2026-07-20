# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Transformations — Cleanse, Deduplicate, MERGE
# MAGIC
# MAGIC Builds silver Delta tables for all e-commerce entities from bronze via
# MAGIC `build_all_silver_tables` (cleanse + MERGE on primary keys).

# COMMAND ----------

import sys
from pathlib import Path

def _seed_project_root() -> str:
    import os
    def _is_root(p: Path) -> bool:
        return (p / "config" / "config.py").exists()
    candidates = []
    env = os.getenv("ECOMMERCE_LAKEHOUSE_ROOT")
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
        "Databricks_pipeline root not found. Set ECOMMERCE_LAKEHOUSE_ROOT."
    )

_PROJECT_ROOT = _seed_project_root()

from src.utilities.bootstrap import bootstrap_notebook
_PROJECT_ROOT = str(bootstrap_notebook(dbutils=globals().get("dbutils"), reload_modules=True))


# COMMAND ----------

from pyspark.sql import SparkSession

from config.config import get_config
from config.constants import ALL_ENTITIES, PIPELINE_SILVER_TRANSFORM
from src.audit.auditor import PipelineAuditor
from src.logging.logger import ensure_log_table, get_logger
from src.transformations.silver_transforms import build_all_silver_tables
from src.utilities.dataframe_utils import generate_run_id
from src.utilities.databricks_runtime import prepare_databricks_runtime
from src.utilities.delta_helpers import table_exists
from src.utilities.table_registry import register_silver_tables

# COMMAND ----------

spark = globals().get("spark") or SparkSession.getActiveSession()
cfg = get_config()
cfg = prepare_databricks_runtime(spark, cfg)
run_id = generate_run_id()
logger = get_logger(spark, PIPELINE_SILVER_TRANSFORM, run_id)
ensure_log_table(spark)
auditor = PipelineAuditor(spark, PIPELINE_SILVER_TRANSFORM, run_id, cfg.environment, logger)

logger.info("Silver pipeline started", module="silver", details=cfg.to_dict())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Build all silver tables

# COMMAND ----------

status_map = {}

try:
    with auditor.track("silver_build_all") as ctx:
        build_status = build_all_silver_tables(
            spark, logger=logger, auditor=auditor, run_id=run_id
        )
        status_map["build"] = build_status
        ctx["rows_inserted"] = sum(1 for v in build_status.values() if v == "SUCCESS")

    for entity in ALL_ENTITIES:
        path = cfg.paths.silver_path(entity)
        if not table_exists(spark, path):
            status_map[entity] = 0
            continue
        sdf = spark.read.format("delta").load(path)
        cnt = sdf.count()
        status_map[entity] = cnt
        logger.info(f"Silver {entity}", module="silver", details={"rows": cnt, "path": path})
        try:
            display(sdf.limit(5))  # noqa: F821
        except Exception as display_exc:
            logger.warning(f"display skipped for {entity}: {display_exc}", module="silver")
except Exception as exc:
    logger.error("Silver pipeline failed", module="silver", exc=exc)
    logger.flush()
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Register Silver Unity Catalog tables

# COMMAND ----------

if cfg.unity_catalog.register_tables:
    try:
        reg = register_silver_tables(spark, cfg, logger)
        status_map["registered_tables"] = reg
        logger.info("Silver tables registered", module="silver", details=reg)
        catalog = cfg.unity_catalog.catalog or spark.sql("SELECT current_catalog()").collect()[0][0]
        try:
            display(spark.sql(f"SHOW TABLES IN `{catalog}`.`{cfg.unity_catalog.silver_schema}`"))  # noqa: F821
        except Exception:
            display(spark.createDataFrame([(k, v) for k, v in reg.items()], ["table", "status"]))  # noqa: F821
    except Exception as exc:
        logger.warning(f"Silver table registration skipped: {exc}", module="silver")

# COMMAND ----------

logger.flush()
logger.info("Silver pipeline completed", module="silver", details=status_map)
dbutils.notebook.exit(str(status_map))  # type: ignore[name-defined]

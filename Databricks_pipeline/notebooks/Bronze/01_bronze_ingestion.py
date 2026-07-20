# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion — E-Commerce Lakehouse
# MAGIC
# MAGIC Incremental raw ingestion using **Databricks Auto Loader** (`cloudFiles`) with
# MAGIC automatic batch JSON fallback (Free Edition / Serverless friendly).
# MAGIC
# MAGIC **Layer contract:** store data exactly as received + lineage metadata. No business cleansing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup — path bootstrap

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
from config.constants import ALL_ENTITIES, PIPELINE_BRONZE_INGESTION
from src.audit.auditor import PipelineAuditor
from src.ingestion.autoloader import AutoLoaderIngestion
from src.logging.logger import ensure_log_table, get_logger
from src.utilities.dataframe_utils import generate_run_id
from src.utilities.databricks_runtime import prepare_databricks_runtime
from src.utilities.delta_helpers import table_exists
from src.utilities.table_registry import register_bronze_tables

# COMMAND ----------

spark = globals().get("spark") or SparkSession.getActiveSession()
cfg = get_config()
cfg = prepare_databricks_runtime(spark, cfg)
run_id = generate_run_id()
logger = get_logger(spark, PIPELINE_BRONZE_INGESTION, run_id)
ensure_log_table(spark)
auditor = PipelineAuditor(spark, PIPELINE_BRONZE_INGESTION, run_id, cfg.environment, logger)

logger.info(
    "Bronze pipeline started",
    module="bronze",
    details={
        **cfg.to_dict(),
        "catalog": cfg.unity_catalog.catalog,
        "volume_path": cfg.paths.storage_base,
    },
)
print(f"catalog={cfg.unity_catalog.catalog!r}  volume_path={cfg.paths.storage_base}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ingest all entities (Auto Loader / batch fallback)

# COMMAND ----------

status_map = {}

try:
    with auditor.track("bronze_ingest_all") as ctx:
        ingestion = AutoLoaderIngestion(spark, cfg, logger, run_id)
        ingest_status = ingestion.ingest_all(fmt="json")
        status_map["ingest"] = ingest_status
        ctx["rows_inserted"] = sum(1 for v in ingest_status.values() if v == "SUCCESS")

    for entity in ALL_ENTITIES:
        path = cfg.paths.bronze_path(entity)
        if not table_exists(spark, path):
            status_map[entity] = 0
            continue
        bronze_df = spark.read.format("delta").load(path)
        cnt = bronze_df.count()
        status_map[entity] = cnt
        logger.info(
            f"Bronze loaded {entity}",
            module="bronze",
            details={"rows": cnt, "path": path},
        )
        try:
            display(bronze_df.limit(5))  # noqa: F821
        except Exception as display_exc:
            logger.warning(f"display skipped for {entity}: {display_exc}", module="bronze")
except Exception as exc:
    logger.error("Bronze ingestion failed", module="bronze", exc=exc)
    logger.flush()
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Register Bronze Unity Catalog tables

# COMMAND ----------

if cfg.unity_catalog.register_tables:
    try:
        reg = register_bronze_tables(spark, cfg, logger)
        status_map["registered_tables"] = reg
        logger.info("Bronze tables registered", module="bronze", details=reg)
        catalog = cfg.unity_catalog.catalog or spark.sql("SELECT current_catalog()").collect()[0][0]
        try:
            display(spark.sql(f"SHOW TABLES IN `{catalog}`.`{cfg.unity_catalog.bronze_schema}`"))  # noqa: F821
        except Exception:
            display(spark.createDataFrame([(k, v) for k, v in reg.items()], ["table", "status"]))  # noqa: F821
    except Exception as exc:
        logger.warning(f"Bronze table registration skipped: {exc}", module="bronze")

# COMMAND ----------

logger.flush()
logger.info("Bronze pipeline completed", module="bronze", details=status_map)
dbutils.notebook.exit(str(status_map))  # type: ignore[name-defined]

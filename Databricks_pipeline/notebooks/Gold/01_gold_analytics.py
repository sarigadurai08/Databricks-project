# Databricks notebook source
# MAGIC %md
# MAGIC # Gold E-Commerce Analytics Marts
# MAGIC
# MAGIC Builds analytical Delta tables for commerce KPIs via `build_all_gold_tables`.
# MAGIC Displays sample marts: top_products, revenue_by_region, cart_abandonment.

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
from config.constants import PIPELINE_GOLD_ANALYTICS
from src.audit.auditor import PipelineAuditor
from src.logging.logger import ensure_log_table, get_logger
from src.transformations.gold_transforms import build_all_gold_tables
from src.utilities.dataframe_utils import generate_run_id
from src.utilities.databricks_runtime import prepare_databricks_runtime
from src.utilities.delta_helpers import table_exists
from src.utilities.table_registry import GOLD_TABLES, register_gold_tables

# COMMAND ----------

spark = globals().get("spark") or SparkSession.getActiveSession()
cfg = get_config()
cfg = prepare_databricks_runtime(spark, cfg)

try:
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
except Exception:
    pass

run_id = generate_run_id()
logger = get_logger(spark, PIPELINE_GOLD_ANALYTICS, run_id)
ensure_log_table(spark)
auditor = PipelineAuditor(spark, PIPELINE_GOLD_ANALYTICS, run_id, cfg.environment, logger)

logger.info("Gold pipeline started", module="gold", details=cfg.to_dict())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Build all gold marts

# COMMAND ----------

status_map = {}

try:
    with auditor.track("gold_all_marts") as ctx:
        build_status = build_all_gold_tables(
            spark, logger=logger, auditor=auditor, run_id=run_id
        )
        status_map["build"] = build_status
        ctx["rows_inserted"] = sum(1 for v in build_status.values() if v == "SUCCESS")

    for name in GOLD_TABLES:
        path = cfg.paths.gold_path(name)
        if not table_exists(spark, path):
            continue
        gdf = spark.read.format("delta").load(path)
        status_map[name] = gdf.count()
except Exception as exc:
    logger.error("Gold mart build failed", module="gold", exc=exc)
    logger.flush()
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Sample marts

# COMMAND ----------

for sample in ("top_products", "revenue_by_region", "cart_abandonment"):
    try:
        path = cfg.paths.gold_path(sample)
        if table_exists(spark, path):
            display(spark.read.format("delta").load(path).limit(20))  # noqa: F821
        else:
            logger.warning(f"Gold sample missing: {sample}", module="gold")
    except Exception as exc:
        logger.warning(f"Could not display {sample}: {exc}", module="gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Register Gold Unity Catalog tables

# COMMAND ----------

if cfg.unity_catalog.register_tables:
    try:
        gold_names = [k for k in status_map.keys() if k in GOLD_TABLES] or list(GOLD_TABLES)
        reg = register_gold_tables(spark, cfg, table_names=gold_names, logger=logger)
        status_map["registered_tables"] = reg
        logger.info("Gold tables registered", module="gold", details=reg)
        catalog = cfg.unity_catalog.catalog or spark.sql("SELECT current_catalog()").collect()[0][0]
        try:
            display(spark.sql(f"SHOW TABLES IN `{catalog}`.`{cfg.unity_catalog.gold_schema}`"))  # noqa: F821
        except Exception:
            display(spark.createDataFrame([(k, v) for k, v in reg.items()], ["table", "status"]))  # noqa: F821
    except Exception as exc:
        logger.warning(f"Gold table registration skipped: {exc}", module="gold")

# COMMAND ----------

logger.flush()
logger.info("Gold pipeline completed", module="gold", details=status_map)
dbutils.notebook.exit(str(status_map))  # type: ignore[name-defined]

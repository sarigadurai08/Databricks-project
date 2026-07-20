# Databricks notebook source
# MAGIC %md
# MAGIC # End-to-End Orchestration — E-Commerce Lakehouse
# MAGIC
# MAGIC Runs Generate → Bronze → Silver → Gold → Streaming → Data Quality → Monitoring sequentially.
# MAGIC Suitable as a Databricks Job multi-task workflow entrypoint or single notebook driver.
# MAGIC
# MAGIC **Runtime standard:** portable seed + bootstrap; no hardcoded catalog / volume / username paths.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run order
# MAGIC 1. Generate streaming events
# MAGIC 2. Bronze ingestion
# MAGIC 3. Silver transformations
# MAGIC 4. Gold analytics
# MAGIC 5. Structured streaming
# MAGIC 6. Data quality
# MAGIC 7. Monitoring / maintenance

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
from src.audit.auditor import PipelineAuditor
from src.logging.logger import ensure_log_table, get_logger
from src.utilities.dataframe_utils import generate_run_id
from src.utilities.databricks_runtime import prepare_databricks_runtime
from src.utilities.bootstrap import resolve_notebook_path

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
    ("./00_generate_streaming_events", "generate_events"),
    ("./Bronze/01_bronze_ingestion", "bronze"),
    ("./Silver/01_silver_transformations", "silver"),
    ("./Gold/01_gold_analytics", "gold"),
    ("./Streaming/01_structured_streaming", "streaming"),
    ("./DataQuality/01_data_quality_framework", "data_quality"),
    ("./Monitoring/01_monitoring_maintenance", "monitoring"),
]

status_map = {}
_dbutils = globals().get("dbutils")

try:
    for rel_path, name in NOTEBOOKS:
        path = resolve_notebook_path(rel_path, dbutils=_dbutils)
        with auditor.track(f"orchestrate_{name}") as ctx:
            logger.info(f"Starting notebook {path}", module="orchestration")
            try:
                result = _dbutils.notebook.run(path, timeout_seconds=3600)  # type: ignore[union-attr]
            except Exception as nb_exc:
                logger.error(
                    f"Notebook failed: {path}",
                    module="orchestration",
                    exc=nb_exc,
                    details={"step": name},
                )
                raise
            ctx["rows_inserted"] = 1
            status_map[name] = result or "SUCCESS"
            logger.info(
                f"Completed notebook {path}",
                module="orchestration",
                details={"result": status_map[name]},
            )
except AttributeError:
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

# Databricks notebook source
# MAGIC %md
# MAGIC # Monitoring, Maintenance & Observability — E-Commerce Lakehouse
# MAGIC
# MAGIC - Pipeline audit metrics & operational logs
# MAGIC - Delta OPTIMIZE / VACUUM (gated by config flags; skip gracefully)
# MAGIC - Time travel & history demo
# MAGIC - Liquid clustering attempt when enabled

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
from pyspark.sql import functions as F

from config.config import get_config
from config.constants import ALL_ENTITIES, ENTITY_ORDERS, PIPELINE_MAINTENANCE
from src.audit.auditor import PipelineAuditor
from src.logging.logger import ensure_log_table, get_logger
from src.utilities.dataframe_utils import generate_run_id
from src.utilities.databricks_runtime import prepare_databricks_runtime
from src.utilities.delta_helpers import (
    enable_liquid_clustering,
    history,
    maintain_entity,
    table_exists,
    time_travel,
)
from src.utilities.table_registry import register_ops_tables

# COMMAND ----------

spark = globals().get("spark") or SparkSession.getActiveSession()
cfg = get_config()
cfg = prepare_databricks_runtime(spark, cfg)
run_id = generate_run_id()
logger = get_logger(spark, PIPELINE_MAINTENANCE, run_id)
ensure_log_table(spark)
auditor = PipelineAuditor(spark, PIPELINE_MAINTENANCE, run_id, cfg.environment, logger)

logger.info("Monitoring pipeline started", module="monitoring", details=cfg.to_dict())
status_map = {}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Audit dashboard

# COMMAND ----------

try:
    with auditor.track("monitoring_audit_dashboard") as ctx:
        audit = spark.read.format("delta").load(cfg.paths.audit_path())
        audit_summary = (
            audit.groupBy("PipelineName", "Status")
            .agg(
                F.count("*").alias("Runs"),
                F.round(F.avg("ExecutionTimeSeconds"), 2).alias("AvgSeconds"),
                F.sum("RowsRead").alias("TotalRowsRead"),
                F.sum("RowsInserted").alias("TotalRowsInserted"),
                F.max("EndTime").alias("LastRun"),
            )
            .orderBy("PipelineName")
        )
        display(audit_summary)  # noqa: F821
        display(audit.orderBy(F.desc("StartTime")).limit(50))  # noqa: F821
        ctx["rows_read"] = audit.count()
        status_map["audit_rows"] = ctx["rows_read"]
except Exception as exc:
    logger.warning(f"Audit table not available yet: {exc}", module="monitoring")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pipeline logs

# COMMAND ----------

try:
    with auditor.track("monitoring_logs") as ctx:
        logs = spark.read.format("delta").load(cfg.paths.log_path())
        display(  # noqa: F821
            logs.filter(F.col("LogLevel").isin("ERROR", "WARNING", "CRITICAL"))
            .orderBy(F.desc("LoggedAt"))
            .limit(100)
        )
        display(  # noqa: F821
            logs.groupBy("PipelineName", "LogLevel")
            .count()
            .orderBy("PipelineName", "LogLevel")
        )
        ctx["rows_read"] = logs.count()
        status_map["log_rows"] = ctx["rows_read"]
except Exception as exc:
    logger.warning(f"Log table not available yet: {exc}", module="monitoring")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Delta maintenance — OPTIMIZE / VACUUM (config-gated)

# COMMAND ----------

if cfg.delta_maintenance.optimize_enabled or cfg.delta_maintenance.vacuum_enabled:
    with auditor.track("monitoring_delta_maintenance") as ctx:
        maintained = 0
        for entity in ALL_ENTITIES:
            path = cfg.paths.silver_path(entity)
            try:
                maintain_entity(
                    spark,
                    entity,
                    path,
                    vacuum_hours=cfg.delta_maintenance.vacuum_retention_hours,
                    vacuum_enabled=cfg.delta_maintenance.vacuum_enabled,
                    optimize_enabled=cfg.delta_maintenance.optimize_enabled,
                    zorder_enabled=cfg.delta_maintenance.zorder_enabled,
                    logger=logger,
                )
                maintained += 1
            except Exception as exc:
                logger.warning(
                    f"Maintenance skipped for {entity}: {exc}",
                    module="monitoring",
                )
        ctx["rows_updated"] = maintained
        status_map["entities_maintained"] = maintained
else:
    logger.info(
        "Delta OPTIMIZE/VACUUM disabled (Free Edition / profile flags)",
        module="monitoring",
    )
    status_map["entities_maintained"] = 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Time travel & table history

# COMMAND ----------

demo_path = cfg.paths.silver_path(ENTITY_ORDERS)
try:
    with auditor.track("monitoring_time_travel") as ctx:
        if not table_exists(spark, demo_path):
            raise FileNotFoundError(f"Silver orders missing: {demo_path}")
        hist = history(spark, demo_path, limit=20)
        display(hist)  # noqa: F821

        versions = [r["version"] for r in hist.select("version").collect()]
        if len(versions) > 1:
            prev = time_travel(spark, demo_path, version=int(versions[1]))
            logger.info(
                "Time travel read succeeded",
                module="monitoring",
                details={"version": versions[1], "rows": prev.count()},
            )
            display(prev.limit(5))  # noqa: F821
            ctx["rows_read"] = prev.count()
        status_map["history_versions"] = len(versions)
except Exception as exc:
    logger.warning(f"Time travel unavailable: {exc}", module="monitoring")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Liquid Clustering (when enabled)

# COMMAND ----------

if cfg.delta_maintenance.liquid_clustering_enabled:
    try:
        from src.utilities.table_registry import ensure_schema, register_external_delta_table, resolve_catalog

        gold_rev = cfg.paths.gold_path("revenue_by_region")
        if not table_exists(spark, gold_rev):
            raise FileNotFoundError(f"Gold mart missing for clustering: {gold_rev}")
        catalog = resolve_catalog(spark, cfg.unity_catalog.catalog or None)
        ensure_schema(spark, catalog, cfg.unity_catalog.gold_schema)
        fqn = f"`{catalog}`.`{cfg.unity_catalog.gold_schema}`.`revenue_by_region`"
        register_external_delta_table(spark, fqn, gold_rev, logger=logger)
        enable_liquid_clustering(
            spark,
            f"{catalog}.{cfg.unity_catalog.gold_schema}.revenue_by_region",
            ["region"],
        )
        logger.info("Liquid clustering requested for gold.revenue_by_region", module="monitoring")
        status_map["liquid_clustering"] = "requested"
    except Exception as exc:
        logger.warning(f"Liquid clustering not applied: {exc}", module="monitoring")
        status_map["liquid_clustering"] = f"skipped: {exc}"
else:
    status_map["liquid_clustering"] = "disabled"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Register ops tables

# COMMAND ----------

if cfg.unity_catalog.register_tables:
    try:
        reg = register_ops_tables(spark, cfg, logger)
        status_map["registered_ops_tables"] = reg
        logger.info("Ops tables registered", module="monitoring", details=reg)
    except Exception as exc:
        logger.warning(f"Ops table registration skipped: {exc}", module="monitoring")

# COMMAND ----------

logger.flush()
logger.info("Monitoring notebook completed", module="monitoring", details=status_map)
dbutils.notebook.exit(str(status_map))  # type: ignore[name-defined]

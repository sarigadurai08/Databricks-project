# Databricks notebook source
# MAGIC %md
# MAGIC # Monitoring, Maintenance & Observability
# MAGIC
# MAGIC - Pipeline audit metrics
# MAGIC - Operational logs
# MAGIC - Delta OPTIMIZE / VACUUM / ZORDER
# MAGIC - Time travel & history
# MAGIC - Liquid clustering (when available)
# MAGIC
# MAGIC **Runtime standard:** same bootstrap / Spark / Volume / audit pattern as `new_bronze.py`.

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
from pyspark.sql import functions as F

from config.config import get_config
from config.constants import ALL_ENTITIES, PIPELINE_MAINTENANCE
from config.paths import PATHS
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
# MAGIC ## 1. Audit dashboard query

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
# MAGIC ## 3. Delta maintenance — OPTIMIZE + VACUUM + ZORDER

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
        "Delta OPTIMIZE/VACUUM disabled for Free Edition profile",
        module="monitoring",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Time travel & table history

# COMMAND ----------

patients_path = cfg.paths.silver_path("patients")
try:
    with auditor.track("monitoring_time_travel") as ctx:
        if not table_exists(spark, patients_path):
            raise FileNotFoundError(f"Silver patients missing: {patients_path}")
        hist = history(spark, patients_path, limit=20)
        display(hist)  # noqa: F821

        versions = [r["version"] for r in hist.select("version").collect()]
        if len(versions) > 1:
            prev = time_travel(spark, patients_path, version=int(versions[1]))
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
# MAGIC ## 5. Liquid Clustering (Databricks Runtime 13.3+)

# COMMAND ----------

if cfg.delta_maintenance.liquid_clustering_enabled:
    try:
        gold_rev = cfg.paths.gold_path("revenue_analytics")
        spark.sql(f"CREATE TABLE IF NOT EXISTS gold_revenue_analytics USING DELTA LOCATION '{gold_rev}'")
        enable_liquid_clustering(
            spark,
            "gold_revenue_analytics",
            ["PaymentDate", "Hospital", "Department"],
        )
        logger.info("Liquid clustering requested for gold_revenue_analytics", module="monitoring")
        status_map["liquid_clustering"] = "requested"
    except Exception as exc:
        logger.warning(f"Liquid clustering not applied: {exc}", module="monitoring")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. DQ results trend

# COMMAND ----------

try:
    dq = spark.read.format("delta").load(cfg.paths.dq_results_path())
    display(  # noqa: F821
        dq.groupBy("Entity", "Status")
        .agg(F.count("*").alias("RuleExecutions"), F.sum("FailedCount").alias("FailedRows"))
        .orderBy("Entity")
    )
    status_map["dq_result_rows"] = dq.count()
except Exception as exc:
    logger.warning(f"DQ results not available: {exc}", module="monitoring")

# COMMAND ----------

logger.flush()
logger.info("Monitoring notebook completed", module="monitoring", details=status_map)
dbutils.notebook.exit(str(status_map))  # type: ignore[name-defined]

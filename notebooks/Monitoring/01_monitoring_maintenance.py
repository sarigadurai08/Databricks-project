# Databricks notebook source
# MAGIC %md
# MAGIC # Monitoring, Maintenance & Observability
# MAGIC
# MAGIC - Pipeline audit metrics
# MAGIC - Operational logs
# MAGIC - Delta OPTIMIZE / VACUUM / ZORDER
# MAGIC - Time travel & history
# MAGIC - Liquid clustering (when available)

# COMMAND ----------

import sys
from pathlib import Path

def _bootstrap_project_root() -> None:
    for cand in [Path.cwd(), Path.cwd().parent, Path("/Workspace/Repos/Healthcare_Lakehouse")]:
        if (cand / "config" / "config.py").exists():
            if str(cand) not in sys.path:
                sys.path.insert(0, str(cand))
            return

_bootstrap_project_root()

# COMMAND ----------

from pyspark.sql import functions as F

from config.config import get_config
from config.constants import ALL_ENTITIES, PIPELINE_MAINTENANCE
from src.logging.logger import ensure_log_table, get_logger
from src.utilities.dataframe_utils import generate_run_id
from src.utilities.delta_helpers import (
    enable_liquid_clustering,
    history,
    maintain_entity,
    time_travel,
)
from src.utilities.spark_session import get_spark

# COMMAND ----------

spark = get_spark("MonitoringMaintenance")
cfg = get_config()
run_id = generate_run_id()
logger = get_logger(spark, PIPELINE_MAINTENANCE, run_id)
ensure_log_table(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Audit dashboard query

# COMMAND ----------

try:
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
except Exception as exc:
    logger.warning(f"Audit table not available yet: {exc}", module="monitoring")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pipeline logs

# COMMAND ----------

try:
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
except Exception as exc:
    logger.warning(f"Log table not available yet: {exc}", module="monitoring")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Delta maintenance — OPTIMIZE + VACUUM + ZORDER

# COMMAND ----------

if cfg.delta_maintenance.optimize_enabled:
    for entity in ALL_ENTITIES:
        path = cfg.paths.silver_path(entity)
        try:
            maintain_entity(
                spark,
                entity,
                path,
                vacuum_hours=cfg.delta_maintenance.vacuum_retention_hours,
                logger=logger,
            )
        except Exception as exc:
            logger.warning(
                f"Maintenance skipped for {entity}: {exc}",
                module="monitoring",
            )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Time travel & table history

# COMMAND ----------

patients_path = cfg.paths.silver_path("patients")
try:
    hist = history(spark, patients_path, limit=20)
    display(hist)  # noqa: F821

    # Read previous version if available
    versions = [r["version"] for r in hist.select("version").collect()]
    if len(versions) > 1:
        prev = time_travel(spark, patients_path, version=int(versions[1]))
        logger.info(
            "Time travel read succeeded",
            module="monitoring",
            details={"version": versions[1], "rows": prev.count()},
        )
        display(prev.limit(5))  # noqa: F821
except Exception as exc:
    logger.warning(f"Time travel unavailable: {exc}", module="monitoring")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Liquid Clustering (Databricks Runtime 13.3+)

# COMMAND ----------

if cfg.delta_maintenance.liquid_clustering_enabled:
    # Register temp views / managed tables when Unity Catalog is enabled
    try:
        gold_rev = cfg.paths.gold_path("revenue_analytics")
        spark.sql(f"CREATE TABLE IF NOT EXISTS gold_revenue_analytics USING DELTA LOCATION '{gold_rev}'")
        enable_liquid_clustering(
            spark,
            "gold_revenue_analytics",
            ["PaymentDate", "Hospital", "Department"],
        )
        logger.info("Liquid clustering requested for gold_revenue_analytics", module="monitoring")
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
except Exception as exc:
    logger.warning(f"DQ results not available: {exc}", module="monitoring")

logger.flush()
logger.info("Monitoring notebook completed", module="monitoring")

# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Clinical Analytics Marts
# MAGIC
# MAGIC Builds analytical Delta tables for hospital network KPIs:
# MAGIC Patient Summary, Doctor Performance, Revenue, Insurance, Appointments,
# MAGIC Laboratory Trends, Pharmacy Sales, Top Diseases, and more.

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
from config.constants import PIPELINE_GOLD_ANALYTICS
from src.audit.auditor import PipelineAuditor
from src.logging.logger import ensure_log_table, get_logger
from src.transformations.gold_transforms import build_all_gold_tables
from src.utilities.dataframe_utils import generate_run_id
from src.utilities.delta_helpers import (
    maintain_entity,
    optimize_table,
    write_delta,
)
from src.utilities.spark_session import get_spark

# COMMAND ----------

spark = get_spark("GoldAnalytics")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

cfg = get_config()
run_id = generate_run_id()
logger = get_logger(spark, PIPELINE_GOLD_ANALYTICS, run_id)
ensure_log_table(spark)
auditor = PipelineAuditor(spark, PIPELINE_GOLD_ANALYTICS, run_id, cfg.environment, logger)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Silver sources

# COMMAND ----------

patients = (
    spark.read.format("delta")
    .load(cfg.paths.silver_path("patients"))
    .filter(F.col("IsCurrent") == True)  # noqa: E712
)
doctors = spark.read.format("delta").load(cfg.paths.silver_path("doctors"))
appointments = spark.read.format("delta").load(cfg.paths.silver_path("appointments"))
claims = spark.read.format("delta").load(cfg.paths.silver_path("insurance_claims"))
pharmacy = spark.read.format("delta").load(cfg.paths.silver_path("pharmacy_orders"))
labs = spark.read.format("delta").load(cfg.paths.silver_path("laboratory_results"))
billing = spark.read.format("delta").load(cfg.paths.silver_path("billing"))

logger.info("Silver sources loaded for gold build", module="gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build all gold marts

# COMMAND ----------

with auditor.track("gold_all_marts") as ctx:
    gold_tables = build_all_gold_tables(
        spark, patients, doctors, appointments, claims, pharmacy, labs, billing
    )
    total_rows = 0
    for name, df in gold_tables.items():
        path = cfg.paths.gold_path(name)
        write_delta(df, path, mode="overwrite", merge_schema=True)
        cnt = df.count()
        total_rows += cnt
        logger.info(f"Wrote gold.{name}", module="gold", details={"rows": cnt, "path": path})
        display(df.limit(5))  # noqa: F821
    ctx["rows_read"] = appointments.count()
    ctx["rows_inserted"] = total_rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Performance — OPTIMIZE + ZORDER on key gold tables

# COMMAND ----------

if cfg.delta_maintenance.optimize_enabled:
    optimize_table(
        spark,
        cfg.paths.gold_path("revenue_analytics"),
        zorder_columns=["PaymentDate", "Hospital", "Department"],
        logger=logger,
    )
    optimize_table(
        spark,
        cfg.paths.gold_path("patient_summary"),
        zorder_columns=["PatientID", "InsuranceID"],
        logger=logger,
    )
    optimize_table(
        spark,
        cfg.paths.gold_path("doctor_performance"),
        zorder_columns=["DoctorID", "Department"],
        logger=logger,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. KPI snapshot

# COMMAND ----------

monthly = spark.read.format("delta").load(cfg.paths.gold_path("monthly_revenue"))
hospital = spark.read.format("delta").load(cfg.paths.gold_path("hospital_revenue"))
top_dx = spark.read.format("delta").load(cfg.paths.gold_path("top_diseases"))

display(monthly)  # noqa: F821
display(hospital)  # noqa: F821
display(top_dx.limit(10))  # noqa: F821

logger.flush()
logger.info("Gold pipeline completed", module="gold")

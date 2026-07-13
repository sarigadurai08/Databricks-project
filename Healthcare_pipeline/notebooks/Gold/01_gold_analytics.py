# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Clinical Analytics Marts
# MAGIC
# MAGIC Builds analytical Delta tables for hospital network KPIs:
# MAGIC Patient Summary, Doctor Performance, Revenue, Insurance, Appointments,
# MAGIC Laboratory Trends, Pharmacy Sales, Top Diseases, and more.
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
from config.constants import PIPELINE_GOLD_ANALYTICS
from config.paths import PATHS
from src.audit.auditor import PipelineAuditor
from src.logging.logger import ensure_log_table, get_logger
from src.transformations.gold_transforms import build_all_gold_tables
from src.utilities.dataframe_utils import generate_run_id
from src.utilities.databricks_runtime import prepare_databricks_runtime
from src.utilities.delta_helpers import (
    maintain_entity,
    optimize_table,
    table_exists,
    write_delta,
)

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
# MAGIC ## 1. Load Silver sources

# COMMAND ----------

try:
    required = {
        "patients": cfg.paths.silver_path("patients"),
        "doctors": cfg.paths.silver_path("doctors"),
        "appointments": cfg.paths.silver_path("appointments"),
        "insurance_claims": cfg.paths.silver_path("insurance_claims"),
        "pharmacy_orders": cfg.paths.silver_path("pharmacy_orders"),
        "laboratory_results": cfg.paths.silver_path("laboratory_results"),
        "billing": cfg.paths.silver_path("billing"),
    }
    missing = [name for name, path in required.items() if not table_exists(spark, path)]
    if missing:
        raise FileNotFoundError(
            f"Missing Silver tables {missing}. Run Bronze then Silver before Gold."
        )

    patients = (
        spark.read.format("delta")
        .load(required["patients"])
        .filter(F.col("IsCurrent") == True)  # noqa: E712
    )
    doctors = spark.read.format("delta").load(required["doctors"])
    appointments = spark.read.format("delta").load(required["appointments"])
    claims = spark.read.format("delta").load(required["insurance_claims"])
    pharmacy = spark.read.format("delta").load(required["pharmacy_orders"])
    labs = spark.read.format("delta").load(required["laboratory_results"])
    billing = spark.read.format("delta").load(required["billing"])

    logger.info("Silver sources loaded for gold build", module="gold")
except Exception as exc:
    logger.error("Failed loading Silver sources", module="gold", exc=exc)
    logger.flush()
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build all gold marts

# COMMAND ----------

status_map = {}

try:
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
            status_map[name] = cnt
            logger.info(f"Wrote gold.{name}", module="gold", details={"rows": cnt, "path": path})
            display(df.limit(5))  # noqa: F821
        ctx["rows_read"] = appointments.count()
        ctx["rows_inserted"] = total_rows
except Exception as exc:
    logger.error("Gold mart build failed", module="gold", exc=exc)
    logger.flush()
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Performance — OPTIMIZE + ZORDER on key gold tables

# COMMAND ----------

if cfg.delta_maintenance.optimize_enabled:
    try:
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
    except Exception as exc:
        logger.warning(f"Gold OPTIMIZE skipped: {exc}", module="gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. KPI snapshot

# COMMAND ----------

try:
    monthly = spark.read.format("delta").load(cfg.paths.gold_path("monthly_revenue"))
    hospital = spark.read.format("delta").load(cfg.paths.gold_path("hospital_revenue"))
    top_dx = spark.read.format("delta").load(cfg.paths.gold_path("top_diseases"))

    display(monthly)  # noqa: F821
    display(hospital)  # noqa: F821
    display(top_dx.limit(10))  # noqa: F821
except Exception as exc:
    logger.warning(f"KPI snapshot skipped: {exc}", module="gold")

# COMMAND ----------

logger.flush()
logger.info("Gold pipeline completed", module="gold", details=status_map)
dbutils.notebook.exit(str(status_map))  # type: ignore[name-defined]

# Databricks notebook source
# MAGIC %md
# MAGIC # SCD Type 1 & Type 2 — Focused Demo
# MAGIC
# MAGIC Demonstrates production MERGE patterns used by the Silver layer:
# MAGIC - **SCD1** overwrite-in-place for doctors
# MAGIC - **SCD2** historical versioning for patients
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
from config.constants import PIPELINE_SILVER_TRANSFORM
from config.paths import PATHS
from src.audit.auditor import PipelineAuditor
from src.logging.logger import ensure_log_table, get_logger
from src.transformations.scd import apply_scd_type1, apply_scd_type2
from src.utilities.dataframe_utils import generate_run_id
from src.utilities.databricks_runtime import prepare_databricks_runtime

# COMMAND ----------

spark = globals().get("spark") or SparkSession.getActiveSession()
cfg = get_config()
cfg = prepare_databricks_runtime(spark, cfg)
run_id = generate_run_id()
logger = get_logger(spark, "scd_demo", run_id)
ensure_log_table(spark)
auditor = PipelineAuditor(spark, "scd_demo", run_id, cfg.environment, logger)

logger.info("SCD demo started", module="scd_demo", details=cfg.to_dict())

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 1 — Doctors

# COMMAND ----------

status_map = {}

try:
    with auditor.track("scd1_doctors_demo") as ctx:
        doctors_v1 = spark.createDataFrame(
            [("DOC9999", "Dr. Demo", "Cardiology", "Cardiology", "Mercy General Hospital", 10)],
            ["DoctorID", "DoctorName", "Specialization", "Department", "Hospital", "Experience"],
        )
        path_d = cfg.paths.silver_path("doctors_scd1_demo")
        apply_scd_type1(spark, doctors_v1, path_d, "DoctorID", logger=logger)

        doctors_v2 = spark.createDataFrame(
            [("DOC9999", "Dr. Demo Updated", "Cardiology", "Cardiology", "Mercy General Hospital", 11)],
            doctors_v1.columns,
        )
        result = apply_scd_type1(
            spark,
            doctors_v2,
            path_d,
            "DoctorID",
            compare_columns=["DoctorName", "Experience"],
            logger=logger,
        )
        ctx["rows_read"] = 2
        ctx["rows_inserted"] = result.count()
        ctx["rows_updated"] = 1
        status_map["scd1_rows"] = ctx["rows_inserted"]
        display(spark.read.format("delta").load(path_d))  # noqa: F821
except Exception as exc:
    logger.error("SCD1 demo failed", module="scd_demo", exc=exc)
    logger.flush()
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 — Patients

# COMMAND ----------

try:
    with auditor.track("scd2_patients_demo") as ctx:
        cols = [
            "PatientID", "FirstName", "LastName", "Phone", "Email", "Address",
            "InsuranceID", "Gender", "DOB", "CreatedDate", "ModifiedDate",
        ]
        patients_v1 = spark.createDataFrame(
            [("PATSCD1", "Sam", "Patient", "111", "sam@ex.com", "100 Main St", "INS1", "Male", "1980-01-01", "2024-01-01 00:00:00", "2024-01-01 00:00:00")],
            cols,
        )
        path_p = cfg.paths.silver_path("patients_scd2_demo")
        apply_scd_type2(spark, patients_v1, path_p, "PatientID", ["Address", "Phone"], logger=logger)

        patients_v2 = spark.createDataFrame(
            [("PATSCD1", "Sam", "Patient", "222", "sam@ex.com", "200 Oak Ave", "INS1", "Male", "1980-01-01", "2024-01-01 00:00:00", "2024-06-01 00:00:00")],
            cols,
        )
        apply_scd_type2(spark, patients_v2, path_p, "PatientID", ["Address", "Phone"], logger=logger)

        history_df = spark.read.format("delta").load(path_p).orderBy("VersionNumber")
        display(history_df)  # noqa: F821
        assert history_df.count() == 2
        assert history_df.filter(F.col("IsCurrent") == True).count() == 1  # noqa: E712
        ctx["rows_read"] = 2
        ctx["rows_inserted"] = 2
        ctx["rows_updated"] = 1
        status_map["scd2_rows"] = history_df.count()
        logger.info("SCD demo assertions passed", module="scd_demo")
except Exception as exc:
    logger.error("SCD2 demo failed", module="scd_demo", exc=exc)
    logger.flush()
    raise

# COMMAND ----------

logger.flush()
logger.info("SCD demo completed", module="scd_demo", details=status_map)
dbutils.notebook.exit(str(status_map))  # type: ignore[name-defined]

# Databricks notebook source
# MAGIC %md
# MAGIC # SCD Type 1 & Type 2 — Focused Demo
# MAGIC
# MAGIC Demonstrates production MERGE patterns used by the Silver layer:
# MAGIC - **SCD1** overwrite-in-place for doctors
# MAGIC - **SCD2** historical versioning for patients
# MAGIC
# MAGIC Demo tables are hard-reset each run so assertions stay idempotent.

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
from src.audit.auditor import PipelineAuditor
from src.logging.logger import ensure_log_table, get_logger
from src.transformations.scd import apply_scd_type1, apply_scd_type2, filter_current
from src.utilities.dataframe_utils import generate_run_id
from src.utilities.databricks_runtime import prepare_databricks_runtime
from src.utilities.delta_helpers import ensure_delta_parent, table_exists, write_delta

# COMMAND ----------

spark = globals().get("spark") or SparkSession.getActiveSession()
cfg = get_config()
cfg = prepare_databricks_runtime(spark, cfg)
run_id = generate_run_id()
logger = get_logger(spark, "scd_demo", run_id)
ensure_log_table(spark)
auditor = PipelineAuditor(spark, "scd_demo", run_id, cfg.environment, logger)

logger.info("SCD demo started", module="scd_demo", details=cfg.to_dict())


def _reset_demo_path(path: str) -> None:
    """Hard-delete a demo Delta path so the next SCD call bootstraps cleanly."""
    ensure_delta_parent(spark, path)
    removed = False
    try:
        dbutils.fs.rm(path, True)  # type: ignore[name-defined]
        removed = True
    except Exception as exc:
        logger.warning(f"dbutils.fs.rm failed for {path}: {exc}", module="scd_demo")
    if not removed:
        try:
            from pathlib import Path as _P

            import shutil

            shutil.rmtree(str(path), ignore_errors=True)
        except Exception:
            pass
    # Final fallback: overwrite empty marker then rely on SCD bootstrap if path still odd
    if table_exists(spark, path):
        logger.warning(f"Path still present after delete, forcing overwrite bootstrap for {path}", module="scd_demo")

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
        _reset_demo_path(path_d)

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
        _reset_demo_path(path_p)

        # Version 1 — initial current row
        hist1 = apply_scd_type2(
            spark, patients_v1, path_p, "PatientID", ["Address", "Phone"], logger=logger
        )
        current_after_v1 = filter_current(hist1).count()
        assert current_after_v1 == 1, f"After v1 expected 1 current row, found {current_after_v1}"

        # Version 2 — address/phone change must open a new SCD2 version
        patients_v2 = spark.createDataFrame(
            [("PATSCD1", "Sam", "Patient", "222", "sam@ex.com", "200 Oak Ave", "INS1", "Male", "1980-01-01", "2024-01-01 00:00:00", "2024-06-01 00:00:00")],
            cols,
        )
        hist2 = apply_scd_type2(
            spark, patients_v2, path_p, "PatientID", ["Address", "Phone"], logger=logger
        )

        history_df = hist2.orderBy("VersionNumber")
        display(history_df)  # noqa: F821

        current_cnt = filter_current(history_df).count()
        total_cnt = history_df.count()
        assert current_cnt == 1, f"Expected 1 current SCD2 row, found {current_cnt}"
        assert total_cnt >= 2, f"Expected at least 2 SCD2 history rows after address change, found {total_cnt}"

        # Current row should reflect the new address/phone
        cur = filter_current(history_df).collect()[0]
        assert cur["Address"] == "200 Oak Ave", f"Current Address mismatch: {cur['Address']}"
        assert cur["Phone"] == "222", f"Current Phone mismatch: {cur['Phone']}"

        ctx["rows_read"] = 2
        ctx["rows_inserted"] = total_cnt
        ctx["rows_updated"] = 1
        status_map["scd2_rows"] = total_cnt
        status_map["scd2_current"] = current_cnt
        logger.info("SCD demo assertions passed", module="scd_demo", details=status_map)
except Exception as exc:
    logger.error("SCD2 demo failed", module="scd_demo", exc=exc)
    logger.flush()
    raise

# COMMAND ----------

logger.flush()
logger.info("SCD demo completed", module="scd_demo", details=status_map)
dbutils.notebook.exit(str(status_map))  # type: ignore[name-defined]

# Databricks notebook source
# MAGIC %md
# MAGIC # SCD Type 1 & Type 2 — Focused Demo
# MAGIC
# MAGIC Working implementation (synced from `updated02_scd.py` after Databricks debug).
# MAGIC - **SCD1** overwrite-in-place for doctors
# MAGIC - **SCD2** historical versioning for patients
# MAGIC
# MAGIC Demo tables are hard-reset each run so assertions stay idempotent.

# COMMAND ----------

import sys
from pathlib import Path

def _seed_project_root() -> str:
    import os
    def _is_root(p: Path) -> bool:
        return (p / "config" / "config.py").exists()
    candidates = []
    env = os.getenv("HEALTHCARE_LAKEHOUSE_ROOT")
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
        "Healthcare_pipeline root not found. Set HEALTHCARE_LAKEHOUSE_ROOT."
    )

_PROJECT_ROOT = _seed_project_root()

from src.utilities.bootstrap import bootstrap_notebook
_PROJECT_ROOT = str(bootstrap_notebook(dbutils=globals().get("dbutils"), reload_modules=True))


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.config import get_config
from src.audit.auditor import PipelineAuditor
from src.logging.logger import ensure_log_table, get_logger
from src.transformations.scd import apply_scd_type1, apply_scd_type2, filter_current
from src.utilities.dataframe_utils import generate_run_id
from src.utilities.databricks_runtime import prepare_databricks_runtime
from src.utilities.delta_helpers import soft_reset_delta_path

spark = globals().get("spark") or SparkSession.getActiveSession()
cfg = get_config()
cfg = prepare_databricks_runtime(spark, cfg)
run_id = generate_run_id()
logger = get_logger(spark, "scd_demo", run_id)
ensure_log_table(spark)
auditor = PipelineAuditor(spark, "scd_demo", run_id, cfg.environment, logger)

logger.info("SCD demo started", module="scd_demo", details=cfg.to_dict())


def _reset_demo_path(path: str, schema_df=None) -> None:
    """
    Idempotent demo reset without destructive filesystem deletes.

    Uses Delta DELETE / empty overwrite so it works under Serverless and
    workspaces that block dbutils.fs.rm / shutil.rmtree.
    """
    status = soft_reset_delta_path(spark, path, schema_df=schema_df)
    logger.info(f"Demo path reset ({status}): {path}", module="scd_demo")

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
        _reset_demo_path(path_d, schema_df=doctors_v1)

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
        _reset_demo_path(path_p, schema_df=patients_v1)

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

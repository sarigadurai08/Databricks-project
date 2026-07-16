# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Framework Execution
# MAGIC
# MAGIC Runs enterprise DQ rules across Silver entities and writes:
# MAGIC - Validation results → Delta
# MAGIC - Failed / quarantined records → Delta
# MAGIC
# MAGIC **Runtime standard:** same bootstrap / Spark / Volume / audit pattern as `new_bronze.py`.

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
from config.constants import (
    PIPELINE_DQ_FRAMEWORK,
    VALID_CLAIM_STATUSES,
    VALID_PAYMENT_STATUSES,
)
from config.paths import PATHS
from src.audit.auditor import PipelineAuditor
from src.logging.logger import ensure_log_table, get_logger
from src.transformations.scd import filter_current
from src.utilities.data_quality import DataQualityFramework, Severity
from src.utilities.dataframe_utils import generate_run_id
from src.utilities.databricks_runtime import prepare_databricks_runtime
from src.utilities.delta_helpers import table_exists

# COMMAND ----------

spark = globals().get("spark") or SparkSession.getActiveSession()
cfg = get_config()
cfg = prepare_databricks_runtime(spark, cfg)
run_id = generate_run_id()
logger = get_logger(spark, PIPELINE_DQ_FRAMEWORK, run_id)
ensure_log_table(spark)
auditor = PipelineAuditor(spark, PIPELINE_DQ_FRAMEWORK, run_id, cfg.environment, logger)

logger.info("Data quality pipeline started", module="data_quality", details=cfg.to_dict())

# COMMAND ----------

try:
    required = [
        "patients",
        "doctors",
        "appointments",
        "insurance_claims",
        "billing",
        "pharmacy_orders",
        "laboratory_results",
    ]
    missing = [e for e in required if not table_exists(spark, cfg.paths.silver_path(e))]
    if missing:
        raise FileNotFoundError(
            f"Missing Silver tables {missing}. Run Bronze then Silver before Data Quality."
        )

    patients = filter_current(spark.read.format("delta").load(cfg.paths.silver_path("patients")))
    doctors = spark.read.format("delta").load(cfg.paths.silver_path("doctors"))
    appointments = spark.read.format("delta").load(cfg.paths.silver_path("appointments"))
    claims = spark.read.format("delta").load(cfg.paths.silver_path("insurance_claims"))
    billing = spark.read.format("delta").load(cfg.paths.silver_path("billing"))
    pharmacy = spark.read.format("delta").load(cfg.paths.silver_path("pharmacy_orders"))
    labs = spark.read.format("delta").load(cfg.paths.silver_path("laboratory_results"))
except Exception as exc:
    logger.error("Failed loading Silver sources for DQ", module="data_quality", exc=exc)
    logger.flush()
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entity rule suites

# COMMAND ----------

all_results = []
status_map = {}

try:
    with auditor.track("dq_patients") as ctx:
        dq = (
            DataQualityFramework(spark, "patients", run_id, logger)
            .require_not_null(["PatientID", "FirstName", "LastName", "DOB", "Gender"])
            .require_unique(["PatientID"])
            .require_regex("Email", r"^[^@]+@[^@]+\.[^@]+$")
            .add_business_rule(
                "dob_not_future",
                "DOB must not be in the future",
                lambda df: F.col("DOB") > F.current_date(),
                Severity.CRITICAL,
                ["DOB"],
            )
        )
        results = dq.validate(patients)
        all_results.extend(results)
        ctx["rows_read"] = patients.count()
        status_map["patients"] = ctx["rows_read"]

    with auditor.track("dq_doctors") as ctx:
        dq = (
            DataQualityFramework(spark, "doctors", run_id, logger)
            .require_not_null(["DoctorID", "DoctorName", "Specialization", "Department"])
            .require_unique(["DoctorID"])
            .require_range("Experience", min_value=0, max_value=60)
        )
        results = dq.validate(doctors)
        all_results.extend(results)
        ctx["rows_read"] = doctors.count()
        status_map["doctors"] = ctx["rows_read"]

    with auditor.track("dq_appointments") as ctx:
        dq = (
            DataQualityFramework(spark, "appointments", run_id, logger)
            .require_not_null(["AppointmentID", "PatientID", "DoctorID", "AppointmentDate", "Status"])
            .require_unique(["AppointmentID"])
            .require_fk("PatientID", patients, "PatientID")
            .require_fk("DoctorID", doctors, "DoctorID")
            .add_business_rule(
                "completed_requires_diagnosis",
                "Completed appointments should have a diagnosis",
                lambda df: (F.col("Status") == "Completed")
                & (F.col("Diagnosis").isNull() | (F.trim(F.col("Diagnosis")) == "")),
                Severity.WARNING,
                ["AppointmentID", "Status", "Diagnosis"],
            )
        )
        results = dq.validate(appointments)
        all_results.extend(results)
        ctx["rows_read"] = appointments.count()
        status_map["appointments"] = ctx["rows_read"]

    with auditor.track("dq_claims") as ctx:
        dq = (
            DataQualityFramework(spark, "insurance_claims", run_id, logger)
            .require_not_null(["ClaimID", "PatientID", "ClaimAmount", "ApprovalStatus"])
            .require_unique(["ClaimID"])
            .require_values_in("ApprovalStatus", VALID_CLAIM_STATUSES)
            .require_range("ClaimAmount", min_value=0, max_value=1_000_000)
            .require_fk("PatientID", patients, "PatientID")
        )
        results = dq.validate(claims)
        all_results.extend(results)
        ctx["rows_read"] = claims.count()
        status_map["insurance_claims"] = ctx["rows_read"]

    with auditor.track("dq_billing") as ctx:
        dq = (
            DataQualityFramework(spark, "billing", run_id, logger)
            .require_not_null(["InvoiceID", "PatientID", "TotalAmount", "PaymentStatus"])
            .require_unique(["InvoiceID"])
            .require_values_in("PaymentStatus", VALID_PAYMENT_STATUSES)
            .require_range("TotalAmount", min_value=0)
            .require_fk("PatientID", patients, "PatientID")
            .require_fk("AppointmentID", appointments, "AppointmentID")
        )
        results = dq.validate(billing)
        all_results.extend(results)
        ctx["rows_read"] = billing.count()
        status_map["billing"] = ctx["rows_read"]

    with auditor.track("dq_pharmacy") as ctx:
        dq = (
            DataQualityFramework(spark, "pharmacy_orders", run_id, logger)
            .require_not_null(["PrescriptionID", "PatientID", "Medicine", "Quantity", "Price"])
            .require_unique(["PrescriptionID"])
            .require_range("Quantity", min_value=1)
            .require_range("Price", min_value=0)
            .require_fk("PatientID", patients, "PatientID")
        )
        results = dq.validate(pharmacy)
        all_results.extend(results)
        ctx["rows_read"] = pharmacy.count()
        status_map["pharmacy_orders"] = ctx["rows_read"]

    with auditor.track("dq_labs") as ctx:
        dq = (
            DataQualityFramework(spark, "laboratory_results", run_id, logger)
            .require_not_null(["LabID", "PatientID", "TestName", "Result"])
            .require_unique(["LabID"])
            .require_fk("PatientID", patients, "PatientID")
        )
        results = dq.validate(labs)
        all_results.extend(results)
        ctx["rows_read"] = labs.count()
        status_map["laboratory_results"] = ctx["rows_read"]
except Exception as exc:
    logger.error("DQ rule execution failed", module="data_quality", exc=exc)
    logger.flush()
    raise

# COMMAND ----------

summary = spark.createDataFrame([r.__dict__ for r in all_results])
try:
    display(summary.orderBy("severity", "status"))  # noqa: F821
except Exception as display_exc:
    logger.warning(f"Could not display DQ summary: {display_exc}", module="data_quality")

failed = summary.filter(F.col("status") == "FAILED")
logger.info(
    "DQ run complete",
    module="data_quality",
    details={"rules": summary.count(), "failed_rules": failed.count()},
)

try:
    display(spark.read.format("delta").load(cfg.paths.dq_failed_records_path()).limit(50))  # noqa: F821
except Exception as exc:
    logger.warning(f"DQ failed-records table not available yet: {exc}", module="data_quality")

# COMMAND ----------

logger.flush()
logger.info("Data quality pipeline completed", module="data_quality", details=status_map)
dbutils.notebook.exit(str(status_map))  # type: ignore[name-defined]

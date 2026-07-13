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
from config.constants import (
    PIPELINE_DQ_FRAMEWORK,
    VALID_CLAIM_STATUSES,
    VALID_PAYMENT_STATUSES,
)
from config.paths import PATHS
from src.audit.auditor import PipelineAuditor
from src.logging.logger import ensure_log_table, get_logger
from src.utilities.data_quality import DataQualityFramework, Severity
from src.utilities.dataframe_utils import generate_run_id
from src.utilities.databricks_runtime import prepare_databricks_runtime

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
    patients = spark.read.format("delta").load(cfg.paths.silver_path("patients")).filter(F.col("IsCurrent") == True)  # noqa: E712
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
display(summary.orderBy("severity", "status"))  # noqa: F821

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

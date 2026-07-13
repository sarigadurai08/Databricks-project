# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Framework Execution
# MAGIC
# MAGIC Runs enterprise DQ rules across Silver entities and writes:
# MAGIC - Validation results → Delta
# MAGIC - Failed / quarantined records → Delta

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
from config.constants import (
    PIPELINE_DQ_FRAMEWORK,
    VALID_CLAIM_STATUSES,
    VALID_PAYMENT_STATUSES,
)
from src.audit.auditor import PipelineAuditor
from src.logging.logger import ensure_log_table, get_logger
from src.utilities.data_quality import DataQualityFramework, Severity
from src.utilities.dataframe_utils import generate_run_id
from src.utilities.spark_session import get_spark

# COMMAND ----------

spark = get_spark("DataQuality")
cfg = get_config()
run_id = generate_run_id()
logger = get_logger(spark, PIPELINE_DQ_FRAMEWORK, run_id)
ensure_log_table(spark)
auditor = PipelineAuditor(spark, PIPELINE_DQ_FRAMEWORK, run_id, cfg.environment, logger)

# COMMAND ----------

patients = spark.read.format("delta").load(cfg.paths.silver_path("patients")).filter(F.col("IsCurrent") == True)  # noqa: E712
doctors = spark.read.format("delta").load(cfg.paths.silver_path("doctors"))
appointments = spark.read.format("delta").load(cfg.paths.silver_path("appointments"))
claims = spark.read.format("delta").load(cfg.paths.silver_path("insurance_claims"))
billing = spark.read.format("delta").load(cfg.paths.silver_path("billing"))
pharmacy = spark.read.format("delta").load(cfg.paths.silver_path("pharmacy_orders"))
labs = spark.read.format("delta").load(cfg.paths.silver_path("laboratory_results"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entity rule suites

# COMMAND ----------

all_results = []

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

# COMMAND ----------

summary = spark.createDataFrame([r.__dict__ for r in all_results])
display(summary.orderBy("severity", "status"))  # noqa: F821

failed = summary.filter(F.col("status") == "FAILED")
logger.info(
    "DQ run complete",
    module="data_quality",
    details={"rules": summary.count(), "failed_rules": failed.count()},
)
logger.flush()

display(spark.read.format("delta").load(cfg.paths.dq_failed_records_path()).limit(50))  # noqa: F821

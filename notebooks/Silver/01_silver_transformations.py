# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Transformations — Cleanse, Validate, SCD1 & SCD2
# MAGIC
# MAGIC - Data cleaning, deduplication, type casting, standardization
# MAGIC - Primary / foreign key validation via DQ framework
# MAGIC - **SCD Type 1** for doctors, appointments, claims, pharmacy, labs, billing
# MAGIC - **SCD Type 2** for patients (address / insurance / contact history)

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
    ALL_ENTITIES,
    ENTITY_PATIENTS,
    PIPELINE_SILVER_TRANSFORM,
    VALID_APPOINTMENT_STATUSES,
)
from src.audit.auditor import PipelineAuditor
from src.logging.logger import ensure_log_table, get_logger
from src.transformations.scd import apply_scd_type1, apply_scd_type2
from src.transformations.silver_transforms import clean_entity
from src.utilities.data_quality import (
    DataQualityFramework,
    Severity,
    build_appointment_dq,
    build_patient_dq,
)
from src.utilities.dataframe_utils import generate_run_id
from src.utilities.delta_helpers import write_delta
from src.utilities.spark_session import get_spark

# COMMAND ----------

spark = get_spark("SilverTransform")
cfg = get_config()
run_id = generate_run_id()
logger = get_logger(spark, PIPELINE_SILVER_TRANSFORM, run_id)
ensure_log_table(spark)
auditor = PipelineAuditor(spark, PIPELINE_SILVER_TRANSFORM, run_id, cfg.environment, logger)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Bronze & apply entity cleaners

# COMMAND ----------

bronze = {
    e: spark.read.format("delta").load(cfg.paths.bronze_path(e))
    for e in ALL_ENTITIES
}

silver_staged = {}
for entity, df in bronze.items():
    with auditor.track(f"silver_clean_{entity}") as ctx:
        cleaned = clean_entity(entity, df)
        silver_staged[entity] = cleaned
        ctx["rows_read"] = df.count()
        ctx["rows_inserted"] = cleaned.count()
        logger.info(
            f"Cleaned {entity}",
            module="silver",
            details={"in": ctx["rows_read"], "out": ctx["rows_inserted"]},
        )
        display(cleaned.limit(5))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data quality — patients & appointments

# COMMAND ----------

patient_dq = build_patient_dq(spark, run_id, logger)
patient_results = patient_dq.validate(silver_staged["patients"])
display(spark.createDataFrame([r.__dict__ for r in patient_results]))  # noqa: F821

appt_dq = build_appointment_dq(
    spark,
    run_id,
    silver_staged["patients"],
    silver_staged["doctors"],
    logger,
)
appt_results = appt_dq.validate(silver_staged["appointments"])
display(spark.createDataFrame([r.__dict__ for r in appt_results]))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. SCD Type 2 — Patients (historical tracking)

# COMMAND ----------

patient_tracked_cols = [
    "FirstName",
    "LastName",
    "Phone",
    "Email",
    "Address",
    "InsuranceID",
    "Gender",
]

with auditor.track("silver_patients_scd2") as ctx:
    patients_scd2 = apply_scd_type2(
        spark,
        silver_staged["patients"],
        cfg.paths.silver_path("patients"),
        primary_key="PatientID",
        tracked_columns=patient_tracked_cols,
        logger=logger,
    )
    ctx["rows_read"] = silver_staged["patients"].count()
    ctx["rows_inserted"] = patients_scd2.filter(F.col("IsCurrent") == True).count()  # noqa: E712
    display(patients_scd2.filter(F.col("IsCurrent") == True).limit(10))  # noqa: F821,E712

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. SCD Type 1 — Remaining dimensions / facts

# COMMAND ----------

scd1_entities = {
    "doctors": ("DoctorID", ["DoctorName", "Specialization", "Department", "Hospital", "Experience"]),
    "appointments": ("AppointmentID", ["PatientID", "DoctorID", "AppointmentDate", "Status", "Diagnosis"]),
    "insurance_claims": ("ClaimID", ["PatientID", "InsuranceCompany", "ClaimAmount", "ApprovalStatus", "ClaimDate"]),
    "pharmacy_orders": ("PrescriptionID", ["PatientID", "Medicine", "Quantity", "Price", "LineAmount"]),
    "laboratory_results": ("LabID", ["PatientID", "TestName", "Result", "NormalRange", "IsAbnormal"]),
    "billing": ("InvoiceID", ["PatientID", "AppointmentID", "TotalAmount", "PaymentStatus", "PaymentDate"]),
}

for entity, (pk, compare_cols) in scd1_entities.items():
    with auditor.track(f"silver_{entity}_scd1") as ctx:
        result = apply_scd_type1(
            spark,
            silver_staged[entity],
            cfg.paths.silver_path(entity),
            primary_key=pk,
            compare_columns=compare_cols,
            logger=logger,
        )
        ctx["rows_read"] = silver_staged[entity].count()
        ctx["rows_inserted"] = result.count()
        logger.info(f"SCD1 complete for {entity}", module="silver", details={"rows": result.count()})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Current patient view (IsCurrent = true)

# COMMAND ----------

patients_current = (
    spark.read.format("delta")
    .load(cfg.paths.silver_path("patients"))
    .filter(F.col("IsCurrent") == True)  # noqa: E712
)
write_delta(patients_current, cfg.paths.silver_path("patients_current"), mode="overwrite")
display(patients_current.limit(10))  # noqa: F821

logger.flush()
logger.info("Silver pipeline completed", module="silver")

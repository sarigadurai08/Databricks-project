# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Transformations — Cleanse, Validate, SCD1 & SCD2
# MAGIC
# MAGIC - Data cleaning, deduplication, type casting, standardization
# MAGIC - Primary / foreign key validation via DQ framework
# MAGIC - **SCD Type 1** for doctors, appointments, claims, pharmacy, labs, billing
# MAGIC - **SCD Type 2** for patients (address / insurance / contact history)
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
    ALL_ENTITIES,
    ENTITY_PATIENTS,
    PIPELINE_SILVER_TRANSFORM,
    VALID_APPOINTMENT_STATUSES,
)
from config.paths import PATHS
from src.audit.auditor import PipelineAuditor
from src.logging.logger import ensure_log_table, get_logger
from src.transformations.scd import apply_scd_type1, apply_scd_type2, filter_current
from src.transformations.silver_transforms import clean_entity
from src.utilities.data_quality import (
    DataQualityFramework,
    Severity,
    build_appointment_dq,
    build_patient_dq,
)
from src.utilities.dataframe_utils import generate_run_id
from src.utilities.databricks_runtime import prepare_databricks_runtime
from src.utilities.delta_helpers import table_exists, write_delta
from src.utilities.table_registry import register_silver_tables

# COMMAND ----------

spark = globals().get("spark") or SparkSession.getActiveSession()
cfg = get_config()
cfg = prepare_databricks_runtime(spark, cfg)
run_id = generate_run_id()
logger = get_logger(spark, PIPELINE_SILVER_TRANSFORM, run_id)
ensure_log_table(spark)
auditor = PipelineAuditor(spark, PIPELINE_SILVER_TRANSFORM, run_id, cfg.environment, logger)

logger.info("Silver pipeline started", module="silver", details=cfg.to_dict())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Bronze & apply entity cleaners

# COMMAND ----------

status_map = {}

try:
    missing_bronze = [e for e in ALL_ENTITIES if not table_exists(spark, cfg.paths.bronze_path(e))]
    if missing_bronze:
        raise FileNotFoundError(
            f"Missing Bronze tables {missing_bronze}. Run Bronze ingestion first."
        )

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
            status_map[f"clean_{entity}"] = ctx["rows_inserted"]
            logger.info(
                f"Cleaned {entity}",
                module="silver",
                details={"in": ctx["rows_read"], "out": ctx["rows_inserted"]},
            )
            display(cleaned.limit(5))  # noqa: F821
except Exception as exc:
    logger.error("Silver cleanse failed", module="silver", exc=exc)
    logger.flush()
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data quality — patients & appointments

# COMMAND ----------

try:
    patient_dq = build_patient_dq(spark, run_id, logger)
    patient_results = patient_dq.validate(silver_staged["patients"])
    try:
        display(spark.createDataFrame([r.__dict__ for r in patient_results]))  # noqa: F821
    except Exception as display_exc:
        logger.warning(f"Could not display patient DQ results: {display_exc}", module="silver")

    appt_dq = build_appointment_dq(
        spark,
        run_id,
        silver_staged["patients"],
        silver_staged["doctors"],
        logger,
    )
    appt_results = appt_dq.validate(silver_staged["appointments"])
    try:
        display(spark.createDataFrame([r.__dict__ for r in appt_results]))  # noqa: F821
    except Exception as display_exc:
        logger.warning(f"Could not display appointment DQ results: {display_exc}", module="silver")
except Exception as exc:
    # DQ is advisory in Silver — log and continue so SCD layers still execute
    logger.error("Silver DQ validation failed (continuing to SCD)", module="silver", exc=exc)

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

try:
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
        ctx["rows_inserted"] = filter_current(patients_scd2).count()
        status_map["patients_scd2"] = ctx["rows_inserted"]
        display(filter_current(patients_scd2).limit(10))  # noqa: F821
except Exception as exc:
    logger.error("SCD Type 2 failed for patients", module="silver", exc=exc)
    logger.flush()
    raise

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

try:
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
            status_map[f"{entity}_scd1"] = ctx["rows_inserted"]
            logger.info(f"SCD1 complete for {entity}", module="silver", details={"rows": result.count()})
except Exception as exc:
    logger.error("SCD Type 1 failed", module="silver", exc=exc)
    logger.flush()
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Current patient view (IsCurrent = true)

# COMMAND ----------

try:
    patients_current = filter_current(
        spark.read.format("delta").load(cfg.paths.silver_path("patients"))
    )
    write_delta(patients_current, cfg.paths.silver_path("patients_current"), mode="overwrite")
    display(patients_current.limit(10))  # noqa: F821
except Exception as exc:
    logger.error("Failed writing patients_current", module="silver", exc=exc)
    logger.flush()
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Register Silver as physical Unity Catalog tables

# COMMAND ----------

if cfg.unity_catalog.register_tables:
    try:
        reg = register_silver_tables(spark, cfg, logger)
        status_map["registered_tables"] = reg
        logger.info("Silver tables registered", module="silver", details=reg)
        catalog = cfg.unity_catalog.catalog or spark.sql("SELECT current_catalog()").collect()[0][0]
        try:
            display(spark.sql(f"SHOW TABLES IN `{catalog}`.silver"))  # noqa: F821
        except Exception:
            display(spark.createDataFrame([(k, v) for k, v in reg.items()], ["table", "status"]))  # noqa: F821
    except Exception as exc:
        logger.warning(f"Silver table registration skipped: {exc}", module="silver")

# COMMAND ----------

logger.flush()
logger.info("Silver pipeline completed", module="silver", details=status_map)
dbutils.notebook.exit(str(status_map))  # type: ignore[name-defined]

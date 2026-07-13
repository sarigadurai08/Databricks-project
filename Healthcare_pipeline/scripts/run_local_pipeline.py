"""
Local end-to-end pipeline runner (portfolio / CI without Databricks Runtime).

Executes Bronze → Silver → Gold → DQ using the same modules as the notebooks.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pyspark.sql import functions as F

from config.config import get_config
from config.constants import ALL_ENTITIES, PIPELINE_BRONZE_INGESTION, PIPELINE_GOLD_ANALYTICS, PIPELINE_SILVER_TRANSFORM
from src.audit.auditor import PipelineAuditor
from src.ingestion.autoloader import AutoLoaderIngestion, stage_sample_files_to_landing
from src.logging.logger import ensure_log_table, get_logger
from src.transformations.gold_transforms import build_all_gold_tables
from src.transformations.scd import apply_scd_type1, apply_scd_type2
from src.transformations.silver_transforms import clean_entity
from src.utilities.data_quality import build_patient_dq
from src.utilities.dataframe_utils import generate_run_id
from src.utilities.delta_helpers import write_delta
from src.utilities.spark_session import get_spark, stop_spark


def main() -> None:
    cfg = get_config()
    spark = get_spark("LocalHealthcarePipeline")
    run_id = generate_run_id()
    logger = get_logger(spark, "local_e2e", run_id)
    ensure_log_table(spark)

    print("=== Staging landing files ===")
    stage_sample_files_to_landing("csv", spark=spark)

    print("=== Bronze ===")
    bronze_logger = get_logger(spark, PIPELINE_BRONZE_INGESTION, run_id)
    auditor = PipelineAuditor(spark, PIPELINE_BRONZE_INGESTION, run_id, cfg.environment, bronze_logger)
    ingestion = AutoLoaderIngestion(spark, cfg, bronze_logger, run_id)
    for entity in ALL_ENTITIES:
        with auditor.track(f"bronze_{entity}") as ctx:
            df = ingestion.ingest_entity(entity, fmt="csv")
            ctx["rows_read"] = df.count()
            ctx["rows_inserted"] = ctx["rows_read"]
            print(f"  bronze.{entity}: {ctx['rows_read']} rows")

    print("=== Silver ===")
    silver_logger = get_logger(spark, PIPELINE_SILVER_TRANSFORM, run_id)
    silver_auditor = PipelineAuditor(spark, PIPELINE_SILVER_TRANSFORM, run_id, cfg.environment, silver_logger)
    staged = {}
    for entity in ALL_ENTITIES:
        raw = spark.read.format("delta").load(cfg.paths.bronze_path(entity))
        staged[entity] = clean_entity(entity, raw)

    with silver_auditor.track("patients_scd2") as ctx:
        apply_scd_type2(
            spark,
            staged["patients"],
            cfg.paths.silver_path("patients"),
            "PatientID",
            ["FirstName", "LastName", "Phone", "Email", "Address", "InsuranceID", "Gender"],
            silver_logger,
        )
        ctx["rows_read"] = staged["patients"].count()

    scd1 = {
        "doctors": "DoctorID",
        "appointments": "AppointmentID",
        "insurance_claims": "ClaimID",
        "pharmacy_orders": "PrescriptionID",
        "laboratory_results": "LabID",
        "billing": "InvoiceID",
    }
    for entity, pk in scd1.items():
        with silver_auditor.track(entity) as ctx:
            apply_scd_type1(spark, staged[entity], cfg.paths.silver_path(entity), pk, logger=silver_logger)
            ctx["rows_read"] = staged[entity].count()
            print(f"  silver.{entity} SCD1 complete")

    print("=== Data Quality (patients) ===")
    patients_cur = (
        spark.read.format("delta")
        .load(cfg.paths.silver_path("patients"))
        .filter(F.col("IsCurrent") == True)  # noqa: E712
    )
    build_patient_dq(spark, run_id, logger).validate(patients_cur)

    print("=== Gold ===")
    gold_logger = get_logger(spark, PIPELINE_GOLD_ANALYTICS, run_id)
    doctors = spark.read.format("delta").load(cfg.paths.silver_path("doctors"))
    appointments = spark.read.format("delta").load(cfg.paths.silver_path("appointments"))
    claims = spark.read.format("delta").load(cfg.paths.silver_path("insurance_claims"))
    pharmacy = spark.read.format("delta").load(cfg.paths.silver_path("pharmacy_orders"))
    labs = spark.read.format("delta").load(cfg.paths.silver_path("laboratory_results"))
    billing = spark.read.format("delta").load(cfg.paths.silver_path("billing"))

    gold = build_all_gold_tables(
        spark, patients_cur, doctors, appointments, claims, pharmacy, labs, billing
    )
    for name, gdf in gold.items():
        write_delta(gdf, cfg.paths.gold_path(name), mode="overwrite")
        print(f"  gold.{name}: {gdf.count()} rows")

    logger.flush()
    print("=== Pipeline completed successfully ===")
    stop_spark(spark)


if __name__ == "__main__":
    main()

"""
Register path-based Delta folders as queryable physical tables.

Catalog and Volume locations are discovered at runtime — never hardcoded to
a single workspace catalog such as `workspace`.

Example (resolved dynamically):
    {catalog}.bronze.patients
    → LOCATION '/Volumes/{catalog}/{schema}/healthcare_lakehouse/bronze/patients'
"""

from __future__ import annotations

from typing import Iterable, Optional, Sequence

from pyspark.sql import SparkSession

from config.config import HealthcareConfig, get_config
from config.constants import ALL_ENTITIES
from src.logging.logger import HealthcareLogger
from src.utilities.databricks_runtime import discover_catalog

# Gold marts produced by build_all_gold_tables
GOLD_TABLES = (
    "patient_summary",
    "doctor_performance",
    "revenue_analytics",
    "hospital_revenue",
    "insurance_analytics",
    "appointment_analytics",
    "monthly_revenue",
    "daily_revenue",
    "laboratory_trends",
    "pharmacy_sales",
    "patient_visit_summary",
    "doctor_utilization",
    "top_diseases",
    "cancelled_appointments",
)

SILVER_EXTRA_TABLES = ("patients_current",)

OPS_TABLES = (
    ("audit", "pipeline_audit", "audit_path"),
    ("ops_logging", "pipeline_logs", "log_path"),
    ("data_quality", "validation_results", "dq_results_path"),
    ("data_quality", "failed_records", "dq_failed_records_path"),
)


def resolve_catalog(spark: SparkSession, preferred: Optional[str] = None) -> str:
    """Resolve catalog via portable discovery (env → config → current → SHOW CATALOGS)."""
    return discover_catalog(spark, preferred or None)


def ensure_schema(spark: SparkSession, catalog: str, schema: str) -> None:
    """CREATE SCHEMA IF NOT EXISTS catalog.schema (or database for hive)."""
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    except Exception:
        # Fallback for non-UC / hive metastore
        try:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS `{schema}`")
        except Exception:
            pass


def table_fqn(catalog: str, schema: str, table: str, use_catalog: bool = True) -> str:
    if use_catalog and catalog and catalog not in {"spark_catalog", "hive_metastore"}:
        return f"`{catalog}`.`{schema}`.`{table}`"
    return f"`{schema}`.`{table}`"


def register_external_delta_table(
    spark: SparkSession,
    fqn: str,
    location: str,
    logger: Optional[HealthcareLogger] = None,
) -> bool:
    """
    Create (or replace location for) an external Delta table over an existing path.

    Returns True on success.
    """
    try:
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {fqn}
            USING DELTA
            LOCATION '{location}'
            """
        )
        if logger:
            logger.info(
                f"Registered table {fqn}",
                module="table_registry",
                details={"location": location},
            )
        return True
    except Exception as exc:
        # Table may already exist with wrong location — try CREATE OR REPLACE for Free Edition
        try:
            spark.sql(
                f"""
                CREATE OR REPLACE TABLE {fqn}
                USING DELTA
                LOCATION '{location}'
                """
            )
            if logger:
                logger.info(
                    f"Re-registered table {fqn}",
                    module="table_registry",
                    details={"location": location},
                )
            return True
        except Exception as exc2:
            if logger:
                logger.warning(
                    f"Failed to register {fqn}: {exc2}",
                    module="table_registry",
                    details={"first_error": str(exc), "location": location},
                )
            return False


def register_layer_tables(
    spark: SparkSession,
    catalog: str,
    schema: str,
    tables: Sequence[tuple[str, str]],
    logger: Optional[HealthcareLogger] = None,
    use_catalog: bool = True,
) -> dict[str, str]:
    """
    Register many (table_name, location) pairs under catalog.schema.

    Returns status map: fqn -> SUCCESS|FAILED
    """
    ensure_schema(spark, catalog, schema)
    status: dict[str, str] = {}
    for table, location in tables:
        fqn = table_fqn(catalog, schema, table, use_catalog=use_catalog)
        ok = register_external_delta_table(spark, fqn, location, logger=logger)
        status[fqn.replace("`", "")] = "SUCCESS" if ok else "FAILED"
    return status


def register_bronze_tables(
    spark: SparkSession,
    cfg: Optional[HealthcareConfig] = None,
    logger: Optional[HealthcareLogger] = None,
) -> dict[str, str]:
    cfg = cfg or get_config()
    catalog = resolve_catalog(spark, cfg.unity_catalog.catalog or None)
    cfg.unity_catalog.catalog = catalog
    pairs = [(e, cfg.paths.bronze_path(e)) for e in ALL_ENTITIES]
    return register_layer_tables(
        spark,
        catalog,
        cfg.unity_catalog.bronze_schema,
        pairs,
        logger=logger,
    )


def register_silver_tables(
    spark: SparkSession,
    cfg: Optional[HealthcareConfig] = None,
    logger: Optional[HealthcareLogger] = None,
) -> dict[str, str]:
    cfg = cfg or get_config()
    catalog = resolve_catalog(spark, cfg.unity_catalog.catalog or None)
    cfg.unity_catalog.catalog = catalog
    names = list(ALL_ENTITIES) + list(SILVER_EXTRA_TABLES)
    pairs = [(e, cfg.paths.silver_path(e)) for e in names]
    return register_layer_tables(
        spark,
        catalog,
        cfg.unity_catalog.silver_schema,
        pairs,
        logger=logger,
    )


def register_gold_tables(
    spark: SparkSession,
    cfg: Optional[HealthcareConfig] = None,
    table_names: Optional[Iterable[str]] = None,
    logger: Optional[HealthcareLogger] = None,
) -> dict[str, str]:
    cfg = cfg or get_config()
    catalog = resolve_catalog(spark, cfg.unity_catalog.catalog or None)
    cfg.unity_catalog.catalog = catalog
    names = list(table_names) if table_names is not None else list(GOLD_TABLES)
    pairs = [(n, cfg.paths.gold_path(n)) for n in names]
    return register_layer_tables(
        spark,
        catalog,
        cfg.unity_catalog.gold_schema,
        pairs,
        logger=logger,
    )


def register_ops_tables(
    spark: SparkSession,
    cfg: Optional[HealthcareConfig] = None,
    logger: Optional[HealthcareLogger] = None,
) -> dict[str, str]:
    """Register audit / logs / DQ Delta paths as queryable tables."""
    cfg = cfg or get_config()
    catalog = resolve_catalog(spark, cfg.unity_catalog.catalog or None)
    cfg.unity_catalog.catalog = catalog
    status: dict[str, str] = {}
    path_getters = {
        "audit_path": cfg.paths.audit_path,
        "log_path": cfg.paths.log_path,
        "dq_results_path": cfg.paths.dq_results_path,
        "dq_failed_records_path": cfg.paths.dq_failed_records_path,
    }
    for schema, table, getter_name in OPS_TABLES:
        ensure_schema(spark, catalog, schema)
        location = path_getters[getter_name]()
        fqn = table_fqn(catalog, schema, table)
        ok = register_external_delta_table(spark, fqn, location, logger=logger)
        status[fqn.replace("`", "")] = "SUCCESS" if ok else "FAILED"
    return status


def register_all_medallion_tables(
    spark: SparkSession,
    cfg: Optional[HealthcareConfig] = None,
    logger: Optional[HealthcareLogger] = None,
) -> dict[str, str]:
    """Register Bronze + Silver + Gold + ops tables."""
    status: dict[str, str] = {}
    status.update(register_bronze_tables(spark, cfg, logger))
    status.update(register_silver_tables(spark, cfg, logger))
    status.update(register_gold_tables(spark, cfg, logger=logger))
    status.update(register_ops_tables(spark, cfg, logger))
    return status

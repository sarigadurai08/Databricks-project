"""
Register path-based Delta folders as queryable physical Unity Catalog tables.

IMPORTANT (Databricks / Unity Catalog rule):
  You CANNOT create an external UC table with LOCATION pointing at a Volume
  (/Volumes/...). Volumes and tables must not overlap.

  Pipeline data remains on the Volume (path-based Delta). Registration creates
  **managed** UC tables via CTAS / saveAsTable so they are SQL-queryable:

      CREATE OR REPLACE TABLE catalog.bronze.orders AS
      SELECT * FROM delta.`/Volumes/.../bronze/orders`
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, Optional, Sequence

from pyspark.sql import SparkSession

from config.config import CONFIG, EcommerceConfig, get_config
from config.constants import ALL_ENTITIES
from src.utilities.databricks_runtime import discover_catalog
from src.utilities.delta_helpers import table_exists

if TYPE_CHECKING:
    from src.logging.logger import EcommerceLogger


# Gold marts produced by build_all_gold_tables
GOLD_TABLES = (
    "customer_journey",
    "top_products",
    "conversion_funnel",
    "hourly_orders",
    "revenue_dashboard",
    "sales_dashboard",
    "inventory_dashboard",
    "payment_dashboard",
    "customer_lifetime_value",
    "cart_abandonment",
    "repeat_customers",
    "session_analytics",
    "website_traffic",
    "top_categories",
    "peak_shopping_hours",
    "average_order_value",
    "revenue_by_region",
    "coupon_analytics",
    "inventory_trends",
)

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
        try:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS `{schema}`")
        except Exception:
            pass


def table_fqn(catalog: str, schema: str, table: str, use_catalog: bool = True) -> str:
    if use_catalog and catalog and catalog not in {"spark_catalog", "hive_metastore"}:
        return f"`{catalog}`.`{schema}`.`{table}`"
    return f"`{schema}`.`{table}`"


def _is_volume_path(location: str) -> bool:
    loc = (location or "").replace("\\", "/")
    return loc.startswith("/Volumes/") or loc.startswith("dbfs:/Volumes/")


def _verify_table(spark: SparkSession, fqn: str) -> bool:
    try:
        spark.sql(f"SELECT 1 FROM {fqn} LIMIT 0")
        return True
    except Exception:
        try:
            spark.table(fqn.replace("`", "")).limit(0).count()
            return True
        except Exception:
            return False


def register_external_delta_table(
    spark: SparkSession,
    fqn: str,
    location: str,
    logger: Optional["EcommerceLogger"] = None,
) -> bool:
    """
    Create a queryable UC table over pipeline Delta data.

    Strategy (first success wins):
      1. If LOCATION is NOT a Volume path — try external table (cloud URI / mount)
      2. Managed CTAS from delta.`location` (works for Volumes on Free Edition)
      3. DataFrame saveAsTable overwrite (managed)
    """
    if not table_exists(spark, location):
        if logger:
            logger.warning(
                f"Skip register {fqn}: Delta path missing",
                module="table_registry",
                details={"location": location},
            )
        return False

    errors: list[str] = []

    if not _is_volume_path(location):
        try:
            spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {fqn}
                USING DELTA
                LOCATION '{location}'
                """
            )
            if _verify_table(spark, fqn):
                if logger:
                    logger.info(
                        f"Registered external table {fqn}",
                        module="table_registry",
                        details={"location": location, "mode": "external"},
                    )
                return True
        except Exception as exc:
            errors.append(f"external:{exc}")

    try:
        spark.sql(
            f"""
            CREATE OR REPLACE TABLE {fqn}
            AS SELECT * FROM delta.`{location}`
            """
        )
        if _verify_table(spark, fqn):
            if logger:
                logger.info(
                    f"Registered managed table {fqn}",
                    module="table_registry",
                    details={"location": location, "mode": "ctas"},
                )
            return True
    except Exception as exc:
        errors.append(f"ctas:{exc}")

    try:
        plain = fqn.replace("`", "")
        (
            spark.read.format("delta")
            .load(location)
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(plain)
        )
        if _verify_table(spark, fqn):
            if logger:
                logger.info(
                    f"Registered managed table {fqn}",
                    module="table_registry",
                    details={"location": location, "mode": "saveAsTable"},
                )
            return True
    except Exception as exc:
        errors.append(f"saveAsTable:{exc}")

    if logger:
        logger.warning(
            f"Failed to register {fqn}",
            module="table_registry",
            details={"location": location, "errors": errors[:3]},
        )
    return False


def register_layer_tables(
    spark: SparkSession,
    catalog: str,
    schema: str,
    tables: Sequence[tuple[str, str]],
    logger: Optional["EcommerceLogger"] = None,
    use_catalog: bool = True,
) -> dict[str, str]:
    """Register many (table_name, location) pairs under catalog.schema."""
    ensure_schema(spark, catalog, schema)
    status: dict[str, str] = {}
    for table, location in tables:
        fqn = table_fqn(catalog, schema, table, use_catalog=use_catalog)
        if not table_exists(spark, location):
            status[fqn.replace("`", "")] = "SKIPPED"
            if logger:
                logger.warning(
                    f"Skip {fqn}: path not ready",
                    module="table_registry",
                    details={"location": location},
                )
            continue
        ok = register_external_delta_table(spark, fqn, location, logger=logger)
        status[fqn.replace("`", "")] = "SUCCESS" if ok else "FAILED"
    return status


def register_bronze_tables(
    spark: SparkSession,
    cfg: Optional[EcommerceConfig] = None,
    logger: Optional["EcommerceLogger"] = None,
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
    cfg: Optional[EcommerceConfig] = None,
    logger: Optional["EcommerceLogger"] = None,
) -> dict[str, str]:
    cfg = cfg or get_config()
    catalog = resolve_catalog(spark, cfg.unity_catalog.catalog or None)
    cfg.unity_catalog.catalog = catalog
    pairs = [(e, cfg.paths.silver_path(e)) for e in ALL_ENTITIES]
    return register_layer_tables(
        spark,
        catalog,
        cfg.unity_catalog.silver_schema,
        pairs,
        logger=logger,
    )


def register_gold_tables(
    spark: SparkSession,
    cfg: Optional[EcommerceConfig] = None,
    table_names: Optional[Iterable[str]] = None,
    logger: Optional["EcommerceLogger"] = None,
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
    cfg: Optional[EcommerceConfig] = None,
    logger: Optional["EcommerceLogger"] = None,
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
        if not table_exists(spark, location):
            status[fqn.replace("`", "")] = "SKIPPED"
            continue
        ok = register_external_delta_table(spark, fqn, location, logger=logger)
        status[fqn.replace("`", "")] = "SUCCESS" if ok else "FAILED"
    return status


def register_all_layers(
    spark: SparkSession,
    cfg: Optional[EcommerceConfig] = None,
    logger: Optional["EcommerceLogger"] = None,
) -> dict[str, str]:
    """Register Bronze + Silver + Gold + ops tables (managed CTAS for Volumes)."""
    cfg = cfg or CONFIG
    status: dict[str, str] = {}
    status.update(register_bronze_tables(spark, cfg, logger))
    status.update(register_silver_tables(spark, cfg, logger))
    status.update(register_gold_tables(spark, cfg, logger=logger))
    status.update(register_ops_tables(spark, cfg, logger))
    return status


# Alias for notebooks that mirror the healthcare naming
register_all_medallion_tables = register_all_layers

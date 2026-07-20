# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Framework — E-Commerce Lakehouse
# MAGIC
# MAGIC Runs DQ rules on key silver entities (users, products, orders, payments).
# MAGIC Quarantines failures and persists validation results to Delta.

# COMMAND ----------

import sys
from pathlib import Path

def _seed_project_root() -> str:
    import os
    def _is_root(p: Path) -> bool:
        return (p / "config" / "config.py").exists()
    candidates = []
    env = os.getenv("ECOMMERCE_LAKEHOUSE_ROOT")
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
        "Databricks_pipeline root not found. Set ECOMMERCE_LAKEHOUSE_ROOT."
    )

_PROJECT_ROOT = _seed_project_root()

from src.utilities.bootstrap import bootstrap_notebook
_PROJECT_ROOT = str(bootstrap_notebook(dbutils=globals().get("dbutils"), reload_modules=True))


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.config import get_config
from config.constants import (
    ENTITY_ORDERS,
    ENTITY_PAYMENTS,
    ENTITY_PRODUCTS,
    ENTITY_USERS,
    PIPELINE_DQ_FRAMEWORK,
    VALID_ORDER_STATUSES,
    VALID_PAYMENT_STATUSES,
)
from src.audit.auditor import PipelineAuditor
from src.logging.logger import ensure_log_table, get_logger
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
    required = [ENTITY_USERS, ENTITY_PRODUCTS, ENTITY_ORDERS, ENTITY_PAYMENTS]
    missing = [e for e in required if not table_exists(spark, cfg.paths.silver_path(e))]
    if missing:
        raise FileNotFoundError(
            f"Missing Silver tables {missing}. Run Bronze then Silver before Data Quality."
        )

    users = spark.read.format("delta").load(cfg.paths.silver_path(ENTITY_USERS))
    products = spark.read.format("delta").load(cfg.paths.silver_path(ENTITY_PRODUCTS))
    orders = spark.read.format("delta").load(cfg.paths.silver_path(ENTITY_ORDERS))
    payments = spark.read.format("delta").load(cfg.paths.silver_path(ENTITY_PAYMENTS))
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
dq_kwargs = dict(
    fail_on_critical=cfg.data_quality.fail_pipeline_on_critical,
    write_failed_records=cfg.data_quality.write_failed_records,
    quarantine_invalid_rows=cfg.data_quality.quarantine_invalid_rows,
)

try:
    with auditor.track("dq_users") as ctx:
        dq = (
            DataQualityFramework(spark, ENTITY_USERS, run_id, logger, **dq_kwargs)
            .require_not_null(["user_id", "email", "region"])
            .require_unique(["user_id"])
            .require_in_set("status", ["Active", "Inactive", "Suspended"], Severity.WARNING)
        )
        results = dq.validate(users)
        all_results.extend(results)
        ctx["rows_read"] = users.count()
        status_map["users"] = ctx["rows_read"]

    with auditor.track("dq_products") as ctx:
        dq = (
            DataQualityFramework(spark, ENTITY_PRODUCTS, run_id, logger, **dq_kwargs)
            .require_not_null(["product_id", "product_name", "category", "price"])
            .require_unique(["product_id"])
            .require_range("price", min_value=0)
            .require_positive(["price"], allow_zero=False, severity=Severity.WARNING)
        )
        results = dq.validate(products)
        all_results.extend(results)
        ctx["rows_read"] = products.count()
        status_map["products"] = ctx["rows_read"]

    with auditor.track("dq_orders") as ctx:
        dq = (
            DataQualityFramework(spark, ENTITY_ORDERS, run_id, logger, **dq_kwargs)
            .require_not_null(["order_id", "user_id", "order_time", "status", "total_amount"])
            .require_unique(["order_id"])
            .require_in_set("status", VALID_ORDER_STATUSES)
            .require_range("total_amount", min_value=0)
            .require_fk("user_id", users, "user_id")
        )
        results = dq.validate(orders)
        all_results.extend(results)
        ctx["rows_read"] = orders.count()
        status_map["orders"] = ctx["rows_read"]

    with auditor.track("dq_payments") as ctx:
        dq = (
            DataQualityFramework(spark, ENTITY_PAYMENTS, run_id, logger, **dq_kwargs)
            .require_not_null(["payment_id", "order_id", "amount", "status", "method"])
            .require_unique(["payment_id"])
            .require_in_set("status", VALID_PAYMENT_STATUSES)
            .require_range("amount", min_value=0)
            .require_fk("order_id", orders, "order_id")
        )
        results = dq.validate(payments)
        all_results.extend(results)
        ctx["rows_read"] = payments.count()
        status_map["payments"] = ctx["rows_read"]
except Exception as exc:
    logger.error("DQ rule execution failed", module="data_quality", exc=exc)
    logger.flush()
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## DQ results

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
    display(spark.read.format("delta").load(cfg.paths.dq_results_path()).limit(50))  # noqa: F821
except Exception as exc:
    logger.warning(f"DQ results table not available yet: {exc}", module="data_quality")

try:
    display(spark.read.format("delta").load(cfg.paths.dq_failed_records_path()).limit(50))  # noqa: F821
except Exception as exc:
    logger.warning(f"DQ failed-records table not available yet: {exc}", module="data_quality")

# COMMAND ----------

logger.flush()
logger.info("Data quality pipeline completed", module="data_quality", details=status_map)
dbutils.notebook.exit(str(status_map))  # type: ignore[name-defined]

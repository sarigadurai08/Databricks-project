# Databricks notebook source
# MAGIC %md
# MAGIC # Structured Streaming Analytics — E-Commerce Lakehouse
# MAGIC
# MAGIC Windowed aggregations over silver `orders` and `click_logs` with watermarking.
# MAGIC Uses **availableNow / once** triggers for Serverless friendliness.
# MAGIC
# MAGIC Outputs (idempotent overwrite):
# MAGIC - `gold/streaming_hourly_orders`
# MAGIC - `gold/streaming_session_traffic`
# MAGIC
# MAGIC Falls back to batch window aggregations when streaming is unsupported.

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
from pyspark.sql.types import TimestampType

from config.config import get_config
from config.constants import (
    ENTITY_CLICK_LOGS,
    ENTITY_ORDERS,
    PIPELINE_STREAMING,
)
from config.paths import PATHS
from src.audit.auditor import PipelineAuditor
from src.logging.logger import ensure_log_table, get_logger
from src.utilities.dataframe_utils import generate_run_id
from src.utilities.databricks_runtime import prepare_databricks_runtime
from src.utilities.delta_helpers import soft_reset_delta_path, table_exists, write_delta

# COMMAND ----------

spark = globals().get("spark") or SparkSession.getActiveSession()
cfg = get_config()
cfg = prepare_databricks_runtime(spark, cfg)
run_id = generate_run_id()
logger = get_logger(spark, PIPELINE_STREAMING, run_id)
ensure_log_table(spark)
auditor = PipelineAuditor(spark, PIPELINE_STREAMING, run_id, cfg.environment, logger)

logger.info("Structured streaming pipeline started", module="streaming", details=cfg.to_dict())
status_map = {}
watermark = cfg.streaming.watermark

orders_path = cfg.paths.silver_path(ENTITY_ORDERS)
clicks_path = cfg.paths.silver_path(ENTITY_CLICK_LOGS)
hourly_out = cfg.paths.gold_path("streaming_hourly_orders")
session_out = cfg.paths.gold_path("streaming_session_traffic")
ckpt_orders = PATHS.streaming_checkpoint("streaming_hourly_orders")
ckpt_session = PATHS.streaming_checkpoint("streaming_session_traffic")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers — batch fallback + streaming writers

# COMMAND ----------

def _as_ts(df, col_name: str):
    if col_name not in df.columns:
        return df.withColumn(col_name, F.current_timestamp())
    return df.withColumn(col_name, F.col(col_name).cast(TimestampType()))


def batch_hourly_orders(orders_df):
    o = _as_ts(orders_df, "order_time")
    return (
        o.groupBy(F.window("order_time", "1 hour").alias("w"), "status", "shipping_region")
        .agg(
            F.count("*").alias("order_count"),
            F.sum("total_amount").alias("revenue"),
            F.countDistinct("user_id").alias("unique_customers"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "status",
            "shipping_region",
            "order_count",
            "revenue",
            "unique_customers",
            F.current_timestamp().alias("_computed_at"),
        )
    )


def batch_session_traffic(clicks_df):
    c = _as_ts(clicks_df, "event_time")
    return (
        c.groupBy(F.window("event_time", "10 minutes").alias("w"), "event_type", "device_type")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("session_id").alias("sessions"),
            F.countDistinct("user_id").alias("users"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "event_type",
            "device_type",
            "event_count",
            "sessions",
            "users",
            F.current_timestamp().alias("_computed_at"),
        )
    )


def _start_available_now(writer):
    """Prefer availableNow; fall back to once for older runtimes."""
    try:
        return writer.trigger(availableNow=True).start()
    except Exception:
        return writer.trigger(once=True).start()


def run_streaming_hourly(orders_path: str, out_path: str, checkpoint: str) -> str:
    soft_reset_delta_path(spark, out_path)
    raw = spark.readStream.format("delta").load(orders_path)
    stream = (
        _as_ts(raw, "order_time")
        .withWatermark("order_time", watermark)
        .groupBy(F.window("order_time", "1 hour"), "status", "shipping_region")
        .agg(
            F.count("*").alias("order_count"),
            F.sum("total_amount").alias("revenue"),
            F.countDistinct("user_id").alias("unique_customers"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "status",
            "shipping_region",
            "order_count",
            "revenue",
            "unique_customers",
            F.current_timestamp().alias("_computed_at"),
        )
    )
    writer = (
        stream.writeStream.format("delta")
        .outputMode("complete")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .option("path", out_path)
    )
    q = _start_available_now(writer)
    q.awaitTermination()
    return "streaming"


def run_streaming_session(clicks_path: str, out_path: str, checkpoint: str) -> str:
    soft_reset_delta_path(spark, out_path)
    raw = spark.readStream.format("delta").load(clicks_path)
    stream = (
        _as_ts(raw, "event_time")
        .withWatermark("event_time", watermark)
        .groupBy(F.window("event_time", "10 minutes"), "event_type", "device_type")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("session_id").alias("sessions"),
            F.countDistinct("user_id").alias("users"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "event_type",
            "device_type",
            "event_count",
            "sessions",
            "users",
            F.current_timestamp().alias("_computed_at"),
        )
    )
    writer = (
        stream.writeStream.format("delta")
        .outputMode("complete")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .option("path", out_path)
    )
    q = _start_available_now(writer)
    q.awaitTermination()
    return "streaming"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Hourly orders aggregation

# COMMAND ----------

try:
    with auditor.track("streaming_hourly_orders") as ctx:
        if not table_exists(spark, orders_path):
            raise FileNotFoundError(f"Silver orders missing: {orders_path}")
        mode = "batch"
        try:
            mode = run_streaming_hourly(orders_path, hourly_out, ckpt_orders)
        except Exception as stream_exc:
            logger.warning(
                f"Streaming hourly orders unsupported; batch fallback: {stream_exc}",
                module="streaming",
            )
            orders = spark.read.format("delta").load(orders_path)
            result = batch_hourly_orders(orders)
            write_delta(result, hourly_out, mode="overwrite", merge_schema=True)
            mode = "batch_fallback"
        out_df = spark.read.format("delta").load(hourly_out)
        ctx["rows_inserted"] = out_df.count()
        status_map["streaming_hourly_orders"] = {"mode": mode, "rows": ctx["rows_inserted"]}
        try:
            display(out_df.limit(20))  # noqa: F821
        except Exception:
            pass
except Exception as exc:
    logger.error("Hourly orders aggregation failed", module="streaming", exc=exc)
    status_map["streaming_hourly_orders"] = f"FAILED: {exc}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Session traffic aggregation

# COMMAND ----------

try:
    with auditor.track("streaming_session_traffic") as ctx:
        if not table_exists(spark, clicks_path):
            raise FileNotFoundError(f"Silver click_logs missing: {clicks_path}")
        mode = "batch"
        try:
            mode = run_streaming_session(clicks_path, session_out, ckpt_session)
        except Exception as stream_exc:
            logger.warning(
                f"Streaming session traffic unsupported; batch fallback: {stream_exc}",
                module="streaming",
            )
            clicks = spark.read.format("delta").load(clicks_path)
            result = batch_session_traffic(clicks)
            write_delta(result, session_out, mode="overwrite", merge_schema=True)
            mode = "batch_fallback"
        out_df = spark.read.format("delta").load(session_out)
        ctx["rows_inserted"] = out_df.count()
        status_map["streaming_session_traffic"] = {"mode": mode, "rows": ctx["rows_inserted"]}
        try:
            display(out_df.limit(20))  # noqa: F821
        except Exception:
            pass
except Exception as exc:
    logger.error("Session traffic aggregation failed", module="streaming", exc=exc)
    status_map["streaming_session_traffic"] = f"FAILED: {exc}"

# COMMAND ----------

logger.flush()
logger.info("Structured streaming pipeline completed", module="streaming", details=status_map)
dbutils.notebook.exit(str(status_map))  # type: ignore[name-defined]

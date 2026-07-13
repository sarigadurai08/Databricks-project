"""Spark session factory with Delta Lake and AQE defaults."""

from __future__ import annotations

from typing import Optional

from pyspark.sql import SparkSession

from config.config import CONFIG, HealthcareConfig


def _is_databricks_runtime() -> bool:
    """Detect Databricks Runtime / Serverless without requiring an active session."""
    import os

    return bool(
        os.environ.get("DATABRICKS_RUNTIME_VERSION")
        or os.environ.get("DB_HOME")
        or os.path.exists("/databricks")
    )


def get_spark(
    app_name: Optional[str] = None,
    config: Optional[HealthcareConfig] = None,
    enable_hive: bool = False,
) -> SparkSession:
    """
    Retrieve the active SparkSession (Databricks) or build a local Delta session.

    On Databricks Free Edition / Serverless / clusters the managed session is
    always reused — never creates local[*].
    """
    cfg = config or CONFIG

    active = SparkSession.getActiveSession()
    if active is not None:
        _apply_safe_conf(active, cfg)
        return active

    if _is_databricks_runtime():
        # Databricks notebook global may not yet be bound to getActiveSession
        # in rare edge cases; still refuse to spawn local[*].
        raise RuntimeError(
            "No active SparkSession found on Databricks. "
            "Use the notebook `spark` session: "
            "spark = globals().get('spark') or SparkSession.getActiveSession()"
        )

    builder = (
        SparkSession.builder.appName(app_name or cfg.spark.app_name)
        .master(cfg.spark.master)
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
    )

    for key, value in cfg.spark.as_spark_conf().items():
        builder = builder.config(key, value)

    try:
        import delta  # noqa: F401

        builder = builder.config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    except Exception:
        pass

    if enable_hive:
        builder = builder.enableHiveSupport()

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _apply_safe_conf(spark: SparkSession, cfg: HealthcareConfig) -> None:
    """Apply non-destructive SQL conf on an existing Databricks session."""
    safe_keys = {
        "spark.sql.shuffle.partitions": str(cfg.spark.shuffle_partitions),
        "spark.sql.adaptive.enabled": str(cfg.spark.enable_aqe).lower(),
        "spark.sql.adaptive.coalescePartitions.enabled": str(
            cfg.spark.enable_adaptive_coalesce
        ).lower(),
        "spark.sql.adaptive.skewJoin.enabled": str(
            cfg.spark.enable_adaptive_skew_join
        ).lower(),
        "spark.sql.session.timeZone": cfg.spark.timezone,
    }
    for key, value in safe_keys.items():
        try:
            spark.conf.set(key, value)
        except Exception:
            pass


def stop_spark(spark: SparkSession) -> None:
    """Stop SparkSession if running outside Databricks managed runtime."""
    if _is_databricks_runtime():
        return
    try:
        if spark.conf.get("spark.databricks.clusterUsageTags.clusterId", None):
            return
    except Exception:
        pass
    spark.stop()

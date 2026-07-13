"""Spark session factory with Delta Lake and AQE defaults."""

from __future__ import annotations

from typing import Optional

from pyspark.sql import SparkSession

from config.config import CONFIG, HealthcareConfig


def get_spark(
    app_name: Optional[str] = None,
    config: Optional[HealthcareConfig] = None,
    enable_hive: bool = False,
) -> SparkSession:
    """
    Build or retrieve an active SparkSession configured for Delta Lake.

    On Databricks, the runtime session is reused; locally a new session is created
    with delta-spark extensions when available.
    """
    cfg = config or CONFIG
    builder = (
        SparkSession.builder.appName(app_name or cfg.spark.app_name)
        .master(cfg.spark.master)
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
    )

    for key, value in cfg.spark.as_spark_conf().items():
        builder = builder.config(key, value)

    # Prefer delta-spark package on local runs when installed
    try:
        import delta  # noqa: F401

        builder = (
            builder.config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
            if "databricks" not in cfg.spark.master
            else builder
        )
    except Exception:
        pass

    if enable_hive:
        builder = builder.enableHiveSupport()

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def stop_spark(spark: SparkSession) -> None:
    """Stop SparkSession if running outside Databricks managed runtime."""
    try:
        # Databricks notebooks manage the session lifecycle
        if spark.conf.get("spark.databricks.clusterUsageTags.clusterId", None):
            return
    except Exception:
        pass
    spark.stop()

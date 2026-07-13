"""
Enterprise logging framework for Healthcare Lakehouse.

Provides structured application logging with optional persistence to Delta tables
for operational observability on Databricks.
"""

from __future__ import annotations

import json
import logging
import sys
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config.constants import LogLevel
from config.paths import PATHS


LOG_TABLE_SCHEMA = StructType(
    [
        StructField("LogID", StringType(), False),
        StructField("RunID", StringType(), True),
        StructField("PipelineName", StringType(), True),
        StructField("Module", StringType(), True),
        StructField("LogLevel", StringType(), False),
        StructField("Message", StringType(), False),
        StructField("Details", StringType(), True),
        StructField("ExceptionType", StringType(), True),
        StructField("StackTrace", StringType(), True),
        StructField("LoggedAt", TimestampType(), False),
    ]
)


class HealthcareLogger:
    """
    Dual logger: stdlib console + optional Delta append for enterprise ops.

    Usage:
        logger = HealthcareLogger(spark, pipeline_name="bronze_ingestion", run_id=run_id)
        logger.info("Starting ingestion", module="autoloader", details={"entity": "patients"})
    """

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        pipeline_name: str = "healthcare_lakehouse",
        run_id: Optional[str] = None,
        persist_to_delta: bool = True,
        console_level: str = logging.INFO,
    ) -> None:
        self.spark = spark
        self.pipeline_name = pipeline_name
        self.run_id = run_id or str(uuid.uuid4())
        self.persist_to_delta = persist_to_delta and spark is not None
        self._buffer: list[dict[str, Any]] = []

        self._py_logger = logging.getLogger(f"healthcare.{pipeline_name}")
        if not self._py_logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
            )
            self._py_logger.addHandler(handler)
            self._py_logger.setLevel(console_level)
            self._py_logger.propagate = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def debug(self, message: str, module: str = "", details: Any = None) -> None:
        self._log(LogLevel.DEBUG, message, module, details)

    def info(self, message: str, module: str = "", details: Any = None) -> None:
        self._log(LogLevel.INFO, message, module, details)

    def warning(self, message: str, module: str = "", details: Any = None) -> None:
        self._log(LogLevel.WARNING, message, module, details)

    def error(
        self,
        message: str,
        module: str = "",
        details: Any = None,
        exc: Optional[BaseException] = None,
    ) -> None:
        self._log(LogLevel.ERROR, message, module, details, exc)

    def critical(
        self,
        message: str,
        module: str = "",
        details: Any = None,
        exc: Optional[BaseException] = None,
    ) -> None:
        self._log(LogLevel.CRITICAL, message, module, details, exc)

    def flush(self) -> Optional[DataFrame]:
        """Persist buffered log records to Delta."""
        if not self.persist_to_delta or not self._buffer or self.spark is None:
            return None

        from src.utilities.delta_helpers import write_delta

        df = self.spark.createDataFrame(self._buffer, schema=LOG_TABLE_SCHEMA)
        write_delta(df, PATHS.log_path(), mode="append", merge_schema=True)
        self._buffer.clear()
        return df

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _log(
        self,
        level: LogLevel,
        message: str,
        module: str,
        details: Any,
        exc: Optional[BaseException] = None,
    ) -> None:
        details_str = None
        if details is not None:
            details_str = details if isinstance(details, str) else json.dumps(details, default=str)

        exc_type = type(exc).__name__ if exc else None
        stack = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)) if exc else None

        record = {
            "LogID": str(uuid.uuid4()),
            "RunID": self.run_id,
            "PipelineName": self.pipeline_name,
            "Module": module or None,
            "LogLevel": level.value,
            "Message": message,
            "Details": details_str,
            "ExceptionType": exc_type,
            "StackTrace": stack,
            "LoggedAt": datetime.now(timezone.utc).replace(tzinfo=None),
        }
        self._buffer.append(record)

        console_msg = f"[{module}] {message}" if module else message
        if details_str:
            console_msg = f"{console_msg} | {details_str}"

        level_map = {
            LogLevel.DEBUG: self._py_logger.debug,
            LogLevel.INFO: self._py_logger.info,
            LogLevel.WARNING: self._py_logger.warning,
            LogLevel.ERROR: self._py_logger.error,
            LogLevel.CRITICAL: self._py_logger.critical,
        }
        level_map[level](console_msg)

        # Auto-flush on ERROR/CRITICAL to avoid losing diagnostic context
        if level in (LogLevel.ERROR, LogLevel.CRITICAL) and self.persist_to_delta:
            self.flush()


def get_logger(
    spark: Optional[SparkSession] = None,
    pipeline_name: str = "healthcare_lakehouse",
    run_id: Optional[str] = None,
) -> HealthcareLogger:
    return HealthcareLogger(spark=spark, pipeline_name=pipeline_name, run_id=run_id)


def ensure_log_table(spark: SparkSession) -> None:
    """Create empty Delta log table if it does not exist."""
    from src.utilities.delta_helpers import write_delta

    path = PATHS.log_path()
    try:
        spark.read.format("delta").load(path).limit(1).count()
    except Exception:
        empty = spark.createDataFrame([], schema=LOG_TABLE_SCHEMA)
        write_delta(empty, path, mode="overwrite", merge_schema=True)

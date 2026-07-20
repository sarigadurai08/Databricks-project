"""
Pipeline audit framework.

Tracks run-level metrics: rows read/written, timing, streaming batch ID, and
status for each pipeline invocation. Persists to a Delta audit table for SLA
and ops reporting.
"""

from __future__ import annotations

import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Generator, Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config.constants import PipelineStatus
from config.paths import PATHS
from src.utilities.delta_helpers import write_delta

if TYPE_CHECKING:
    from src.logging.logger import EcommerceLogger


AUDIT_SCHEMA = StructType(
    [
        StructField("AuditID", StringType(), False),
        StructField("PipelineName", StringType(), False),
        StructField("TableName", StringType(), True),
        StructField("RunID", StringType(), False),
        StructField("StartTime", TimestampType(), False),
        StructField("EndTime", TimestampType(), True),
        StructField("Status", StringType(), False),
        StructField("RowsRead", LongType(), True),
        StructField("RowsInserted", LongType(), True),
        StructField("RowsUpdated", LongType(), True),
        StructField("RowsDeleted", LongType(), True),
        StructField("ExecutionTimeSeconds", DoubleType(), True),
        StructField("ErrorMessage", StringType(), True),
        StructField("Environment", StringType(), True),
        StructField("StreamingBatchID", StringType(), True),
    ]
)


class PipelineAuditor:
    """
    Records pipeline execution metrics.

    Example:
        auditor = PipelineAuditor(spark, "silver_transform", run_id)
        with auditor.track("orders") as ctx:
            # ... transform ...
            ctx["rows_read"] = 1000
            ctx["rows_inserted"] = 50
            ctx["streaming_batch_id"] = "BATCH_abc123"
    """

    def __init__(
        self,
        spark: SparkSession,
        pipeline_name: str,
        run_id: Optional[str] = None,
        environment: str = "dev",
        logger: Optional["EcommerceLogger"] = None,
    ) -> None:
        self.spark = spark
        self.pipeline_name = pipeline_name
        self.run_id = run_id or str(uuid.uuid4())
        self.environment = environment
        self.logger = logger
        ensure_audit_table(spark)

    @contextmanager
    def track(self, table_name: str) -> Generator[dict[str, Any], None, None]:
        start = datetime.now(timezone.utc).replace(tzinfo=None)
        t0 = time.perf_counter()
        ctx: dict[str, Any] = {
            "rows_read": 0,
            "rows_inserted": 0,
            "rows_updated": 0,
            "rows_deleted": 0,
            "error_message": None,
            "streaming_batch_id": None,
        }
        status = PipelineStatus.SUCCESS
        try:
            if self.logger:
                self.logger.info(
                    f"Audit start for table={table_name}",
                    module="audit",
                    details={"run_id": self.run_id},
                )
            yield ctx
        except Exception as exc:
            status = PipelineStatus.FAILED
            ctx["error_message"] = str(exc)
            if self.logger:
                self.logger.error(
                    f"Audit captured failure for table={table_name}",
                    module="audit",
                    exc=exc,
                )
            raise
        finally:
            end = datetime.now(timezone.utc).replace(tzinfo=None)
            elapsed = time.perf_counter() - t0
            self._write_record(
                table_name=table_name,
                start=start,
                end=end,
                status=status,
                ctx=ctx,
                elapsed=elapsed,
            )

    def record(
        self,
        table_name: str,
        status: PipelineStatus | str,
        start_time: datetime,
        end_time: datetime,
        rows_read: int = 0,
        rows_inserted: int = 0,
        rows_updated: int = 0,
        rows_deleted: int = 0,
        error_message: Optional[str] = None,
        streaming_batch_id: Optional[str] = None,
    ) -> None:
        elapsed = (end_time - start_time).total_seconds()
        ctx = {
            "rows_read": rows_read,
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
            "rows_deleted": rows_deleted,
            "error_message": error_message,
            "streaming_batch_id": streaming_batch_id,
        }
        if isinstance(status, str):
            status = PipelineStatus(status)
        self._write_record(table_name, start_time, end_time, status, ctx, elapsed)

    def _write_record(
        self,
        table_name: str,
        start: datetime,
        end: datetime,
        status: PipelineStatus,
        ctx: dict[str, Any],
        elapsed: float,
    ) -> None:
        row = {
            "AuditID": str(uuid.uuid4()),
            "PipelineName": self.pipeline_name,
            "TableName": table_name,
            "RunID": self.run_id,
            "StartTime": start,
            "EndTime": end,
            "Status": status.value,
            "RowsRead": int(ctx.get("rows_read") or 0),
            "RowsInserted": int(ctx.get("rows_inserted") or 0),
            "RowsUpdated": int(ctx.get("rows_updated") or 0),
            "RowsDeleted": int(ctx.get("rows_deleted") or 0),
            "ExecutionTimeSeconds": float(elapsed),
            "ErrorMessage": ctx.get("error_message"),
            "Environment": self.environment,
            "StreamingBatchID": ctx.get("streaming_batch_id"),
        }
        df = self.spark.createDataFrame([row], schema=AUDIT_SCHEMA)
        write_delta(df, PATHS.audit_path(), mode="append", merge_schema=True)
        if self.logger:
            self.logger.info(
                f"Audit recorded status={status.value} table={table_name}",
                module="audit",
                details={
                    "execution_seconds": round(elapsed, 3),
                    "rows_read": row["RowsRead"],
                    "rows_inserted": row["RowsInserted"],
                    "rows_updated": row["RowsUpdated"],
                    "streaming_batch_id": row["StreamingBatchID"],
                },
            )


def ensure_audit_table(spark: SparkSession) -> None:
    """Create empty Delta audit table at PATHS.audit_path() if missing."""
    path = PATHS.audit_path()
    try:
        spark.read.format("delta").load(path).limit(1).count()
    except Exception:
        empty = spark.createDataFrame([], schema=AUDIT_SCHEMA)
        write_delta(empty, path, mode="overwrite", merge_schema=True)

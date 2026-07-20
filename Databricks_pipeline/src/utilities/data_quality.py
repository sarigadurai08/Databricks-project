"""
Reusable Data Quality validation framework for the E-Commerce Lakehouse.

Supports nulls, uniqueness, set membership, ranges, foreign keys, positivity,
and late-event checks. Failed records are quarantined to Delta paths.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Optional, Sequence

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config.paths import PATHS
from src.utilities.delta_helpers import write_delta
from src.utilities.exceptions import DataQualityError

if TYPE_CHECKING:
    from src.logging.logger import EcommerceLogger


def _dataframe_is_empty(df: DataFrame) -> bool:
    """Serverless-safe emptiness check (avoids .rdd which is restricted on some runtimes)."""
    try:
        return df.isEmpty()
    except Exception:
        return df.limit(1).count() == 0


class Severity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


@dataclass
class DQRule:
    name: str
    description: str
    severity: Severity
    # Returns a boolean Column: True = row FAILS the rule
    fail_condition: Callable[[DataFrame], Column]
    columns: Sequence[str] = field(default_factory=tuple)


@dataclass
class DQResult:
    rule_name: str
    description: str
    severity: str
    failed_count: int
    total_count: int
    pass_rate: float
    status: str  # PASSED | FAILED


DQ_RESULT_SCHEMA = StructType(
    [
        StructField("ResultID", StringType(), False),
        StructField("RunID", StringType(), False),
        StructField("Entity", StringType(), False),
        StructField("RuleName", StringType(), False),
        StructField("Description", StringType(), True),
        StructField("Severity", StringType(), False),
        StructField("FailedCount", LongType(), False),
        StructField("TotalCount", LongType(), False),
        StructField("PassRate", StringType(), False),
        StructField("Status", StringType(), False),
        StructField("ValidatedAt", TimestampType(), False),
    ]
)


class DataQualityFramework:
    """Execute a suite of DQ rules against a DataFrame and persist results."""

    def __init__(
        self,
        spark: SparkSession,
        entity: str,
        run_id: Optional[str] = None,
        logger: Optional["EcommerceLogger"] = None,
        fail_on_critical: bool = False,
        write_failed_records: bool = True,
        quarantine_invalid_rows: bool = True,
    ) -> None:
        self.spark = spark
        self.entity = entity
        self.run_id = run_id or str(uuid.uuid4())
        self.logger = logger
        self.fail_on_critical = fail_on_critical
        self.write_failed_records = write_failed_records
        self.quarantine_invalid_rows = quarantine_invalid_rows
        self.rules: list[DQRule] = []
        self._fk_rules: list[dict[str, Any]] = []

    def add_rule(self, rule: DQRule) -> "DataQualityFramework":
        self.rules.append(rule)
        return self

    def require_not_null(
        self, columns: Sequence[str], severity: Severity = Severity.CRITICAL
    ) -> "DataQualityFramework":
        for col in columns:
            self.add_rule(
                DQRule(
                    name=f"not_null_{col}",
                    description=f"Column {col} must not be null",
                    severity=severity,
                    fail_condition=lambda df, c=col: F.col(c).isNull(),
                    columns=[col],
                )
            )
        return self

    def require_unique(
        self, columns: Sequence[str], severity: Severity = Severity.CRITICAL
    ) -> "DataQualityFramework":
        cols = list(columns)

        def fail_cond(df: DataFrame) -> Column:
            from pyspark.sql.window import Window

            w = Window.partitionBy(*cols)
            return F.count(F.lit(1)).over(w) > 1

        self.add_rule(
            DQRule(
                name=f"unique_{'_'.join(cols)}",
                description=f"Columns {cols} must be unique",
                severity=severity,
                fail_condition=fail_cond,
                columns=cols,
            )
        )
        return self

    def require_in_set(
        self,
        column: str,
        allowed: set[str] | Sequence[str],
        severity: Severity = Severity.WARNING,
    ) -> "DataQualityFramework":
        allowed_list = list(allowed)
        self.add_rule(
            DQRule(
                name=f"in_set_{column}",
                description=f"Column {column} must be in {allowed_list}",
                severity=severity,
                fail_condition=lambda df, c=column, a=allowed_list: (
                    ~F.col(c).isin(a) & F.col(c).isNotNull()
                ),
                columns=[column],
            )
        )
        return self

    def require_range(
        self,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        severity: Severity = Severity.WARNING,
    ) -> "DataQualityFramework":
        def fail_cond(df: DataFrame) -> Column:
            cond = F.lit(False)
            if min_value is not None:
                cond = cond | (F.col(column) < F.lit(min_value))
            if max_value is not None:
                cond = cond | (F.col(column) > F.lit(max_value))
            return cond & F.col(column).isNotNull()

        self.add_rule(
            DQRule(
                name=f"range_{column}",
                description=f"Column {column} must be between {min_value} and {max_value}",
                severity=severity,
                fail_condition=fail_cond,
                columns=[column],
            )
        )
        return self

    def require_fk(
        self,
        column: str,
        ref_df: DataFrame,
        ref_column: str,
        severity: Severity = Severity.CRITICAL,
    ) -> "DataQualityFramework":
        ref_keys = ref_df.select(F.col(ref_column).alias("_fk_key")).distinct()
        self._fk_rules.append(
            {
                "name": f"fk_{column}_to_{ref_column}",
                "description": f"Foreign key {column} must exist in reference.{ref_column}",
                "severity": severity,
                "column": column,
                "ref_df": ref_keys,
            }
        )
        return self

    def require_positive(
        self,
        columns: Sequence[str],
        allow_zero: bool = False,
        severity: Severity = Severity.WARNING,
    ) -> "DataQualityFramework":
        for col in columns:
            if allow_zero:
                fail = lambda df, c=col: (F.col(c) < 0) & F.col(c).isNotNull()
                desc = f"Column {col} must be >= 0"
            else:
                fail = lambda df, c=col: (F.col(c) <= 0) & F.col(c).isNotNull()
                desc = f"Column {col} must be > 0"
            self.add_rule(
                DQRule(
                    name=f"positive_{col}",
                    description=desc,
                    severity=severity,
                    fail_condition=fail,
                    columns=[col],
                )
            )
        return self

    def require_event_not_late(
        self,
        event_time_column: str,
        max_delay_hours: float = 24.0,
        reference_time_column: Optional[str] = None,
        severity: Severity = Severity.WARNING,
    ) -> "DataQualityFramework":
        """
        Flag events whose event time is older than max_delay_hours relative to
        reference_time_column (default: current_timestamp).
        """

        def fail_cond(df: DataFrame) -> Column:
            if reference_time_column and reference_time_column in df.columns:
                ref = F.to_timestamp(F.col(reference_time_column))
            else:
                ref = F.current_timestamp()
            evt = F.to_timestamp(F.col(event_time_column))
            delay_hours = (F.unix_timestamp(ref) - F.unix_timestamp(evt)) / F.lit(3600.0)
            return evt.isNotNull() & (delay_hours > F.lit(max_delay_hours))

        self.add_rule(
            DQRule(
                name=f"event_not_late_{event_time_column}",
                description=(
                    f"Column {event_time_column} must not be more than "
                    f"{max_delay_hours}h behind reference"
                ),
                severity=severity,
                fail_condition=fail_cond,
                columns=[event_time_column],
            )
        )
        return self

    def validate(self, df: DataFrame) -> list[DQResult]:
        total = df.count()
        results: list[DQResult] = []
        critical_failures = 0

        for rule in self.rules:
            marked = df.withColumn("__dq_fail__", rule.fail_condition(df))
            failed_df = marked.filter(F.col("__dq_fail__")).drop("__dq_fail__")
            failed_count = failed_df.count()
            pass_rate = 0.0 if total == 0 else round((total - failed_count) / total * 100.0, 4)
            status = "PASSED" if failed_count == 0 else "FAILED"
            result = DQResult(
                rule_name=rule.name,
                description=rule.description,
                severity=rule.severity.value,
                failed_count=failed_count,
                total_count=total,
                pass_rate=pass_rate,
                status=status,
            )
            results.append(result)

            if failed_count > 0:
                self._quarantine_failed(failed_df, rule)
                if rule.severity == Severity.CRITICAL:
                    critical_failures += failed_count

            if self.logger:
                self.logger.info(
                    f"DQ rule {rule.name}: {status}",
                    module="data_quality",
                    details={
                        "failed": failed_count,
                        "total": total,
                        "pass_rate": pass_rate,
                        "severity": rule.severity.value,
                    },
                )

        for fk in self._fk_rules:
            orphan_df = df.join(
                fk["ref_df"],
                df[fk["column"]] == fk["ref_df"]["_fk_key"],
                how="left_anti",
            ).filter(F.col(fk["column"]).isNotNull())
            failed_count = orphan_df.count()
            pass_rate = 0.0 if total == 0 else round((total - failed_count) / total * 100.0, 4)
            status = "PASSED" if failed_count == 0 else "FAILED"
            sev = fk["severity"] if isinstance(fk["severity"], Severity) else Severity(fk["severity"])
            results.append(
                DQResult(
                    rule_name=fk["name"],
                    description=fk["description"],
                    severity=sev.value,
                    failed_count=failed_count,
                    total_count=total,
                    pass_rate=pass_rate,
                    status=status,
                )
            )
            if failed_count > 0:
                pseudo_rule = DQRule(
                    name=fk["name"],
                    description=fk["description"],
                    severity=sev,
                    fail_condition=lambda d: F.lit(True),
                    columns=[fk["column"]],
                )
                self._quarantine_failed(orphan_df, pseudo_rule)
                if sev == Severity.CRITICAL:
                    critical_failures += failed_count

        self._persist_results(results)

        if self.fail_on_critical and critical_failures > 0:
            raise DataQualityError(
                f"Critical DQ failures for entity={self.entity}: {critical_failures} rows",
                details={"run_id": self.run_id, "results": [r.__dict__ for r in results]},
            )
        return results

    def _quarantine_failed(self, failed_df: DataFrame, rule: DQRule) -> None:
        if _dataframe_is_empty(failed_df):
            return
        if not (self.write_failed_records or self.quarantine_invalid_rows):
            return

        payload_cols = [c for c in failed_df.columns if not c.startswith("__")]
        if not payload_cols:
            payload_cols = list(failed_df.columns)

        payload = failed_df.select(
            F.to_json(F.struct(*[F.col(c) for c in payload_cols])).alias("Payload")
        )
        enriched = (
            payload.withColumn("FailedRecordID", F.expr("uuid()"))
            .withColumn("RunID", F.lit(self.run_id))
            .withColumn("Entity", F.lit(self.entity))
            .withColumn("RuleName", F.lit(rule.name))
            .withColumn("Severity", F.lit(rule.severity.value))
            .withColumn("DetectedAt", F.current_timestamp())
            .select(
                "FailedRecordID",
                "RunID",
                "Entity",
                "RuleName",
                "Severity",
                "Payload",
                "DetectedAt",
            )
        )

        if self.write_failed_records:
            write_delta(enriched, PATHS.dq_failed_records_path(), mode="append", merge_schema=True)

        if self.quarantine_invalid_rows:
            quarantine_df = (
                failed_df.withColumn("_dq_rule", F.lit(rule.name))
                .withColumn("_dq_severity", F.lit(rule.severity.value))
                .withColumn("_dq_run_id", F.lit(self.run_id))
                .withColumn("_dq_detected_at", F.current_timestamp())
            )
            write_delta(
                quarantine_df,
                PATHS.quarantine_path(self.entity),
                mode="append",
                merge_schema=True,
            )

    def _persist_results(self, results: list[DQResult]) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        rows = [
            {
                "ResultID": str(uuid.uuid4()),
                "RunID": self.run_id,
                "Entity": self.entity,
                "RuleName": r.rule_name,
                "Description": r.description,
                "Severity": r.severity,
                "FailedCount": r.failed_count,
                "TotalCount": r.total_count,
                "PassRate": str(r.pass_rate),
                "Status": r.status,
                "ValidatedAt": now,
            }
            for r in results
        ]
        if not rows:
            return
        df = self.spark.createDataFrame(rows, schema=DQ_RESULT_SCHEMA)
        write_delta(df, PATHS.dq_results_path(), mode="append", merge_schema=True)

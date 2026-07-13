"""
Reusable Data Quality validation framework.

Supports nulls, duplicates, data types, ranges, mandatory fields, primary keys,
foreign keys, invalid values, and business rules. Failed records are written
to a quarantine Delta table.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Optional, Sequence

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config.paths import PATHS
from src.logging.logger import HealthcareLogger
from src.utilities.exceptions import DataQualityError


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
    # Optional columns to project into failed_records payload
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

FAILED_RECORD_SCHEMA = StructType(
    [
        StructField("FailedRecordID", StringType(), False),
        StructField("RunID", StringType(), False),
        StructField("Entity", StringType(), False),
        StructField("RuleName", StringType(), False),
        StructField("Severity", StringType(), False),
        StructField("Payload", StringType(), True),
        StructField("DetectedAt", TimestampType(), False),
    ]
)


class DataQualityFramework:
    """
    Execute a suite of DQ rules against a DataFrame and persist results.
    """

    def __init__(
        self,
        spark: SparkSession,
        entity: str,
        run_id: Optional[str] = None,
        logger: Optional[HealthcareLogger] = None,
        fail_on_critical: bool = False,
    ) -> None:
        self.spark = spark
        self.entity = entity
        self.run_id = run_id or str(uuid.uuid4())
        self.logger = logger
        self.fail_on_critical = fail_on_critical
        self.rules: list[DQRule] = []

    def add_rule(self, rule: DQRule) -> "DataQualityFramework":
        self.rules.append(rule)
        return self

    # ---- rule factories -------------------------------------------------
    def require_not_null(self, columns: Sequence[str], severity: Severity = Severity.CRITICAL) -> "DataQualityFramework":
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

    def require_unique(self, columns: Sequence[str], severity: Severity = Severity.CRITICAL) -> "DataQualityFramework":
        cols = list(columns)

        def fail_cond(df: DataFrame) -> Column:
            # Window expr is projected via withColumn in validate() — never used in WHERE.
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

    def require_values_in(
        self,
        column: str,
        allowed: set[str] | Sequence[str],
        severity: Severity = Severity.WARNING,
    ) -> "DataQualityFramework":
        allowed_list = list(allowed)

        self.add_rule(
            DQRule(
                name=f"valid_values_{column}",
                description=f"Column {column} must be in {allowed_list}",
                severity=severity,
                fail_condition=lambda df, c=column, a=allowed_list: ~F.col(c).isin(a) & F.col(c).isNotNull(),
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

        def fail_cond(df: DataFrame) -> Column:
            # Mark rows whose FK is not null and not found in reference
            joined = df.join(
                ref_keys,
                df[column] == ref_keys["_fk_key"],
                how="left",
            )
            # We cannot return Column from joined DF easily in fail_condition contract;
            # instead we use a broadcasted set approach via left_anti precompute below.
            return F.lit(False)

        # Custom execution path handled in validate via _fk_rules
        self._fk_rules = getattr(self, "_fk_rules", [])
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

    def require_regex(
        self,
        column: str,
        pattern: str,
        severity: Severity = Severity.WARNING,
    ) -> "DataQualityFramework":
        self.add_rule(
            DQRule(
                name=f"regex_{column}",
                description=f"Column {column} must match /{pattern}/",
                severity=severity,
                fail_condition=lambda df, c=column, p=pattern: ~F.col(c).rlike(p) & F.col(c).isNotNull(),
                columns=[column],
            )
        )
        return self

    def add_business_rule(
        self,
        name: str,
        description: str,
        fail_condition: Callable[[DataFrame], Column],
        severity: Severity = Severity.WARNING,
        columns: Sequence[str] = (),
    ) -> "DataQualityFramework":
        self.add_rule(
            DQRule(
                name=name,
                description=description,
                severity=severity,
                fail_condition=fail_condition,
                columns=columns,
            )
        )
        return self

    # ---- execution ------------------------------------------------------
    def validate(self, df: DataFrame) -> list[DQResult]:
        total = df.count()
        results: list[DQResult] = []
        critical_failures = 0

        for rule in self.rules:
            # Materialize fail predicate as a column first.
            # Window functions (e.g. unique checks) are illegal in WHERE/filter on Spark.
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
                self._write_failed_records(failed_df, rule)
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

        # FK rules
        for fk in getattr(self, "_fk_rules", []):
            orphan_df = df.join(
                fk["ref_df"],
                df[fk["column"]] == fk["ref_df"]["_fk_key"],
                how="left_anti",
            ).filter(F.col(fk["column"]).isNotNull())
            failed_count = orphan_df.count()
            pass_rate = 0.0 if total == 0 else round((total - failed_count) / total * 100.0, 4)
            status = "PASSED" if failed_count == 0 else "FAILED"
            results.append(
                DQResult(
                    rule_name=fk["name"],
                    description=fk["description"],
                    severity=fk["severity"].value if isinstance(fk["severity"], Severity) else fk["severity"],
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
                    severity=fk["severity"] if isinstance(fk["severity"], Severity) else Severity(fk["severity"]),
                    fail_condition=lambda d: F.lit(True),
                    columns=[fk["column"]],
                )
                self._write_failed_records(orphan_df, pseudo_rule)
                if pseudo_rule.severity == Severity.CRITICAL:
                    critical_failures += failed_count

        self._persist_results(results)

        if self.fail_on_critical and critical_failures > 0:
            raise DataQualityError(
                f"Critical DQ failures for entity={self.entity}: {critical_failures} rows",
                details={"run_id": self.run_id, "results": [r.__dict__ for r in results]},
            )
        return results

    def _write_failed_records(self, failed_df: DataFrame, rule: DQRule) -> None:
        if _dataframe_is_empty(failed_df):
            return
        # Avoid packing non-JSON-friendly / huge metadata columns into quarantine payload
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
        (
            enriched.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(PATHS.dq_failed_records_path())
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
        (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(PATHS.dq_results_path())
        )


def build_patient_dq(spark: SparkSession, run_id: str, logger: Optional[HealthcareLogger] = None) -> DataQualityFramework:
    from config.constants import VALID_GENDERS

    return (
        DataQualityFramework(spark, "patients", run_id, logger)
        .require_not_null(["PatientID", "FirstName", "LastName", "DOB"])
        .require_unique(["PatientID"])
        .require_values_in("Gender", VALID_GENDERS)
        .require_regex("Email", r"^[^@]+@[^@]+\.[^@]+$")
    )


def build_appointment_dq(
    spark: SparkSession,
    run_id: str,
    patients_df: DataFrame,
    doctors_df: DataFrame,
    logger: Optional[HealthcareLogger] = None,
) -> DataQualityFramework:
    from config.constants import VALID_APPOINTMENT_STATUSES

    dq = (
        DataQualityFramework(spark, "appointments", run_id, logger)
        .require_not_null(["AppointmentID", "PatientID", "DoctorID", "AppointmentDate"])
        .require_unique(["AppointmentID"])
        .require_values_in("Status", VALID_APPOINTMENT_STATUSES)
        .require_fk("PatientID", patients_df, "PatientID")
        .require_fk("DoctorID", doctors_df, "DoctorID")
    )
    return dq

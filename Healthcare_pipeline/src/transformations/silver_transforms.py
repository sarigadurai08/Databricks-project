"""
Silver-layer cleansing transformers for each healthcare entity.

Reusable, unit-testable functions that clean, cast, standardize, and validate
bronze records prior to SCD application.
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.constants import (
    VALID_APPOINTMENT_STATUSES,
    VALID_CLAIM_STATUSES,
    VALID_GENDERS,
    VALID_PAYMENT_STATUSES,
)
from src.utilities.dataframe_utils import (
    cast_columns,
    dedupe_keep_latest,
    drop_exact_duplicates,
    null_safe_fill,
    standardize_email,
    standardize_phone,
    standardize_string_columns,
)


META_DROP = [
    "_ingestion_time",
    "_source_file",
    "_load_id",
    "_batch_id",
    "_record_hash",
    "_rescued_data",
    "ingestion_date",
]


def _drop_meta(df: DataFrame, keep_lineage: bool = False) -> DataFrame:
    if keep_lineage:
        return df
    drop_cols = [c for c in META_DROP if c in df.columns]
    return df.drop(*drop_cols) if drop_cols else df


def clean_patients(df: DataFrame, keep_lineage: bool = False) -> DataFrame:
    df = _drop_meta(df, keep_lineage)
    df = standardize_string_columns(
        df,
        ["PatientID", "FirstName", "LastName", "Gender", "Phone", "Email", "Address", "InsuranceID"],
    )
    df = (
        df.withColumn("Email", standardize_email("Email"))
        .withColumn("Phone", standardize_phone("Phone"))
        .withColumn(
            "Gender",
            F.when(F.initcap(F.col("Gender")).isin(list(VALID_GENDERS)), F.initcap(F.col("Gender")))
            .otherwise(F.lit("Unknown")),
        )
    )
    df = cast_columns(
        df,
        {
            "DOB": "date",
            "CreatedDate": "timestamp",
            "ModifiedDate": "timestamp",
        },
    )
    df = df.filter(F.col("PatientID").isNotNull() & (F.trim(F.col("PatientID")) != ""))
    df = dedupe_keep_latest(df, ["PatientID"], "ModifiedDate")
    return df


def clean_doctors(df: DataFrame, keep_lineage: bool = False) -> DataFrame:
    df = _drop_meta(df, keep_lineage)
    df = standardize_string_columns(
        df,
        ["DoctorID", "DoctorName", "Specialization", "Department", "Hospital"],
    )
    df = cast_columns(df, {"Experience": "int"})
    df = df.filter(F.col("DoctorID").isNotNull())
    df = df.withColumn(
        "Experience",
        F.when(F.col("Experience") < 0, F.lit(0)).otherwise(F.col("Experience")),
    )
    df = drop_exact_duplicates(df, ["DoctorID"])
    return df


def clean_appointments(df: DataFrame, keep_lineage: bool = False) -> DataFrame:
    df = _drop_meta(df, keep_lineage)
    df = standardize_string_columns(
        df,
        ["AppointmentID", "PatientID", "DoctorID", "Status", "Diagnosis"],
    )
    df = cast_columns(df, {"AppointmentDate": "timestamp"})
    df = df.withColumn(
        "Status",
        F.when(F.col("Status").isin(list(VALID_APPOINTMENT_STATUSES)), F.col("Status"))
        .otherwise(F.lit("Scheduled")),
    )
    df = df.filter(F.col("AppointmentID").isNotNull())
    df = dedupe_keep_latest(df, ["AppointmentID"], "AppointmentDate")
    return df


def clean_insurance_claims(df: DataFrame, keep_lineage: bool = False) -> DataFrame:
    df = _drop_meta(df, keep_lineage)
    df = standardize_string_columns(
        df,
        ["ClaimID", "PatientID", "InsuranceCompany", "ApprovalStatus"],
    )
    df = cast_columns(df, {"ClaimAmount": "decimal(12,2)", "ClaimDate": "date"})
    df = df.withColumn(
        "ApprovalStatus",
        F.when(F.col("ApprovalStatus").isin(list(VALID_CLAIM_STATUSES)), F.col("ApprovalStatus"))
        .otherwise(F.lit("Pending")),
    )
    df = df.filter(F.col("ClaimID").isNotNull() & (F.col("ClaimAmount") >= 0))
    df = drop_exact_duplicates(df, ["ClaimID"])
    return df


def clean_pharmacy_orders(df: DataFrame, keep_lineage: bool = False) -> DataFrame:
    df = _drop_meta(df, keep_lineage)
    df = standardize_string_columns(df, ["PrescriptionID", "PatientID", "Medicine"])
    df = cast_columns(df, {"Quantity": "int", "Price": "decimal(12,2)"})
    df = df.filter(
        F.col("PrescriptionID").isNotNull()
        & (F.col("Quantity") > 0)
        & (F.col("Price") >= 0)
    )
    df = drop_exact_duplicates(df, ["PrescriptionID"])
    df = df.withColumn("LineAmount", F.col("Quantity") * F.col("Price"))
    return df


def clean_laboratory_results(df: DataFrame, keep_lineage: bool = False) -> DataFrame:
    df = _drop_meta(df, keep_lineage)
    df = standardize_string_columns(
        df, ["LabID", "PatientID", "TestName", "Result", "NormalRange"]
    )
    df = df.filter(F.col("LabID").isNotNull())
    df = drop_exact_duplicates(df, ["LabID"])
    # Flag abnormal when Result is numeric and outside simple parsed range "low-high"
    df = df.withColumn(
        "_low",
        F.regexp_extract(F.col("NormalRange"), r"^\s*([0-9.]+)\s*-", 1).cast("double"),
    ).withColumn(
        "_high",
        F.regexp_extract(F.col("NormalRange"), r"-\s*([0-9.]+)\s*$", 1).cast("double"),
    ).withColumn(
        "_result_num",
        F.col("Result").cast("double"),
    ).withColumn(
        "IsAbnormal",
        F.when(
            F.col("_result_num").isNotNull()
            & F.col("_low").isNotNull()
            & F.col("_high").isNotNull()
            & ((F.col("_result_num") < F.col("_low")) | (F.col("_result_num") > F.col("_high"))),
            F.lit(True),
        ).otherwise(F.lit(False)),
    ).drop("_low", "_high", "_result_num")
    return df


def clean_billing(df: DataFrame, keep_lineage: bool = False) -> DataFrame:
    df = _drop_meta(df, keep_lineage)
    df = standardize_string_columns(
        df, ["InvoiceID", "PatientID", "AppointmentID", "PaymentStatus"]
    )
    df = cast_columns(
        df,
        {"TotalAmount": "decimal(12,2)", "PaymentDate": "date"},
    )
    df = df.withColumn(
        "PaymentStatus",
        F.when(F.col("PaymentStatus").isin(list(VALID_PAYMENT_STATUSES)), F.col("PaymentStatus"))
        .otherwise(F.lit("Pending")),
    )
    df = df.filter(F.col("InvoiceID").isNotNull() & (F.col("TotalAmount") >= 0))
    df = drop_exact_duplicates(df, ["InvoiceID"])
    return df


CLEANERS = {
    "patients": clean_patients,
    "doctors": clean_doctors,
    "appointments": clean_appointments,
    "insurance_claims": clean_insurance_claims,
    "pharmacy_orders": clean_pharmacy_orders,
    "laboratory_results": clean_laboratory_results,
    "billing": clean_billing,
}


def clean_entity(entity: str, df: DataFrame, keep_lineage: bool = False) -> DataFrame:
    if entity not in CLEANERS:
        raise ValueError(f"No cleaner registered for entity={entity}")
    return CLEANERS[entity](df, keep_lineage=keep_lineage)

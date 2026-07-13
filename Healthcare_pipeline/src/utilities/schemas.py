"""
Explicit Spark schemas for healthcare entities.

Used by ingestion (schema hints), tests, and documentation of the canonical
Bronze/Silver contracts.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config.constants import (
    META_BATCH_ID,
    META_INGESTION_TIME,
    META_LOAD_ID,
    META_RECORD_HASH,
    META_RESCUED_DATA,
    META_SOURCE_FILE,
    SCD2_EFFECTIVE_END,
    SCD2_EFFECTIVE_START,
    SCD2_IS_CURRENT,
    SCD2_VERSION,
)


PATIENTS_SCHEMA = StructType(
    [
        StructField("PatientID", StringType(), False),
        StructField("FirstName", StringType(), True),
        StructField("LastName", StringType(), True),
        StructField("DOB", DateType(), True),
        StructField("Gender", StringType(), True),
        StructField("Phone", StringType(), True),
        StructField("Email", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("InsuranceID", StringType(), True),
        StructField("CreatedDate", TimestampType(), True),
        StructField("ModifiedDate", TimestampType(), True),
    ]
)

DOCTORS_SCHEMA = StructType(
    [
        StructField("DoctorID", StringType(), False),
        StructField("DoctorName", StringType(), True),
        StructField("Specialization", StringType(), True),
        StructField("Department", StringType(), True),
        StructField("Hospital", StringType(), True),
        StructField("Experience", IntegerType(), True),
    ]
)

APPOINTMENTS_SCHEMA = StructType(
    [
        StructField("AppointmentID", StringType(), False),
        StructField("PatientID", StringType(), False),
        StructField("DoctorID", StringType(), False),
        StructField("AppointmentDate", TimestampType(), True),
        StructField("Status", StringType(), True),
        StructField("Diagnosis", StringType(), True),
    ]
)

INSURANCE_CLAIMS_SCHEMA = StructType(
    [
        StructField("ClaimID", StringType(), False),
        StructField("PatientID", StringType(), False),
        StructField("InsuranceCompany", StringType(), True),
        StructField("ClaimAmount", DecimalType(12, 2), True),
        StructField("ApprovalStatus", StringType(), True),
        StructField("ClaimDate", DateType(), True),
    ]
)

PHARMACY_ORDERS_SCHEMA = StructType(
    [
        StructField("PrescriptionID", StringType(), False),
        StructField("PatientID", StringType(), False),
        StructField("Medicine", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("Price", DecimalType(12, 2), True),
    ]
)

LABORATORY_RESULTS_SCHEMA = StructType(
    [
        StructField("LabID", StringType(), False),
        StructField("PatientID", StringType(), False),
        StructField("TestName", StringType(), True),
        StructField("Result", StringType(), True),
        StructField("NormalRange", StringType(), True),
    ]
)

BILLING_SCHEMA = StructType(
    [
        StructField("InvoiceID", StringType(), False),
        StructField("PatientID", StringType(), False),
        StructField("AppointmentID", StringType(), True),
        StructField("TotalAmount", DecimalType(12, 2), True),
        StructField("PaymentStatus", StringType(), True),
        StructField("PaymentDate", DateType(), True),
    ]
)

BRONZE_METADATA_FIELDS = [
    StructField(META_INGESTION_TIME, TimestampType(), False),
    StructField(META_SOURCE_FILE, StringType(), True),
    StructField(META_LOAD_ID, StringType(), True),
    StructField(META_BATCH_ID, StringType(), True),
    StructField(META_RECORD_HASH, StringType(), True),
    StructField(META_RESCUED_DATA, StringType(), True),
    StructField("ingestion_date", DateType(), True),
]

SCD2_FIELDS = [
    StructField(SCD2_EFFECTIVE_START, TimestampType(), True),
    StructField(SCD2_EFFECTIVE_END, TimestampType(), True),
    StructField(SCD2_IS_CURRENT, BooleanType(), True),
    StructField(SCD2_VERSION, IntegerType(), True),
]

ENTITY_SCHEMAS = {
    "patients": PATIENTS_SCHEMA,
    "doctors": DOCTORS_SCHEMA,
    "appointments": APPOINTMENTS_SCHEMA,
    "insurance_claims": INSURANCE_CLAIMS_SCHEMA,
    "pharmacy_orders": PHARMACY_ORDERS_SCHEMA,
    "laboratory_results": LABORATORY_RESULTS_SCHEMA,
    "billing": BILLING_SCHEMA,
}


def with_bronze_metadata(schema: StructType) -> StructType:
    return StructType(list(schema.fields) + BRONZE_METADATA_FIELDS)


def with_scd2(schema: StructType) -> StructType:
    return StructType(list(schema.fields) + SCD2_FIELDS)

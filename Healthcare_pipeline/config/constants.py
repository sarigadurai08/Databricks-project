"""
Enterprise constants for the Healthcare Lakehouse platform.

Centralizes magic strings, status codes, and domain enumerations so that
pipelines, notebooks, and tests share a single source of truth.
"""

from __future__ import annotations

from enum import Enum


# ---------------------------------------------------------------------------
# Catalog / schema naming (Unity Catalog compatible)
# ---------------------------------------------------------------------------
CATALOG_NAME = "healthcare_catalog"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
AUDIT_SCHEMA = "audit"
DQ_SCHEMA = "data_quality"
LOG_SCHEMA = "ops_logging"
DLQ_SCHEMA = "dead_letter"

# ---------------------------------------------------------------------------
# Pipeline identifiers
# ---------------------------------------------------------------------------
PIPELINE_BRONZE_INGESTION = "bronze_auto_loader_ingestion"
PIPELINE_SILVER_TRANSFORM = "silver_cleanse_and_scd"
PIPELINE_GOLD_ANALYTICS = "gold_clinical_analytics"
PIPELINE_DQ_FRAMEWORK = "data_quality_framework"
PIPELINE_MAINTENANCE = "delta_table_maintenance"

# ---------------------------------------------------------------------------
# Layer names
# ---------------------------------------------------------------------------
LAYER_BRONZE = "bronze"
LAYER_SILVER = "silver"
LAYER_GOLD = "gold"

# ---------------------------------------------------------------------------
# Source system identifiers
# ---------------------------------------------------------------------------
SOURCE_EHR = "ehr_system"
SOURCE_CLAIMS = "claims_clearinghouse"
SOURCE_PHARMACY = "pharmacy_system"
SOURCE_LAB = "lis_laboratory"
SOURCE_BILLING = "billing_rcm"

# ---------------------------------------------------------------------------
# Entity / table base names
# ---------------------------------------------------------------------------
ENTITY_PATIENTS = "patients"
ENTITY_DOCTORS = "doctors"
ENTITY_APPOINTMENTS = "appointments"
ENTITY_INSURANCE_CLAIMS = "insurance_claims"
ENTITY_PHARMACY_ORDERS = "pharmacy_orders"
ENTITY_LABORATORY_RESULTS = "laboratory_results"
ENTITY_BILLING = "billing"

ALL_ENTITIES = (
    ENTITY_PATIENTS,
    ENTITY_DOCTORS,
    ENTITY_APPOINTMENTS,
    ENTITY_INSURANCE_CLAIMS,
    ENTITY_PHARMACY_ORDERS,
    ENTITY_LABORATORY_RESULTS,
    ENTITY_BILLING,
)

# ---------------------------------------------------------------------------
# Metadata column names (bronze lineage)
# ---------------------------------------------------------------------------
META_INGESTION_TIME = "_ingestion_time"
META_SOURCE_FILE = "_source_file"
META_LOAD_ID = "_load_id"
META_BATCH_ID = "_batch_id"
META_RECORD_HASH = "_record_hash"
META_RESCUED_DATA = "_rescued_data"

# ---------------------------------------------------------------------------
# SCD Type 2 column names
# ---------------------------------------------------------------------------
SCD2_EFFECTIVE_START = "EffectiveStartDate"
SCD2_EFFECTIVE_END = "EffectiveEndDate"
SCD2_IS_CURRENT = "IsCurrent"
SCD2_VERSION = "VersionNumber"

# ---------------------------------------------------------------------------
# Domain enumerations
# ---------------------------------------------------------------------------
class AppointmentStatus(str, Enum):
    SCHEDULED = "Scheduled"
    COMPLETED = "Completed"
    CANCELLED = "Cancelled"
    NO_SHOW = "NoShow"
    RESCHEDULED = "Rescheduled"


class ClaimApprovalStatus(str, Enum):
    PENDING = "Pending"
    APPROVED = "Approved"
    DENIED = "Denied"
    PARTIAL = "Partial"
    APPEALED = "Appealed"


class PaymentStatus(str, Enum):
    PAID = "Paid"
    PENDING = "Pending"
    PARTIAL = "Partial"
    OVERDUE = "Overdue"
    WRITTEN_OFF = "WrittenOff"


class Gender(str, Enum):
    MALE = "Male"
    FEMALE = "Female"
    OTHER = "Other"
    UNKNOWN = "Unknown"


class PipelineStatus(str, Enum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    PARTIAL = "PARTIAL"


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


# ---------------------------------------------------------------------------
# Valid domain value sets (used by DQ framework)
# ---------------------------------------------------------------------------
VALID_APPOINTMENT_STATUSES = {s.value for s in AppointmentStatus}
VALID_CLAIM_STATUSES = {s.value for s in ClaimApprovalStatus}
VALID_PAYMENT_STATUSES = {s.value for s in PaymentStatus}
VALID_GENDERS = {g.value for g in Gender}

SPECIALIZATIONS = (
    "Cardiology",
    "Orthopedics",
    "Neurology",
    "Pediatrics",
    "Oncology",
    "Dermatology",
    "Gastroenterology",
    "Endocrinology",
    "Pulmonology",
    "Psychiatry",
    "Radiology",
    "Emergency Medicine",
    "General Surgery",
    "Internal Medicine",
    "Obstetrics & Gynecology",
)

DEPARTMENTS = (
    "Cardiology",
    "Orthopedics",
    "Neurology",
    "Pediatrics",
    "Oncology",
    "Dermatology",
    "Gastroenterology",
    "Endocrinology",
    "Pulmonology",
    "Behavioral Health",
    "Radiology",
    "Emergency",
    "Surgery",
    "Internal Medicine",
    "Women's Health",
)

HOSPITALS = (
    "Mercy General Hospital",
    "St. Luke Medical Center",
    "Sunrise Regional Health",
    "Harborview Clinical Campus",
    "Northside Community Hospital",
)

INSURANCE_COMPANIES = (
    "BlueCross Health",
    "Aetna Care",
    "United Healthcare",
    "Cigna Medical",
    "Humana Plus",
    "Kaiser Permanente",
    "Medicare",
    "Medicaid",
)

# ---------------------------------------------------------------------------
# Auto Loader / CloudFiles defaults
# ---------------------------------------------------------------------------
AUTOLOADER_FORMAT_CSV = "cloudFiles"
AUTOLOADER_FORMAT_JSON = "cloudFiles"
RESCUE_DATA_COLUMN = "_rescued_data"
BAD_RECORDS_PATH_SUFFIX = "_bad_records"
SCHEMA_LOCATION_SUFFIX = "_schemas"
CHECKPOINT_SUFFIX = "_checkpoints"

# ---------------------------------------------------------------------------
# Performance defaults
# ---------------------------------------------------------------------------
DEFAULT_SHUFFLE_PARTITIONS = 8
BROADCAST_THRESHOLD_BYTES = 10 * 1024 * 1024  # 10 MB
OPTIMIZE_ZORDER_COLUMNS = {
    ENTITY_PATIENTS: ["PatientID", "InsuranceID"],
    ENTITY_APPOINTMENTS: ["AppointmentDate", "PatientID", "DoctorID"],
    ENTITY_INSURANCE_CLAIMS: ["ClaimDate", "PatientID", "InsuranceCompany"],
    ENTITY_BILLING: ["PaymentDate", "PatientID"],
    ENTITY_PHARMACY_ORDERS: ["PatientID", "Medicine"],
    ENTITY_LABORATORY_RESULTS: ["PatientID", "TestName"],
    ENTITY_DOCTORS: ["DoctorID", "Department"],
}

# ---------------------------------------------------------------------------
# Retry / error handling
# ---------------------------------------------------------------------------
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2
RETRY_MAX_BACKOFF_SECONDS = 30

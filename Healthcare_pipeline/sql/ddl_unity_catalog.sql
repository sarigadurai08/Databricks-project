-- =============================================================================
-- Delta Lake DDL — Unity Catalog compatible (optional reference)
-- =============================================================================
-- PORTABLE: replace {{CATALOG}} with the catalog discovered at runtime
-- (see prepare_databricks_runtime / current_catalog()).
-- Prefer notebook auto-registration over running this file.
-- =============================================================================

-- CREATE CATALOG IF NOT EXISTS {{CATALOG}};  -- requires metastore admin
USE CATALOG {{CATALOG}};

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS data_quality;
CREATE SCHEMA IF NOT EXISTS ops_logging;

-- Example managed table with generated column + check constraint
CREATE TABLE IF NOT EXISTS silver.patients (
  PatientID STRING NOT NULL,
  FirstName STRING,
  LastName STRING,
  DOB DATE,
  Gender STRING,
  Phone STRING,
  Email STRING,
  Address STRING,
  InsuranceID STRING,
  CreatedDate TIMESTAMP,
  ModifiedDate TIMESTAMP,
  EffectiveStartDate TIMESTAMP,
  EffectiveEndDate TIMESTAMP,
  IsCurrent BOOLEAN,
  VersionNumber INT,
  AgeYears INT GENERATED ALWAYS AS (FLOOR(MONTHS_BETWEEN(CURRENT_DATE(), DOB) / 12))
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

ALTER TABLE silver.patients
  ADD CONSTRAINT chk_patients_gender
  CHECK (Gender IN ('Male', 'Female', 'Other', 'Unknown'));

-- Liquid Clustering (DBR 13.3+)
-- ALTER TABLE gold.revenue_analytics CLUSTER BY (PaymentDate, Hospital, Department);

-- External / path-based registration uses the runtime Volume, e.g.:
-- CREATE TABLE gold.hospital_revenue
-- USING DELTA
-- LOCATION '{{VOLUME_BASE}}/gold/hospital_revenue';

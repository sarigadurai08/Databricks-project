-- =============================================================================
-- Register path-based Delta folders as physical queryable tables
-- =============================================================================
-- PORTABLE: do NOT hardcode a catalog or Volume path.
--
-- Recommended: skip this file — Bronze / Silver / Gold / Monitoring notebooks
-- already call register_*_tables() which discovers catalog + Volume at runtime.
--
-- Manual run (Databricks SQL Editor):
--   1. Replace {{CATALOG}} with your catalog (e.g. workspace, main, or custom)
--   2. Replace {{VOLUME_BASE}} with your Volume root, e.g.
--        /Volumes/<catalog>/default/healthcare_lakehouse
--   3. Run the script
--
-- Discover values in a Python cell:
--   from src.utilities.databricks_runtime import prepare_databricks_runtime
--   cfg = prepare_databricks_runtime(spark)
--   print(cfg.unity_catalog.catalog, cfg.paths.storage_base)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS {{CATALOG}}.bronze;
CREATE SCHEMA IF NOT EXISTS {{CATALOG}}.silver;
CREATE SCHEMA IF NOT EXISTS {{CATALOG}}.gold;
CREATE SCHEMA IF NOT EXISTS {{CATALOG}}.audit;
CREATE SCHEMA IF NOT EXISTS {{CATALOG}}.data_quality;
CREATE SCHEMA IF NOT EXISTS {{CATALOG}}.ops_logging;

-- Bronze
CREATE TABLE IF NOT EXISTS {{CATALOG}}.bronze.patients USING DELTA LOCATION '{{VOLUME_BASE}}/bronze/patients';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.bronze.doctors USING DELTA LOCATION '{{VOLUME_BASE}}/bronze/doctors';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.bronze.appointments USING DELTA LOCATION '{{VOLUME_BASE}}/bronze/appointments';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.bronze.insurance_claims USING DELTA LOCATION '{{VOLUME_BASE}}/bronze/insurance_claims';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.bronze.pharmacy_orders USING DELTA LOCATION '{{VOLUME_BASE}}/bronze/pharmacy_orders';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.bronze.laboratory_results USING DELTA LOCATION '{{VOLUME_BASE}}/bronze/laboratory_results';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.bronze.billing USING DELTA LOCATION '{{VOLUME_BASE}}/bronze/billing';

-- Silver
CREATE TABLE IF NOT EXISTS {{CATALOG}}.silver.patients USING DELTA LOCATION '{{VOLUME_BASE}}/silver/patients';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.silver.patients_current USING DELTA LOCATION '{{VOLUME_BASE}}/silver/patients_current';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.silver.doctors USING DELTA LOCATION '{{VOLUME_BASE}}/silver/doctors';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.silver.appointments USING DELTA LOCATION '{{VOLUME_BASE}}/silver/appointments';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.silver.insurance_claims USING DELTA LOCATION '{{VOLUME_BASE}}/silver/insurance_claims';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.silver.pharmacy_orders USING DELTA LOCATION '{{VOLUME_BASE}}/silver/pharmacy_orders';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.silver.laboratory_results USING DELTA LOCATION '{{VOLUME_BASE}}/silver/laboratory_results';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.silver.billing USING DELTA LOCATION '{{VOLUME_BASE}}/silver/billing';

-- Gold
CREATE TABLE IF NOT EXISTS {{CATALOG}}.gold.patient_summary USING DELTA LOCATION '{{VOLUME_BASE}}/gold/patient_summary';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.gold.doctor_performance USING DELTA LOCATION '{{VOLUME_BASE}}/gold/doctor_performance';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.gold.revenue_analytics USING DELTA LOCATION '{{VOLUME_BASE}}/gold/revenue_analytics';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.gold.hospital_revenue USING DELTA LOCATION '{{VOLUME_BASE}}/gold/hospital_revenue';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.gold.insurance_analytics USING DELTA LOCATION '{{VOLUME_BASE}}/gold/insurance_analytics';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.gold.appointment_analytics USING DELTA LOCATION '{{VOLUME_BASE}}/gold/appointment_analytics';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.gold.monthly_revenue USING DELTA LOCATION '{{VOLUME_BASE}}/gold/monthly_revenue';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.gold.daily_revenue USING DELTA LOCATION '{{VOLUME_BASE}}/gold/daily_revenue';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.gold.laboratory_trends USING DELTA LOCATION '{{VOLUME_BASE}}/gold/laboratory_trends';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.gold.pharmacy_sales USING DELTA LOCATION '{{VOLUME_BASE}}/gold/pharmacy_sales';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.gold.patient_visit_summary USING DELTA LOCATION '{{VOLUME_BASE}}/gold/patient_visit_summary';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.gold.doctor_utilization USING DELTA LOCATION '{{VOLUME_BASE}}/gold/doctor_utilization';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.gold.top_diseases USING DELTA LOCATION '{{VOLUME_BASE}}/gold/top_diseases';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.gold.cancelled_appointments USING DELTA LOCATION '{{VOLUME_BASE}}/gold/cancelled_appointments';

-- Ops
CREATE TABLE IF NOT EXISTS {{CATALOG}}.audit.pipeline_audit USING DELTA LOCATION '{{VOLUME_BASE}}/audit/pipeline_audit';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.ops_logging.pipeline_logs USING DELTA LOCATION '{{VOLUME_BASE}}/ops_logging/pipeline_logs';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.data_quality.validation_results USING DELTA LOCATION '{{VOLUME_BASE}}/data_quality/validation_results';
CREATE TABLE IF NOT EXISTS {{CATALOG}}.data_quality.failed_records USING DELTA LOCATION '{{VOLUME_BASE}}/data_quality/failed_records';

-- Example queries (after registration / USE CATALOG)
-- SELECT * FROM {{CATALOG}}.bronze.patients LIMIT 10;
-- SELECT * FROM {{CATALOG}}.silver.patients WHERE IsCurrent = true LIMIT 10;
-- SELECT * FROM {{CATALOG}}.gold.monthly_revenue;

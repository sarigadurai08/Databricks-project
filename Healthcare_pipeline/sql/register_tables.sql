-- =============================================================================
-- Register path-based Volume Delta as queryable managed UC tables
-- =============================================================================
-- Unity Catalog does NOT allow: CREATE TABLE ... LOCATION '/Volumes/...'
-- (tables and volumes cannot overlap).
--
-- Recommended: run Bronze/Silver/Gold notebooks — they auto-register via CTAS.
--
-- Manual (replace {{CATALOG}} and {{VOLUME_BASE}}):
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS {{CATALOG}}.bronze;
CREATE SCHEMA IF NOT EXISTS {{CATALOG}}.silver;
CREATE SCHEMA IF NOT EXISTS {{CATALOG}}.gold;
CREATE SCHEMA IF NOT EXISTS {{CATALOG}}.audit;
CREATE SCHEMA IF NOT EXISTS {{CATALOG}}.data_quality;
CREATE SCHEMA IF NOT EXISTS {{CATALOG}}.ops_logging;

-- Bronze (managed copies from Volume Delta paths)
CREATE OR REPLACE TABLE {{CATALOG}}.bronze.patients AS SELECT * FROM delta.`{{VOLUME_BASE}}/bronze/patients`;
CREATE OR REPLACE TABLE {{CATALOG}}.bronze.doctors AS SELECT * FROM delta.`{{VOLUME_BASE}}/bronze/doctors`;
CREATE OR REPLACE TABLE {{CATALOG}}.bronze.appointments AS SELECT * FROM delta.`{{VOLUME_BASE}}/bronze/appointments`;
CREATE OR REPLACE TABLE {{CATALOG}}.bronze.insurance_claims AS SELECT * FROM delta.`{{VOLUME_BASE}}/bronze/insurance_claims`;
CREATE OR REPLACE TABLE {{CATALOG}}.bronze.pharmacy_orders AS SELECT * FROM delta.`{{VOLUME_BASE}}/bronze/pharmacy_orders`;
CREATE OR REPLACE TABLE {{CATALOG}}.bronze.laboratory_results AS SELECT * FROM delta.`{{VOLUME_BASE}}/bronze/laboratory_results`;
CREATE OR REPLACE TABLE {{CATALOG}}.bronze.billing AS SELECT * FROM delta.`{{VOLUME_BASE}}/bronze/billing`;

-- Silver
CREATE OR REPLACE TABLE {{CATALOG}}.silver.patients AS SELECT * FROM delta.`{{VOLUME_BASE}}/silver/patients`;
CREATE OR REPLACE TABLE {{CATALOG}}.silver.patients_current AS SELECT * FROM delta.`{{VOLUME_BASE}}/silver/patients_current`;
CREATE OR REPLACE TABLE {{CATALOG}}.silver.doctors AS SELECT * FROM delta.`{{VOLUME_BASE}}/silver/doctors`;
CREATE OR REPLACE TABLE {{CATALOG}}.silver.appointments AS SELECT * FROM delta.`{{VOLUME_BASE}}/silver/appointments`;
CREATE OR REPLACE TABLE {{CATALOG}}.silver.insurance_claims AS SELECT * FROM delta.`{{VOLUME_BASE}}/silver/insurance_claims`;
CREATE OR REPLACE TABLE {{CATALOG}}.silver.pharmacy_orders AS SELECT * FROM delta.`{{VOLUME_BASE}}/silver/pharmacy_orders`;
CREATE OR REPLACE TABLE {{CATALOG}}.silver.laboratory_results AS SELECT * FROM delta.`{{VOLUME_BASE}}/silver/laboratory_results`;
CREATE OR REPLACE TABLE {{CATALOG}}.silver.billing AS SELECT * FROM delta.`{{VOLUME_BASE}}/silver/billing`;

-- Gold
CREATE OR REPLACE TABLE {{CATALOG}}.gold.patient_summary AS SELECT * FROM delta.`{{VOLUME_BASE}}/gold/patient_summary`;
CREATE OR REPLACE TABLE {{CATALOG}}.gold.doctor_performance AS SELECT * FROM delta.`{{VOLUME_BASE}}/gold/doctor_performance`;
CREATE OR REPLACE TABLE {{CATALOG}}.gold.revenue_analytics AS SELECT * FROM delta.`{{VOLUME_BASE}}/gold/revenue_analytics`;
CREATE OR REPLACE TABLE {{CATALOG}}.gold.hospital_revenue AS SELECT * FROM delta.`{{VOLUME_BASE}}/gold/hospital_revenue`;
CREATE OR REPLACE TABLE {{CATALOG}}.gold.insurance_analytics AS SELECT * FROM delta.`{{VOLUME_BASE}}/gold/insurance_analytics`;
CREATE OR REPLACE TABLE {{CATALOG}}.gold.appointment_analytics AS SELECT * FROM delta.`{{VOLUME_BASE}}/gold/appointment_analytics`;
CREATE OR REPLACE TABLE {{CATALOG}}.gold.monthly_revenue AS SELECT * FROM delta.`{{VOLUME_BASE}}/gold/monthly_revenue`;
CREATE OR REPLACE TABLE {{CATALOG}}.gold.daily_revenue AS SELECT * FROM delta.`{{VOLUME_BASE}}/gold/daily_revenue`;
CREATE OR REPLACE TABLE {{CATALOG}}.gold.laboratory_trends AS SELECT * FROM delta.`{{VOLUME_BASE}}/gold/laboratory_trends`;
CREATE OR REPLACE TABLE {{CATALOG}}.gold.pharmacy_sales AS SELECT * FROM delta.`{{VOLUME_BASE}}/gold/pharmacy_sales`;
CREATE OR REPLACE TABLE {{CATALOG}}.gold.patient_visit_summary AS SELECT * FROM delta.`{{VOLUME_BASE}}/gold/patient_visit_summary`;
CREATE OR REPLACE TABLE {{CATALOG}}.gold.doctor_utilization AS SELECT * FROM delta.`{{VOLUME_BASE}}/gold/doctor_utilization`;
CREATE OR REPLACE TABLE {{CATALOG}}.gold.top_diseases AS SELECT * FROM delta.`{{VOLUME_BASE}}/gold/top_diseases`;
CREATE OR REPLACE TABLE {{CATALOG}}.gold.cancelled_appointments AS SELECT * FROM delta.`{{VOLUME_BASE}}/gold/cancelled_appointments`;

-- Ops
CREATE OR REPLACE TABLE {{CATALOG}}.audit.pipeline_audit AS SELECT * FROM delta.`{{VOLUME_BASE}}/audit/pipeline_audit`;
CREATE OR REPLACE TABLE {{CATALOG}}.ops_logging.pipeline_logs AS SELECT * FROM delta.`{{VOLUME_BASE}}/ops_logging/pipeline_logs`;
CREATE OR REPLACE TABLE {{CATALOG}}.data_quality.validation_results AS SELECT * FROM delta.`{{VOLUME_BASE}}/data_quality/validation_results`;
CREATE OR REPLACE TABLE {{CATALOG}}.data_quality.failed_records AS SELECT * FROM delta.`{{VOLUME_BASE}}/data_quality/failed_records`;

-- =============================================================================
-- Table registration helpers for path-based Delta tables (non-UC / local)
-- Replace ${storage_base} with your LakehousePaths.storage_base value
-- =============================================================================

CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS bronze.patients USING DELTA LOCATION '${storage_base}/bronze/patients';
CREATE TABLE IF NOT EXISTS bronze.doctors USING DELTA LOCATION '${storage_base}/bronze/doctors';
CREATE TABLE IF NOT EXISTS bronze.appointments USING DELTA LOCATION '${storage_base}/bronze/appointments';
CREATE TABLE IF NOT EXISTS bronze.insurance_claims USING DELTA LOCATION '${storage_base}/bronze/insurance_claims';
CREATE TABLE IF NOT EXISTS bronze.pharmacy_orders USING DELTA LOCATION '${storage_base}/bronze/pharmacy_orders';
CREATE TABLE IF NOT EXISTS bronze.laboratory_results USING DELTA LOCATION '${storage_base}/bronze/laboratory_results';
CREATE TABLE IF NOT EXISTS bronze.billing USING DELTA LOCATION '${storage_base}/bronze/billing';

CREATE TABLE IF NOT EXISTS silver.patients USING DELTA LOCATION '${storage_base}/silver/patients';
CREATE TABLE IF NOT EXISTS silver.patients_current USING DELTA LOCATION '${storage_base}/silver/patients_current';
CREATE TABLE IF NOT EXISTS silver.doctors USING DELTA LOCATION '${storage_base}/silver/doctors';
CREATE TABLE IF NOT EXISTS silver.appointments USING DELTA LOCATION '${storage_base}/silver/appointments';
CREATE TABLE IF NOT EXISTS silver.insurance_claims USING DELTA LOCATION '${storage_base}/silver/insurance_claims';
CREATE TABLE IF NOT EXISTS silver.pharmacy_orders USING DELTA LOCATION '${storage_base}/silver/pharmacy_orders';
CREATE TABLE IF NOT EXISTS silver.laboratory_results USING DELTA LOCATION '${storage_base}/silver/laboratory_results';
CREATE TABLE IF NOT EXISTS silver.billing USING DELTA LOCATION '${storage_base}/silver/billing';

CREATE TABLE IF NOT EXISTS gold.patient_summary USING DELTA LOCATION '${storage_base}/gold/patient_summary';
CREATE TABLE IF NOT EXISTS gold.doctor_performance USING DELTA LOCATION '${storage_base}/gold/doctor_performance';
CREATE TABLE IF NOT EXISTS gold.revenue_analytics USING DELTA LOCATION '${storage_base}/gold/revenue_analytics';
CREATE TABLE IF NOT EXISTS gold.hospital_revenue USING DELTA LOCATION '${storage_base}/gold/hospital_revenue';
CREATE TABLE IF NOT EXISTS gold.insurance_analytics USING DELTA LOCATION '${storage_base}/gold/insurance_analytics';
CREATE TABLE IF NOT EXISTS gold.appointment_analytics USING DELTA LOCATION '${storage_base}/gold/appointment_analytics';
CREATE TABLE IF NOT EXISTS gold.monthly_revenue USING DELTA LOCATION '${storage_base}/gold/monthly_revenue';
CREATE TABLE IF NOT EXISTS gold.daily_revenue USING DELTA LOCATION '${storage_base}/gold/daily_revenue';
CREATE TABLE IF NOT EXISTS gold.laboratory_trends USING DELTA LOCATION '${storage_base}/gold/laboratory_trends';
CREATE TABLE IF NOT EXISTS gold.pharmacy_sales USING DELTA LOCATION '${storage_base}/gold/pharmacy_sales';
CREATE TABLE IF NOT EXISTS gold.patient_visit_summary USING DELTA LOCATION '${storage_base}/gold/patient_visit_summary';
CREATE TABLE IF NOT EXISTS gold.doctor_utilization USING DELTA LOCATION '${storage_base}/gold/doctor_utilization';
CREATE TABLE IF NOT EXISTS gold.top_diseases USING DELTA LOCATION '${storage_base}/gold/top_diseases';
CREATE TABLE IF NOT EXISTS gold.cancelled_appointments USING DELTA LOCATION '${storage_base}/gold/cancelled_appointments';

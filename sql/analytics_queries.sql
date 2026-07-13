-- =============================================================================
-- Healthcare Lakehouse — SQL Analytics Scripts
-- Run against Gold / Silver Delta tables (Databricks SQL warehouse or Spark SQL)
-- =============================================================================

-- Register path-based tables (adjust storage base for your environment)
-- CREATE DATABASE IF NOT EXISTS gold;
-- CREATE TABLE IF NOT EXISTS gold.doctor_performance USING DELTA LOCATION '${storage_base}/gold/doctor_performance';

-- -----------------------------------------------------------------------------
-- 1. Top 10 Doctors by attributed revenue
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.v_top_10_doctors AS
SELECT
  DoctorID,
  DoctorName,
  Specialization,
  Department,
  Hospital,
  TotalAppointments,
  CompletedAppointments,
  UniquePatients,
  AttributedRevenue,
  CompletionRatePct
FROM gold.doctor_performance
ORDER BY AttributedRevenue DESC
LIMIT 10;

SELECT * FROM gold.v_top_10_doctors;

-- -----------------------------------------------------------------------------
-- 2. Monthly Revenue
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.v_monthly_revenue AS
SELECT
  PaymentYear,
  PaymentMonth,
  PaymentYearMonth,
  TotalRevenue,
  CollectedRevenue,
  InvoiceCount,
  AvgInvoiceAmount,
  ROUND(CollectedRevenue / NULLIF(TotalRevenue, 0) * 100, 2) AS CollectionRatePct
FROM gold.monthly_revenue
ORDER BY PaymentYear, PaymentMonth;

SELECT * FROM gold.v_monthly_revenue;

-- -----------------------------------------------------------------------------
-- 3. Insurance Approval Percentage
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.v_insurance_approval_pct AS
SELECT
  InsuranceCompany,
  TotalClaims,
  ApprovedCount,
  DeniedCount,
  PendingCount,
  ApprovalRatePct,
  TotalClaimAmount,
  AvgClaimAmount
FROM gold.insurance_analytics
ORDER BY ApprovalRatePct DESC;

SELECT * FROM gold.v_insurance_approval_pct;

-- -----------------------------------------------------------------------------
-- 4. Revenue by Department
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.v_revenue_by_department AS
SELECT
  Department,
  SUM(TotalAmount) AS TotalRevenue,
  SUM(CASE WHEN PaymentStatus = 'Paid' THEN TotalAmount ELSE 0 END) AS CollectedRevenue,
  COUNT(DISTINCT InvoiceID) AS InvoiceCount,
  COUNT(DISTINCT PatientID) AS PatientCount,
  ROUND(AVG(TotalAmount), 2) AS AvgInvoiceAmount
FROM gold.revenue_analytics
WHERE Department IS NOT NULL
GROUP BY Department
ORDER BY TotalRevenue DESC;

SELECT * FROM gold.v_revenue_by_department;

-- -----------------------------------------------------------------------------
-- 5. Patient Growth (new patients by month from CreatedDate)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.v_patient_growth AS
SELECT
  date_format(CreatedDate, 'yyyy-MM') AS CohortMonth,
  COUNT(*) AS NewPatients
FROM silver.patients_current
GROUP BY date_format(CreatedDate, 'yyyy-MM')
ORDER BY CohortMonth;

-- Fallback using first visit if CreatedDate not projected to current view:
CREATE OR REPLACE VIEW gold.v_patient_growth_by_first_visit AS
SELECT
  date_format(FirstVisitDate, 'yyyy-MM') AS CohortMonth,
  COUNT(*) AS NewPatients
FROM gold.patient_summary
WHERE FirstVisitDate IS NOT NULL
GROUP BY date_format(FirstVisitDate, 'yyyy-MM')
ORDER BY CohortMonth;

SELECT * FROM gold.v_patient_growth_by_first_visit;

-- -----------------------------------------------------------------------------
-- 6. Daily Appointments
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.v_daily_appointments AS
SELECT
  AppointmentDay,
  SUM(AppointmentCount) AS TotalAppointments,
  SUM(CASE WHEN Status = 'Completed' THEN AppointmentCount ELSE 0 END) AS Completed,
  SUM(CASE WHEN Status = 'Cancelled' THEN AppointmentCount ELSE 0 END) AS Cancelled,
  SUM(CASE WHEN Status = 'NoShow' THEN AppointmentCount ELSE 0 END) AS NoShows,
  SUM(UniquePatients) AS UniquePatients
FROM gold.appointment_analytics
GROUP BY AppointmentDay
ORDER BY AppointmentDay;

SELECT * FROM gold.v_daily_appointments;

-- -----------------------------------------------------------------------------
-- 7. Average Billing
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.v_average_billing AS
SELECT
  ROUND(AVG(TotalAmount), 2) AS AvgInvoiceAmount,
  ROUND(PERCENTILE_APPROX(TotalAmount, 0.5), 2) AS MedianInvoiceAmount,
  ROUND(MIN(TotalAmount), 2) AS MinInvoiceAmount,
  ROUND(MAX(TotalAmount), 2) AS MaxInvoiceAmount,
  COUNT(*) AS InvoiceCount,
  ROUND(SUM(TotalAmount), 2) AS TotalBilled
FROM gold.revenue_analytics;

SELECT * FROM gold.v_average_billing;

-- -----------------------------------------------------------------------------
-- 8. Lab Trends
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.v_lab_trends AS
SELECT
  TestName,
  TestCount,
  AbnormalCount,
  AbnormalRatePct
FROM gold.laboratory_trends
ORDER BY AbnormalRatePct DESC;

SELECT * FROM gold.v_lab_trends;

-- -----------------------------------------------------------------------------
-- 9. Prescription Trends
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.v_prescription_trends AS
SELECT
  Medicine,
  TotalQuantity,
  TotalSales,
  UniquePatients,
  PrescriptionCount,
  AvgUnitPrice
FROM gold.pharmacy_sales
ORDER BY TotalSales DESC;

SELECT * FROM gold.v_prescription_trends;

-- -----------------------------------------------------------------------------
-- 10. Hospital network executive KPIs
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.v_executive_kpis AS
SELECT
  (SELECT COUNT(*) FROM gold.patient_summary) AS TotalPatients,
  (SELECT COUNT(*) FROM gold.doctor_performance) AS TotalDoctors,
  (SELECT SUM(TotalRevenue) FROM gold.hospital_revenue) AS NetworkRevenue,
  (SELECT SUM(CollectedRevenue) FROM gold.hospital_revenue) AS NetworkCollected,
  (SELECT SUM(CancelledAppointments) FROM gold.patient_summary) AS CancelledAppointments,
  (SELECT AVG(ApprovalRatePct) FROM gold.insurance_analytics) AS AvgInsuranceApprovalPct;

SELECT * FROM gold.v_executive_kpis;

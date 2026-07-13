# Databricks SQL Dashboard Definitions
# Import these queries into Databricks SQL dashboards / Lakeview.

## Executive Dashboard
**Purpose:** Network-wide KPIs for hospital leadership.

### Widgets
1. **KPI — Total Patients**  
   `SELECT COUNT(*) AS total_patients FROM gold.patient_summary;`

2. **KPI — Network Revenue**  
   `SELECT ROUND(SUM(TotalRevenue), 2) AS network_revenue FROM gold.hospital_revenue;`

3. **KPI — Collection Rate**  
   ```sql
   SELECT ROUND(SUM(CollectedRevenue) / NULLIF(SUM(TotalRevenue), 0) * 100, 2) AS collection_rate_pct
   FROM gold.hospital_revenue;
   ```

4. **KPI — Avg Insurance Approval %**  
   `SELECT ROUND(AVG(ApprovalRatePct), 2) AS avg_approval_pct FROM gold.insurance_analytics;`

5. **Chart — Monthly Revenue Trend**  
   ```sql
   SELECT PaymentYearMonth, TotalRevenue, CollectedRevenue
   FROM gold.monthly_revenue
   ORDER BY PaymentYearMonth;
   ```

6. **Chart — Revenue by Hospital**  
   ```sql
   SELECT Hospital, TotalRevenue, CollectedRevenue, PendingRevenue
   FROM gold.hospital_revenue
   ORDER BY TotalRevenue DESC;
   ```

7. **Table — Top Diseases**  
   `SELECT * FROM gold.top_diseases LIMIT 15;`

---

## Hospital Dashboard
**Purpose:** Facility operations and department performance.

### Widgets
1. Hospital revenue breakdown — `gold.hospital_revenue`
2. Appointments by day/status — `gold.appointment_analytics`
3. Cancelled appointments detail — `gold.cancelled_appointments`
4. Department revenue — `gold.v_revenue_by_department`
5. Doctor utilization by hospital — `gold.doctor_utilization`

```sql
SELECT Hospital, Department, DoctorName, UtilizationPct, AttributedRevenue
FROM gold.doctor_utilization
ORDER BY Hospital, UtilizationPct DESC;
```

---

## Finance Dashboard
**Purpose:** RCM / billing analytics.

### Widgets
1. Daily revenue — `gold.daily_revenue`
2. Monthly revenue + collection rate — `gold.v_monthly_revenue`
3. Average / median billing — `gold.v_average_billing`
4. Payment status mix:
   ```sql
   SELECT PaymentStatus, COUNT(*) AS invoices, ROUND(SUM(TotalAmount), 2) AS amount
   FROM gold.revenue_analytics
   GROUP BY PaymentStatus;
   ```
5. Outstanding AR (Pending + Overdue):
   ```sql
   SELECT ROUND(SUM(TotalAmount), 2) AS outstanding_ar
   FROM gold.revenue_analytics
   WHERE PaymentStatus IN ('Pending', 'Overdue', 'Partial');
   ```

---

## Doctor Dashboard
**Purpose:** Provider productivity and quality of care proxies.

### Widgets
1. Top 10 doctors — `gold.v_top_10_doctors`
2. Completion / no-show rates — `gold.doctor_performance`
3. Utilization — `gold.doctor_utilization`
4. Attributed revenue by specialization:
   ```sql
   SELECT Specialization,
          SUM(AttributedRevenue) AS revenue,
          SUM(TotalAppointments) AS appointments
   FROM gold.doctor_performance
   GROUP BY Specialization
   ORDER BY revenue DESC;
   ```

---

## Insurance Dashboard
**Purpose:** Payer performance and denial monitoring.

### Widgets
1. Approval % by payer — `gold.v_insurance_approval_pct`
2. Claim volume & amount by company — `gold.insurance_analytics`
3. Denied claim dollars:
   ```sql
   SELECT InsuranceCompany,
          DeniedCount,
          ROUND(TotalClaimAmount * (DeniedCount / NULLIF(TotalClaims, 0)), 2) AS estimated_denied_amount
   FROM gold.insurance_analytics
   ORDER BY DeniedCount DESC;
   ```

---

## Patient Dashboard
**Purpose:** Population health & engagement.

### Widgets
1. Patient summary KPIs — visit counts, lifetime billed
2. Visit summary by age/gender — `gold.patient_visit_summary`
3. Patient growth cohorts — `gold.v_patient_growth_by_first_visit`
4. High utilizers:
   ```sql
   SELECT PatientID, FirstName, LastName, TotalAppointments, LifetimeBilled, CancelledAppointments
   FROM gold.patient_summary
   ORDER BY TotalAppointments DESC
   LIMIT 25;
   ```
5. Lab abnormal rates — `gold.v_lab_trends`
6. Pharmacy spend leaders — `gold.v_prescription_trends`

---

## Lakeview / SQL Dashboard Import Notes
1. Create a Databricks SQL warehouse.
2. Run `sql/register_tables.sql` (replace `${storage_base}`).
3. Run `sql/analytics_queries.sql` to create views.
4. Create six Lakeview dashboards and paste widget SQL above.
5. Schedule dashboard refresh aligned with the Gold job (e.g., hourly / nightly).

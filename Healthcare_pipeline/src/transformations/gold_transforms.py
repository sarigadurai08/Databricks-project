"""
Gold-layer analytical table builders for clinical and financial KPIs.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utilities.dataframe_utils import cache_df


def build_patient_summary(
    patients: DataFrame,
    appointments: DataFrame,
    billing: DataFrame,
    claims: DataFrame,
) -> DataFrame:
    appt_agg = appointments.groupBy("PatientID").agg(
        F.count("*").alias("TotalAppointments"),
        F.sum(F.when(F.col("Status") == "Completed", 1).otherwise(0)).alias("CompletedAppointments"),
        F.sum(F.when(F.col("Status") == "Cancelled", 1).otherwise(0)).alias("CancelledAppointments"),
        F.max("AppointmentDate").alias("LastVisitDate"),
        F.min("AppointmentDate").alias("FirstVisitDate"),
    )
    bill_agg = billing.groupBy("PatientID").agg(
        F.sum("TotalAmount").alias("LifetimeBilled"),
        F.sum(F.when(F.col("PaymentStatus") == "Paid", F.col("TotalAmount")).otherwise(0)).alias(
            "LifetimePaid"
        ),
    )
    claim_agg = claims.groupBy("PatientID").agg(
        F.count("*").alias("TotalClaims"),
        F.sum("ClaimAmount").alias("TotalClaimAmount"),
        F.sum(F.when(F.col("ApprovalStatus") == "Approved", 1).otherwise(0)).alias("ApprovedClaims"),
    )

    result = (
        patients.alias("p")
        .join(appt_agg.alias("a"), on="PatientID", how="left")
        .join(bill_agg.alias("b"), on="PatientID", how="left")
        .join(claim_agg.alias("c"), on="PatientID", how="left")
        .select(
            "PatientID",
            "FirstName",
            "LastName",
            "Gender",
            "DOB",
            "InsuranceID",
            F.coalesce(F.col("TotalAppointments"), F.lit(0)).alias("TotalAppointments"),
            F.coalesce(F.col("CompletedAppointments"), F.lit(0)).alias("CompletedAppointments"),
            F.coalesce(F.col("CancelledAppointments"), F.lit(0)).alias("CancelledAppointments"),
            "LastVisitDate",
            "FirstVisitDate",
            F.coalesce(F.col("LifetimeBilled"), F.lit(0)).alias("LifetimeBilled"),
            F.coalesce(F.col("LifetimePaid"), F.lit(0)).alias("LifetimePaid"),
            F.coalesce(F.col("TotalClaims"), F.lit(0)).alias("TotalClaims"),
            F.coalesce(F.col("TotalClaimAmount"), F.lit(0)).alias("TotalClaimAmount"),
            F.coalesce(F.col("ApprovedClaims"), F.lit(0)).alias("ApprovedClaims"),
        )
        .withColumn(
            "Age",
            F.floor(F.months_between(F.current_date(), F.col("DOB")) / 12).cast("int"),
        )
    )
    return result


def build_doctor_performance(
    doctors: DataFrame,
    appointments: DataFrame,
    billing: DataFrame,
) -> DataFrame:
    doctors_b = F.broadcast(doctors)
    appt = appointments.join(doctors_b, on="DoctorID", how="inner")
    # Join billing via AppointmentID for revenue attribution
    appt_bill = appt.join(
        billing.select("AppointmentID", "TotalAmount", "PaymentStatus"),
        on="AppointmentID",
        how="left",
    )
    return appt_bill.groupBy(
        "DoctorID", "DoctorName", "Specialization", "Department", "Hospital", "Experience"
    ).agg(
        F.countDistinct("AppointmentID").alias("TotalAppointments"),
        F.sum(F.when(F.col("Status") == "Completed", 1).otherwise(0)).alias("CompletedAppointments"),
        F.sum(F.when(F.col("Status") == "Cancelled", 1).otherwise(0)).alias("CancelledAppointments"),
        F.sum(F.when(F.col("Status") == "NoShow", 1).otherwise(0)).alias("NoShowAppointments"),
        F.countDistinct("PatientID").alias("UniquePatients"),
        F.coalesce(F.sum("TotalAmount"), F.lit(0)).alias("AttributedRevenue"),
        F.round(
            F.sum(F.when(F.col("Status") == "Completed", 1).otherwise(0))
            / F.countDistinct("AppointmentID")
            * 100,
            2,
        ).alias("CompletionRatePct"),
    )


def build_revenue_analytics(billing: DataFrame, appointments: DataFrame, doctors: DataFrame) -> DataFrame:
    joined = (
        billing.alias("b")
        .join(appointments.alias("a"), on="AppointmentID", how="left")
        .join(F.broadcast(doctors).alias("d"), on="DoctorID", how="left")
    )
    return joined.select(
        F.col("b.InvoiceID"),
        F.col("b.PatientID"),
        F.col("b.AppointmentID"),
        F.col("b.TotalAmount"),
        F.col("b.PaymentStatus"),
        F.col("b.PaymentDate"),
        F.col("d.Department"),
        F.col("d.Hospital"),
        F.col("d.DoctorID"),
        F.col("d.DoctorName"),
        F.year("b.PaymentDate").alias("PaymentYear"),
        F.month("b.PaymentDate").alias("PaymentMonth"),
        F.date_format("b.PaymentDate", "yyyy-MM").alias("PaymentYearMonth"),
    )


def build_hospital_revenue(revenue_analytics: DataFrame) -> DataFrame:
    return revenue_analytics.groupBy("Hospital").agg(
        F.sum("TotalAmount").alias("TotalRevenue"),
        F.sum(F.when(F.col("PaymentStatus") == "Paid", F.col("TotalAmount")).otherwise(0)).alias(
            "CollectedRevenue"
        ),
        F.sum(F.when(F.col("PaymentStatus") == "Pending", F.col("TotalAmount")).otherwise(0)).alias(
            "PendingRevenue"
        ),
        F.countDistinct("InvoiceID").alias("InvoiceCount"),
        F.countDistinct("PatientID").alias("PatientCount"),
        F.round(F.avg("TotalAmount"), 2).alias("AvgInvoiceAmount"),
    )


def build_insurance_analytics(claims: DataFrame, patients: DataFrame) -> DataFrame:
    joined = claims.join(
        patients.select("PatientID", "InsuranceID", "Gender"),
        on="PatientID",
        how="left",
    )
    return joined.groupBy("InsuranceCompany").agg(
        F.count("*").alias("TotalClaims"),
        F.sum("ClaimAmount").alias("TotalClaimAmount"),
        F.sum(F.when(F.col("ApprovalStatus") == "Approved", 1).otherwise(0)).alias("ApprovedCount"),
        F.sum(F.when(F.col("ApprovalStatus") == "Denied", 1).otherwise(0)).alias("DeniedCount"),
        F.sum(F.when(F.col("ApprovalStatus") == "Pending", 1).otherwise(0)).alias("PendingCount"),
        F.round(
            F.sum(F.when(F.col("ApprovalStatus") == "Approved", 1).otherwise(0))
            / F.count("*")
            * 100,
            2,
        ).alias("ApprovalRatePct"),
        F.round(F.avg("ClaimAmount"), 2).alias("AvgClaimAmount"),
    )


def build_appointment_analytics(appointments: DataFrame, doctors: DataFrame) -> DataFrame:
    joined = appointments.join(F.broadcast(doctors), on="DoctorID", how="left")
    return joined.withColumn("AppointmentDay", F.to_date("AppointmentDate")).groupBy(
        "AppointmentDay", "Status", "Department", "Hospital"
    ).agg(
        F.count("*").alias("AppointmentCount"),
        F.countDistinct("PatientID").alias("UniquePatients"),
        F.countDistinct("DoctorID").alias("UniqueDoctors"),
    )


def build_monthly_revenue(revenue_analytics: DataFrame) -> DataFrame:
    return revenue_analytics.groupBy("PaymentYear", "PaymentMonth", "PaymentYearMonth").agg(
        F.sum("TotalAmount").alias("TotalRevenue"),
        F.sum(F.when(F.col("PaymentStatus") == "Paid", F.col("TotalAmount")).otherwise(0)).alias(
            "CollectedRevenue"
        ),
        F.countDistinct("InvoiceID").alias("InvoiceCount"),
        F.round(F.avg("TotalAmount"), 2).alias("AvgInvoiceAmount"),
    ).orderBy("PaymentYear", "PaymentMonth")


def build_daily_revenue(revenue_analytics: DataFrame) -> DataFrame:
    return revenue_analytics.groupBy(F.col("PaymentDate").alias("RevenueDate")).agg(
        F.sum("TotalAmount").alias("TotalRevenue"),
        F.countDistinct("InvoiceID").alias("InvoiceCount"),
        F.countDistinct("PatientID").alias("PatientCount"),
    ).orderBy("RevenueDate")


def build_laboratory_trends(labs: DataFrame) -> DataFrame:
    return labs.groupBy("TestName").agg(
        F.count("*").alias("TestCount"),
        F.sum(F.when(F.col("IsAbnormal") == True, 1).otherwise(0)).alias("AbnormalCount"),  # noqa: E712
        F.round(
            F.sum(F.when(F.col("IsAbnormal") == True, 1).otherwise(0)) / F.count("*") * 100,  # noqa: E712
            2,
        ).alias("AbnormalRatePct"),
    ).orderBy(F.desc("TestCount"))


def build_pharmacy_sales(pharmacy: DataFrame) -> DataFrame:
    return pharmacy.groupBy("Medicine").agg(
        F.sum("Quantity").alias("TotalQuantity"),
        F.sum("LineAmount").alias("TotalSales"),
        F.countDistinct("PatientID").alias("UniquePatients"),
        F.countDistinct("PrescriptionID").alias("PrescriptionCount"),
        F.round(F.avg("Price"), 2).alias("AvgUnitPrice"),
    ).orderBy(F.desc("TotalSales"))


def build_patient_visit_summary(appointments: DataFrame, patients: DataFrame) -> DataFrame:
    return (
        appointments.join(patients.select("PatientID", "Gender", "DOB"), on="PatientID", how="left")
        .withColumn("VisitDate", F.to_date("AppointmentDate"))
        .withColumn(
            "AgeGroup",
            F.when(F.floor(F.months_between(F.current_date(), F.col("DOB")) / 12) < 18, "0-17")
            .when(F.floor(F.months_between(F.current_date(), F.col("DOB")) / 12) < 40, "18-39")
            .when(F.floor(F.months_between(F.current_date(), F.col("DOB")) / 12) < 65, "40-64")
            .otherwise("65+"),
        )
        .groupBy("VisitDate", "Gender", "AgeGroup", "Status")
        .agg(F.count("*").alias("VisitCount"))
    )


def build_doctor_utilization(doctor_performance: DataFrame) -> DataFrame:
    return doctor_performance.select(
        "DoctorID",
        "DoctorName",
        "Department",
        "Hospital",
        "TotalAppointments",
        "CompletedAppointments",
        "CancelledAppointments",
        "NoShowAppointments",
        "CompletionRatePct",
        "UniquePatients",
        "AttributedRevenue",
        F.round(
            F.col("CompletedAppointments")
            / F.when(F.col("TotalAppointments") == 0, F.lit(1)).otherwise(F.col("TotalAppointments"))
            * 100,
            2,
        ).alias("UtilizationPct"),
    )


def build_top_diseases(appointments: DataFrame) -> DataFrame:
    return (
        appointments.filter(F.col("Diagnosis").isNotNull() & (F.trim(F.col("Diagnosis")) != ""))
        .groupBy("Diagnosis")
        .agg(
            F.count("*").alias("DiagnosisCount"),
            F.countDistinct("PatientID").alias("UniquePatients"),
            F.countDistinct("DoctorID").alias("UniqueDoctors"),
        )
        .orderBy(F.desc("DiagnosisCount"))
    )


def build_cancelled_appointments(appointments: DataFrame, doctors: DataFrame, patients: DataFrame) -> DataFrame:
    return (
        appointments.filter(F.col("Status") == "Cancelled")
        .join(F.broadcast(doctors.select("DoctorID", "DoctorName", "Department", "Hospital")), on="DoctorID", how="left")
        .join(patients.select("PatientID", "FirstName", "LastName"), on="PatientID", how="left")
        .select(
            "AppointmentID",
            "AppointmentDate",
            "PatientID",
            "FirstName",
            "LastName",
            "DoctorID",
            "DoctorName",
            "Department",
            "Hospital",
            "Diagnosis",
        )
    )


def build_all_gold_tables(
    spark: SparkSession,
    patients: DataFrame,
    doctors: DataFrame,
    appointments: DataFrame,
    claims: DataFrame,
    pharmacy: DataFrame,
    labs: DataFrame,
    billing: DataFrame,
) -> dict[str, DataFrame]:
    """Build the full gold mart dictionary used by the Gold notebook."""
    patients_c = cache_df(patients)
    doctors_c = cache_df(doctors)
    appointments_c = cache_df(appointments)
    billing_c = cache_df(billing)

    patient_summary = build_patient_summary(patients_c, appointments_c, billing_c, claims)
    doctor_performance = build_doctor_performance(doctors_c, appointments_c, billing_c)
    revenue_analytics = build_revenue_analytics(billing_c, appointments_c, doctors_c)

    tables = {
        "patient_summary": patient_summary,
        "doctor_performance": doctor_performance,
        "revenue_analytics": revenue_analytics,
        "hospital_revenue": build_hospital_revenue(revenue_analytics),
        "insurance_analytics": build_insurance_analytics(claims, patients_c),
        "appointment_analytics": build_appointment_analytics(appointments_c, doctors_c),
        "monthly_revenue": build_monthly_revenue(revenue_analytics),
        "daily_revenue": build_daily_revenue(revenue_analytics),
        "laboratory_trends": build_laboratory_trends(labs),
        "pharmacy_sales": build_pharmacy_sales(pharmacy),
        "patient_visit_summary": build_patient_visit_summary(appointments_c, patients_c),
        "doctor_utilization": build_doctor_utilization(doctor_performance),
        "top_diseases": build_top_diseases(appointments_c),
        "cancelled_appointments": build_cancelled_appointments(appointments_c, doctors_c, patients_c),
    }
    return tables

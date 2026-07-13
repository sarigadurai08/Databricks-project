"""Unit tests for silver transformation cleaners."""

from __future__ import annotations

from pyspark.sql import functions as F

from src.transformations.silver_transforms import clean_appointments, clean_doctors, clean_patients


def test_clean_patients_dedupes_and_standardizes(sample_patients):
    result = clean_patients(sample_patients)
    assert result.count() == 3  # PAT000002 deduped to latest ModifiedDate
    alan = result.filter(F.col("PatientID") == "PAT000002").collect()[0]
    assert "5550199" in alan["Phone"].replace("-", "").replace(" ", "")
    assert alan["Email"] == "alan@example.com"
    # Gender standardization / invalid email patient retained but email lowercased
    assert "Gender" in result.columns


def test_clean_patients_drops_null_patient_id(spark):
    df = spark.createDataFrame(
        [(None, "X", "Y", "1990-01-01", "Male", "1", "a@b.com", "addr", "INS1", "2024-01-01 00:00:00", "2024-01-02 00:00:00")],
        ["PatientID", "FirstName", "LastName", "DOB", "Gender", "Phone", "Email", "Address", "InsuranceID", "CreatedDate", "ModifiedDate"],
    )
    assert clean_patients(df).count() == 0


def test_clean_doctors_experience_floor(sample_doctors, spark):
    bad = spark.createDataFrame(
        [("DOC0003", "Dr. Neg", "Cardiology", "Cardiology", "Mercy General Hospital", -5)],
        sample_doctors.columns,
    )
    result = clean_doctors(sample_doctors.unionByName(bad))
    neg = result.filter(F.col("DoctorID") == "DOC0003").collect()[0]
    assert neg["Experience"] == 0
    assert result.count() == 3


def test_clean_appointments_status_default(sample_appointments, spark):
    bad = spark.createDataFrame(
        [("APT000099", "PAT000001", "DOC0001", "2024-06-01 09:00:00", "WeirdStatus", "Flu")],
        sample_appointments.columns,
    )
    result = clean_appointments(sample_appointments.unionByName(bad))
    weird = result.filter(F.col("AppointmentID") == "APT000099").collect()[0]
    assert weird["Status"] == "Scheduled"

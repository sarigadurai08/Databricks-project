"""Unit tests for SCD Type 1 and Type 2 MERGE logic."""

from __future__ import annotations

from pyspark.sql import functions as F

from src.transformations.scd import apply_scd_type1, apply_scd_type2


def test_scd_type1_insert_and_update(spark, sample_doctors, tmp_delta_dir):
    target = f"{tmp_delta_dir}/doctors_scd1"
    apply_scd_type1(spark, sample_doctors, target, "DoctorID")
    assert spark.read.format("delta").load(target).count() == 2

    updated = spark.createDataFrame(
        [("DOC0001", "Dr. House Updated", "Internal Medicine", "Internal Medicine", "Mercy General Hospital", 21)],
        sample_doctors.columns,
    )
    apply_scd_type1(
        spark,
        updated,
        target,
        "DoctorID",
        compare_columns=["DoctorName", "Experience"],
    )
    row = spark.read.format("delta").load(target).filter(F.col("DoctorID") == "DOC0001").collect()[0]
    assert row["DoctorName"] == "Dr. House Updated"
    assert row["Experience"] == 21
    assert spark.read.format("delta").load(target).count() == 2


def test_scd_type2_versioning(spark, tmp_delta_dir):
    target = f"{tmp_delta_dir}/patients_scd2"
    cols = [
        "PatientID", "FirstName", "LastName", "Phone", "Email", "Address", "InsuranceID", "Gender",
        "DOB", "CreatedDate", "ModifiedDate",
    ]
    v1 = spark.createDataFrame(
        [("PAT1", "Ada", "Lovelace", "111", "ada@ex.com", "Addr1", "INS1", "Female", "1815-12-10", "2024-01-01 00:00:00", "2024-01-01 00:00:00")],
        cols,
    )
    apply_scd_type2(
        spark,
        v1,
        target,
        "PatientID",
        tracked_columns=["Address", "Phone", "InsuranceID"],
    )
    cur = spark.read.format("delta").load(target).filter(F.col("IsCurrent") == True)  # noqa: E712
    assert cur.count() == 1
    assert cur.collect()[0]["VersionNumber"] == 1

    v2 = spark.createDataFrame(
        [("PAT1", "Ada", "Lovelace", "222", "ada@ex.com", "Addr2", "INS1", "Female", "1815-12-10", "2024-01-01 00:00:00", "2024-02-01 00:00:00")],
        cols,
    )
    apply_scd_type2(
        spark,
        v2,
        target,
        "PatientID",
        tracked_columns=["Address", "Phone", "InsuranceID"],
    )
    all_rows = spark.read.format("delta").load(target)
    assert all_rows.count() == 2
    current = all_rows.filter(F.col("IsCurrent") == True).collect()[0]  # noqa: E712
    expired = all_rows.filter(F.col("IsCurrent") == False).collect()[0]  # noqa: E712
    assert current["VersionNumber"] == 2
    assert current["Address"] == "Addr2"
    assert expired["Address"] == "Addr1"
    assert expired["EffectiveEndDate"] is not None


def test_scd_type2_new_key(spark, tmp_delta_dir):
    target = f"{tmp_delta_dir}/patients_scd2_new"
    cols = [
        "PatientID", "FirstName", "LastName", "Phone", "Email", "Address", "InsuranceID", "Gender",
        "DOB", "CreatedDate", "ModifiedDate",
    ]
    v1 = spark.createDataFrame(
        [("PAT1", "Ada", "Lovelace", "111", "ada@ex.com", "Addr1", "INS1", "Female", "1815-12-10", "2024-01-01 00:00:00", "2024-01-01 00:00:00")],
        cols,
    )
    apply_scd_type2(spark, v1, target, "PatientID", ["Address"])
    v2 = spark.createDataFrame(
        [
            ("PAT1", "Ada", "Lovelace", "111", "ada@ex.com", "Addr1", "INS1", "Female", "1815-12-10", "2024-01-01 00:00:00", "2024-01-01 00:00:00"),
            ("PAT2", "Alan", "Turing", "333", "alan@ex.com", "Addr9", "INS2", "Male", "1912-06-23", "2024-01-01 00:00:00", "2024-01-01 00:00:00"),
        ],
        cols,
    )
    apply_scd_type2(spark, v2, target, "PatientID", ["Address"])
    assert spark.read.format("delta").load(target).filter(F.col("IsCurrent") == True).count() == 2  # noqa: E712

"""Unit tests for the data quality framework."""

from __future__ import annotations

from pyspark.sql import functions as F

from src.utilities.data_quality import DataQualityFramework, Severity


def test_not_null_and_unique_rules(spark, tmp_path, monkeypatch):
    # Redirect DQ output paths into temp dir
    from config import paths as paths_mod

    base = str(tmp_path / "dq").replace("\\", "/")
    monkeypatch.setattr(paths_mod.PATHS, "storage_base", base)

    df = spark.createDataFrame(
        [
            ("A", "x@y.com"),
            ("A", "dup@y.com"),
            (None, "z@y.com"),
        ],
        ["PatientID", "Email"],
    )
    dq = (
        DataQualityFramework(spark, "patients", "run-test")
        .require_not_null(["PatientID"])
        .require_unique(["PatientID"])
        .require_regex("Email", r"^[^@]+@[^@]+\.[^@]+$")
    )
    results = {r.rule_name: r for r in dq.validate(df)}
    assert results["not_null_PatientID"].failed_count == 1
    assert results["unique_PatientID"].failed_count >= 2
    assert results["not_null_PatientID"].status == "FAILED"


def test_range_and_values_in(spark, tmp_path, monkeypatch):
    from config import paths as paths_mod

    monkeypatch.setattr(paths_mod.PATHS, "storage_base", str(tmp_path / "dq2").replace("\\", "/"))

    df = spark.createDataFrame(
        [("C1", 50.0, "Approved"), ("C2", -1.0, "Nope"), ("C3", 10.0, "Denied")],
        ["ClaimID", "ClaimAmount", "ApprovalStatus"],
    )
    dq = (
        DataQualityFramework(spark, "claims", "run-2")
        .require_range("ClaimAmount", min_value=0)
        .require_values_in("ApprovalStatus", {"Approved", "Denied", "Pending"})
    )
    results = {r.rule_name: r for r in dq.validate(df)}
    assert results["range_ClaimAmount"].failed_count == 1
    assert results["valid_values_ApprovalStatus"].failed_count == 1


def test_foreign_key_rule(spark, tmp_path, monkeypatch, sample_patients, sample_appointments):
    from config import paths as paths_mod

    monkeypatch.setattr(paths_mod.PATHS, "storage_base", str(tmp_path / "dq3").replace("\\", "/"))

    patients = sample_patients.select("PatientID").dropDuplicates()
    # Add orphan appointment
    orphan = spark.createDataFrame(
        [("APT999", "PAT_MISSING", "DOC0001", "2024-05-01 09:00:00", "Completed", "X")],
        sample_appointments.columns,
    )
    appts = sample_appointments.unionByName(orphan)

    dq = DataQualityFramework(spark, "appointments", "run-fk").require_fk(
        "PatientID", patients, "PatientID"
    )
    results = {r.rule_name: r for r in dq.validate(appts)}
    assert results["fk_PatientID_to_PatientID"].failed_count >= 1

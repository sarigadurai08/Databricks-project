"""
Pytest configuration and shared Spark fixture for unit tests.

Uses a local SparkSession with Delta Lake when delta-spark is installed.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def spark():
    from src.utilities.spark_session import get_spark, stop_spark

    session = get_spark("HealthcareLakehouseTests")
    yield session
    stop_spark(session)


@pytest.fixture
def sample_patients(spark):
    data = [
        ("PAT000001", "Ada", "Lovelace", "1815-12-10", "Female", "+1-555-0100", "ada@example.com", "1 Analytical Way", "INS00001", "2024-01-01 10:00:00", "2024-06-01 10:00:00"),
        ("PAT000002", "Alan", "Turing", "1912-06-23", "Male", "+1-555-0101", "alan@example.com", "2 Enigma Rd", "INS00002", "2024-01-02 10:00:00", "2024-06-02 10:00:00"),
        ("PAT000002", "Alan", "Turing", "1912-06-23", "Male", "+1-555-0199", "alan@example.com", "2 Enigma Rd", "INS00002", "2024-01-02 10:00:00", "2024-07-01 10:00:00"),  # duplicate newer
        ("PAT000003", None, "NoName", "1990-01-01", "Other", "+1-555-0102", "bad-email", "3 Null St", "INS00003", "2024-01-03 10:00:00", "2024-06-03 10:00:00"),
    ]
    cols = [
        "PatientID", "FirstName", "LastName", "DOB", "Gender", "Phone", "Email",
        "Address", "InsuranceID", "CreatedDate", "ModifiedDate",
    ]
    return spark.createDataFrame(data, cols)


@pytest.fixture
def sample_doctors(spark):
    data = [
        ("DOC0001", "Dr. House", "Internal Medicine", "Internal Medicine", "Mercy General Hospital", 20),
        ("DOC0002", "Dr. Grey", "General Surgery", "Surgery", "St. Luke Medical Center", 12),
    ]
    cols = ["DoctorID", "DoctorName", "Specialization", "Department", "Hospital", "Experience"]
    return spark.createDataFrame(data, cols)


@pytest.fixture
def sample_appointments(spark):
    data = [
        ("APT000001", "PAT000001", "DOC0001", "2024-05-01 09:00:00", "Completed", "Hypertension"),
        ("APT000002", "PAT000002", "DOC0002", "2024-05-02 10:00:00", "Cancelled", ""),
        ("APT000003", "PAT000001", "DOC0001", "2024-05-03 11:00:00", "Completed", "Migraine"),
    ]
    cols = ["AppointmentID", "PatientID", "DoctorID", "AppointmentDate", "Status", "Diagnosis"]
    return spark.createDataFrame(data, cols)


@pytest.fixture
def tmp_delta_dir(tmp_path):
    return str(tmp_path / "delta").replace("\\", "/")

"""
Generate realistic healthcare sample datasets for the Lakehouse portfolio.

Produces CSV (and JSON) files under datasets/ plus Auto Loader landing copies.
Run:  python scripts/generate_datasets.py
"""

from __future__ import annotations

import json
import random
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from faker import Faker

# Allow running from project root
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config.config import CONFIG  # noqa: E402
from config.constants import (  # noqa: E402
    DEPARTMENTS,
    HOSPITALS,
    INSURANCE_COMPANIES,
    SPECIALIZATIONS,
    AppointmentStatus,
    ClaimApprovalStatus,
    Gender,
    PaymentStatus,
)
from config.paths import PATHS  # noqa: E402

fake = Faker()
Faker.seed(42)
random.seed(42)

DIAGNOSES = [
    "Hypertension",
    "Type 2 Diabetes",
    "Acute Bronchitis",
    "Migraine",
    "Lower Back Pain",
    "Coronary Artery Disease",
    "Asthma",
    "Osteoarthritis",
    "Depression",
    "GERD",
    "Hypothyroidism",
    "Urinary Tract Infection",
    "Pneumonia",
    "Anemia",
    "Chronic Kidney Disease",
    "Allergic Rhinitis",
    "Anxiety Disorder",
    "Atrial Fibrillation",
    "COPD",
    "Hyperlipidemia",
]

MEDICINES = [
    ("Lisinopril", 12.50),
    ("Metformin", 8.75),
    ("Atorvastatin", 15.20),
    ("Amlodipine", 9.40),
    ("Omeprazole", 11.00),
    ("Albuterol", 28.50),
    ("Levothyroxine", 14.10),
    ("Gabapentin", 18.60),
    ("Sertraline", 16.80),
    ("Losartan", 13.25),
    ("Amoxicillin", 7.90),
    ("Ibuprofen", 5.50),
    ("Prednisone", 10.30),
    ("Insulin Glargine", 95.00),
    ("Clopidogrel", 22.40),
]

LAB_TESTS = [
    ("Complete Blood Count", "4.0-11.0", "WBC"),
    ("Hemoglobin A1c", "4.0-5.6", "%"),
    ("LDL Cholesterol", "0-100", "mg/dL"),
    ("HDL Cholesterol", "40-60", "mg/dL"),
    ("Serum Creatinine", "0.6-1.3", "mg/dL"),
    ("TSH", "0.4-4.0", "mIU/L"),
    ("Fasting Glucose", "70-99", "mg/dL"),
    ("Vitamin D", "30-100", "ng/mL"),
    ("CRP", "0-5", "mg/L"),
    ("Platelet Count", "150-450", "K/uL"),
]


def _id(prefix: str, n: int, width: int = 6) -> str:
    return f"{prefix}{n:0{width}d}"


def generate_patients(n: int) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        created = fake.date_time_between(start_date="-3y", end_date="-30d")
        modified = created + timedelta(days=random.randint(0, 400))
        if modified.date() > date.today():
            modified = datetime.combine(date.today(), created.time())
        rows.append(
            {
                "PatientID": _id("PAT", i),
                "FirstName": fake.first_name(),
                "LastName": fake.last_name(),
                "DOB": fake.date_of_birth(minimum_age=1, maximum_age=95).isoformat(),
                "Gender": random.choice(list(Gender)).value,
                "Phone": fake.numerify(text="+1-###-###-####"),
                "Email": fake.email(),
                "Address": fake.address().replace("\n", ", "),
                "InsuranceID": _id("INS", random.randint(1, 200), 5),
                "CreatedDate": created.isoformat(sep=" ", timespec="seconds"),
                "ModifiedDate": modified.isoformat(sep=" ", timespec="seconds"),
            }
        )
    return rows


def generate_doctors(n: int) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        spec = SPECIALIZATIONS[(i - 1) % len(SPECIALIZATIONS)]
        dept = DEPARTMENTS[(i - 1) % len(DEPARTMENTS)]
        rows.append(
            {
                "DoctorID": _id("DOC", i, 4),
                "DoctorName": f"Dr. {fake.first_name()} {fake.last_name()}",
                "Specialization": spec,
                "Department": dept,
                "Hospital": random.choice(HOSPITALS),
                "Experience": random.randint(1, 35),
            }
        )
    return rows


def generate_appointments(n: int, patients: list[dict], doctors: list[dict]) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        status = random.choices(
            list(AppointmentStatus),
            weights=[20, 55, 12, 8, 5],
            k=1,
        )[0].value
        appt_dt = fake.date_time_between(start_date="-18m", end_date="+30d")
        rows.append(
            {
                "AppointmentID": _id("APT", i),
                "PatientID": random.choice(patients)["PatientID"],
                "DoctorID": random.choice(doctors)["DoctorID"],
                "AppointmentDate": appt_dt.isoformat(sep=" ", timespec="seconds"),
                "Status": status,
                "Diagnosis": random.choice(DIAGNOSES) if status == "Completed" else (
                    random.choice(DIAGNOSES + [""]) if random.random() > 0.3 else ""
                ),
            }
        )
    return rows


def generate_claims(n: int, patients: list[dict]) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        status = random.choices(
            list(ClaimApprovalStatus),
            weights=[15, 55, 15, 10, 5],
            k=1,
        )[0].value
        rows.append(
            {
                "ClaimID": _id("CLM", i),
                "PatientID": random.choice(patients)["PatientID"],
                "InsuranceCompany": random.choice(INSURANCE_COMPANIES),
                "ClaimAmount": round(random.uniform(50, 25000), 2),
                "ApprovalStatus": status,
                "ClaimDate": fake.date_between(start_date="-18m", end_date="today").isoformat(),
            }
        )
    return rows


def generate_pharmacy(n: int, patients: list[dict]) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        med, price = random.choice(MEDICINES)
        qty = random.randint(1, 90)
        rows.append(
            {
                "PrescriptionID": _id("RX", i),
                "PatientID": random.choice(patients)["PatientID"],
                "Medicine": med,
                "Quantity": qty,
                "Price": price,
            }
        )
    return rows


def generate_labs(n: int, patients: list[dict]) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        test_name, normal, _unit = random.choice(LAB_TESTS)
        low, high = [float(x) for x in normal.split("-")]
        # ~20% abnormal
        if random.random() < 0.2:
            result = round(random.choice([low * 0.5, high * 1.4]), 2)
        else:
            result = round(random.uniform(low, high), 2)
        rows.append(
            {
                "LabID": _id("LAB", i),
                "PatientID": random.choice(patients)["PatientID"],
                "TestName": test_name,
                "Result": str(result),
                "NormalRange": normal,
            }
        )
    return rows


def generate_billing(n: int, patients: list[dict], appointments: list[dict]) -> list[dict]:
    rows = []
    # Prefer linking to real appointments
    completed = [a for a in appointments if a["Status"] in ("Completed", "Scheduled")]
    for i in range(1, n + 1):
        appt = random.choice(completed) if completed else random.choice(appointments)
        status = random.choices(
            list(PaymentStatus),
            weights=[60, 20, 10, 7, 3],
            k=1,
        )[0].value
        pay_date = fake.date_between(start_date="-18m", end_date="today")
        rows.append(
            {
                "InvoiceID": _id("INV", i),
                "PatientID": appt["PatientID"],
                "AppointmentID": appt["AppointmentID"],
                "TotalAmount": round(random.uniform(75, 15000), 2),
                "PaymentStatus": status,
                "PaymentDate": pay_date.isoformat(),
            }
        )
    return rows


def write_csv(path: Path, rows: list[dict]) -> None:
    import csv

    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)


def main() -> None:
    cfg = CONFIG
    PATHS.ensure_local_directories()
    datasets = PATHS.datasets_dir

    print("Generating patients...")
    patients = generate_patients(cfg.num_patients)
    print("Generating doctors...")
    doctors = generate_doctors(cfg.num_doctors)
    print("Generating appointments...")
    appointments = generate_appointments(cfg.num_appointments, patients, doctors)
    print("Generating insurance claims...")
    claims = generate_claims(cfg.num_claims, patients)
    print("Generating pharmacy orders...")
    pharmacy = generate_pharmacy(cfg.num_pharmacy, patients)
    print("Generating laboratory results...")
    labs = generate_labs(cfg.num_labs, patients)
    print("Generating billing...")
    billing = generate_billing(cfg.num_billing, patients, appointments)

    entities = {
        "patients": patients,
        "doctors": doctors,
        "appointments": appointments,
        "insurance_claims": claims,
        "pharmacy_orders": pharmacy,
        "laboratory_results": labs,
        "billing": billing,
    }

    for name, rows in entities.items():
        csv_path = datasets / f"{name}.csv"
        json_path = datasets / f"{name}.json"
        write_csv(csv_path, rows)
        write_json(json_path, rows)
        # Stage into Auto Loader landing zones
        landing_csv = Path(PATHS.landing_path(name, "csv"))
        landing_json = Path(PATHS.landing_path(name, "json"))
        write_csv(landing_csv / f"{name}.csv", rows)
        write_json(landing_json / f"{name}.json", rows)
        print(f"  Wrote {len(rows):,} rows -> {csv_path.name}")

    print("Dataset generation complete.")


if __name__ == "__main__":
    main()

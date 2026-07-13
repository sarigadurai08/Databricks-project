# Healthcare Lakehouse & Clinical Analytics Platform

Enterprise-grade **Databricks + Delta Lake** medallion architecture for a multi-hospital clinical and revenue analytics platform.

Built as a production-style portfolio project demonstrating Senior Data Engineer practices: Auto Loader ingestion, SCD1/SCD2, reusable DQ/audit/logging frameworks, gold marts, SQL analytics, and performance optimization.

---

## Project Overview

A hospital network needs a governed lakehouse that:

1. Ingests EHR, claims, pharmacy, lab, and billing feeds incrementally
2. Preserves raw history in **Bronze**
3. Cleanses and historizes in **Silver** (SCD Type 2 for patients)
4. Publishes KPI marts in **Gold** for executives, finance, clinicians, and payers
5. Enforces data quality, auditability, and operational observability

This repository implements that platform end-to-end with modular Python packages, Databricks notebooks, SQL views, dashboard specs, and unit tests.

---

## Architecture Diagram

```text
┌──────────────────────────────────────────────────────────────────────────┐
│                        SOURCE SYSTEMS (CSV / JSON)                       │
│   EHR Patients │ Doctors │ Appointments │ Claims │ Pharmacy │ Lab │ Bill │
└───────────────────────────────┬──────────────────────────────────────────┘
                                │
                    Databricks Auto Loader (cloudFiles)
                    Schema Evolution │ Rescue Data │ Checkpoints │ Bad Records
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ BRONZE  — Raw + lineage metadata                                         │
│  _ingestion_time │ _source_file │ _load_id │ _batch_id │ _record_hash    │
└───────────────────────────────┬──────────────────────────────────────────┘
                                │ Cleanse │ Dedup │ Cast │ Validate
                                ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ SILVER  — Conformed enterprise entities                                  │
│  Patients SCD2 │ Doctors/Appts/Claims/Rx/Lab/Billing SCD1 (MERGE)        │
└───────────────────────────────┬──────────────────────────────────────────┘
                                │ Broadcast joins │ AQE │ Aggregations
                                ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ GOLD  — Clinical & financial analytics marts                             │
│  Patient Summary │ Doctor Perf │ Revenue │ Insurance │ Lab │ Pharmacy …  │
└───────────────────────────────┬──────────────────────────────────────────┘
                                │
          ┌─────────────────────┼─────────────────────┐
          ▼                     ▼                     ▼
   Databricks SQL         Lakeview Dashboards    DQ / Audit / Logs
```

---

## Folder Structure

```text
Healthcare_pipeline/
├── config/                 # Environment-aware configuration
│   ├── config.py
│   ├── constants.py
│   └── paths.py
├── datasets/               # Realistic sample CSV/JSON entities
│   └── landing/            # Auto Loader landing zones (generated)
├── notebooks/
│   ├── Bronze/             # Auto Loader ingestion
│   ├── Silver/             # Cleanse + SCD1/SCD2
│   ├── Gold/               # Analytics marts
│   ├── DataQuality/        # DQ framework execution
│   └── Monitoring/         # OPTIMIZE, VACUUM, time travel, audits
├── src/
│   ├── ingestion/          # Auto Loader wrapper
│   ├── transformations/    # Silver cleaners, SCD, gold builders
│   ├── utilities/          # Spark, Delta, DQ, exceptions
│   ├── logging/            # Enterprise logger → Delta
│   └── audit/              # Pipeline audit → Delta
├── sql/                    # Analytics + DDL + table registration
├── dashboards/             # Databricks SQL / Lakeview definitions
├── scripts/                # Dataset generator + local E2E runner
├── tests/                  # Unit tests (transforms, SCD, DQ, MERGE)
├── requirements.txt
└── README.md
```

---

## Technology Stack

| Layer | Technology |
|-------|------------|
| Language | Python 3.10+, SQL, PySpark |
| Platform | Databricks (Repos / Jobs / SQL Warehouse) |
| Storage | Delta Lake |
| Architecture | Medallion (Bronze / Silver / Gold) |
| Ingestion | Auto Loader (`cloudFiles`) |
| Governance | Unity Catalog ready (optional) |
| Quality | Custom reusable DQ framework |
| Testing | pytest + local Spark |

---

## Data Model

| Entity | Grain | Keys |
|--------|-------|------|
| Patients | One row per patient (SCD2 versions in Silver) | `PatientID` |
| Doctors | One row per provider | `DoctorID` |
| Appointments | One row per visit | `AppointmentID` → Patient, Doctor |
| Insurance Claims | One row per claim | `ClaimID` → Patient |
| Pharmacy Orders | One row per prescription | `PrescriptionID` → Patient |
| Laboratory Results | One row per test | `LabID` → Patient |
| Billing | One row per invoice | `InvoiceID` → Patient, Appointment |

---

## Data Flow

1. **Generate / land files** → `datasets/` and `datasets/landing/{csv\|json}/{entity}/`
2. **Bronze** → Auto Loader appends raw + metadata Delta tables
3. **Silver** → Cleaners + DQ + SCD MERGE into conformed tables
4. **Gold** → KPI marts overwritten / merged for analytics
5. **Ops** → Audit, logs, failed records, OPTIMIZE/VACUUM

---

## Pipeline Flow

| Order | Notebook | Responsibility |
|------:|----------|----------------|
| 0 | `notebooks/00_run_all_pipelines.py` | Orchestrator |
| 1 | `notebooks/Bronze/01_bronze_ingestion.py` | Auto Loader ingestion |
| 2 | `notebooks/Silver/01_silver_transformations.py` | Cleanse + SCD1/SCD2 |
| 3 | `notebooks/Gold/01_gold_analytics.py` | Gold marts + OPTIMIZE |
| 4 | `notebooks/DataQuality/01_data_quality_framework.py` | DQ suite |
| 5 | `notebooks/Monitoring/01_monitoring_maintenance.py` | Maintenance & observability |

---

## Execution Steps

### A. Local (portfolio / CI)

> **Runtime note:** Use **Python 3.10–3.12** and **Java 17** (`JAVA_HOME` must point to JDK 17).
> PySpark does not support Python 3.14 worker processes. On Databricks, use DBR 13.3 LTS+.

```bash
# 1. Clone and install
git clone <your-repo-url>
cd Healthcare_pipeline
python -m venv .venv
# Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 2. Generate realistic datasets
python scripts/generate_datasets.py

# 3. Run unit tests
pytest tests/unit -q

# 4. Run end-to-end local pipeline (Bronze→Silver→Gold)
python scripts/run_local_pipeline.py
```

### B. Databricks

1. Import this repo into **Databricks Repos** (or upload as a Workspace folder).
2. Set cluster runtime **DBR 13.3 LTS+** (Photon optional) with Spark configs from `config/config.py`.
3. Optional env vars:
   - `HEALTHCARE_USE_DBFS=true`
   - `HEALTHCARE_STORAGE_BASE=dbfs:/mnt/healthcare_lakehouse`
   - `HEALTHCARE_UC_ENABLED=true`
   - `HEALTHCARE_UC_CATALOG=healthcare_catalog`
4. Attach the repo root to `sys.path` (notebooks bootstrap automatically).
5. Run `notebooks/00_run_all_pipelines.py` or create a **Databricks Workflow**:
   - Task 1 Bronze → Task 2 Silver → Task 3 Gold → Task 4 DQ → Task 5 Monitoring
6. Register SQL tables (`sql/register_tables.sql`) and create Lakeview dashboards from `dashboards/dashboard_definitions.md`.

---

## Business Use Case

**Sunrise Health Network** operates five hospitals. Leadership needs a single governed platform to answer:

- Which departments and doctors drive revenue and completion rates?
- What is our insurance approval rate by payer?
- Which diagnoses and lab abnormalities are trending?
- Where is AR outstanding and collection lagging?
- How is patient volume growing by cohort?

The Gold layer and SQL dashboards answer these questions with auditable, replayable pipelines.

---

## Features

- Medallion architecture on Delta Lake
- Auto Loader with schema evolution, rescue data, checkpoints, bad records
- Bronze lineage metadata and record hashing
- Reusable silver cleaners (dedupe, cast, standardize, validate)
- SCD Type 1 and SCD Type 2 via Delta MERGE
- 14 Gold analytical tables
- Enterprise DQ framework with failed-record quarantine
- Pipeline audit + structured logging to Delta
- Retry / DLQ / continue-on-error patterns
- OPTIMIZE, ZORDER, VACUUM, time travel, liquid clustering hooks
- Unity Catalog DDL templates
- Databricks SQL dashboard specifications
- Unit tests for transforms, SCD, DQ, and MERGE

---

## Databricks Commands

```sql
-- Optimize + ZORDER
OPTIMIZE delta.`/mnt/healthcare_lakehouse/gold/revenue_analytics`
ZORDER BY (PaymentDate, Hospital, Department);

-- Vacuum old files (7 days)
VACUUM delta.`/mnt/healthcare_lakehouse/silver/patients` RETAIN 168 HOURS;

-- Time travel
SELECT * FROM delta.`/mnt/healthcare_lakehouse/silver/patients` VERSION AS OF 1;
DESCRIBE HISTORY delta.`/mnt/healthcare_lakehouse/silver/patients`;

-- Change data feed (if enabled)
SELECT * FROM table_changes('silver.patients', 1, 5);
```

```python
# Spark AQE / broadcast (also set in config)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)
```

---

## SQL Examples

```sql
-- Top 10 doctors by revenue
SELECT DoctorName, Department, AttributedRevenue, CompletionRatePct
FROM gold.doctor_performance
ORDER BY AttributedRevenue DESC
LIMIT 10;

-- Insurance approval %
SELECT InsuranceCompany, ApprovalRatePct, TotalClaims
FROM gold.insurance_analytics
ORDER BY ApprovalRatePct DESC;

-- Monthly revenue trend
SELECT PaymentYearMonth, TotalRevenue, CollectedRevenue
FROM gold.monthly_revenue
ORDER BY PaymentYearMonth;
```

See `sql/analytics_queries.sql` for the full suite.

---

## Optimization Techniques

| Technique | Where applied |
|-----------|---------------|
| Adaptive Query Execution | Gold notebook + SparkConfig |
| Broadcast joins | Doctor / dimension joins in gold transforms |
| Partitioning | Bronze `ingestion_date` |
| Caching | Silver DataFrames before gold aggregations |
| OPTIMIZE + ZORDER | Silver entities + key gold marts |
| Auto optimize write / compact | Spark conf defaults |
| Liquid Clustering | Monitoring notebook (DBR 13.3+) |
| Predicate pushdown / column pruning | Natural via Delta + selective selects |

---

## Gold Tables

| Table | Description |
|-------|-------------|
| `patient_summary` | Lifetime visits, billing, claims per patient |
| `doctor_performance` | Completion, no-shows, attributed revenue |
| `revenue_analytics` | Invoice-level enriched revenue fact |
| `hospital_revenue` | Revenue by facility |
| `insurance_analytics` | Payer approval / denial KPIs |
| `appointment_analytics` | Daily appointment cubes |
| `monthly_revenue` / `daily_revenue` | Time-series revenue |
| `laboratory_trends` | Test volumes & abnormal rates |
| `pharmacy_sales` | Rx sales by medicine |
| `patient_visit_summary` | Visits by age/gender/status |
| `doctor_utilization` | Utilization % by provider |
| `top_diseases` | Diagnosis frequency ranking |
| `cancelled_appointments` | Cancellation detail for ops |

---

## Interview Questions (Talking Points)

1. **Why Medallion?** Separation of concerns: raw auditability (Bronze), conformed enterprise model (Silver), consumer-specific marts (Gold).
2. **Auto Loader vs. COPY INTO?** Auto Loader scales with directory listing or notification modes, tracks offsets via checkpoints, and supports schema evolution / rescue data natively.
3. **SCD2 design?** Close current row (`IsCurrent=false`, set `EffectiveEndDate`), insert new version with incremented `VersionNumber` — implemented with Delta MERGE + append.
4. **How do you handle bad records?** `badRecordsPath` + `_rescued_data` + DLQ Delta tables + continue-on-error ingestion.
5. **DQ in pipelines?** Reusable rule framework writing results + quarantined payloads; critical failures can fail the job via config.
6. **Performance?** AQE, broadcast for small dimensions, ZORDER on high-cardinality filter columns, OPTIMIZE for small-file compaction, liquid clustering where available.
7. **Governance?** Unity Catalog schemas, constraints, generated columns, Change Data Feed table properties.
8. **Idempotency?** MERGE on natural keys, deterministic record hashes, checkpointed streaming ingestion.

---

## Configuration

| Variable | Default | Purpose |
|----------|---------|---------|
| `HEALTHCARE_ENV` | `dev` | Environment tag in audit |
| `HEALTHCARE_STORAGE_BASE` | `<repo>/data` | Delta root |
| `HEALTHCARE_USE_DBFS` | `false` | Use DBFS paths |
| `HEALTHCARE_UC_ENABLED` | `false` | Unity Catalog FQNs |
| `HC_NUM_PATIENTS` | `500` | Sample size knobs |

---

## Testing

```bash
pytest tests/unit -v
```

Coverage includes:

- Transformation / cleaner tests
- SCD Type 1 & Type 2 tests
- Data quality rule tests
- MERGE / metadata utility tests

---

## License

MIT — suitable for portfolio and interview demonstration use. Do not use sample data as real PHI; all datasets are synthetic.

---

## Author Notes

This project is intentionally structured like a real hospital-network lakehouse codebase: shared libraries under `src/`, thin notebooks as orchestration, config externalized, and ops concerns (audit, logging, DQ, maintenance) treated as first-class platforms — the standard expected of a Senior Data Engineer owning Databricks platforms in production.


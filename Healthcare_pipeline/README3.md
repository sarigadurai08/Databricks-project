# README3 — Pipeline End-to-End (Operations, Dataflow & Transformations)

A clear walkthrough of **what happens** in this Healthcare Lakehouse from first file load to final KPI tables — plus the Databricks features that make it work.

---

## 1) Big picture

```text
datasets/*.csv  (Git — read only)
        │
        ▼  [1] BRONZE — ingest raw + metadata
        │      Volume: .../landing/ → .../bronze/
        ▼
        │  [2] SILVER — clean + validate + SCD
        │      Volume: .../silver/
        ▼
        │  [3] GOLD — KPI / analytics marts
        │      Volume: .../gold/
        ▼
   [4] DQ + [5] MONITORING
        audit, logs, failed records, OPTIMIZE, time travel
```

**Storage root (all runtime data — discovered per workspace):**  
`/Volumes/<catalog>/<schema>/healthcare_lakehouse`

**Physical queryable tables (auto-registered after each layer):**  
`<catalog>.bronze.*` · `<catalog>.silver.*` · `<catalog>.gold.*`

Example (replace `<catalog>` with `current_catalog()`):
```sql
SELECT * FROM bronze.patients LIMIT 10;
SELECT * FROM silver.patients WHERE IsCurrent = true LIMIT 10;
SELECT * FROM gold.monthly_revenue;
```

**Entities in the pipeline:** patients, doctors, appointments, insurance_claims, pharmacy_orders, laboratory_results, billing

---

## 2) Notebook run order

| Step | Notebook | What it does |
|------|----------|--------------|
| 1 | `Bronze/01_bronze_ingestion` | Load raw CSV into Bronze Delta |
| 2 | `Silver/01_silver_transformations` | Clean, DQ check, SCD1/SCD2 into Silver |
| 3 | `Silver/02_scd_type1_type2_demo` | Small SCD1/SCD2 proof (optional demo) |
| 4 | `Gold/01_gold_analytics` | Build all Gold KPI tables |
| 5 | `DataQuality/01_data_quality_framework` | Full DQ rules on Silver |
| 6 | `Monitoring/01_monitoring_maintenance` | Audit, logs, maintenance, time travel |

**Or run once:** `00_run_all_pipelines` (calls 1→6 in order).

---

## 3) Stage-by-stage: operations & dataflow

### STEP 1 — Bronze ingestion  
**Notebook:** `01_bronze_ingestion`  
**Module:** `src/ingestion/autoloader.py`

| | |
|---|---|
| **Input** | `datasets/{entity}.csv` (patients, doctors, appointments, claims, pharmacy, labs, billing) |
| **Operations** | 1. Bootstrap project + Spark + UC Volume<br>2. Copy samples → `landing/csv/{entity}/`<br>3. Read CSV (batch path on Free Edition)<br>4. Append to Bronze Delta with metadata |
| **Transformations** | None on business data — **store as received**<br>Add: `_ingestion_time`, `_source_file`, `_load_id`, `_batch_id`, `_record_hash`, `ingestion_date` |
| **Output** | `.../bronze/{entity}` (Delta) |
| **Audit** | Per-entity: rows read / inserted |

```text
CSV file  →  landing zone  →  Bronze Delta (+ lineage columns)
```

---

### STEP 2 — Silver transformations  
**Notebook:** `01_silver_transformations`  
**Modules:** `silver_transforms.py`, `scd.py`, `data_quality.py`

| | |
|---|---|
| **Input** | Bronze Delta tables (all 7 entities) |
| **Operations** | 1. Read Bronze<br>2. **Clean** each entity<br>3. **DQ** on patients & appointments<br>4. **SCD2** patients<br>5. **SCD1** other entities<br>6. Write `patients_current` view |

#### Cleaning (per entity) — `clean_entity()`
| Entity | Main transforms |
|--------|-----------------|
| **patients** | Trim strings, standardize email/phone, cast dates, dedupe by `PatientID` (keep latest `ModifiedDate`) |
| **doctors** | Trim, cast `Experience`, drop duplicate `DoctorID` |
| **appointments** | Trim, cast `AppointmentDate`, validate status values |
| **claims** | Trim, cast amounts/dates, validate approval status |
| **pharmacy** | Trim, cast quantity/price |
| **labs** | Trim, cast flags |
| **billing** | Trim, cast amounts/dates, validate payment status |

Bronze metadata columns (`_ingestion_time`, etc.) are **dropped** in Silver.

#### SCD (slowly changing dimensions) — `scd.py`
| Type | Entity | Behavior |
|------|--------|----------|
| **SCD2** | patients | Track history when Address, Phone, Email, Insurance, etc. change → `IsCurrent`, `VersionNumber`, effective dates |
| **SCD1** | doctors, appointments, claims, pharmacy, labs, billing | Update row in place on change (Delta MERGE) |

| **Output** | `.../silver/{entity}` + `.../silver/patients_current` |
| **Audit** | Per clean + per SCD step |

```text
Bronze  →  clean  →  DQ (sample)  →  SCD MERGE  →  Silver Delta
```

---

### STEP 3 — SCD demo (optional)  
**Notebook:** `02_scd_type1_type2_demo`

| | |
|---|---|
| **Input** | Small test DataFrames (1 doctor, 1 patient) |
| **Operations** | Reset demo paths → run SCD1 twice (update doctor) → run SCD2 twice (change patient address) |
| **Output** | `.../silver/doctors_scd1_demo`, `.../silver/patients_scd2_demo` |
| **Purpose** | Prove MERGE patterns work; does not affect main Silver tables |

---

### STEP 4 — Gold analytics  
**Notebook:** `01_gold_analytics`  
**Module:** `gold_transforms.py`

| | |
|---|---|
| **Input** | Silver tables (current patients via `filter_current`) |
| **Operations** | Join + aggregate Silver facts → build 14 KPI marts → overwrite Gold Delta |
| **Transformations** | Broadcast joins, groupBy, sums, counts, date buckets |

#### Gold tables built
| Gold table | Built from | What it answers |
|------------|------------|-----------------|
| `patient_summary` | patients + appointments + billing + claims | Lifetime visits, spend, claims per patient |
| `doctor_performance` | doctors + appointments + billing | Visits & revenue by doctor |
| `revenue_analytics` | billing + appointments + doctors | Payment/revenue detail |
| `hospital_revenue` | revenue_analytics | Revenue by hospital |
| `insurance_analytics` | claims + patients | Claims by insurer / patient |
| `appointment_analytics` | appointments + doctors | Volume by dept / status |
| `monthly_revenue` / `daily_revenue` | revenue_analytics | Time-series revenue |
| `laboratory_trends` | labs | Test trends |
| `pharmacy_sales` | pharmacy | Medicine sales |
| `patient_visit_summary` | appointments + patients | Visit patterns |
| `doctor_utilization` | doctor_performance | Capacity / utilization |
| `top_diseases` | appointments (diagnosis) | Most common diagnoses |
| `cancelled_appointments` | appointments + doctors + patients | Cancellation analysis |

| **Output** | `.../gold/{table_name}` (Delta, overwrite) |
| **Optional** | OPTIMIZE + ZORDER on key Gold tables |

```text
Silver (current patients + facts)  →  joins & aggregates  →  Gold KPI tables
```

---

### STEP 5 — Data quality  
**Notebook:** `01_data_quality_framework`  
**Module:** `data_quality.py`

| | |
|---|---|
| **Input** | All Silver tables |
| **Operations** | Run rule suites per entity |
| **Rules** | Not null, unique keys, regex (email), ranges, FK (patient→doctor), allowed values, business rules |
| **Output** | `.../data_quality/validation_results`, `.../data_quality/failed_records` |
| **On failure** | Bad rows quarantined; pipeline logs failures (does not always stop Silver/Gold) |

```text
Silver  →  rule engine  →  pass/fail results + quarantine Delta
```

---

### STEP 6 — Monitoring  
**Notebook:** `01_monitoring_maintenance`

| | |
|---|---|
| **Input** | Audit, logs, DQ results, Silver/Gold Delta paths |
| **Operations** | Display audit KPIs & error logs<br>Optional OPTIMIZE/VACUUM on Silver<br>Time travel on patients<br>DQ trend summary |
| **Output** | Read-only dashboards in notebook; maintenance compacts tables |

---

## 4) Cross-cutting operations (every notebook)

| Operation | How | Where stored |
|-----------|-----|--------------|
| **Bootstrap** | Find project root, `sys.path`, bind Volume | — |
| **Spark** | Reuse Databricks session | — |
| **Audit** | `PipelineAuditor.track(...)` — rows, time, status | `.../audit/pipeline_audit` |
| **Logging** | `HealthcareLogger` → flush on error/exit | `.../ops_logging/pipeline_logs` |
| **Exit** | `dbutils.notebook.exit(status_map)` | — |

---

## 5) Dataflow summary (one table)

| Layer | Read from | Transform | Write to |
|-------|-----------|-----------|----------|
| **Landing** | `datasets/*.csv` | Copy/stage files | `.../landing/csv/{entity}/` |
| **Bronze** | Landing CSV | Add metadata only | `.../bronze/{entity}` |
| **Silver** | Bronze | Clean + SCD MERGE | `.../silver/{entity}` |
| **Gold** | Silver | Joins + KPI aggregates | `.../gold/{mart}` |
| **DQ** | Silver | Rule validation | `.../data_quality/*` |
| **Ops** | All layers | Audit / logs / maintain | `.../audit`, `.../ops_logging` |

---

## 6) Databricks features (how we achieve it)

| Feature | Role in this pipeline |
|---------|----------------------|
| **UC Volume** | All Delta data writable on Free Edition |
| **Managed Spark** | No local cluster; notebooks reuse `spark` |
| **Delta Lake** | ACID tables at every layer |
| **Auto Loader design** | Bronze pattern (batch fallback on Free Edition) |
| **Delta MERGE** | SCD1/SCD2 in Silver |
| **AQE + broadcast** | Faster Gold joins |
| **OPTIMIZE / ZORDER** | Optional read performance |
| **Time travel** | Debug past Silver versions |
| **Audit + logging** | Every run tracked in Delta |

---

## 7) One-line summary

**CSV samples land on a Volume → Bronze stores raw + lineage → Silver cleans and MERGEs with SCD → Gold aggregates KPIs → DQ validates → Monitoring audits and maintains — all via Databricks notebooks on Delta Lake.**

# README2 — Healthcare Pipeline (Simple End-to-End Guide)

A short, simple explanation of what this project does, how data moves, and how the pieces fit together.

---

## What is this project?

This is a **Healthcare Lakehouse** built on **Databricks + Delta Lake**.

It takes hospital-style data (patients, doctors, appointments, claims, pharmacy, labs, billing), loads it into Databricks, cleans it, keeps history where needed, and builds ready-to-use analytics tables for business and clinical reporting.

Think of it as a data factory:

**Raw hospital files → Clean enterprise tables → KPI / dashboard tables**

---

## Architecture in simple words

We use the **Medallion Architecture** — three layers:

| Layer | Meaning | What we keep here |
|-------|---------|-------------------|
| **Bronze** | Raw landing zone | Data almost exactly as received + lineage metadata |
| **Silver** | Clean business data | Deduped, typed, validated; patients keep history (SCD2) |
| **Gold** | Analytics / reporting | Summaries and KPIs (revenue, doctor performance, etc.) |

```text
Source CSV/JSON
      │
      ▼
  BRONZE   ← raw + metadata (_ingestion_time, _source_file, ...)
      │
      ▼
  SILVER   ← cleaned + SCD1 / SCD2
      │
      ▼
   GOLD    ← KPI marts for dashboards & SQL analytics
      │
      ├── Data Quality checks
      ├── Audit & logs
      └── Monitoring / maintenance
```

All runtime data (Bronze / Silver / Gold / logs / audit) is written to a writable Databricks Volume that is **discovered at runtime** (never hardcoded):

`/Volumes/<catalog>/<schema>/healthcare_lakehouse`

Catalog and schema come from `prepare_databricks_runtime` (`current_catalog` / `SHOW CATALOGS`, or `HEALTHCARE_UC_CATALOG`).

(Not into the Git repo.)

---

## Data flow end to end

### 1) Sources
Sample healthcare files live under `datasets/`:

- patients  
- doctors  
- appointments  
- insurance_claims  
- pharmacy_orders  
- laboratory_results  
- billing  

### 2) Bronze (ingest)
**Notebook:** `notebooks/Bronze/01_bronze_ingestion.py`

- Copies sample files into a landing area on the Volume  
- Loads each entity into Bronze Delta tables  
- Adds lineage columns so we know when/how each row was loaded  

**Output example:**  
`/Volumes/.../bronze/patients`

### 3) Silver (clean + SCD)
**Notebook:** `notebooks/Silver/01_silver_transformations.py`

- Reads Bronze  
- Cleans nulls, types, phones, emails, duplicates  
- Runs light data-quality checks  
- Applies:
  - **SCD Type 2** for **patients** (keeps history when address/phone/etc. change)
  - **SCD Type 1** for doctors, appointments, claims, pharmacy, labs, billing (overwrite in place)

**Demo notebook:** `notebooks/Silver/02_scd_type1_type2_demo.py`  
Shows SCD1/SCD2 with a small example so the pattern is clear.

### 4) Gold (analytics)
**Notebook:** `notebooks/Gold/01_gold_analytics.py`

- Reads current Silver data  
- Builds reporting tables such as:
  - patient summary  
  - doctor performance  
  - revenue analytics  
  - insurance analytics  
  - lab trends  
  - pharmacy sales  
  - top diseases  
  - monthly / hospital revenue  

These are the tables dashboards and SQL analysts use.

### 5) Data Quality
**Notebook:** `notebooks/DataQuality/01_data_quality_framework.py`

- Re-checks Silver data with rules (nulls, uniqueness, FK, ranges, regex, business rules)  
- Writes pass/fail results and bad rows for review  

### 6) Monitoring
**Notebook:** `notebooks/Monitoring/01_monitoring_maintenance.py`

- Shows audit metrics and logs  
- Optionally runs Delta maintenance (OPTIMIZE / VACUUM)  
- Supports time travel / history inspection  

### 7) Run everything
**Notebook:** `notebooks/00_run_all_pipelines.py`

Runs the full chain in order (or you can run notebooks manually one by one).

---

## Notebook run order

0. *(Optional)* `00_generate_datasets` — run Faker manually to create fresh/more sample data  
1. `Bronze/01_bronze_ingestion`  
2. `Silver/01_silver_transformations`  
3. `Silver/02_scd_type1_type2_demo`  
4. `Gold/01_gold_analytics`  
5. `DataQuality/01_data_quality_framework`  
6. `Monitoring/01_monitoring_maintenance`  

Optional: `00_run_all_pipelines` (runs 1→6 for you).

---

## Generating more data (Faker)

Faker does **not** run automatically — the repo already ships sample CSVs in `datasets/`,
and Bronze stages those into the Volume landing zone on first run.

When you want **more or different data**, open `notebooks/00_generate_datasets.py` in Databricks and run it:

- Set widgets at the top: `num_patients`, `num_appointments`, … and `seed` (change seed for different values).
- It generates all 7 entities, saves CSV + JSON into `datasets/` (when writable) and into the
  Volume landing zone (`/Volumes/.../landing/csv/<entity>`).
- Then run `Bronze/01_bronze_ingestion` — it will pick up the new files automatically
  (Bronze never overwrites your freshly generated landing files with the repo samples).

---

## What each major folder means

```text
Healthcare_pipeline/
├── datasets/          Sample source files (CSV/JSON)
├── notebooks/         Databricks jobs you click & run
├── config/            Paths, constants, environment settings
├── src/
│   ├── ingestion/     Bronze load logic
│   ├── transformations/  Silver cleaners, SCD, Gold builders
│   ├── utilities/     Spark/Delta helpers, DQ framework
│   ├── logging/       Pipeline logs → Delta
│   └── audit/         Run metrics → Delta
├── sql/               Analytics / UC SQL
└── dashboards/        Dashboard specs
```

---

## Supporting systems (built into the pipeline)

| Piece | Purpose |
|-------|---------|
| **Audit** | Tracks each step: rows read/written, status, runtime |
| **Logging** | Writes structured pipeline logs to Delta |
| **Data Quality** | Rule engine for nulls, duplicates, FK, ranges, business rules |
| **SCD1** | Update existing row in place (no history) |
| **SCD2** | Keep old + new versions (history with `IsCurrent`) |
| **Delta Lake** | Reliable storage format used by all layers |

---

## One-sentence summary

**This project ingests healthcare files into Bronze, cleans and historizes them in Silver, publishes KPI tables in Gold, and surrounds the whole flow with quality checks, audit, and monitoring — all running on Databricks Free Edition / Serverless using a Unity Catalog Volume for storage.**

---

## Quick mental model

```text
Hospital files
    → Bronze = "save the raw receipt"
    → Silver = "make it correct and trustworthy"
    → Gold   = "answer business questions fast"
    → DQ/Audit/Monitoring = "prove it is reliable"
```

# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Sample Datasets (Faker) — Run Manually
# MAGIC
# MAGIC Run this notebook **only when you want new / more sample data**.
# MAGIC
# MAGIC What it does:
# MAGIC 1. Installs `faker`
# MAGIC 2. Generates all 7 healthcare entities (patients, doctors, appointments, claims, pharmacy, labs, billing)
# MAGIC 3. Writes CSV + JSON into the project `datasets/` folder (when writable)
# MAGIC 4. Writes CSV + JSON into the Volume landing zone so Bronze can ingest immediately
# MAGIC
# MAGIC After this, run `Bronze/01_bronze_ingestion` to load the new data.

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Bootstrap project root

# COMMAND ----------

import sys
from pathlib import Path

def _seed_project_root() -> str:
    import os
    def _is_root(p: Path) -> bool:
        return (p / "config" / "config.py").exists()
    candidates = []
    env = os.getenv("HEALTHCARE_LAKEHOUSE_ROOT")
    if env:
        candidates.append(Path(env))
    try:
        candidates.extend([Path.cwd(), *list(Path.cwd().parents)[:12]])
    except Exception:
        pass
    try:
        nb = Path(
            dbutils.notebook.entry_point.getDbutils()  # type: ignore[name-defined]
            .notebook().getContext().notebookPath().get()
        )
        ws = nb if str(nb).startswith("/Workspace") else Path("/Workspace") / str(nb).lstrip("/")
        candidates = [ws, *list(ws.parents)[:12]] + candidates
    except Exception:
        pass
    for base_name in ("/Workspace/Users", "/Workspace/Repos", "/Workspace"):
        base = Path(base_name)
        if not base.exists():
            continue
        try:
            for child in list(base.iterdir())[:80]:
                if not child.is_dir():
                    continue
                candidates.append(child)
                try:
                    for gc in list(child.iterdir())[:40]:
                        if gc.is_dir():
                            candidates.append(gc)
                except Exception:
                    pass
        except Exception:
            pass
    seen = set()
    for cand in candidates:
        key = str(cand)
        if key in seen:
            continue
        seen.add(key)
        if _is_root(cand):
            root = str(cand)
            if root in sys.path:
                sys.path.remove(root)
            sys.path.insert(0, root)
            return root
    raise FileNotFoundError(
        "Healthcare_pipeline root not found. Set HEALTHCARE_LAKEHOUSE_ROOT."
    )

_PROJECT_ROOT = _seed_project_root()

from src.utilities.bootstrap import bootstrap_notebook
_PROJECT_ROOT = str(bootstrap_notebook(dbutils=globals().get("dbutils"), reload_modules=True))


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Choose how many rows to generate
# MAGIC Change the widget values (top of the notebook), then re-run the cells below.
# MAGIC
# MAGIC - **Want more data?** Increase the numbers (e.g. `num_patients` 500 → 2000) and re-run.
# MAGIC - **Want different data?** Change `seed` to any other number.
# MAGIC - IDs are deterministic (`PAT000001`...), so re-running with the same sizes
# MAGIC   updates existing records — SCD in Silver will track those changes as history.

# COMMAND ----------

dbutils.widgets.text("seed", "42")  # type: ignore[name-defined]
dbutils.widgets.text("num_patients", "500")  # type: ignore[name-defined]
dbutils.widgets.text("num_doctors", "50")  # type: ignore[name-defined]
dbutils.widgets.text("num_appointments", "2000")  # type: ignore[name-defined]
dbutils.widgets.text("num_claims", "1500")  # type: ignore[name-defined]
dbutils.widgets.text("num_pharmacy", "1200")  # type: ignore[name-defined]
dbutils.widgets.text("num_labs", "1800")  # type: ignore[name-defined]
dbutils.widgets.text("num_billing", "1800")  # type: ignore[name-defined]

SEED = int(dbutils.widgets.get("seed"))  # type: ignore[name-defined]
NUM_PATIENTS = int(dbutils.widgets.get("num_patients"))  # type: ignore[name-defined]
NUM_DOCTORS = int(dbutils.widgets.get("num_doctors"))  # type: ignore[name-defined]
NUM_APPOINTMENTS = int(dbutils.widgets.get("num_appointments"))  # type: ignore[name-defined]
NUM_CLAIMS = int(dbutils.widgets.get("num_claims"))  # type: ignore[name-defined]
NUM_PHARMACY = int(dbutils.widgets.get("num_pharmacy"))  # type: ignore[name-defined]
NUM_LABS = int(dbutils.widgets.get("num_labs"))  # type: ignore[name-defined]
NUM_BILLING = int(dbutils.widgets.get("num_billing"))  # type: ignore[name-defined]

print(
    f"Will generate: {NUM_PATIENTS} patients, {NUM_DOCTORS} doctors, "
    f"{NUM_APPOINTMENTS} appointments, {NUM_CLAIMS} claims, "
    f"{NUM_PHARMACY} pharmacy, {NUM_LABS} labs, {NUM_BILLING} billing"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate data with Faker

# COMMAND ----------

import random

from faker import Faker
from pyspark.sql import SparkSession

from config.config import get_config
from src.logging.logger import ensure_log_table, get_logger
from src.utilities.dataframe_utils import generate_run_id
from src.utilities.databricks_runtime import prepare_databricks_runtime

# Reuse the generator functions from the existing script
import scripts.generate_datasets as gen
from scripts.generate_datasets import (
    generate_appointments,
    generate_billing,
    generate_claims,
    generate_doctors,
    generate_labs,
    generate_patients,
    generate_pharmacy,
    write_csv,
    write_json,
)

# Apply the chosen seed so users can get different data each run
Faker.seed(SEED)
random.seed(SEED)
gen.fake = Faker()

spark = globals().get("spark") or SparkSession.getActiveSession()
cfg = get_config()
cfg = prepare_databricks_runtime(spark, cfg)
run_id = generate_run_id()
logger = get_logger(spark, "dataset_generator", run_id)
ensure_log_table(spark)

logger.info("Dataset generation started", module="faker", details={"patients": NUM_PATIENTS, "seed": SEED})

patients = generate_patients(NUM_PATIENTS)
doctors = generate_doctors(NUM_DOCTORS)
appointments = generate_appointments(NUM_APPOINTMENTS, patients, doctors)
claims = generate_claims(NUM_CLAIMS, patients)
pharmacy = generate_pharmacy(NUM_PHARMACY, patients)
labs = generate_labs(NUM_LABS, patients)
billing = generate_billing(NUM_BILLING, patients, appointments)

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
    print(f"  {name}: {len(rows):,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Save to project `datasets/` (CSV + JSON)
# MAGIC Skipped gracefully if the Git folder is read-only.

# COMMAND ----------

datasets_dir = cfg.paths.datasets_dir
saved_to_repo = []

for name, rows in entities.items():
    try:
        write_csv(datasets_dir / f"{name}.csv", rows)
        write_json(datasets_dir / f"{name}.json", rows)
        saved_to_repo.append(name)
    except OSError as exc:
        logger.warning(f"datasets/ not writable for {name}: {exc}", module="faker")

if saved_to_repo:
    logger.info("Saved to project datasets/", module="faker", details={"entities": saved_to_repo})
    print(f"Saved to {datasets_dir}: {saved_to_repo}")
else:
    print("Project datasets/ folder not writable (read-only Git folder) — Volume landing still updated below.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Save to Volume landing zone (Bronze reads from here)

# COMMAND ----------

volume_landing_status = {}

for name, rows in entities.items():
    csv_dir = Path(cfg.paths.landing_path(name, "csv"))
    json_dir = Path(cfg.paths.landing_path(name, "json"))
    try:
        write_csv(csv_dir / f"{name}.csv", rows)
        write_json(json_dir / f"{name}.json", rows)
        volume_landing_status[name] = len(rows)
    except OSError as exc:
        logger.error(f"Failed writing landing for {name}", module="faker", exc=exc)
        raise

logger.info("Landing zone updated with fresh Faker data", module="faker", details=volume_landing_status)
print("Volume landing updated:")
for name, cnt in volume_landing_status.items():
    print(f"  {cfg.paths.landing_path(name, 'csv')}  ({cnt:,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Preview generated data

# COMMAND ----------

preview = spark.createDataFrame(patients[:20])
display(preview)  # noqa: F821

# COMMAND ----------

logger.info("Dataset generation completed", module="faker", details=volume_landing_status)
logger.flush()
print("Done. Now run Bronze/01_bronze_ingestion to load this data.")
dbutils.notebook.exit(str(volume_landing_status))  # type: ignore[name-defined]

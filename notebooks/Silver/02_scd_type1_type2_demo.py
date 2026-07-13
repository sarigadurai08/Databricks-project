# Databricks notebook source
# MAGIC %md
# MAGIC # SCD Type 1 & Type 2 — Focused Demo
# MAGIC
# MAGIC Demonstrates production MERGE patterns used by the Silver layer:
# MAGIC - **SCD1** overwrite-in-place for doctors
# MAGIC - **SCD2** historical versioning for patients

# COMMAND ----------

import sys
from pathlib import Path

for cand in [Path.cwd(), Path.cwd().parent, Path("/Workspace/Repos/Healthcare_Lakehouse")]:
    if (cand / "config" / "config.py").exists():
        if str(cand) not in sys.path:
            sys.path.insert(0, str(cand))
        break

# COMMAND ----------

from pyspark.sql import functions as F

from config.config import get_config
from src.logging.logger import get_logger
from src.transformations.scd import apply_scd_type1, apply_scd_type2
from src.utilities.dataframe_utils import generate_run_id
from src.utilities.spark_session import get_spark

spark = get_spark("SCDDemo")
cfg = get_config()
logger = get_logger(spark, "scd_demo", generate_run_id())

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 1 — Doctors

# COMMAND ----------

doctors_v1 = spark.createDataFrame(
    [("DOC9999", "Dr. Demo", "Cardiology", "Cardiology", "Mercy General Hospital", 10)],
    ["DoctorID", "DoctorName", "Specialization", "Department", "Hospital", "Experience"],
)
path_d = cfg.paths.silver_path("doctors_scd1_demo")
apply_scd_type1(spark, doctors_v1, path_d, "DoctorID", logger=logger)

doctors_v2 = spark.createDataFrame(
    [("DOC9999", "Dr. Demo Updated", "Cardiology", "Cardiology", "Mercy General Hospital", 11)],
    doctors_v1.columns,
)
apply_scd_type1(
    spark,
    doctors_v2,
    path_d,
    "DoctorID",
    compare_columns=["DoctorName", "Experience"],
    logger=logger,
)
display(spark.read.format("delta").load(path_d))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 — Patients

# COMMAND ----------

cols = [
    "PatientID", "FirstName", "LastName", "Phone", "Email", "Address",
    "InsuranceID", "Gender", "DOB", "CreatedDate", "ModifiedDate",
]
patients_v1 = spark.createDataFrame(
    [("PATSCD1", "Sam", "Patient", "111", "sam@ex.com", "100 Main St", "INS1", "Male", "1980-01-01", "2024-01-01 00:00:00", "2024-01-01 00:00:00")],
    cols,
)
path_p = cfg.paths.silver_path("patients_scd2_demo")
apply_scd_type2(spark, patients_v1, path_p, "PatientID", ["Address", "Phone"], logger=logger)

patients_v2 = spark.createDataFrame(
    [("PATSCD1", "Sam", "Patient", "222", "sam@ex.com", "200 Oak Ave", "INS1", "Male", "1980-01-01", "2024-01-01 00:00:00", "2024-06-01 00:00:00")],
    cols,
)
apply_scd_type2(spark, patients_v2, path_p, "PatientID", ["Address", "Phone"], logger=logger)

history_df = spark.read.format("delta").load(path_p).orderBy("VersionNumber")
display(history_df)  # noqa: F821
assert history_df.count() == 2
assert history_df.filter(F.col("IsCurrent") == True).count() == 1  # noqa: E712
logger.info("SCD demo assertions passed", module="scd_demo")

# Databricks notebook source
# MAGIC %md
# MAGIC # End-to-End Orchestration
# MAGIC
# MAGIC Runs Bronze → Silver → Gold → Data Quality → Monitoring sequentially.
# MAGIC Suitable as a Databricks Job multi-task workflow entrypoint or single notebook driver.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run order
# MAGIC 1. Bronze ingestion
# MAGIC 2. Silver SCD transforms
# MAGIC 3. Gold analytics
# MAGIC 4. Data quality
# MAGIC 5. Monitoring / maintenance

# COMMAND ----------

try:
    dbutils.notebook.run("./Bronze/01_bronze_ingestion", timeout_seconds=0)  # type: ignore[name-defined]
    dbutils.notebook.run("./Silver/01_silver_transformations", timeout_seconds=0)  # type: ignore[name-defined]
    dbutils.notebook.run("./Gold/01_gold_analytics", timeout_seconds=0)  # type: ignore[name-defined]
    dbutils.notebook.run("./DataQuality/01_data_quality_framework", timeout_seconds=0)  # type: ignore[name-defined]
    dbutils.notebook.run("./Monitoring/01_monitoring_maintenance", timeout_seconds=0)  # type: ignore[name-defined]
except NameError:
    # Local / non-Databricks: import and document manual execution
    print(
        "dbutils unavailable — execute notebooks individually or run:\n"
        "  python scripts/run_local_pipeline.py"
    )

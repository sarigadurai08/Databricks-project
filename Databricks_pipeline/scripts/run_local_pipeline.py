"""
Local end-to-end pipeline runner (portfolio / CI without Databricks Runtime).

Executes Generate → Bronze → Silver → Gold using the same modules as the notebooks.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config.config import get_config
from config.constants import (
    ALL_ENTITIES,
    PIPELINE_BRONZE_INGESTION,
    PIPELINE_GOLD_ANALYTICS,
    PIPELINE_SILVER_TRANSFORM,
    PIPELINE_STREAMING_SIMULATOR,
)
from src.audit.auditor import PipelineAuditor
from src.ingestion.autoloader import AutoLoaderIngestion
from src.ingestion.streaming_simulator import StreamingEventSimulator
from src.logging.logger import ensure_log_table, get_logger
from src.transformations.gold_transforms import build_all_gold_tables
from src.transformations.silver_transforms import build_all_silver_tables
from src.utilities.dataframe_utils import generate_run_id
from src.utilities.delta_helpers import table_exists
from src.utilities.spark_session import get_spark, stop_spark


def main() -> None:
    cfg = get_config()
    spark = get_spark("LocalEcommercePipeline")
    run_id = generate_run_id()
    logger = get_logger(spark, "local_e2e", run_id)
    ensure_log_table(spark)

    print(f"=== storage_base: {cfg.paths.storage_base} ===")

    print("=== Generate streaming events ===")
    sim_logger = get_logger(spark, PIPELINE_STREAMING_SIMULATOR, run_id)
    sim_auditor = PipelineAuditor(
        spark, PIPELINE_STREAMING_SIMULATOR, run_id, cfg.environment, sim_logger
    )
    with sim_auditor.track("simulate_streaming_events") as ctx:
        sim = StreamingEventSimulator(
            spark=spark,
            config=cfg,
            ticks=cfg.streaming.simulator_ticks,
            events_per_tick=cfg.streaming.simulator_events_per_tick,
            interval_seconds=0,
        )
        totals = sim.run_ticks()
        ctx["rows_inserted"] = sum(totals.values())
        for entity, count in totals.items():
            print(f"  landing.{entity}: {count} events")

    print("=== Bronze ===")
    bronze_logger = get_logger(spark, PIPELINE_BRONZE_INGESTION, run_id)
    bronze_auditor = PipelineAuditor(
        spark, PIPELINE_BRONZE_INGESTION, run_id, cfg.environment, bronze_logger
    )
    with bronze_auditor.track("bronze_ingest_all") as ctx:
        ingestion = AutoLoaderIngestion(spark, cfg, bronze_logger, run_id)
        status = ingestion.ingest_all(fmt="json")
        ctx["rows_inserted"] = sum(1 for v in status.values() if v == "SUCCESS")
        for entity in ALL_ENTITIES:
            path = cfg.paths.bronze_path(entity)
            if table_exists(spark, path):
                cnt = spark.read.format("delta").load(path).count()
                print(f"  bronze.{entity}: {cnt} rows")

    print("=== Silver ===")
    silver_logger = get_logger(spark, PIPELINE_SILVER_TRANSFORM, run_id)
    silver_auditor = PipelineAuditor(
        spark, PIPELINE_SILVER_TRANSFORM, run_id, cfg.environment, silver_logger
    )
    silver_status = build_all_silver_tables(
        spark, logger=silver_logger, auditor=silver_auditor, run_id=run_id
    )
    for entity, st in silver_status.items():
        path = cfg.paths.silver_path(entity)
        rows = (
            spark.read.format("delta").load(path).count()
            if st == "SUCCESS" and table_exists(spark, path)
            else 0
        )
        print(f"  silver.{entity}: {st} ({rows} rows)")

    print("=== Gold ===")
    gold_logger = get_logger(spark, PIPELINE_GOLD_ANALYTICS, run_id)
    gold_auditor = PipelineAuditor(
        spark, PIPELINE_GOLD_ANALYTICS, run_id, cfg.environment, gold_logger
    )
    gold_status = build_all_gold_tables(
        spark, logger=gold_logger, auditor=gold_auditor, run_id=run_id
    )
    for name, st in gold_status.items():
        path = cfg.paths.gold_path(name)
        rows = (
            spark.read.format("delta").load(path).count()
            if st == "SUCCESS" and table_exists(spark, path)
            else 0
        )
        print(f"  gold.{name}: {st} ({rows} rows)")

    logger.flush()
    print("=== Pipeline completed successfully ===")
    stop_spark(spark)


if __name__ == "__main__":
    main()

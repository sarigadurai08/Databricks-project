# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Streaming Events — E-Commerce Lakehouse
# MAGIC
# MAGIC Writes simulated JSON event files into the Volume landing zone for all entities.
# MAGIC Run before Bronze ingestion (or as the first step of end-to-end orchestration).
# MAGIC
# MAGIC Uses `StreamingEventSimulator` with configurable ticks from `cfg.streaming.simulator_ticks`.

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
    env = os.getenv("ECOMMERCE_LAKEHOUSE_ROOT")
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
        "Databricks_pipeline root not found. Set ECOMMERCE_LAKEHOUSE_ROOT."
    )

_PROJECT_ROOT = _seed_project_root()

from src.utilities.bootstrap import bootstrap_notebook
_PROJECT_ROOT = str(bootstrap_notebook(dbutils=globals().get("dbutils"), reload_modules=True))


# COMMAND ----------

from pyspark.sql import SparkSession

from config.config import get_config
from config.constants import PIPELINE_STREAMING_SIMULATOR
from src.audit.auditor import PipelineAuditor
from src.ingestion.streaming_simulator import StreamingEventSimulator
from src.logging.logger import ensure_log_table, get_logger
from src.utilities.dataframe_utils import generate_run_id
from src.utilities.databricks_runtime import prepare_databricks_runtime

# COMMAND ----------

spark = globals().get("spark") or SparkSession.getActiveSession()
cfg = get_config()
cfg = prepare_databricks_runtime(spark, cfg)
run_id = generate_run_id()
logger = get_logger(spark, PIPELINE_STREAMING_SIMULATOR, run_id)
ensure_log_table(spark)
auditor = PipelineAuditor(spark, PIPELINE_STREAMING_SIMULATOR, run_id, cfg.environment, logger)

logger.info(
    "Streaming event simulator started",
    module="simulator",
    details={
        **cfg.to_dict(),
        "ticks": cfg.streaming.simulator_ticks,
        "events_per_tick": cfg.streaming.simulator_events_per_tick,
        "interval_seconds": cfg.streaming.simulator_interval_seconds,
        "storage_base": cfg.paths.storage_base,
    },
)

print(f"Runtime storage_base: {cfg.paths.storage_base}")
print(f"Ticks: {cfg.streaming.simulator_ticks} | Events/tick: {cfg.streaming.simulator_events_per_tick}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate JSON landing files

# COMMAND ----------

status_map = {}

try:
    with auditor.track("simulate_streaming_events") as ctx:
        sim = StreamingEventSimulator(
            spark=spark,
            config=cfg,
            ticks=cfg.streaming.simulator_ticks,
            events_per_tick=cfg.streaming.simulator_events_per_tick,
            interval_seconds=cfg.streaming.simulator_interval_seconds,
        )
        totals = sim.run_ticks()
        ctx["rows_inserted"] = sum(totals.values())
        status_map.update(totals)
        status_map["ticks"] = cfg.streaming.simulator_ticks
        status_map["storage_base"] = cfg.paths.storage_base

    print("Events written per entity:")
    for entity, count in totals.items():
        landing = cfg.paths.landing_path(entity, "json")
        print(f"  {entity}: {count:,} → {landing}")

    try:
        display(  # noqa: F821
            spark.createDataFrame(
                [(k, int(v)) for k, v in totals.items()],
                ["entity", "events_written"],
            )
        )
    except Exception as display_exc:
        logger.warning(f"display skipped: {display_exc}", module="simulator")
except Exception as exc:
    logger.error("Streaming event simulation failed", module="simulator", exc=exc)
    logger.flush()
    raise

# COMMAND ----------

logger.flush()
logger.info("Streaming event simulation completed", module="simulator", details=status_map)
dbutils.notebook.exit(str(status_map))  # type: ignore[name-defined]

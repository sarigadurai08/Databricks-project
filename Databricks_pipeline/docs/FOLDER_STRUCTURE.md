# Folder Structure

Complete layout of the E-Commerce Lakehouse repository with module responsibilities.

---

## Top-Level Tree

```text
Databricks_pipeline/
├── config/                         # Configuration package
├── notebooks/                      # Databricks notebooks (primary entry points)
├── src/                            # Reusable Python packages
├── scripts/                        # Local runners and utilities
├── sql/                            # Parameterized SQL ({{CATALOG}} templates)
├── dashboards/                     # Lakeview / SQL dashboard definitions
├── tests/                          # pytest unit + integration tests
├── docs/                           # Architecture and flow documentation
├── data/                           # Local runtime data (gitignored, created at run time)
├── requirements.txt
├── pytest.ini
├── .gitignore
├── README.md
└── COMPATIBILITY_REPORT.md
```

---

## `config/` — Configuration

| File | Purpose |
|------|---------|
| `config.py` | `EcommerceConfig` dataclass: Spark, Auto Loader, streaming, UC, DQ settings |
| `constants.py` | Entity names, PKs, event-time columns, domain enums, ZORDER maps |
| `paths.py` | `LakehousePaths` — landing, bronze, silver, gold, checkpoint, audit paths |
| `__init__.py` | Package marker |

**Key design:** `CATALOG_NAME = ""` — catalog resolved at runtime, never hardcoded.

---

## `notebooks/` — Databricks Entry Points

| Path | Purpose |
|------|---------|
| `00_generate_streaming_events.py` | JSON event simulator driver |
| `00_run_all_pipelines.py` | End-to-end orchestrator |
| `Bronze/01_bronze_ingestion.py` | Auto Loader bronze ingestion |
| `Silver/01_silver_transformations.py` | Silver cleanse + MERGE |
| `Gold/01_gold_analytics.py` | Gold mart builder + UC registration |
| `Streaming/01_structured_streaming.py` | Structured streaming demo |
| `DataQuality/01_data_quality_framework.py` | DQ rule execution |
| `Monitoring/01_monitoring_maintenance.py` | OPTIMIZE, VACUUM, time travel |
| `databricks_job.json` | Multi-task job template (portable paths) |

Each notebook includes `_seed_project_root()` and `bootstrap_notebook(reload_modules=True)`.

---

## `src/` — Core Packages

### `src/ingestion/`

| File | Purpose |
|------|---------|
| `autoloader.py` | Auto Loader wrapper with batch JSON fallback |
| `streaming_simulator.py` | Generates 11 entity JSON landing files |
| `__init__.py` | Package exports |

### `src/transformations/`

| File | Purpose |
|------|---------|
| `silver_transforms.py` | Per-entity silver cleaners and MERGE orchestration |
| `gold_transforms.py` | 19 gold mart builders + `build_all_gold_tables()` |
| `__init__.py` | Package exports |

### `src/utilities/`

| File | Purpose |
|------|---------|
| `bootstrap.py` | Project root discovery, module reload, notebook path resolution |
| `databricks_runtime.py` | Catalog/Volume discovery, DBFS fallback, runtime prep |
| `spark_session.py` | Local/Databricks SparkSession factory |
| `delta_helpers.py` | MERGE, write, OPTIMIZE, VACUUM, soft_reset |
| `dataframe_utils.py` | Hash, metadata, dedupe, DLQ writer |
| `data_quality.py` | Reusable DQ framework with quarantine |
| `table_registry.py` | UC managed table registration (CTAS) |
| `schemas.py` | Spark schema definitions for entities |
| `notebook_seed.py` | Shared notebook bootstrap helpers |
| `exceptions.py` | Domain-specific exception types |

### `src/logging/`

| File | Purpose |
|------|---------|
| `logger.py` | Structured Delta-backed logger |

### `src/audit/`

| File | Purpose |
|------|---------|
| `auditor.py` | Pipeline step audit context manager |

---

## `scripts/`

| File | Purpose |
|------|---------|
| `run_local_pipeline.py` | Local Bronze → Silver → Gold runner |
| `__init__.py` | Package marker |

---

## `sql/`

| File | Purpose |
|------|---------|
| `analytics_queries.sql` | Gold analytics views/queries with `{{CATALOG}}` |
| `ddl_unity_catalog.sql` | Schema DDL template with `{{CATALOG}}` |

Replace `{{CATALOG}}` before running, or use `USE CATALOG` after notebook registration.

---

## `dashboards/`

| File | Purpose |
|------|---------|
| `dashboard_definitions.md` | 7 dashboard specs with gold table queries |

---

## `tests/`

| Path | Purpose |
|------|---------|
| `conftest.py` | Spark session fixture, tmp storage binding |
| `unit/test_dataframe_utils.py` | Hash, metadata, dedupe tests |
| `unit/test_data_quality.py` | DQ rule tests |
| `unit/test_merge_and_utils.py` | Delta MERGE and soft_reset tests |
| `unit/test_simulator.py` | Simulator batch generation tests |
| `integration/test_gold_smoke.py` | Gold Delta smoke test (skips if absent) |

---

## `docs/`

| File | Purpose |
|------|---------|
| `ARCHITECTURE.md` | System diagrams, data flow, optimization |
| `NOTEBOOK_FLOW.md` | Execution order and dependencies |
| `FOLDER_STRUCTURE.md` | This document |
| `INTERVIEW_QUESTIONS.md` | Expanded Databricks interview Q&A |

---

## Runtime Data Layout (Not in Git)

When pipelines run, data is written under `PATHS.storage_base`:

```text
{storage_base}/
├── landing/json/{entity}/          # Simulator output
├── bronze/{entity}/                  # Bronze Delta
├── bronze/_schemas/{entity}/         # Auto Loader schema location
├── bronze/_checkpoints/{entity}/     # Auto Loader checkpoints
├── bronze/_bad_records/{entity}/     # Bad records quarantine
├── silver/{entity}/                  # Silver Delta
├── gold/{mart_name}/                 # Gold marts
├── streaming/_checkpoints/           # Structured streaming state
├── audit/pipeline_audit/             # Audit Delta
├── ops_logging/pipeline_logs/        # Log Delta
├── data_quality/
│   ├── validation_results/
│   └── failed_records/
├── dead_letter/{entity}/
└── quarantine/{entity}/
```

**Local default:** `{project_root}/data/`  
**Databricks default:** `/Volumes/{catalog}/{schema}/ecommerce_lakehouse/`  
**DBFS fallback:** `dbfs:/FileStore/ecommerce_lakehouse/`

---

## Import Conventions

Notebooks and scripts add the project root to `sys.path` via bootstrap, then import:

```python
from config.config import get_config
from config.constants import ALL_ENTITIES
from config.paths import PATHS
from src.ingestion.streaming_simulator import StreamingEventSimulator
from src.transformations.gold_transforms import build_all_gold_tables
from src.utilities.databricks_runtime import prepare_databricks_runtime
```

Tests use the same pattern via `tests/conftest.py`.

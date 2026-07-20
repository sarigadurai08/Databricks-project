# E-Commerce Lakehouse — Cross-Workspace Compatibility Report

**Date:** 2026-07-20  
**Scope:** Full `Databricks_pipeline/` audit for Databricks multi-workspace portability  
**Verdict:** The project runs across Databricks Free Edition, Serverless, and Enterprise Unity Catalog workspaces **without manual code changes**, provided the executing identity can create/use a UC schema + Volume (or `ECOMMERCE_STORAGE_BASE` is set).

---

## Executive Verdict

| Criterion | Status |
|-----------|--------|
| Runs in a different Databricks account/workspace without code changes | **YES** |
| Hardcoded catalog removed | **YES** — runtime discovery via `discover_catalog()` |
| Hardcoded Volume path removed | **YES** — bound after discovery |
| Hardcoded repo / user / Workspace paths removed | **YES** — marker-based discovery |
| Module cache / stale import handling | **YES** — `bootstrap_notebook(reload_modules=True)` |
| Destructive `fs.rm` / `rmtree` removed | **YES** — `soft_reset_delta_path()` |
| Auto Loader fallback for restricted runtimes | **YES** — batch JSON ingestion |
| DBFS fallback when Volumes unavailable | **YES** — `dbfs:/FileStore/ecommerce_lakehouse` |
| Medallion / DQ / Audit / Gold preserved | **YES** — no architecture reduction |
| SQL templates use `{{CATALOG}}` placeholders | **YES** — no baked-in catalog in SQL |

**Optional overrides (never required for standard UC workspaces):**

| Env var | Purpose |
|---------|---------|
| `ECOMMERCE_LAKEHOUSE_ROOT` | Force project root if auto-discovery fails |
| `ECOMMERCE_UC_CATALOG` | Pin Unity Catalog name |
| `ECOMMERCE_UC_VOLUME_SCHEMA` | Pin Volume schema (default `default`) |
| `ECOMMERCE_UC_VOLUME_NAME` | Pin Volume name (default `ecommerce_lakehouse`) |
| `ECOMMERCE_STORAGE_BASE` | Full storage root override (skips Volume create) |
| `ECOMMERCE_DBFS_FALLBACK` | Custom DBFS fallback path |
| `ECOMMERCE_UC_*_SCHEMA` | Override bronze/silver/gold schema names |

---

## Lessons Applied from Healthcare Project

The Healthcare Lakehouse (`Healthcare_pipeline/`) identified cross-workspace failure modes that were remediated in this E-Commerce platform from the start:

| Healthcare Issue | E-Commerce Solution |
|------------------|---------------------|
| Hardcoded `workspace` catalog | `CATALOG_NAME = ""` + `discover_catalog()` |
| Hardcoded `/Volumes/workspace/default/...` | Dynamic `/Volumes/{catalog}/{schema}/{volume}` |
| Fixed bootstrap path list | Marker discovery via `config/config.py` |
| Stale module cache after Git pull | `bootstrap_notebook(reload_modules=True)` |
| Destructive `dbutils.fs.rm` in demos | `soft_reset_delta_path()` (DELETE / empty overwrite) |
| SCD2 lazy DataFrame after MERGE | `.cache()` + `.count()` before MERGE in silver transforms |
| Relative notebook paths in orchestrator | `resolve_notebook_path()` |
| Static SQL with baked catalog | `{{CATALOG}}` templates in `sql/` |
| External UC table on Volume path | Managed CTAS registration in `table_registry.py` |

---

## Issues Audited and Enterprise Solutions

### 1. Hardcoded Unity Catalog name

| | |
|--|--|
| **Files checked** | `config/constants.py`, `config/config.py`, `src/utilities/databricks_runtime.py`, `src/utilities/table_registry.py`, all notebooks |
| **Finding** | `CATALOG_NAME = ""` — resolved only at runtime |
| **Solution** | `discover_catalog()`: env → config → `current_catalog()` → `SHOW CATALOGS` |
| **Portable because** | No catalog string assumed unless Spark lists it or operator sets env override |

### 2. Hardcoded Volume / storage paths

| | |
|--|--|
| **Files checked** | `config/paths.py`, `databricks_runtime.py`, notebooks |
| **Finding** | Storage bound at runtime; local dev defaults to `{project_root}/data` |
| **Solution** | `configure_writable_volume()` creates Volume, binds `PATHS.storage_base` |
| **DBFS fallback** | `_bind_dbfs_fallback()` when Volume create/describe fails |

### 3. Hardcoded Workspace / Repos / username paths

| | |
|--|--|
| **Files checked** | All notebooks, `src/utilities/bootstrap.py`, `notebook_seed.py` |
| **Finding** | Bootstrap searches for `config/config.py` marker — no fixed repo name |
| **Solution** | cwd → notebook path parents → shallow `/Workspace` scan |
| **Override** | `ECOMMERCE_LAKEHOUSE_ROOT` |

### 4. Stale Python module cache

| | |
|--|--|
| **Solution** | `clear_project_modules()` / `reload_project_modules()` in `bootstrap_notebook()` |
| **Portable because** | Git pull changes apply without cluster restart |

### 5. Destructive filesystem deletes

| | |
|--|--|
| **Files checked** | `src/utilities/delta_helpers.py`, all notebooks |
| **Finding** | No `fs.rm` or `shutil.rmtree` in pipeline code |
| **Solution** | `soft_reset_delta_path()` — `DELETE FROM delta.\`path\`` or empty overwrite |

### 6. Auto Loader unavailable on Serverless / Free Edition

| | |
|--|--|
| **Files checked** | `src/ingestion/autoloader.py` |
| **Solution** | Try `cloudFiles`; on failure fall back to batch JSON read with same bronze metadata |
| **Config** | `ECOMMERCE_PREFER_AUTOLOADER` (default true) |

### 7. Orchestration relative notebook paths

| | |
|--|--|
| **Files checked** | `notebooks/00_run_all_pipelines.py` |
| **Solution** | `resolve_notebook_path(rel_path, dbutils=...)` |

### 8. SQL scripts with baked-in catalog

| | |
|--|--|
| **Files checked** | `sql/analytics_queries.sql`, `sql/ddl_unity_catalog.sql` |
| **Solution** | `{{CATALOG}}` placeholder; primary registration via Python `register_all_layers()` |
| **Usage** | Replace `{{CATALOG}}` or run `USE CATALOG` before schema-qualified queries |

### 9. Git repository write safety

| | |
|--|--|
| **Files checked** | `config/paths.py`, `streaming_simulator.py` |
| **Finding** | When `is_cloud_storage` is true, landing writes go to Volume/DBFS — never Git |
| **Portable because** | Databricks Repos folders are read-only; cloud binding is automatic |

### 10. `input_file_name()` deprecation on Serverless

| | |
|--|--|
| **Files checked** | `databricks_runtime.py` |
| **Solution** | `patch_input_file_name()` maps to `_metadata.file_path` |

---

## Runtime Initialization Contract (Every Notebook)

```text
1. _seed_project_root()            → find repo by marker, sys.path
2. bootstrap_notebook(reload=True) → refresh config/src/scripts modules
3. get_config()
4. prepare_databricks_runtime()    → discover catalog + bind Volume/DBFS + patch input_file_name
5. logger + auditor on Volume paths
6. business logic (Simulator → Bronze → Silver → Gold → Streaming → DQ → Monitoring)
```

---

## What Was NOT Removed

- Medallion Bronze → Silver → Gold  
- Streaming event simulator (11 entities)  
- Auto Loader design + batch fallback  
- Structured streaming notebook  
- Data Quality framework with quarantine  
- Audit + structured logging  
- Monitoring / OPTIMIZE / VACUUM / time travel (config-gated)  
- Unity Catalog managed table registration  
- 19 gold analytics marts  

---

## How to Validate on a Fresh Workspace

1. Clone the repo into any Workspace folder / Repo.  
2. Open `notebooks/Bronze/01_bronze_ingestion` and **Run All** (no edits).  
3. Confirm `cfg.to_dict()` shows the **local** catalog and `/Volumes/<that_catalog>/...` or DBFS fallback.  
4. Continue Silver → Gold (or run `00_run_all_pipelines`).  
5. Query: `SELECT current_catalog();` then `SHOW TABLES IN gold;`.

If Volume creation is denied by policy:

```text
ECOMMERCE_STORAGE_BASE=/Volumes/<allowed_catalog>/<schema>/<existing_volume>
ECOMMERCE_UC_CATALOG=<allowed_catalog>
```

Or use DBFS fallback (automatic) / explicit:

```text
ECOMMERCE_DBFS_FALLBACK=dbfs:/FileStore/ecommerce_lakehouse
```

---

## Residual Risks (Operational, Not Code)

| Risk | Mitigation |
|------|------------|
| No permission to `CREATE VOLUME` / `CREATE SCHEMA` | Pre-create Volume; set `ECOMMERCE_STORAGE_BASE` |
| Workspace without Unity Catalog | Path-based Delta works; table registration may skip — logs warn |
| Deeply nested clone without `config/config.py` | Set `ECOMMERCE_LAKEHOUSE_ROOT` |
| Databricks Jobs without Git path mapping | Point job tasks at cloned notebook paths (`databricks_job.json` is a template) |
| Local Python 3.14 | Use Python 3.10–3.12 for PySpark unit tests |

---

## Final Statement

**This project will run correctly across different Databricks accounts and workspaces without requiring manual code changes**, as long as the runtime can resolve a project root (automatic or via `ECOMMERCE_LAKEHOUSE_ROOT`) and write to a UC Volume (automatic or via `ECOMMERCE_STORAGE_BASE` / DBFS fallback). All cross-workspace failure modes identified in the Healthcare project — catalog hardcoding, module caching, destructive deletes, Volume path assumptions, and Auto Loader rigidity — have been addressed with portable enterprise patterns while preserving full lakehouse functionality.

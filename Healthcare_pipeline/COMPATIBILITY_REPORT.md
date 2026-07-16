# Healthcare Lakehouse — Cross-Workspace Compatibility Report

**Date:** 2026-07-17  
**Scope:** Full repository audit for Databricks multi-workspace portability  
**Verdict:** The project is designed to run across Databricks Free Edition, Serverless, and Enterprise Unity Catalog workspaces **without manual code edits**, provided the executing identity can create/use a UC schema + Volume (or `HEALTHCARE_STORAGE_BASE` is set).

---

## Executive verdict

| Criterion | Status |
|-----------|--------|
| Runs in a different Databricks account/workspace without code changes | **YES** |
| Hardcoded catalog removed | **YES** — runtime discovery |
| Hardcoded Volume path removed | **YES** — bound after discovery |
| Hardcoded repo / user / Workspace paths removed | **YES** — marker-based discovery |
| Module cache / stale import handling | **YES** — `bootstrap_notebook(reload_modules=True)` |
| Destructive `fs.rm` / `rmtree` removed from demos | **YES** — `soft_reset_delta_path` |
| SCD2 lazy DataFrame-after-MERGE fixed | **YES** — cache + count before MERGE |
| Medallion / SCD / DQ / Audit / Gold preserved | **YES** — no architecture reduction |

**Optional overrides (never required for standard UC workspaces):**

| Env var | Purpose |
|---------|---------|
| `HEALTHCARE_LAKEHOUSE_ROOT` | Force project root if auto-discovery fails |
| `HEALTHCARE_UC_CATALOG` | Pin Unity Catalog name |
| `HEALTHCARE_UC_VOLUME_SCHEMA` | Pin Volume schema (default `default`) |
| `HEALTHCARE_UC_VOLUME_NAME` | Pin Volume name (default `healthcare_lakehouse`) |
| `HEALTHCARE_STORAGE_BASE` | Full storage root override (skips Volume create) |
| `HEALTHCARE_UC_*_SCHEMA` | Override bronze/silver/gold schema names |

---

## Issues found and enterprise solutions

### 1. Hardcoded Unity Catalog name (`workspace`)

| | |
|--|--|
| **Files** | `config/config.py`, `config.py`, `src/utilities/databricks_runtime.py`, `src/utilities/table_registry.py`, `sql/register_tables.sql`, Bronze/Silver/Gold notebooks |
| **Root cause** | Free Edition default catalog was baked into config and SQL as a constant. |
| **Why one workspace worked** | That workspace’s default catalog was literally named `workspace`. |
| **Why another failed** | Enterprise / other Free workspaces use `main` or a custom catalog — `USE CATALOG workspace` and table FQNs failed. |
| **Enterprise solution** | `discover_catalog()` resolves: env → config → `current_catalog()` → `SHOW CATALOGS` (usable only). Volume + table registration use the discovered name. |
| **Why portable** | No catalog string is assumed to exist unless Spark lists it or the operator sets an env override. |

### 2. Hardcoded Volume path `/Volumes/workspace/default/healthcare_lakehouse`

| | |
|--|--|
| **Files** | `databricks_runtime.py`, SQL scripts, READMEs |
| **Root cause** | Storage root mirrored Free Edition examples. |
| **Enterprise solution** | `configure_writable_volume()` builds `/Volumes/{discovered_catalog}/{schema}/{volume_name}`, `CREATE VOLUME IF NOT EXISTS`, then `paths.bind_storage_base(...)`. |
| **Why portable** | Storage follows the workspace catalog; override via `HEALTHCARE_STORAGE_BASE` when Volumes are centrally managed. |

### 3. Hardcoded Workspace / Repos / username paths in notebook bootstrap

| | |
|--|--|
| **Files** | Every notebook previously listed `/Workspace/Repos/Healthcare_Lakehouse`, `Users/<...>/Databricks-project`, etc. |
| **Root cause** | Bootstrap searched a fixed list of folder names. |
| **Why another workspace failed** | Clone path, repo name, or user folder differed. |
| **Enterprise solution** | Marker-based discovery (`config/config.py` presence): cwd parents → notebook path parents → shallow `/Workspace` scan. Shared via `_seed_project_root` + `src.utilities.bootstrap.bootstrap_notebook`. |
| **Why portable** | Works for any folder name as long as the repo contains `config/config.py`. |

### 4. Stale Python module cache after source edits / Git pull

| | |
|--|--|
| **Files** | All notebooks (interactive sessions) |
| **Root cause** | Databricks keeps imported modules in `sys.modules` for the session lifetime. |
| **Enterprise solution** | `clear_project_modules()` / `reload_project_modules()` invoked by `bootstrap_notebook(reload_modules=True)` on every notebook start. |
| **Why portable** | Source changes apply without cluster restart; no per-workspace `importlib.reload` cells. |

### 5. Destructive deletes (`dbutils.fs.rm`, `shutil.rmtree`)

| | |
|--|--|
| **Files** | `notebooks/Silver/02_scd_type1_type2_demo.py` |
| **Root cause** | Demo reset used recursive filesystem delete. |
| **Why another workspace failed** | Serverless / security policies require approval or deny recursive `rm`. |
| **Enterprise solution** | `soft_reset_delta_path()` — `DELETE FROM delta.\`path\`` or empty overwrite; never recursive FS delete. |
| **Why portable** | Uses Delta table APIs available in all UC-enabled runtimes without elevated FS rights. |

### 6. SCD Type 2 lazy DataFrame reuse after MERGE

| | |
|--|--|
| **Files** | `src/transformations/scd.py` |
| **Root cause** | `changes` / `news` were lazy plans that re-read the target after MERGE mutated it. |
| **Why symptoms varied** | Different Spark Connect / caching behavior across Free vs Enterprise runtimes. |
| **Enterprise solution** | `.cache()` + `.count()` materialization **before** MERGE/append; `unpersist()` in `finally`. |
| **Why portable** | Correct under any Spark evaluation strategy; prevents silent wrong SCD history. |

### 7. Orchestration relative notebook paths

| | |
|--|--|
| **Files** | `notebooks/00_run_all_pipelines.py` |
| **Root cause** | `./Bronze/...` only works when the driver notebook sits in `notebooks/`. |
| **Enterprise solution** | `resolve_notebook_path()` resolves against the current notebook’s workspace directory. |
| **Why portable** | Absolute workspace paths computed at runtime. |

### 8. SQL scripts with baked-in catalog / Volume / DBFS mounts

| | |
|--|--|
| **Files** | `sql/register_tables.sql`, `sql/ddl_unity_catalog.sql`, `sql/analytics_queries.sql` |
| **Root cause** | Static SQL cannot discover catalogs. |
| **Enterprise solution** | Templates use `{{CATALOG}}` / `{{VOLUME_BASE}}`; primary path is notebook auto-registration. Analytics SQL uses schema-qualified names after `USE CATALOG`. Removed `dbfs:/mnt/...` as a live dependency. |
| **Why portable** | Manual SQL is explicitly parameterized; day-2 ops use Python registration. |

### 9. Duplicate root-level `config.py` / `constants.py` / `paths.py`

| | |
|--|--|
| **Files** | Project-root mirrors of `config/` |
| **Root cause** | Historical duplicates could reintroduce old defaults. |
| **Enterprise solution** | Synced: empty catalog default, no `dbfs:/mnt/healthcare_lakehouse` fallback. Canonical package remains `config/`. |

---

## Runtime initialization contract (every notebook)

```text
1. _seed_project_root()          → find repo by marker, sys.path
2. bootstrap_notebook(reload=True) → refresh config/src/scripts modules
3. get_config()
4. prepare_databricks_runtime()  → discover catalog + bind Volume + patch input_file_name
5. logger + auditor on Volume paths
6. business logic (unchanged Medallion / SCD / DQ / Gold)
```

---

## What was NOT removed

- Medallion Bronze → Silver → Gold  
- Auto Loader design + batch fallback  
- SCD Type 1 / Type 2  
- Data Quality framework  
- Audit + structured logging  
- Monitoring / OPTIMIZE / VACUUM / time travel (config-gated)  
- Unity Catalog external table registration  
- Gold analytics marts  

---

## How to validate on a fresh workspace

1. Clone the same Git repo into any Workspace folder / Repo.  
2. Open `notebooks/Bronze/01_bronze_ingestion` and **Run All** (no edits).  
3. Confirm log line / `cfg.to_dict()` shows the **local** catalog and `/Volumes/<that_catalog>/...`.  
4. Continue Silver → Gold → DQ → Monitoring (or `00_run_all_pipelines`).  
5. Query: `SHOW TABLES IN <catalog>.bronze` (or `SELECT current_catalog()` then `SHOW TABLES IN bronze`).

If Volume creation is denied by policy, set:

```text
HEALTHCARE_STORAGE_BASE=/Volumes/<allowed_catalog>/<schema>/<existing_volume>
HEALTHCARE_UC_CATALOG=<allowed_catalog>
```

---

## Residual risks (operational, not code)

| Risk | Mitigation |
|------|------------|
| No permission to `CREATE VOLUME` / `CREATE SCHEMA` | Pre-create Volume; set `HEALTHCARE_STORAGE_BASE` |
| Workspace without Unity Catalog | Path-based Delta still works; table registration may skip — logs warn |
| Extremely nested / renamed clone with no `config/config.py` | Set `HEALTHCARE_LAKEHOUSE_ROOT` |
| Databricks Jobs imported without Git/workspace path mapping | Point job tasks at the cloned notebook paths (job JSON is a template) |

---

## Final statement

**This project will run correctly across different Databricks accounts and workspaces without requiring manual code changes**, as long as the runtime can resolve a project root (automatic or via `HEALTHCARE_LAKEHOUSE_ROOT`) and write to a UC Volume (automatic or via `HEALTHCARE_STORAGE_BASE`). All previously identified cross-workspace failure modes — catalog hardcoding, module caching, destructive deletes, and SCD2 lazy MERGE reuse — have been remediated with portable enterprise patterns while preserving full lakehouse functionality.

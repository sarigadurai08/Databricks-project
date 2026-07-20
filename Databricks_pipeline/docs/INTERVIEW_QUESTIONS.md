# Databricks & Delta Lake — Interview Questions

Expanded Q&A for Senior Data Engineer interviews. Each answer is concise enough for a live conversation but complete enough to demonstrate depth.

---

## Architecture & Medallion

### 1. What is the Medallion architecture?

A layered data design pattern with three tiers:

- **Bronze** — raw, append-only ingestion with lineage metadata; preserves source fidelity for replay and audit.
- **Silver** — cleansed, typed, deduplicated, conformed entities aligned to business keys.
- **Gold** — aggregated KPI marts optimized for BI, dashboards, and ML features.

Each layer adds quality and semantic meaning. Downstream consumers should prefer Gold for analytics and Silver for operational reporting, while Bronze supports forensic replay.

### 2. How does Bronze differ from Silver in this e-commerce project?

Bronze stores JSON events as ingested with metadata columns (`_ingestion_time`, `_record_hash`, etc.). Silver applies string standardization, type casting, deduplication by primary key (keeping the latest event time), and Delta MERGE upserts. Bronze is immutable history; Silver is the current-state conformed model.

### 3. Why separate Gold from Silver instead of querying Silver directly?

Gold pre-computes expensive joins and aggregations (CLV, funnels, hourly rollups) so dashboards query small, stable marts. This reduces warehouse cost, improves query latency, and decouples BI schema from operational entity changes.

---

## Delta Lake

### 4. What is Delta Lake and why use it over Parquet alone?

Delta Lake adds an ACID transaction log on top of Parquet files. It enables:

- Concurrent reads/writes without corruption
- MERGE (upsert/delete) operations
- Time travel (query historical versions)
- Schema evolution and enforcement
- OPTIMIZE/VACUUM file management

Parquet alone lacks transactional guarantees and upsert semantics.

### 5. Explain Delta MERGE and when you would use it.

`MERGE INTO target USING source ON condition WHEN MATCHED ... WHEN NOT MATCHED ...` performs idempotent upserts. In this project, Silver loads use MERGE on entity primary keys so reprocessing the same bronze batch updates existing rows instead of creating duplicates.

### 6. What is Delta time travel?

Time travel lets you query a table as of a previous version number or timestamp via `VERSION AS OF` or `TIMESTAMP AS OF`. Useful for debugging pipeline regressions, auditing historical state, and recovering from bad writes (with retention limits).

### 7. What is the difference between OPTIMIZE and VACUUM?

- **OPTIMIZE** compacts small files into larger ones and optionally ZORDERs data for filter performance.
- **VACUUM** physically deletes old files no longer referenced by the Delta log, subject to a retention window (default 7 days).

Run OPTIMIZE for read performance; VACUUM for storage cost — but never VACUUM below your time-travel retention needs.

### 8. What is ZORDER and when is it beneficial?

ZORDER co-locates related rows in the same files based on specified columns (e.g., `user_id`, `order_time`). After OPTIMIZE ZORDER BY, filters on those columns skip more files. Best for high-cardinality filter columns where partitioning would create too many small partitions.

### 9. Managed vs external Delta tables in Unity Catalog?

- **Managed** — UC controls both metadata and underlying storage location.
- **External** — table metadata in UC points to an existing cloud storage path via `LOCATION`.

On Databricks Volumes, external tables cannot point at Volume paths (Volumes and tables must not overlap). This project registers managed tables via CTAS from Volume Delta paths.

### 10. What is Liquid Clustering?

An incremental clustering approach (DBR 13.3+) alternative to partition + ZORDER. Tables declare `CLUSTER BY (cols)` and Delta incrementally reorganizes data. Useful when partition columns cause skew or too many partitions.

---

## Ingestion & Streaming

### 11. What is Databricks Auto Loader?

Auto Loader (`cloudFiles` format) incrementally ingests files from cloud storage. It tracks processed files via checkpoints, infers/evolves schema, supports rescue data columns for malformed fields, and scales to millions of files without expensive directory listings.

### 12. Auto Loader vs Structured Streaming file source?

Auto Loader is optimized for incremental file discovery at scale with built-in schema evolution. A basic `readStream.format("json")` re-lists files and lacks Auto Loader's notification mode and schema tracking. Auto Loader is preferred for production landing-zone ingestion.

### 13. Why does this project fall back from Auto Loader to batch reads?

Some Databricks Serverless and Free Edition configurations restrict `cloudFiles` or lack file notification infrastructure. The fallback reads landing JSON in batch mode with the same bronze metadata enrichment, preserving pipeline functionality across runtimes.

### 14. What is a Structured Streaming watermark?

A watermark defines how late events can arrive before being dropped or segregated: `withWatermark("event_time", "10 minutes")`. Events older than `max_event_time - watermark` are excluded from aggregations. It bounds state size and handles clock skew / delayed uploads.

### 15. How do streaming checkpoints work?

Structured Streaming persists progress (offsets, aggregation state) to a checkpoint directory. On restart, the query resumes from the last committed offset. Checkpoints must not be modified externally; deleting them resets the query to earliest/latest per configuration.

---

## Spark Performance

### 16. What is Adaptive Query Execution (AQE)?

AQE re-optimizes query plans at runtime based on actual statistics: coalescing shuffle partitions, converting sort-merge joins to broadcast joins, and handling skew. Enabled via `spark.sql.adaptive.enabled=true`. Reduces manual tuning for varying data volumes.

### 17. When should you broadcast a join?

Broadcast when one side is small enough (below `spark.sql.autoBroadcastJoinThreshold`, default ~10 MB). The small DataFrame is sent to all executors, avoiding a shuffle join. This project broadcasts dimension tables (users, products) in gold mart builds.

### 18. What causes shuffle in Spark and how do you minimize it?

Shuffles occur when data must be redistributed across partitions — joins, groupBy, distinct. Minimize via broadcast joins, pre-aggregation, bucketing, ZORDER, and filtering early. AQE coalesces and skew-handles shuffles at runtime.

---

## Unity Catalog & Governance

### 19. Why avoid hardcoding catalog names?

Workspace catalog names vary: `main`, `workspace`, custom enterprise names. Hardcoding breaks `USE CATALOG`, table FQNs, and Volume paths when the repo is cloned elsewhere. Runtime discovery via `SHOW CATALOGS` and `current_catalog()` ensures portability.

### 20. How does this project discover the catalog at runtime?

Priority order in `discover_catalog()`:

1. `ECOMMERCE_UC_CATALOG` environment variable
2. Config preferred value (if usable)
3. `current_catalog()`
4. First usable catalog from `SHOW CATALOGS`
5. Last-resort hints only if listed (`main`, `workspace`)

Each candidate is validated with `USE CATALOG`.

### 21. What is row-level security in Unity Catalog?

RLS policies filter rows based on user identity or group membership at query time. Defined via SQL security policies. Complements table/column ACLs for fine-grained access (e.g., regional managers see only their region's orders).

---

## Data Quality & Operations

### 22. How does the DQ framework quarantine bad records?

Rules produce a boolean fail condition per row. Failed rows are serialized to JSON payloads and appended to `data_quality/failed_records` and entity-specific quarantine Delta paths. Results (pass rate, counts) are persisted to `validation_results` for monitoring.

### 23. What makes a pipeline idempotent?

Same input produces the same output regardless of how many times it runs. Achieved via MERGE on natural keys, deterministic transforms, checkpointed streaming, and overwrite-or-merge gold patterns (not append-only duplicates).

### 24. Why use `soft_reset_delta_path` instead of deleting directories?

Recursive filesystem deletes (`dbutils.fs.rm`, `rmtree`) are blocked or require elevated permissions on Serverless and many enterprise workspaces. Delta-native `DELETE FROM delta.\`path\`` or empty overwrite achieves the same reset without destructive FS operations.

---

## Testing & Local Development

### 25. How do you test PySpark pipelines locally?

Use pytest with a session-scoped local `SparkSession` (Delta extensions configured), write to temp directories, monkeypatch `PATHS.storage_base` to tmp paths, and mock `dbutils` when needed. Integration tests skip gracefully when gold tables are absent.

### 26. What Java/Python versions are required for local PySpark?

Python 3.10–3.12 with Java 17 (`JAVA_HOME`). PySpark workers are incompatible with Python 3.14+. On Databricks, use DBR 13.3 LTS or Serverless — the managed runtime handles JVM/Python alignment.

---

## Scenario Questions

### 27. An order event arrives 2 hours late. How is it handled?

Silver dedupe keeps the latest `order_time` per `order_id`. Structured streaming aggregations with a 10-minute watermark may exclude it from hourly windows but it still lands in bronze/silver. Gold batch rebuilds include it on the next run. DQ can flag events exceeding `max_delay_hours`.

### 28. How would you backfill 30 days of historical orders?

Land historical JSON to the landing zone, run bronze ingestion (Auto Loader picks up existing files with `includeExistingFiles`), silver MERGE upserts by key, then rebuild gold marts. Checkpoints track progress; MERGE ensures idempotent replays.

### 29. Volume creation fails in a locked-down workspace. What do you do?

The runtime automatically falls back to `dbfs:/FileStore/ecommerce_lakehouse`. Alternatively, set `ECOMMERCE_STORAGE_BASE` to a pre-provisioned Volume path and `ECOMMERCE_UC_CATALOG` to the allowed catalog. No code changes required.

### 30. How do you register Volume-based Delta paths as SQL tables?

Cannot create external UC tables with `LOCATION` on Volume paths. Instead, use managed CTAS: `CREATE OR REPLACE TABLE catalog.gold.orders AS SELECT * FROM delta.\`/Volumes/.../gold/orders\``. The `table_registry.py` module automates this with fallback strategies.

---

## Quick-Fire Round

| # | Question | Short Answer |
|---|----------|--------------|
| 31 | CDF? | Change Data Feed — row-level change stream from Delta tables |
| 32 | Partition vs ZORDER? | Partition = directory pruning; ZORDER = intra-file sorting |
| 33 | Rescue column? | Captures malformed JSON fields Auto Loader cannot parse |
| 34 | Photon? | Databricks native vectorized engine for faster SQL/DF ops |
| 35 | Serverless compute? | Per-command billing, no cluster management, some API restrictions |
| 36 | MERGE conflict? | Concurrent writers on same keys — retry with transaction isolation |
| 37 | Schema evolution mode `addNewColumns`? | Auto Loader adds new JSON fields as nullable columns |
| 38 | Dead letter queue? | Failed ingestion rows routed to `{storage_base}/dead_letter/` |
| 39 | Broadcast threshold? | Max size (bytes) for automatic broadcast join |
| 40 | `current_catalog()` vs `SHOW CATALOGS`? | Current = active session catalog; SHOW = all accessible catalogs |

---

## Project-Specific Talking Points

When presenting this E-Commerce Lakehouse:

1. **Portability** — zero hardcoded catalogs; runtime discovery + DBFS fallback
2. **Resilience** — Auto Loader → batch fallback; soft reset instead of fs.rm
3. **Scale patterns** — 11 streaming entities, 19 gold marts, MERGE idempotency
4. **Observability** — audit, structured logs, DQ quarantine, pipeline status
5. **Enterprise readiness** — UC registration, parameterized SQL, dashboard specs, pytest coverage

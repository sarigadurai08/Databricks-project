# E-Commerce Lakehouse — Architecture

Enterprise medallion architecture for real-time e-commerce event ingestion, conformed silver entities, and gold analytics marts on Databricks + Delta Lake.

---

## System Context

```mermaid
flowchart TB
    subgraph Sources["Event Sources"]
        SIM[Streaming Event Simulator]
    end

    subgraph Landing["Landing Zone"]
        JSON[JSON Files per Entity]
    end

    subgraph Bronze["Bronze Layer"]
        AL[Auto Loader / Batch Fallback]
        BDELTA[(Delta Bronze Tables × 11)]
    end

    subgraph Silver["Silver Layer"]
        CL[Cleanse · Cast · Dedup]
        MG[Delta MERGE Upsert]
        SDELTA[(Delta Silver Tables × 11)]
    end

    subgraph Gold["Gold Layer"]
        AGG[Aggregations · Broadcast Joins]
        GMARTS[(19 Analytics Marts)]
    end

    subgraph Consumers["Consumers"]
        SQL[Databricks SQL]
        LV[Lakeview Dashboards]
        STR[Structured Streaming]
    end

    subgraph Ops["Operations"]
        DQ[Data Quality]
        AUD[Audit & Logging]
        MAINT[OPTIMIZE / VACUUM]
    end

    SIM --> JSON
    JSON --> AL
    AL --> BDELTA
    BDELTA --> CL --> MG --> SDELTA
    SDELTA --> AGG --> GMARTS
    GMARTS --> SQL
    GMARTS --> LV
    BDELTA --> STR
    SDELTA --> DQ
    BDELTA --> AUD
    SDELTA --> MAINT
```

---

## Medallion Layers

### Bronze — Raw + Lineage

- **Input:** JSON landing files from simulator (or external producers)
- **Ingestion:** Databricks Auto Loader (`cloudFiles`) with schema evolution, rescue column, checkpoints
- **Fallback:** Batch `spark.read.json()` when Auto Loader unavailable
- **Metadata columns:** `_ingestion_time`, `_source_file`, `_load_id`, `_batch_id`, `_record_hash`, `_event_time`
- **Storage:** `{storage_base}/bronze/{entity}`

### Silver — Conformed Entities

- **Transforms:** Trim strings, cast types, dedupe by primary key (keep latest event time)
- **Upsert:** Delta MERGE on entity primary keys (`ENTITY_PRIMARY_KEYS` in constants)
- **Output:** 11 conformed Delta tables at `{storage_base}/silver/{entity}`
- **Quality gates:** Optional DQ validation before/after merge

### Gold — Business Analytics

- **Pattern:** Read silver → broadcast small dimensions → aggregate → overwrite mart
- **Output:** 19 marts at `{storage_base}/gold/{mart_name}`
- **Registration:** Managed UC tables via CTAS (`register_gold_tables`)

---

## Data Flow

```mermaid
sequenceDiagram
    participant Sim as Simulator
    participant Land as Landing JSON
    participant Bron as Bronze Ingestion
    participant Sil as Silver Transform
    participant Gol as Gold Analytics
    participant UC as Unity Catalog

    Sim->>Land: Write entity JSON files
    Bron->>Land: Auto Loader / batch read
    Bron->>Bron: add_bronze_metadata()
    Bron->>Bron: Append Delta bronze
    Sil->>Bron: Read bronze Delta
    Sil->>Sil: Cleanse + dedupe
    Sil->>Sil: MERGE into silver
    Gol->>Sil: Read silver entities
    Gol->>Gol: build_* mart functions
    Gol->>Gol: Overwrite gold Delta
    Gol->>UC: register_gold_tables (CTAS)
```

---

## Streaming Flow

```mermaid
flowchart LR
    subgraph Gen["Event Generation"]
        T1[Tick 1]
        T2[Tick 2]
        T3[Tick N]
    end

    subgraph Files["Landing Files"]
        F1[users_*.json]
        F2[orders_*.json]
        F3[click_logs_*.json]
    end

    subgraph Ingest["Incremental Ingest"]
        CP[Auto Loader Checkpoint]
        WM[Watermark 10 min]
    end

    subgraph Stream["Structured Streaming"]
        SS[Micro-batch Aggregations]
        SC[Streaming Checkpoint]
    end

    T1 --> F1
    T2 --> F2
    T3 --> F3
    F1 --> CP
    F2 --> CP
    F3 --> CP
    CP --> SS
    WM --> SS
    SS --> SC
```

**Simulator settings** (configurable via `ECOMMERCE_*`):

- Default: 3 ticks, 25 events per entity per tick, 60s interval between ticks
- FK coherence: shared user/product/order/session ID pools within a tick

**Structured streaming notebook** (`Streaming/01_structured_streaming.py`):

- Reads bronze or silver click/order streams
- Applies watermark for late-arrival handling
- Writes aggregated metrics to streaming checkpoint paths

---

## Transformation Flow (Silver)

```mermaid
flowchart TB
    RAW[Bronze DataFrame]
    STD[Standardize Strings / Email]
    CAST[Cast Column Types]
    DED[Dedupe Keep Latest]
    DQ[Optional DQ Rules]
    MERGE[Delta MERGE by PK]
    SILVER[Silver Delta Table]

    RAW --> STD --> CAST --> DED --> DQ --> MERGE --> SILVER
```

Primary keys per entity are defined in `config/constants.py` (`ENTITY_PRIMARY_KEYS`).

---

## Transformation Flow (Gold)

```mermaid
flowchart TB
    U[users]
    P[products]
    O[orders]
    PAY[payments]
    CART[shopping_cart]
    CLK[click_logs]
    INV[inventory]
    CPN[coupons]

    subgraph Marts["Gold Marts"]
        CJ[customer_journey]
        TP[top_products]
        RD[revenue_dashboard]
        CA[cart_abandonment]
        WT[website_traffic]
    end

    U --> CJ
    O --> CJ
    CLK --> CJ
    CART --> CJ

    O --> TP
    P --> TP
    CART --> TP

    O --> RD
    PAY --> RD

    CART --> CA
    U --> CA

    CLK --> WT
```

Each mart builder in `src/transformations/gold_transforms.py` gracefully skips when required silver sources are missing.

---

## Runtime & Storage Architecture

```mermaid
flowchart TB
    NB[Notebook Start]
    SEED[Seed Project Root]
    BOOT[Bootstrap + Reload Modules]
    PREP[prepare_databricks_runtime]

    subgraph Discovery["Runtime Discovery"]
        CAT[discover_catalog]
        VOL[configure_writable_volume]
        DBFS[DBFS Fallback]
    end

    BIND[Bind PATHS.storage_base]
    LOGIC[Pipeline Logic]

    NB --> SEED --> BOOT --> PREP
    PREP --> CAT
    PREP --> VOL
    VOL -->|Volume OK| BIND
    VOL -->|Volume fail| DBFS --> BIND
    BIND --> LOGIC
```

**Storage priority:**

1. `ECOMMERCE_STORAGE_BASE` env override (if set)
2. UC Volume: `/Volumes/{catalog}/{schema}/ecommerce_lakehouse`
3. DBFS fallback: `dbfs:/FileStore/ecommerce_lakehouse`

---

## Optimization Strategy

| Technique | Where Applied | Config Gate |
|-----------|---------------|---------------|
| **AQE** | All Spark jobs | Always on (`SparkConfig.enable_aqe`) |
| **Broadcast joins** | Gold dimension joins (users, products) | Automatic via `F.broadcast()` |
| **Delta optimizeWrite / autoCompact** | All Delta writes | Spark conf defaults |
| **ZORDER** | Silver/bronze entity tables | `ECOMMERCE_ZORDER_ENABLED` |
| **OPTIMIZE** | Monitoring notebook | `ECOMMERCE_OPTIMIZE_ENABLED` |
| **VACUUM** | Monitoring notebook | `ECOMMERCE_VACUUM_ENABLED` |
| **Liquid clustering** | Optional table alteration | `ECOMMERCE_LIQUID_CLUSTERING_ENABLED` |
| **Partitioning** | Bronze `ingestion_date` generated column | Per entity in autoloader |
| **Caching** | Gold build hot silver tables | `.cache()` + `.count()` in gold_transforms |

ZORDER column mappings per entity: `OPTIMIZE_ZORDER_COLUMNS` in `config/constants.py`.

---

## Governance & Observability

| Component | Path / Table | Purpose |
|-----------|--------------|---------|
| Pipeline audit | `{storage_base}/audit/pipeline_audit` | Step timing, row counts, status |
| Structured logs | `{storage_base}/ops_logging/pipeline_logs` | INFO/WARN/ERROR with context |
| DQ results | `{storage_base}/data_quality/validation_results` | Rule pass/fail metrics |
| Failed records | `{storage_base}/data_quality/failed_records` | Quarantined row payloads |
| Dead letter | `{storage_base}/dead_letter/{entity}` | Ingestion failures |
| Quarantine | `{storage_base}/quarantine/{entity}` | DQ-failed silver rows |

UC registration maps ops tables under `audit`, `ops_logging`, and `data_quality` schemas.

---

## Security & Portability Principles

1. **No hardcoded catalog** — discovered at runtime
2. **No writes to Git** — cloud storage binding on Databricks
3. **No destructive FS deletes** — Delta-native soft reset
4. **Idempotent MERGE** — safe replays and backfills
5. **Parameterized SQL** — `{{CATALOG}}` templates for manual ops

See [../COMPATIBILITY_REPORT.md](../COMPATIBILITY_REPORT.md) for the full portability audit.

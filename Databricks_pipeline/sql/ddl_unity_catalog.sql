-- =============================================================================
-- E-Commerce Lakehouse — Unity Catalog DDL Template
-- =============================================================================
-- PORTABLE: replace {{CATALOG}} with the catalog discovered at runtime
-- (see prepare_databricks_runtime / SELECT current_catalog()).
-- Prefer notebook auto-registration (register_all_layers) over running this file.
-- =============================================================================

-- CREATE CATALOG IF NOT EXISTS {{CATALOG}};  -- requires metastore admin
USE CATALOG {{CATALOG}};

-- Medallion schemas
CREATE SCHEMA IF NOT EXISTS bronze
  COMMENT 'Raw ingested e-commerce events with lineage metadata';

CREATE SCHEMA IF NOT EXISTS silver
  COMMENT 'Cleansed and conformed e-commerce entities';

CREATE SCHEMA IF NOT EXISTS gold
  COMMENT 'Analytics marts for dashboards and BI';

CREATE SCHEMA IF NOT EXISTS audit
  COMMENT 'Pipeline audit trail';

CREATE SCHEMA IF NOT EXISTS data_quality
  COMMENT 'DQ validation results and failed records';

CREATE SCHEMA IF NOT EXISTS ops_logging
  COMMENT 'Structured pipeline logs';

-- ---------------------------------------------------------------------------
-- Example: Silver orders table (managed reference schema)
-- Production tables are registered via CTAS from Delta paths on Volumes.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.orders (
  order_id STRING NOT NULL,
  user_id STRING,
  order_time TIMESTAMP,
  status STRING,
  total_amount DOUBLE,
  discount_amount DOUBLE,
  shipping_amount DOUBLE,
  shipping_region STRING,
  coupon_code STRING,
  item_count INT,
  _ingestion_time TIMESTAMP,
  _source_file STRING,
  _load_id STRING,
  _batch_id STRING,
  _record_hash STRING,
  _event_time TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

ALTER TABLE silver.orders
  ADD CONSTRAINT chk_orders_status
  CHECK (status IN (
    'Pending', 'Confirmed', 'Processing', 'Shipped',
    'Delivered', 'Cancelled', 'Returned'
  ));

-- ---------------------------------------------------------------------------
-- Example: Gold top_products mart (reference columns)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.top_products (
  product_id STRING,
  product_name STRING,
  category STRING,
  brand STRING,
  price DOUBLE,
  is_active BOOLEAN,
  units_demanded BIGINT,
  cart_appearances BIGINT,
  units_converted BIGINT,
  review_count BIGINT,
  avg_rating DOUBLE,
  demand_score DOUBLE
)
USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- Liquid Clustering (DBR 13.3+) — optional
-- ALTER TABLE gold.revenue_dashboard CLUSTER BY (revenue_date);

-- External / path-based registration uses the runtime Volume, e.g.:
-- CREATE OR REPLACE TABLE gold.top_products
-- AS SELECT * FROM delta.`{{VOLUME_BASE}}/gold/top_products`;

-- Verify schemas
SHOW SCHEMAS IN {{CATALOG}};
SHOW TABLES IN {{CATALOG}}.gold;

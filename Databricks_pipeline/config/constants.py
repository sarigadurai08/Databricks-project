"""
Enterprise constants for the E-Commerce Lakehouse platform.

Centralizes magic strings, status codes, and domain enumerations so that
pipelines, notebooks, and tests share a single source of truth.
"""

from __future__ import annotations

from enum import Enum


# ---------------------------------------------------------------------------
# Catalog / schema naming (Unity Catalog compatible)
#
# CATALOG_NAME is a documentation default only. Runtime catalog is discovered
# by src.utilities.databricks_runtime.discover_catalog (or ECOMMERCE_UC_CATALOG).
# Schema layer names below are overridable via ECOMMERCE_UC_*_SCHEMA env vars.
# ---------------------------------------------------------------------------
CATALOG_NAME = ""  # resolved at runtime — do not hardcode a workspace catalog
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
AUDIT_SCHEMA = "audit"
DQ_SCHEMA = "data_quality"
LOG_SCHEMA = "ops_logging"
DLQ_SCHEMA = "dead_letter"

# ---------------------------------------------------------------------------
# Pipeline identifiers
# ---------------------------------------------------------------------------
PIPELINE_STREAMING_SIMULATOR = "streaming_event_simulator"
PIPELINE_BRONZE_INGESTION = "bronze_auto_loader_ingestion"
PIPELINE_SILVER_TRANSFORM = "silver_cleanse_and_merge"
PIPELINE_GOLD_ANALYTICS = "gold_ecommerce_analytics"
PIPELINE_DQ_FRAMEWORK = "data_quality_framework"
PIPELINE_MAINTENANCE = "delta_table_maintenance"
PIPELINE_STREAMING = "structured_streaming_analytics"

# ---------------------------------------------------------------------------
# Layer names
# ---------------------------------------------------------------------------
LAYER_BRONZE = "bronze"
LAYER_SILVER = "silver"
LAYER_GOLD = "gold"

# ---------------------------------------------------------------------------
# Entity / table base names (streaming JSON sources)
# ---------------------------------------------------------------------------
ENTITY_USERS = "users"
ENTITY_PRODUCTS = "products"
ENTITY_ORDERS = "orders"
ENTITY_PAYMENTS = "payments"
ENTITY_REVIEWS = "reviews"
ENTITY_SHOPPING_CART = "shopping_cart"
ENTITY_CLICK_LOGS = "click_logs"
ENTITY_INVENTORY = "inventory"
ENTITY_COUPONS = "coupons"
ENTITY_DELIVERY = "delivery"
ENTITY_SUPPORT_EVENTS = "support_events"

ALL_ENTITIES = (
    ENTITY_USERS,
    ENTITY_PRODUCTS,
    ENTITY_ORDERS,
    ENTITY_PAYMENTS,
    ENTITY_REVIEWS,
    ENTITY_SHOPPING_CART,
    ENTITY_CLICK_LOGS,
    ENTITY_INVENTORY,
    ENTITY_COUPONS,
    ENTITY_DELIVERY,
    ENTITY_SUPPORT_EVENTS,
)

# Primary key columns per entity (silver MERGE / DQ)
ENTITY_PRIMARY_KEYS: dict[str, list[str]] = {
    ENTITY_USERS: ["user_id"],
    ENTITY_PRODUCTS: ["product_id"],
    ENTITY_ORDERS: ["order_id"],
    ENTITY_PAYMENTS: ["payment_id"],
    ENTITY_REVIEWS: ["review_id"],
    ENTITY_SHOPPING_CART: ["cart_id", "product_id"],
    ENTITY_CLICK_LOGS: ["event_id"],
    ENTITY_INVENTORY: ["product_id", "warehouse_id"],
    ENTITY_COUPONS: ["coupon_usage_id"],
    ENTITY_DELIVERY: ["delivery_id"],
    ENTITY_SUPPORT_EVENTS: ["ticket_id"],
}

# Event-time column for watermarking / late-arrival handling
ENTITY_EVENT_TIME_COLUMNS: dict[str, str] = {
    ENTITY_USERS: "event_time",
    ENTITY_PRODUCTS: "event_time",
    ENTITY_ORDERS: "order_time",
    ENTITY_PAYMENTS: "payment_time",
    ENTITY_REVIEWS: "review_time",
    ENTITY_SHOPPING_CART: "event_time",
    ENTITY_CLICK_LOGS: "event_time",
    ENTITY_INVENTORY: "event_time",
    ENTITY_COUPONS: "redeemed_at",
    ENTITY_DELIVERY: "status_time",
    ENTITY_SUPPORT_EVENTS: "event_time",
}

# ---------------------------------------------------------------------------
# Metadata column names (bronze lineage)
# ---------------------------------------------------------------------------
META_INGESTION_TIME = "_ingestion_time"
META_SOURCE_FILE = "_source_file"
META_LOAD_ID = "_load_id"
META_BATCH_ID = "_batch_id"
META_RECORD_HASH = "_record_hash"
META_EVENT_TIME = "_event_time"
META_RESCUED_DATA = "_rescued_data"

# ---------------------------------------------------------------------------
# Domain enumerations
# ---------------------------------------------------------------------------
class OrderStatus(str, Enum):
    PENDING = "Pending"
    CONFIRMED = "Confirmed"
    PROCESSING = "Processing"
    SHIPPED = "Shipped"
    DELIVERED = "Delivered"
    CANCELLED = "Cancelled"
    RETURNED = "Returned"


class PaymentStatus(str, Enum):
    AUTHORIZED = "Authorized"
    CAPTURED = "Captured"
    FAILED = "Failed"
    REFUNDED = "Refunded"
    PENDING = "Pending"


class CartStatus(str, Enum):
    ACTIVE = "Active"
    ABANDONED = "Abandoned"
    CONVERTED = "Converted"


class DeliveryStatus(str, Enum):
    LABEL_CREATED = "LabelCreated"
    PICKED_UP = "PickedUp"
    IN_TRANSIT = "InTransit"
    OUT_FOR_DELIVERY = "OutForDelivery"
    DELIVERED = "Delivered"
    FAILED = "Failed"
    RETURNED = "Returned"


class SupportStatus(str, Enum):
    OPEN = "Open"
    IN_PROGRESS = "InProgress"
    RESOLVED = "Resolved"
    CLOSED = "Closed"
    ESCALATED = "Escalated"


class PipelineStatus(str, Enum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    PARTIAL = "PARTIAL"


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


VALID_ORDER_STATUSES = {s.value for s in OrderStatus}
VALID_PAYMENT_STATUSES = {s.value for s in PaymentStatus}
VALID_CART_STATUSES = {s.value for s in CartStatus}
VALID_DELIVERY_STATUSES = {s.value for s in DeliveryStatus}
VALID_SUPPORT_STATUSES = {s.value for s in SupportStatus}

CATEGORIES = (
    "Electronics",
    "Fashion",
    "Home & Kitchen",
    "Beauty",
    "Sports",
    "Books",
    "Toys",
    "Grocery",
    "Automotive",
    "Health",
)

REGIONS = (
    "Northeast",
    "Southeast",
    "Midwest",
    "Southwest",
    "West",
    "Northwest",
)

PAYMENT_METHODS = (
    "CreditCard",
    "DebitCard",
    "PayPal",
    "ApplePay",
    "GooglePay",
    "GiftCard",
    "BNPL",
)

WAREHOUSES = (
    "WH-EAST-01",
    "WH-WEST-01",
    "WH-CENTRAL-01",
    "WH-SOUTH-01",
    "WH-NORTH-01",
)

# ---------------------------------------------------------------------------
# Auto Loader / CloudFiles defaults
# ---------------------------------------------------------------------------
RESCUE_DATA_COLUMN = "_rescued_data"
BAD_RECORDS_PATH_SUFFIX = "_bad_records"
SCHEMA_LOCATION_SUFFIX = "_schemas"
CHECKPOINT_SUFFIX = "_checkpoints"

# ---------------------------------------------------------------------------
# Performance defaults
# ---------------------------------------------------------------------------
DEFAULT_SHUFFLE_PARTITIONS = 8
BROADCAST_THRESHOLD_BYTES = 10 * 1024 * 1024  # 10 MB
STREAMING_WATERMARK = "10 minutes"
STREAMING_TRIGGER_INTERVAL = "30 seconds"
SIMULATOR_INTERVAL_SECONDS = 60  # generate new JSON every minute (configurable)

OPTIMIZE_ZORDER_COLUMNS = {
    ENTITY_USERS: ["user_id", "region"],
    ENTITY_PRODUCTS: ["product_id", "category"],
    ENTITY_ORDERS: ["order_id", "user_id", "order_time"],
    ENTITY_PAYMENTS: ["payment_id", "order_id"],
    ENTITY_REVIEWS: ["product_id", "user_id"],
    ENTITY_SHOPPING_CART: ["user_id", "cart_id"],
    ENTITY_CLICK_LOGS: ["session_id", "user_id", "event_time"],
    ENTITY_INVENTORY: ["product_id", "warehouse_id"],
    ENTITY_COUPONS: ["coupon_code", "user_id"],
    ENTITY_DELIVERY: ["order_id", "delivery_id"],
    ENTITY_SUPPORT_EVENTS: ["ticket_id", "user_id"],
}

# ---------------------------------------------------------------------------
# Retry / error handling
# ---------------------------------------------------------------------------
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2
RETRY_MAX_BACKOFF_SECONDS = 30

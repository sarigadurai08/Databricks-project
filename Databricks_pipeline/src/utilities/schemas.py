"""
Explicit Spark schemas for e-commerce entities.

Used by ingestion (schema hints), the streaming simulator, tests, and
documentation of the canonical Bronze/Silver contracts.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config.constants import (
    ENTITY_CLICK_LOGS,
    ENTITY_COUPONS,
    ENTITY_DELIVERY,
    ENTITY_INVENTORY,
    ENTITY_ORDERS,
    ENTITY_PAYMENTS,
    ENTITY_PRODUCTS,
    ENTITY_REVIEWS,
    ENTITY_SHOPPING_CART,
    ENTITY_SUPPORT_EVENTS,
    ENTITY_USERS,
    META_BATCH_ID,
    META_EVENT_TIME,
    META_INGESTION_TIME,
    META_LOAD_ID,
    META_RECORD_HASH,
    META_RESCUED_DATA,
    META_SOURCE_FILE,
)


USERS_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), False),
        StructField("email", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("region", StringType(), True),
        StructField("signup_date", DateType(), True),
        StructField("status", StringType(), True),
        StructField("loyalty_tier", StringType(), True),
        StructField("event_time", TimestampType(), True),
    ]
)

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DecimalType(12, 2), True),
        StructField("cost", DecimalType(12, 2), True),
        StructField("is_active", BooleanType(), True),
        StructField("sku", StringType(), True),
        StructField("event_time", TimestampType(), True),
    ]
)

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("order_time", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("total_amount", DecimalType(12, 2), True),
        StructField("discount_amount", DecimalType(12, 2), True),
        StructField("shipping_amount", DecimalType(12, 2), True),
        StructField("shipping_region", StringType(), True),
        StructField("coupon_code", StringType(), True),
        StructField("item_count", IntegerType(), True),
    ]
)

PAYMENTS_SCHEMA = StructType(
    [
        StructField("payment_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("user_id", StringType(), True),
        StructField("payment_time", TimestampType(), True),
        StructField("amount", DecimalType(12, 2), True),
        StructField("method", StringType(), True),
        StructField("status", StringType(), True),
        StructField("currency", StringType(), True),
    ]
)

REVIEWS_SCHEMA = StructType(
    [
        StructField("review_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("order_id", StringType(), True),
        StructField("rating", IntegerType(), True),
        StructField("review_text", StringType(), True),
        StructField("review_time", TimestampType(), True),
        StructField("verified_purchase", BooleanType(), True),
    ]
)

SHOPPING_CART_SCHEMA = StructType(
    [
        StructField("cart_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("session_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DecimalType(12, 2), True),
        StructField("status", StringType(), True),
        StructField("event_time", TimestampType(), True),
    ]
)

CLICK_LOGS_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("page_url", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("referrer", StringType(), True),
        StructField("event_time", TimestampType(), True),
    ]
)

INVENTORY_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("warehouse_id", StringType(), False),
        StructField("quantity_on_hand", IntegerType(), True),
        StructField("quantity_reserved", IntegerType(), True),
        StructField("reorder_level", IntegerType(), True),
        StructField("event_time", TimestampType(), True),
    ]
)

COUPONS_SCHEMA = StructType(
    [
        StructField("coupon_usage_id", StringType(), False),
        StructField("coupon_code", StringType(), False),
        StructField("user_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("discount_amount", DecimalType(12, 2), True),
        StructField("discount_pct", DoubleType(), True),
        StructField("redeemed_at", TimestampType(), True),
    ]
)

DELIVERY_SCHEMA = StructType(
    [
        StructField("delivery_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("carrier", StringType(), True),
        StructField("tracking_number", StringType(), True),
        StructField("status", StringType(), True),
        StructField("status_time", TimestampType(), True),
        StructField("region", StringType(), True),
        StructField("estimated_delivery", DateType(), True),
    ]
)

SUPPORT_EVENTS_SCHEMA = StructType(
    [
        StructField("ticket_id", StringType(), False),
        StructField("user_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("status", StringType(), True),
        StructField("priority", StringType(), True),
        StructField("subject", StringType(), True),
        StructField("event_time", TimestampType(), True),
    ]
)

BRONZE_METADATA_FIELDS = [
    StructField(META_INGESTION_TIME, TimestampType(), False),
    StructField(META_SOURCE_FILE, StringType(), True),
    StructField(META_LOAD_ID, StringType(), True),
    StructField(META_BATCH_ID, StringType(), True),
    StructField(META_RECORD_HASH, StringType(), True),
    StructField(META_EVENT_TIME, TimestampType(), True),
    StructField(META_RESCUED_DATA, StringType(), True),
    StructField("ingestion_date", DateType(), True),
]

ENTITY_SCHEMAS: dict[str, StructType] = {
    ENTITY_USERS: USERS_SCHEMA,
    ENTITY_PRODUCTS: PRODUCTS_SCHEMA,
    ENTITY_ORDERS: ORDERS_SCHEMA,
    ENTITY_PAYMENTS: PAYMENTS_SCHEMA,
    ENTITY_REVIEWS: REVIEWS_SCHEMA,
    ENTITY_SHOPPING_CART: SHOPPING_CART_SCHEMA,
    ENTITY_CLICK_LOGS: CLICK_LOGS_SCHEMA,
    ENTITY_INVENTORY: INVENTORY_SCHEMA,
    ENTITY_COUPONS: COUPONS_SCHEMA,
    ENTITY_DELIVERY: DELIVERY_SCHEMA,
    ENTITY_SUPPORT_EVENTS: SUPPORT_EVENTS_SCHEMA,
}


def get_entity_schema(entity: str) -> StructType:
    """Return the canonical StructType for an entity, or raise KeyError."""
    if entity not in ENTITY_SCHEMAS:
        raise KeyError(f"No schema registered for entity={entity}")
    return ENTITY_SCHEMAS[entity]


def with_bronze_metadata(schema: StructType) -> StructType:
    return StructType(list(schema.fields) + BRONZE_METADATA_FIELDS)

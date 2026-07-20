"""
Silver-layer cleansing transformers for each e-commerce entity.

Reusable, unit-testable functions that clean, cast, standardize, and dedupe
bronze records, then MERGE into silver Delta tables using ENTITY_PRIMARY_KEYS.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.constants import (
    ALL_ENTITIES,
    ENTITY_CLICK_LOGS,
    ENTITY_COUPONS,
    ENTITY_DELIVERY,
    ENTITY_EVENT_TIME_COLUMNS,
    ENTITY_INVENTORY,
    ENTITY_ORDERS,
    ENTITY_PAYMENTS,
    ENTITY_PRIMARY_KEYS,
    ENTITY_PRODUCTS,
    ENTITY_REVIEWS,
    ENTITY_SHOPPING_CART,
    ENTITY_SUPPORT_EVENTS,
    ENTITY_USERS,
    PIPELINE_SILVER_TRANSFORM,
    VALID_CART_STATUSES,
    VALID_DELIVERY_STATUSES,
    VALID_ORDER_STATUSES,
    VALID_PAYMENT_STATUSES,
    VALID_SUPPORT_STATUSES,
)
from config.paths import PATHS
from src.utilities.dataframe_utils import (
    cast_columns,
    dedupe_keep_latest,
    standardize_email,
    standardize_string_columns,
)
from src.utilities.delta_helpers import merge_delta, table_exists
from src.utilities.exceptions import TransformationError

if TYPE_CHECKING:
    from src.audit.auditor import PipelineAuditor
    from src.logging.logger import EcommerceLogger


META_DROP = [
    "_ingestion_time",
    "_source_file",
    "_load_id",
    "_batch_id",
    "_record_hash",
    "_rescued_data",
    "_event_time",
    "ingestion_date",
]


def _drop_meta(df: DataFrame, keep_lineage: bool = False) -> DataFrame:
    if keep_lineage:
        return df
    drop_cols = [c for c in META_DROP if c in df.columns]
    return df.drop(*drop_cols) if drop_cols else df


def _filter_late_arrivals(
    df: DataFrame,
    event_col: str,
    max_delay_hours: Optional[float],
) -> DataFrame:
    """Optional watermark-style filter: drop events older than max_delay_hours."""
    if max_delay_hours is None or event_col not in df.columns:
        return df
    return df.filter(
        F.col(event_col).isNull()
        | (
            (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col(event_col)))
            / F.lit(3600.0)
            <= F.lit(max_delay_hours)
        )
    )


def _materialize(df: DataFrame) -> DataFrame:
    """Cache and force materialization before MERGE when the plan is expensive."""
    try:
        cached = df.cache()
        cached.count()
        return cached
    except Exception:
        return df


# ---------------------------------------------------------------------------
# Entity cleaners
# ---------------------------------------------------------------------------
def clean_users(df: DataFrame, keep_lineage: bool = False) -> DataFrame:
    df = _drop_meta(df, keep_lineage)
    df = standardize_string_columns(
        df,
        ["user_id", "email", "first_name", "last_name", "phone", "region", "status", "loyalty_tier"],
    )
    if "email" in df.columns:
        df = df.withColumn("email", standardize_email("email"))
    df = cast_columns(df, {"signup_date": "date", "event_time": "timestamp"})
    df = df.filter(F.col("user_id").isNotNull() & (F.trim(F.col("user_id")) != ""))
    if "status" in df.columns:
        df = df.withColumn(
            "status",
            F.when(F.initcap(F.col("status")).isin("Active", "Inactive", "Suspended"), F.initcap(F.col("status")))
            .otherwise(F.lit("Active")),
        )
    return dedupe_keep_latest(df, ["user_id"], "event_time")


def clean_products(df: DataFrame, keep_lineage: bool = False) -> DataFrame:
    df = _drop_meta(df, keep_lineage)
    df = standardize_string_columns(
        df, ["product_id", "product_name", "category", "brand", "sku"]
    )
    df = cast_columns(
        df,
        {
            "price": "decimal(12,2)",
            "cost": "decimal(12,2)",
            "is_active": "boolean",
            "event_time": "timestamp",
        },
    )
    df = df.filter(F.col("product_id").isNotNull())
    if "price" in df.columns:
        df = df.filter(F.col("price").isNull() | (F.col("price") >= 0))
    return dedupe_keep_latest(df, ["product_id"], "event_time")


def clean_orders(df: DataFrame, keep_lineage: bool = False) -> DataFrame:
    df = _drop_meta(df, keep_lineage)
    df = standardize_string_columns(
        df, ["order_id", "user_id", "status", "shipping_region", "coupon_code"]
    )
    df = cast_columns(
        df,
        {
            "order_time": "timestamp",
            "total_amount": "decimal(12,2)",
            "discount_amount": "decimal(12,2)",
            "shipping_amount": "decimal(12,2)",
            "item_count": "int",
        },
    )
    df = df.withColumn(
        "status",
        F.when(F.col("status").isin(list(VALID_ORDER_STATUSES)), F.col("status")).otherwise(
            F.lit("Pending")
        ),
    )
    df = df.filter(F.col("order_id").isNotNull() & F.col("user_id").isNotNull())
    if "total_amount" in df.columns:
        df = df.filter(F.col("total_amount").isNull() | (F.col("total_amount") >= 0))
    return dedupe_keep_latest(df, ["order_id"], "order_time")


def clean_payments(df: DataFrame, keep_lineage: bool = False) -> DataFrame:
    df = _drop_meta(df, keep_lineage)
    df = standardize_string_columns(
        df, ["payment_id", "order_id", "user_id", "method", "status", "currency"]
    )
    df = cast_columns(df, {"payment_time": "timestamp", "amount": "decimal(12,2)"})
    df = df.withColumn(
        "status",
        F.when(F.col("status").isin(list(VALID_PAYMENT_STATUSES)), F.col("status")).otherwise(
            F.lit("Pending")
        ),
    )
    df = df.filter(F.col("payment_id").isNotNull() & F.col("order_id").isNotNull())
    if "amount" in df.columns:
        df = df.filter(F.col("amount").isNull() | (F.col("amount") >= 0))
    return dedupe_keep_latest(df, ["payment_id"], "payment_time")


def clean_reviews(df: DataFrame, keep_lineage: bool = False) -> DataFrame:
    df = _drop_meta(df, keep_lineage)
    df = standardize_string_columns(
        df, ["review_id", "product_id", "user_id", "order_id", "review_text"]
    )
    df = cast_columns(
        df,
        {
            "rating": "int",
            "review_time": "timestamp",
            "verified_purchase": "boolean",
        },
    )
    df = df.filter(F.col("review_id").isNotNull())
    if "rating" in df.columns:
        df = df.filter(F.col("rating").isNull() | ((F.col("rating") >= 1) & (F.col("rating") <= 5)))
    return dedupe_keep_latest(df, ["review_id"], "review_time")


def clean_shopping_cart(df: DataFrame, keep_lineage: bool = False) -> DataFrame:
    df = _drop_meta(df, keep_lineage)
    df = standardize_string_columns(
        df, ["cart_id", "user_id", "product_id", "session_id", "status"]
    )
    df = cast_columns(
        df,
        {
            "quantity": "int",
            "unit_price": "decimal(12,2)",
            "event_time": "timestamp",
        },
    )
    df = df.withColumn(
        "status",
        F.when(F.col("status").isin(list(VALID_CART_STATUSES)), F.col("status")).otherwise(
            F.lit("Active")
        ),
    )
    df = df.filter(
        F.col("cart_id").isNotNull()
        & F.col("product_id").isNotNull()
        & (F.col("quantity").isNull() | (F.col("quantity") > 0))
    )
    return dedupe_keep_latest(df, ["cart_id", "product_id"], "event_time")


def clean_click_logs(df: DataFrame, keep_lineage: bool = False) -> DataFrame:
    df = _drop_meta(df, keep_lineage)
    df = standardize_string_columns(
        df,
        [
            "event_id",
            "user_id",
            "session_id",
            "product_id",
            "page_url",
            "event_type",
            "device_type",
            "referrer",
        ],
    )
    df = cast_columns(df, {"event_time": "timestamp"})
    df = df.filter(F.col("event_id").isNotNull())
    return dedupe_keep_latest(df, ["event_id"], "event_time")


def clean_inventory(df: DataFrame, keep_lineage: bool = False) -> DataFrame:
    df = _drop_meta(df, keep_lineage)
    df = standardize_string_columns(df, ["product_id", "warehouse_id"])
    df = cast_columns(
        df,
        {
            "quantity_on_hand": "int",
            "quantity_reserved": "int",
            "reorder_level": "int",
            "event_time": "timestamp",
        },
    )
    df = df.filter(F.col("product_id").isNotNull() & F.col("warehouse_id").isNotNull())
    for col in ("quantity_on_hand", "quantity_reserved", "reorder_level"):
        if col in df.columns:
            df = df.withColumn(col, F.when(F.col(col) < 0, F.lit(0)).otherwise(F.col(col)))
    return dedupe_keep_latest(df, ["product_id", "warehouse_id"], "event_time")


def clean_coupons(df: DataFrame, keep_lineage: bool = False) -> DataFrame:
    df = _drop_meta(df, keep_lineage)
    df = standardize_string_columns(
        df, ["coupon_usage_id", "coupon_code", "user_id", "order_id"]
    )
    if "coupon_code" in df.columns:
        df = df.withColumn("coupon_code", F.upper(F.trim(F.col("coupon_code"))))
    df = cast_columns(
        df,
        {
            "discount_amount": "decimal(12,2)",
            "discount_pct": "double",
            "redeemed_at": "timestamp",
        },
    )
    df = df.filter(F.col("coupon_usage_id").isNotNull() & F.col("coupon_code").isNotNull())
    return dedupe_keep_latest(df, ["coupon_usage_id"], "redeemed_at")


def clean_delivery(df: DataFrame, keep_lineage: bool = False) -> DataFrame:
    df = _drop_meta(df, keep_lineage)
    df = standardize_string_columns(
        df, ["delivery_id", "order_id", "carrier", "tracking_number", "status", "region"]
    )
    df = cast_columns(
        df, {"status_time": "timestamp", "estimated_delivery": "date"}
    )
    df = df.withColumn(
        "status",
        F.when(F.col("status").isin(list(VALID_DELIVERY_STATUSES)), F.col("status")).otherwise(
            F.lit("LabelCreated")
        ),
    )
    df = df.filter(F.col("delivery_id").isNotNull() & F.col("order_id").isNotNull())
    return dedupe_keep_latest(df, ["delivery_id"], "status_time")


def clean_support_events(df: DataFrame, keep_lineage: bool = False) -> DataFrame:
    df = _drop_meta(df, keep_lineage)
    df = standardize_string_columns(
        df,
        ["ticket_id", "user_id", "order_id", "channel", "status", "priority", "subject"],
    )
    df = cast_columns(df, {"event_time": "timestamp"})
    df = df.withColumn(
        "status",
        F.when(F.col("status").isin(list(VALID_SUPPORT_STATUSES)), F.col("status")).otherwise(
            F.lit("Open")
        ),
    )
    df = df.filter(F.col("ticket_id").isNotNull())
    return dedupe_keep_latest(df, ["ticket_id"], "event_time")


CLEANERS = {
    ENTITY_USERS: clean_users,
    ENTITY_PRODUCTS: clean_products,
    ENTITY_ORDERS: clean_orders,
    ENTITY_PAYMENTS: clean_payments,
    ENTITY_REVIEWS: clean_reviews,
    ENTITY_SHOPPING_CART: clean_shopping_cart,
    ENTITY_CLICK_LOGS: clean_click_logs,
    ENTITY_INVENTORY: clean_inventory,
    ENTITY_COUPONS: clean_coupons,
    ENTITY_DELIVERY: clean_delivery,
    ENTITY_SUPPORT_EVENTS: clean_support_events,
}


def clean_entity(entity: str, df: DataFrame, keep_lineage: bool = False) -> DataFrame:
    if entity not in CLEANERS:
        raise ValueError(f"No cleaner registered for entity={entity}")
    return CLEANERS[entity](df, keep_lineage=keep_lineage)


def _merge_condition(keys: list[str]) -> str:
    return " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)


def build_silver_entity(
    spark: SparkSession,
    entity: str,
    max_delay_hours: Optional[float] = None,
    logger: Optional["EcommerceLogger"] = None,
) -> DataFrame:
    """
    Read bronze, cleanse, optionally filter late arrivals, dedupe (via cleaner),
    and MERGE into silver using ENTITY_PRIMARY_KEYS.
    """
    if entity not in ALL_ENTITIES:
        raise TransformationError(f"Unknown entity: {entity}", details={"entity": entity})

    bronze_path = PATHS.bronze_path(entity)
    silver_path = PATHS.silver_path(entity)

    if not table_exists(spark, bronze_path):
        raise TransformationError(
            f"Bronze table missing for {entity}",
            details={"path": bronze_path},
        )

    bronze_df = spark.read.format("delta").load(bronze_path)
    event_col = ENTITY_EVENT_TIME_COLUMNS.get(entity, "event_time")
    filtered = _filter_late_arrivals(bronze_df, event_col, max_delay_hours)
    cleaned = clean_entity(entity, filtered)
    cleaned = _materialize(cleaned)

    keys = ENTITY_PRIMARY_KEYS[entity]
    condition = _merge_condition(keys)

    metrics = merge_delta(
        spark,
        cleaned,
        silver_path,
        merge_condition=condition,
        when_matched_update_all=True,
        when_not_matched_insert_all=True,
        logger=logger,
    )

    if logger:
        logger.info(
            f"Silver MERGE complete for {entity}",
            module="silver",
            details={"path": silver_path, "metrics": metrics, "keys": keys},
        )

    return spark.read.format("delta").load(silver_path)


def build_all_silver_tables(
    spark: SparkSession,
    logger: Optional["EcommerceLogger"] = None,
    auditor: Optional["PipelineAuditor"] = None,
    run_id: Optional[str] = None,
    entities: Optional[list[str]] = None,
    max_delay_hours: Optional[float] = None,
    continue_on_error: bool = True,
) -> dict[str, str]:
    """Build silver tables for all (or selected) entities; return status map."""
    entities = entities or list(ALL_ENTITIES)
    status: dict[str, str] = {}
    _ = run_id  # available for callers / future lineage

    for entity in entities:
        try:
            if auditor is not None:
                with auditor.track(f"silver_{entity}") as ctx:
                    bronze_path = PATHS.bronze_path(entity)
                    if table_exists(spark, bronze_path):
                        ctx["rows_read"] = spark.read.format("delta").load(bronze_path).count()
                    result = build_silver_entity(
                        spark, entity, max_delay_hours=max_delay_hours, logger=logger
                    )
                    ctx["rows_inserted"] = result.count()
                    status[entity] = "SUCCESS"
            else:
                build_silver_entity(
                    spark, entity, max_delay_hours=max_delay_hours, logger=logger
                )
                status[entity] = "SUCCESS"
        except Exception as exc:
            status[entity] = f"FAILED: {exc}"
            if logger:
                logger.error(
                    f"Silver build failed for {entity}",
                    module="silver",
                    exc=exc,
                )
            if not continue_on_error:
                raise TransformationError(
                    f"Silver pipeline failed on {entity}: {exc}",
                    details={"pipeline": PIPELINE_SILVER_TRANSFORM},
                ) from exc
    return status

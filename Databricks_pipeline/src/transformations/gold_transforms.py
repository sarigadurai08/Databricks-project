"""
Gold-layer analytical table builders for e-commerce KPIs and dashboards.

Each ``build_*`` function produces an idempotent mart DataFrame from silver
sources. ``build_all_gold_tables`` overwrites Delta paths under PATHS.gold_path.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.constants import (
    ENTITY_CLICK_LOGS,
    ENTITY_COUPONS,
    ENTITY_INVENTORY,
    ENTITY_ORDERS,
    ENTITY_PAYMENTS,
    ENTITY_PRODUCTS,
    ENTITY_SHOPPING_CART,
    ENTITY_USERS,
    PIPELINE_GOLD_ANALYTICS,
)
from config.paths import PATHS
from src.utilities.delta_helpers import table_exists, write_delta
from src.utilities.exceptions import TransformationError
from src.utilities.table_registry import GOLD_TABLES

if TYPE_CHECKING:
    from src.audit.auditor import PipelineAuditor
    from src.logging.logger import EcommerceLogger


def _cache(df: DataFrame) -> DataFrame:
    try:
        cached = df.cache()
        cached.count()
        return cached
    except Exception:
        return df


def _load_silver(spark: SparkSession, entity: str) -> Optional[DataFrame]:
    path = PATHS.silver_path(entity)
    if not table_exists(spark, path):
        return None
    return spark.read.format("delta").load(path)


def _require(*dfs: Optional[DataFrame]) -> bool:
    return all(d is not None for d in dfs)


# ---------------------------------------------------------------------------
# Mart builders
# ---------------------------------------------------------------------------
def build_customer_journey(
    users: DataFrame,
    orders: DataFrame,
    click_logs: DataFrame,
    shopping_cart: DataFrame,
) -> DataFrame:
    users_b = F.broadcast(users.select("user_id", "email", "region", "loyalty_tier", "status"))
    order_agg = orders.groupBy("user_id").agg(
        F.countDistinct("order_id").alias("order_count"),
        F.min("order_time").alias("first_order_time"),
        F.max("order_time").alias("last_order_time"),
        F.sum("total_amount").alias("lifetime_spend"),
    )
    click_agg = click_logs.groupBy("user_id").agg(
        F.count("*").alias("click_events"),
        F.countDistinct("session_id").alias("sessions"),
        F.max("event_time").alias("last_click_time"),
    )
    cart_agg = shopping_cart.groupBy("user_id").agg(
        F.countDistinct("cart_id").alias("cart_count"),
        F.sum(F.when(F.col("status") == "Abandoned", 1).otherwise(0)).alias("abandoned_carts"),
        F.sum(F.when(F.col("status") == "Converted", 1).otherwise(0)).alias("converted_carts"),
    )
    return (
        users_b.alias("u")
        .join(order_agg.alias("o"), on="user_id", how="left")
        .join(click_agg.alias("c"), on="user_id", how="left")
        .join(cart_agg.alias("k"), on="user_id", how="left")
        .select(
            "user_id",
            "email",
            "region",
            "loyalty_tier",
            "status",
            F.coalesce(F.col("order_count"), F.lit(0)).alias("order_count"),
            "first_order_time",
            "last_order_time",
            F.coalesce(F.col("lifetime_spend"), F.lit(0)).alias("lifetime_spend"),
            F.coalesce(F.col("click_events"), F.lit(0)).alias("click_events"),
            F.coalesce(F.col("sessions"), F.lit(0)).alias("sessions"),
            "last_click_time",
            F.coalesce(F.col("cart_count"), F.lit(0)).alias("cart_count"),
            F.coalesce(F.col("abandoned_carts"), F.lit(0)).alias("abandoned_carts"),
            F.coalesce(F.col("converted_carts"), F.lit(0)).alias("converted_carts"),
        )
    )


def build_top_products(
    orders: DataFrame,
    products: DataFrame,
    reviews: Optional[DataFrame] = None,
    shopping_cart: Optional[DataFrame] = None,
) -> DataFrame:
    """Rank products by cart demand and optional review ratings."""
    products_b = F.broadcast(
        products.select("product_id", "product_name", "category", "brand", "price", "is_active")
    )
    if shopping_cart is not None:
        demand = shopping_cart.groupBy("product_id").agg(
            F.sum("quantity").alias("units_demanded"),
            F.countDistinct("cart_id").alias("cart_appearances"),
            F.sum(F.when(F.col("status") == "Converted", F.col("quantity")).otherwise(0)).alias(
                "units_converted"
            ),
        )
        base = products_b.join(demand, on="product_id", how="left")
    else:
        base = (
            products_b.withColumn("units_demanded", F.lit(0))
            .withColumn("cart_appearances", F.lit(0))
            .withColumn("units_converted", F.lit(0))
        )
    _ = orders  # reserved for future order-line joins
    if reviews is not None:
        rev = reviews.groupBy("product_id").agg(
            F.count("*").alias("review_count"),
            F.round(F.avg("rating"), 2).alias("avg_rating"),
        )
        base = base.join(rev, on="product_id", how="left")
    else:
        base = base.withColumn("review_count", F.lit(0)).withColumn(
            "avg_rating", F.lit(None).cast("double")
        )

    return (
        base.withColumn("units_demanded", F.coalesce(F.col("units_demanded"), F.lit(0)))
        .withColumn("cart_appearances", F.coalesce(F.col("cart_appearances"), F.lit(0)))
        .withColumn("units_converted", F.coalesce(F.col("units_converted"), F.lit(0)))
        .withColumn("review_count", F.coalesce(F.col("review_count"), F.lit(0)))
        .withColumn(
            "demand_score",
            F.col("units_demanded")
            + (F.col("cart_appearances") * 2)
            + (F.coalesce(F.col("avg_rating"), F.lit(0)) * F.col("review_count")),
        )
        .orderBy(F.desc("demand_score"), F.desc("price"))
    )


def build_conversion_funnel(click_logs: DataFrame, shopping_cart: DataFrame, orders: DataFrame) -> DataFrame:
    views = click_logs.filter(F.col("event_type").isin("page_view", "product_view", "search")).agg(
        F.countDistinct("session_id").alias("sessions"),
        F.count("*").alias("events"),
    ).withColumn("funnel_step", F.lit("browse"))
    carts = shopping_cart.agg(
        F.countDistinct("session_id").alias("sessions"),
        F.count("*").alias("events"),
    ).withColumn("funnel_step", F.lit("cart"))
    checkouts = click_logs.filter(F.col("event_type") == "checkout_start").agg(
        F.countDistinct("session_id").alias("sessions"),
        F.count("*").alias("events"),
    ).withColumn("funnel_step", F.lit("checkout_start"))
    purchases = orders.agg(
        F.countDistinct("user_id").alias("sessions"),
        F.countDistinct("order_id").alias("events"),
    ).withColumn("funnel_step", F.lit("purchase"))
    return views.unionByName(carts).unionByName(checkouts).unionByName(purchases).select(
        "funnel_step", "sessions", "events"
    )


def build_hourly_orders(orders: DataFrame) -> DataFrame:
    return (
        orders.withColumn("order_hour", F.hour("order_time"))
        .withColumn("order_date", F.to_date("order_time"))
        .groupBy("order_date", "order_hour")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("total_amount").alias("revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
            F.countDistinct("user_id").alias("unique_customers"),
        )
        .orderBy("order_date", "order_hour")
    )


def build_revenue_dashboard(orders: DataFrame, payments: DataFrame) -> DataFrame:
    order_day = (
        orders.withColumn("revenue_date", F.to_date("order_time"))
        .groupBy("revenue_date")
        .agg(
            F.sum("total_amount").alias("gross_revenue"),
            F.sum("discount_amount").alias("total_discounts"),
            F.countDistinct("order_id").alias("orders"),
        )
    )
    pay_day = (
        payments.withColumn("revenue_date", F.to_date("payment_time"))
        .groupBy("revenue_date")
        .agg(
            F.sum(F.when(F.col("status").isin("Captured", "Authorized"), F.col("amount")).otherwise(0)).alias(
                "captured_payments"
            ),
            F.sum(F.when(F.col("status") == "Failed", F.col("amount")).otherwise(0)).alias("failed_payments"),
            F.sum(F.when(F.col("status") == "Refunded", F.col("amount")).otherwise(0)).alias("refunds"),
        )
    )
    return order_day.join(pay_day, on="revenue_date", how="full_outer").orderBy("revenue_date")


def build_sales_dashboard(orders: DataFrame, products: DataFrame) -> DataFrame:
    # Enrich with category coverage from product catalog (broadcast dim).
    catalog = F.broadcast(
        products.groupBy("category").agg(F.countDistinct("product_id").alias("catalog_skus"))
    )
    sales = (
        orders.groupBy(
            F.to_date("order_time").alias("sales_date"),
            "shipping_region",
            "status",
        )
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("total_amount").alias("revenue"),
            F.sum("item_count").alias("items_sold"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
        )
    )
    # Cross-join catalog size as a daily context metric (small dimension).
    return sales.crossJoin(catalog.agg(F.sum("catalog_skus").alias("active_catalog_skus"))).orderBy(
        F.desc("sales_date")
    )


def build_inventory_dashboard(inventory: DataFrame, products: DataFrame) -> DataFrame:
    products_b = F.broadcast(products.select("product_id", "product_name", "category", "brand"))
    return (
        inventory.join(products_b, on="product_id", how="left")
        .withColumn(
            "available_qty",
            F.col("quantity_on_hand") - F.coalesce(F.col("quantity_reserved"), F.lit(0)),
        )
        .withColumn(
            "stock_status",
            F.when(F.col("quantity_on_hand") <= F.col("reorder_level"), F.lit("Reorder"))
            .when(F.col("available_qty") <= 0, F.lit("OutOfStock"))
            .otherwise(F.lit("InStock")),
        )
        .select(
            "product_id",
            "product_name",
            "category",
            "brand",
            "warehouse_id",
            "quantity_on_hand",
            "quantity_reserved",
            "available_qty",
            "reorder_level",
            "stock_status",
            "event_time",
        )
    )


def build_payment_dashboard(payments: DataFrame) -> DataFrame:
    return (
        payments.withColumn("payment_date", F.to_date("payment_time"))
        .groupBy("payment_date", "method", "status")
        .agg(
            F.countDistinct("payment_id").alias("payment_count"),
            F.sum("amount").alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
            F.countDistinct("order_id").alias("order_count"),
        )
        .orderBy(F.desc("payment_date"))
    )


def build_customer_lifetime_value(users: DataFrame, orders: DataFrame, payments: DataFrame) -> DataFrame:
    users_b = F.broadcast(users.select("user_id", "email", "region", "loyalty_tier", "signup_date"))
    order_agg = orders.groupBy("user_id").agg(
        F.countDistinct("order_id").alias("orders"),
        F.sum("total_amount").alias("gross_spend"),
        F.min("order_time").alias("first_order"),
        F.max("order_time").alias("last_order"),
    )
    pay_agg = payments.groupBy("user_id").agg(
        F.sum(F.when(F.col("status").isin("Captured", "Authorized"), F.col("amount")).otherwise(0)).alias(
            "paid_amount"
        ),
        F.sum(F.when(F.col("status") == "Refunded", F.col("amount")).otherwise(0)).alias("refunded_amount"),
    )
    return (
        users_b.join(order_agg, on="user_id", how="left")
        .join(pay_agg, on="user_id", how="left")
        .withColumn("orders", F.coalesce(F.col("orders"), F.lit(0)))
        .withColumn("gross_spend", F.coalesce(F.col("gross_spend"), F.lit(0)))
        .withColumn("paid_amount", F.coalesce(F.col("paid_amount"), F.lit(0)))
        .withColumn("refunded_amount", F.coalesce(F.col("refunded_amount"), F.lit(0)))
        .withColumn(
            "clv",
            F.round(F.col("paid_amount") - F.col("refunded_amount"), 2),
        )
        .withColumn(
            "avg_order_value",
            F.when(F.col("orders") > 0, F.round(F.col("gross_spend") / F.col("orders"), 2)).otherwise(F.lit(0)),
        )
    )


def build_cart_abandonment(shopping_cart: DataFrame, users: DataFrame) -> DataFrame:
    users_b = F.broadcast(users.select("user_id", "email", "region"))
    return (
        shopping_cart.filter(F.col("status") == "Abandoned")
        .join(users_b, on="user_id", how="left")
        .withColumn("line_amount", F.col("quantity") * F.col("unit_price"))
        .groupBy("user_id", "email", "region", "cart_id")
        .agg(
            F.countDistinct("product_id").alias("products"),
            F.sum("quantity").alias("units"),
            F.sum("line_amount").alias("abandoned_value"),
            F.max("event_time").alias("last_event_time"),
        )
    )


def build_repeat_customers(orders: DataFrame, users: DataFrame) -> DataFrame:
    users_b = F.broadcast(users.select("user_id", "email", "region", "loyalty_tier"))
    order_counts = orders.groupBy("user_id").agg(
        F.countDistinct("order_id").alias("order_count"),
        F.sum("total_amount").alias("total_spend"),
        F.min("order_time").alias("first_order"),
        F.max("order_time").alias("last_order"),
    )
    return (
        order_counts.filter(F.col("order_count") >= 2)
        .join(users_b, on="user_id", how="left")
        .withColumn(
            "days_between",
            F.datediff(F.to_date("last_order"), F.to_date("first_order")),
        )
        .orderBy(F.desc("order_count"), F.desc("total_spend"))
    )


def build_session_analytics(click_logs: DataFrame) -> DataFrame:
    return (
        click_logs.groupBy("session_id")
        .agg(
            F.min("event_time").alias("session_start"),
            F.max("event_time").alias("session_end"),
            F.count("*").alias("event_count"),
            F.countDistinct("page_url").alias("pages"),
            F.countDistinct("product_id").alias("products_viewed"),
            F.first("user_id", ignorenulls=True).alias("user_id"),
            F.first("device_type", ignorenulls=True).alias("device_type"),
            F.first("referrer", ignorenulls=True).alias("referrer"),
        )
        .withColumn(
            "duration_seconds",
            F.unix_timestamp("session_end") - F.unix_timestamp("session_start"),
        )
    )


def build_website_traffic(click_logs: DataFrame) -> DataFrame:
    return (
        click_logs.withColumn("traffic_date", F.to_date("event_time"))
        .groupBy("traffic_date", "event_type", "device_type", "referrer")
        .agg(
            F.count("*").alias("events"),
            F.countDistinct("session_id").alias("sessions"),
            F.countDistinct("user_id").alias("users"),
        )
        .orderBy(F.desc("traffic_date"))
    )


def build_top_categories(products: DataFrame, reviews: Optional[DataFrame] = None) -> DataFrame:
    base = products.groupBy("category").agg(
        F.countDistinct("product_id").alias("product_count"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.sum(F.when(F.col("is_active") == True, 1).otherwise(0)).alias("active_products"),  # noqa: E712
    )
    if reviews is None:
        return base.orderBy(F.desc("product_count"))
    products_b = F.broadcast(products.select("product_id", "category"))
    cat_ratings = (
        reviews.join(products_b, on="product_id", how="inner")
        .groupBy("category")
        .agg(
            F.count("*").alias("review_count"),
            F.round(F.avg("rating"), 2).alias("avg_rating"),
        )
    )
    return base.join(cat_ratings, on="category", how="left").orderBy(F.desc("product_count"))


def build_peak_shopping_hours(orders: DataFrame, click_logs: DataFrame) -> DataFrame:
    order_hours = (
        orders.withColumn("hour_of_day", F.hour("order_time"))
        .groupBy("hour_of_day")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum("total_amount").alias("revenue"),
        )
    )
    click_hours = (
        click_logs.withColumn("hour_of_day", F.hour("event_time"))
        .groupBy("hour_of_day")
        .agg(
            F.count("*").alias("clicks"),
            F.countDistinct("session_id").alias("sessions"),
        )
    )
    return order_hours.join(click_hours, on="hour_of_day", how="full_outer").orderBy("hour_of_day")


def build_average_order_value(orders: DataFrame) -> DataFrame:
    return (
        orders.withColumn("order_date", F.to_date("order_time"))
        .withColumn("order_month", F.date_format("order_time", "yyyy-MM"))
        .groupBy("order_month", "shipping_region")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
            F.round(F.percentile_approx("total_amount", 0.5), 2).alias("median_order_value"),
            F.sum("total_amount").alias("total_revenue"),
        )
        .orderBy("order_month", "shipping_region")
    )


def build_revenue_by_region(orders: DataFrame, users: DataFrame) -> DataFrame:
    users_b = F.broadcast(users.select("user_id", "region").withColumnRenamed("region", "user_region"))
    joined = orders.join(users_b, on="user_id", how="left").withColumn(
        "region",
        F.coalesce(F.col("shipping_region"), F.col("user_region"), F.lit("Unknown")),
    )
    return joined.groupBy("region").agg(
        F.countDistinct("order_id").alias("orders"),
        F.countDistinct("user_id").alias("customers"),
        F.sum("total_amount").alias("revenue"),
        F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
        F.sum("discount_amount").alias("total_discounts"),
    ).orderBy(F.desc("revenue"))


def build_coupon_analytics(coupons: DataFrame, orders: DataFrame) -> DataFrame:
    coupon_agg = coupons.groupBy("coupon_code").agg(
        F.countDistinct("coupon_usage_id").alias("redemptions"),
        F.countDistinct("user_id").alias("unique_users"),
        F.sum("discount_amount").alias("total_discount"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
    )
    order_coupon = (
        orders.filter(F.col("coupon_code").isNotNull())
        .groupBy(F.upper(F.col("coupon_code")).alias("coupon_code"))
        .agg(
            F.countDistinct("order_id").alias("orders_with_coupon"),
            F.sum("total_amount").alias("order_revenue"),
        )
    )
    return coupon_agg.join(order_coupon, on="coupon_code", how="full_outer").orderBy(
        F.desc("redemptions")
    )


def build_inventory_trends(inventory: DataFrame, products: DataFrame) -> DataFrame:
    products_b = F.broadcast(products.select("product_id", "category", "brand"))
    return (
        inventory.join(products_b, on="product_id", how="left")
        .withColumn("snapshot_date", F.to_date("event_time"))
        .groupBy("snapshot_date", "warehouse_id", "category")
        .agg(
            F.sum("quantity_on_hand").alias("on_hand"),
            F.sum("quantity_reserved").alias("reserved"),
            F.countDistinct("product_id").alias("skus"),
            F.sum(
                F.when(F.col("quantity_on_hand") <= F.col("reorder_level"), 1).otherwise(0)
            ).alias("below_reorder_skus"),
        )
        .orderBy("snapshot_date", "warehouse_id")
    )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def build_all_gold_tables(
    spark: SparkSession,
    logger: Optional["EcommerceLogger"] = None,
    auditor: Optional["PipelineAuditor"] = None,
    run_id: Optional[str] = None,
    continue_on_error: bool = True,
) -> dict[str, str]:
    """
    Build and overwrite all gold marts listed in GOLD_TABLES.

    Gracefully skips marts whose required silver sources are missing.
    """
    _ = run_id
    status: dict[str, str] = {}

    users = _load_silver(spark, ENTITY_USERS)
    products = _load_silver(spark, ENTITY_PRODUCTS)
    orders = _load_silver(spark, ENTITY_ORDERS)
    payments = _load_silver(spark, ENTITY_PAYMENTS)
    reviews = _load_silver(spark, "reviews")
    shopping_cart = _load_silver(spark, ENTITY_SHOPPING_CART)
    click_logs = _load_silver(spark, ENTITY_CLICK_LOGS)
    inventory = _load_silver(spark, ENTITY_INVENTORY)
    coupons = _load_silver(spark, ENTITY_COUPONS)

    if users is not None:
        users = _cache(users)
    if products is not None:
        products = _cache(products)
    if orders is not None:
        orders = _cache(orders)

    builders: dict[str, tuple] = {
        "customer_journey": (
            lambda: build_customer_journey(users, orders, click_logs, shopping_cart),
            (users, orders, click_logs, shopping_cart),
        ),
        "top_products": (
            lambda: build_top_products(orders, products, reviews, shopping_cart),
            (orders, products),
        ),
        "conversion_funnel": (
            lambda: build_conversion_funnel(click_logs, shopping_cart, orders),
            (click_logs, shopping_cart, orders),
        ),
        "hourly_orders": (lambda: build_hourly_orders(orders), (orders,)),
        "revenue_dashboard": (
            lambda: build_revenue_dashboard(orders, payments),
            (orders, payments),
        ),
        "sales_dashboard": (
            lambda: build_sales_dashboard(orders, products),
            (orders, products),
        ),
        "inventory_dashboard": (
            lambda: build_inventory_dashboard(inventory, products),
            (inventory, products),
        ),
        "payment_dashboard": (lambda: build_payment_dashboard(payments), (payments,)),
        "customer_lifetime_value": (
            lambda: build_customer_lifetime_value(users, orders, payments),
            (users, orders, payments),
        ),
        "cart_abandonment": (
            lambda: build_cart_abandonment(shopping_cart, users),
            (shopping_cart, users),
        ),
        "repeat_customers": (
            lambda: build_repeat_customers(orders, users),
            (orders, users),
        ),
        "session_analytics": (lambda: build_session_analytics(click_logs), (click_logs,)),
        "website_traffic": (lambda: build_website_traffic(click_logs), (click_logs,)),
        "top_categories": (
            lambda: build_top_categories(products, reviews),
            (products,),
        ),
        "peak_shopping_hours": (
            lambda: build_peak_shopping_hours(orders, click_logs),
            (orders, click_logs),
        ),
        "average_order_value": (lambda: build_average_order_value(orders), (orders,)),
        "revenue_by_region": (
            lambda: build_revenue_by_region(orders, users),
            (orders, users),
        ),
        "coupon_analytics": (
            lambda: build_coupon_analytics(coupons, orders),
            (coupons, orders),
        ),
        "inventory_trends": (
            lambda: build_inventory_trends(inventory, products),
            (inventory, products),
        ),
    }

    for table_name in GOLD_TABLES:
        if table_name not in builders:
            status[table_name] = "SKIPPED: no builder"
            continue

        builder_fn, deps = builders[table_name]
        if not _require(*deps):
            status[table_name] = "SKIPPED: missing silver sources"
            if logger:
                logger.warning(
                    f"Skipping gold mart {table_name}: missing silver sources",
                    module="gold",
                )
            continue

        try:
            def _run(name: str = table_name, fn=builder_fn) -> None:
                df = fn()
                path = PATHS.gold_path(name)
                write_delta(df, path, mode="overwrite", merge_schema=True)
                if logger:
                    logger.info(
                        f"Gold mart written: {name}",
                        module="gold",
                        details={"path": path, "rows": df.count()},
                    )

            if auditor is not None:
                with auditor.track(f"gold_{table_name}") as ctx:
                    df = builder_fn()
                    ctx["rows_read"] = df.count()
                    write_delta(df, PATHS.gold_path(table_name), mode="overwrite", merge_schema=True)
                    ctx["rows_inserted"] = ctx["rows_read"]
            else:
                _run()
            status[table_name] = "SUCCESS"
        except Exception as exc:
            status[table_name] = f"FAILED: {exc}"
            if logger:
                logger.error(f"Gold build failed for {table_name}", module="gold", exc=exc)
            if not continue_on_error:
                raise TransformationError(
                    f"Gold pipeline failed on {table_name}: {exc}",
                    details={"pipeline": PIPELINE_GOLD_ANALYTICS},
                ) from exc

    return status

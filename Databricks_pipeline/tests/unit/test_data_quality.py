"""Unit tests for the data quality framework."""

from __future__ import annotations

from src.utilities.data_quality import DataQualityFramework, Severity


def test_not_null_and_unique_rules(spark, tmp_storage_base):
    df = spark.createDataFrame(
        [
            ("ORD1", "ada@example.com"),
            ("ORD1", "dup@example.com"),
            (None, "z@example.com"),
        ],
        ["order_id", "email"],
    )
    dq = (
        DataQualityFramework(spark, "orders", "run-test")
        .require_not_null(["order_id"])
        .require_unique(["order_id"])
    )
    results = {r.rule_name: r for r in dq.validate(df)}
    assert results["not_null_order_id"].failed_count == 1
    assert results["unique_order_id"].failed_count >= 2
    assert results["not_null_order_id"].status == "FAILED"


def test_range_and_in_set(spark, tmp_storage_base):
    df = spark.createDataFrame(
        [
            ("PAY1", 50.0, "Captured"),
            ("PAY2", -1.0, "Failed"),
            ("PAY3", 10.0, "Unknown"),
        ],
        ["payment_id", "amount", "status"],
    )
    dq = (
        DataQualityFramework(spark, "payments", "run-2")
        .require_range("amount", min_value=0)
        .require_in_set("status", {"Captured", "Failed", "Authorized", "Refunded", "Pending"})
    )
    results = {r.rule_name: r for r in dq.validate(df)}
    assert results["range_amount"].failed_count == 1
    assert results["in_set_status"].failed_count == 1


def test_positive_rule(spark, tmp_storage_base):
    df = spark.createDataFrame(
        [("P1", 10.0), ("P2", 0.0), ("P3", -5.0)],
        ["product_id", "price"],
    )
    dq = DataQualityFramework(spark, "products", "run-pos").require_positive(["price"])
    results = {r.rule_name: r for r in dq.validate(df)}
    assert results["positive_price"].failed_count == 2


def test_foreign_key_rule(spark, tmp_storage_base, sample_users, sample_orders):
    users = sample_users.select("user_id").dropDuplicates()
    orphan = spark.createDataFrame(
        [("ORD999", "USR_MISSING", "2024-06-01 12:00:00", "Pending", 99.0, 0.0)],
        sample_orders.columns,
    )
    orders = sample_orders.unionByName(orphan)

    dq = DataQualityFramework(spark, "orders", "run-fk").require_fk(
        "user_id", users, "user_id"
    )
    results = {r.rule_name: r for r in dq.validate(orders)}
    assert results["fk_user_id_to_user_id"].failed_count >= 1

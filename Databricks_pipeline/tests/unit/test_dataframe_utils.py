"""Unit tests for dataframe utilities: hash, metadata, dedupe."""

from __future__ import annotations

from pyspark.sql import functions as F

from src.utilities.dataframe_utils import (
    add_bronze_metadata,
    dedupe_keep_latest,
    drop_exact_duplicates,
    generate_load_id,
    generate_run_id,
    sha256_expr,
    standardize_string_columns,
)


def test_generate_ids_are_unique():
    run_ids = {generate_run_id() for _ in range(10)}
    load_ids = {generate_load_id() for _ in range(10)}
    assert len(run_ids) == 10
    assert len(load_ids) == 10
    assert all(lid.startswith("LOAD_") for lid in load_ids)


def test_add_bronze_metadata(spark):
    df = spark.createDataFrame([("ORD1", "Confirmed")], ["order_id", "status"])
    out = add_bronze_metadata(
        df,
        load_id="LOAD1",
        batch_id="BATCH1",
        hash_columns=["order_id", "status"],
        event_time_column=None,
    )
    for col in ["_ingestion_time", "_source_file", "_load_id", "_batch_id", "_record_hash", "_event_time"]:
        assert col in out.columns
    row = out.collect()[0]
    assert row["_load_id"] == "LOAD1"
    assert row["_batch_id"] == "BATCH1"
    assert row["_record_hash"] is not None


def test_sha256_expr_deterministic(spark):
    df = spark.createDataFrame([("A", "B"), ("A", "B")], ["c1", "c2"])
    out = df.withColumn("h", sha256_expr(["c1", "c2"]))
    hashes = {r["h"] for r in out.collect()}
    assert len(hashes) == 1


def test_drop_exact_duplicates(spark, sample_orders):
    out = drop_exact_duplicates(sample_orders, ["order_id"])
    assert out.count() == 2


def test_dedupe_keep_latest(spark):
    df = spark.createDataFrame(
        [
            ("ORD1", "v1", "2024-01-01 10:00:00"),
            ("ORD1", "v2", "2024-06-01 10:00:00"),
            ("ORD2", "v3", "2024-03-01 10:00:00"),
        ],
        ["order_id", "val", "order_time"],
    )
    out = dedupe_keep_latest(df, ["order_id"], "order_time")
    assert out.count() == 2
    row = out.filter(F.col("order_id") == "ORD1").collect()[0]
    assert row["val"] == "v2"


def test_standardize_string_columns(spark):
    df = spark.createDataFrame([("  hello  ",), ("world",)], ["name"])
    out = standardize_string_columns(df, ["name"])
    assert out.collect()[0]["name"] == "hello"

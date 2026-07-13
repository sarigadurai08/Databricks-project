"""Unit tests for Delta MERGE helpers and dataframe utilities."""

from __future__ import annotations

from pyspark.sql import functions as F

from src.utilities.dataframe_utils import add_bronze_metadata, sha256_expr
from src.utilities.delta_helpers import merge_delta, table_exists, write_delta


def test_add_bronze_metadata(spark):
    df = spark.createDataFrame([("PAT1", "Ada")], ["PatientID", "FirstName"])
    out = add_bronze_metadata(df, load_id="LOAD1", batch_id="BATCH1")
    for col in ["_ingestion_time", "_source_file", "_load_id", "_batch_id", "_record_hash"]:
        assert col in out.columns
    row = out.collect()[0]
    assert row["_load_id"] == "LOAD1"
    assert row["_batch_id"] == "BATCH1"
    assert row["_record_hash"] is not None


def test_merge_delta_upsert(spark, tmp_delta_dir):
    target = f"{tmp_delta_dir}/merge_target"
    base = spark.createDataFrame([("1", "A"), ("2", "B")], ["id", "val"])
    write_delta(base, target, mode="overwrite")
    assert table_exists(spark, target)

    delta_src = spark.createDataFrame([("2", "B2"), ("3", "C")], ["id", "val"])
    merge_delta(
        spark,
        delta_src,
        target,
        merge_condition="t.id = s.id",
        when_matched_update_all=True,
        when_not_matched_insert_all=True,
    )
    result = spark.read.format("delta").load(target).orderBy("id")
    assert result.count() == 3
    assert result.filter(F.col("id") == "2").collect()[0]["val"] == "B2"


def test_sha256_expr_deterministic(spark):
    df = spark.createDataFrame([("A", "B"), ("A", "B")], ["c1", "c2"])
    out = df.withColumn("h", sha256_expr(["c1", "c2"]))
    hashes = {r["h"] for r in out.collect()}
    assert len(hashes) == 1

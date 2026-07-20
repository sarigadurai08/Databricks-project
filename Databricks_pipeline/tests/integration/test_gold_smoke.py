"""
Integration smoke test — run after `scripts/run_local_pipeline.py`.

Skipped automatically when Delta gold tables are not present or Spark/Delta unavailable.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from config.config import get_config


@pytest.fixture(scope="module")
def gold_ready(spark):
    cfg = get_config()
    marker = Path(cfg.paths.gold_path("top_products")) / "_delta_log"
    if not marker.exists():
        pytest.skip("Gold tables not built — run scripts/run_local_pipeline.py first")
    return cfg


def test_gold_tables_exist(spark, gold_ready):
    cfg = gold_ready
    expected = [
        "top_products",
        "revenue_dashboard",
        "hourly_orders",
        "customer_journey",
        "session_analytics",
        "cart_abandonment",
        "repeat_customers",
        "website_traffic",
    ]
    for name in expected:
        path = cfg.paths.gold_path(name)
        df = spark.read.format("delta").load(path)
        assert df.count() >= 0


def test_gold_top_products_schema(spark, gold_ready):
    cfg = gold_ready
    df = spark.read.format("delta").load(cfg.paths.gold_path("top_products"))
    cols = set(df.columns)
    for expected_col in ("product_id", "product_name", "demand_score"):
        assert expected_col in cols

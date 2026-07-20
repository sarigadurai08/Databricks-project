"""
Pytest configuration and shared Spark fixture for E-Commerce Lakehouse unit tests.

Uses a local SparkSession with Delta Lake when delta-spark is installed.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def spark():
    try:
        from src.utilities.spark_session import get_spark, stop_spark

        session = get_spark("EcommerceLakehouseTests")
        yield session
        stop_spark(session)
    except Exception as exc:
        pytest.skip(f"Spark session unavailable: {exc}")


@pytest.fixture
def tmp_storage_base(tmp_path, monkeypatch):
    """Bind PATHS to a temp directory for isolated Delta writes."""
    from config import paths as paths_mod

    base = str(tmp_path / "lakehouse").replace("\\", "/")
    paths_mod.PATHS.bind_storage_base(base, cloud=False)
    return base


@pytest.fixture
def tmp_delta_dir(tmp_path):
    return str(tmp_path / "delta").replace("\\", "/")


@pytest.fixture
def sample_orders(spark):
    data = [
        ("ORD001", "USR001", "2024-06-01 10:00:00", "Confirmed", 100.0, 10.0),
        ("ORD002", "USR002", "2024-06-01 11:00:00", "Shipped", 250.0, 0.0),
        ("ORD002", "USR002", "2024-06-01 11:00:00", "Shipped", 250.0, 0.0),  # duplicate
    ]
    cols = ["order_id", "user_id", "order_time", "status", "total_amount", "discount_amount"]
    return spark.createDataFrame(data, cols)


@pytest.fixture
def sample_users(spark):
    data = [
        ("USR001", "ada@example.com", "Northeast", "Gold"),
        ("USR002", "alan@example.com", "West", "Silver"),
    ]
    cols = ["user_id", "email", "region", "loyalty_tier"]
    return spark.createDataFrame(data, cols)

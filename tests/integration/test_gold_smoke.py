"""
Integration test placeholder — run after `scripts/run_local_pipeline.py`.

Skipped automatically when Delta gold tables are not present.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from config.config import get_config


@pytest.fixture(scope="module")
def gold_ready():
    cfg = get_config()
    marker = Path(cfg.paths.gold_path("patient_summary")) / "_delta_log"
    if not marker.exists():
        pytest.skip("Gold tables not built — run scripts/run_local_pipeline.py first")
    return cfg


def test_gold_tables_exist(spark, gold_ready):
    cfg = gold_ready
    expected = [
        "patient_summary",
        "doctor_performance",
        "hospital_revenue",
        "monthly_revenue",
        "top_diseases",
    ]
    for name in expected:
        df = spark.read.format("delta").load(cfg.paths.gold_path(name))
        assert df.count() >= 0

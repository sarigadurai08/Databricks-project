"""Unit tests for StreamingEventSimulator event generation."""

from __future__ import annotations

import pytest

from config.constants import ALL_ENTITIES
from src.ingestion.streaming_simulator import StreamingEventSimulator


@pytest.fixture
def simulator(tmp_storage_base):
    """Pure-Python event generation bound to local temp storage (no Spark writes)."""
    sim = StreamingEventSimulator(
        spark=None,
        events_per_tick=5,
        entities=list(ALL_ENTITIES),
    )
    return sim


def test_generate_batch_returns_all_entities(simulator):
    counts = simulator.generate_batch()
    assert set(counts.keys()) == set(ALL_ENTITIES)
    for entity, count in counts.items():
        assert count == 5, f"Expected 5 events for {entity}, got {count}"


def test_generate_events_have_primary_keys(simulator):
    for entity in ALL_ENTITIES:
        events = simulator._generate_events(entity, 3)
        assert len(events) == 3
        assert isinstance(events[0], dict)
        assert len(events[0]) > 0


def test_run_ticks_accumulates_counts(simulator):
    totals = simulator.run_ticks(ticks=2)
    assert set(totals.keys()) == set(ALL_ENTITIES)
    for entity, total in totals.items():
        assert total == 10, f"Expected 10 total events for {entity}, got {total}"

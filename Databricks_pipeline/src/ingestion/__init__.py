"""Ingestion package — Auto Loader and streaming event simulator."""

from src.ingestion.autoloader import AutoLoaderIngestion
from src.ingestion.streaming_simulator import StreamingEventSimulator

__all__ = ["AutoLoaderIngestion", "StreamingEventSimulator"]

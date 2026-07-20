"""Runtime configuration package for the E-Commerce Lakehouse."""

from config.config import CONFIG, EcommerceConfig, get_config
from config.paths import PATHS, LakehousePaths

__all__ = ["CONFIG", "EcommerceConfig", "get_config", "PATHS", "LakehousePaths"]

"""
Package initialization for Lakehouse-AppKit.

A modern CLI-driven data applications platform that standardizes FastAPI
development with pluggable adapters for Databricks and other data platforms.
"""

__version__ = "0.1.0"
__author__ = "Lakehouse-AppKit Contributors"

from lakehouse_appkit.config import AppConfig, ConfigManager, get_config

__all__ = [
    "AppConfig",
    "ConfigManager",
    "get_config",
    "__version__",
]


"""
Databricks Apps deployment configuration.
"""
from pathlib import Path

# App metadata
APP_NAME = "lakehouse-appkit"
APP_DESCRIPTION = "Production-ready toolkit for governed data applications"
APP_VERSION = "1.0.0"

# Source files to include
SOURCE_FILES = [
    "lakehouse_appkit/",
    "app.py",
    "requirements.txt",
    "pyproject.toml",
    "databricks.yml",
    "examples/workflows/",
    "config/.env.prod.example"
]

# Files to exclude
EXCLUDE_PATTERNS = [
    "**/__pycache__",
    "**/*.pyc",
    "**/test_*.py",
    "tests/",
    ".git/",
    ".env.dev",
    "*.log",
    "lakehouse-app/",  # venv
]

# Required resources
REQUIRED_RESOURCES = {
    "sql_warehouse": {
        "name": "lakehouse_appkit_warehouse",
        "cluster_size": "Small",
        "auto_stop_mins": 10
    },
    "secret_scope": {
        "name": "lakehouse-appkit-secrets",
        "backend_type": "DATABRICKS"
    }
}

# Environment variables for production
PRODUCTION_ENV = {
    "APP_ENV": "prod",
    "LOG_LEVEL": "INFO",
    "ENABLE_METRICS": "true",
    "ENABLE_AUDIT": "true"
}

# Health check configuration
HEALTH_CHECK = {
    "path": "/api/health",
    "port": 8000,
    "initial_delay_seconds": 30,
    "period_seconds": 10,
    "timeout_seconds": 5,
    "failure_threshold": 3
}

# Resource limits
RESOURCE_LIMITS = {
    "cpu": "2",
    "memory": "4Gi"
}

RESOURCE_REQUESTS = {
    "cpu": "1",
    "memory": "2Gi"
}


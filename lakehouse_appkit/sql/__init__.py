"""Databricks SQL Statement Execution."""
from enum import Enum
from lakehouse_appkit.sql.rest_client import DatabricksSQLClient

class StatementState(str, Enum):
    """SQL statement states."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"

__all__ = ['DatabricksSQLClient', 'StatementState']

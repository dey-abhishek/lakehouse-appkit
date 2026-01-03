"""Databricks AI/BI Dashboards (Lakeview)."""
from enum import Enum
from lakehouse_appkit.dashboards.rest_client import DatabricksAIBIDashboardClient

class DashboardLifecycleState(str, Enum):
    """Dashboard lifecycle states."""
    ACTIVE = "ACTIVE"
    TRASHED = "TRASHED"

__all__ = ['DatabricksAIBIDashboardClient', 'DashboardLifecycleState']

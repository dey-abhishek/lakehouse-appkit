"""Databricks Secrets management."""
from enum import Enum
from lakehouse_appkit.secrets.rest_client import DatabricksSecretsClient

class SecretScope(str, Enum):
    """Secret scope types."""
    DATABRICKS = "DATABRICKS"
    AZURE_KEYVAULT = "AZURE_KEYVAULT"

__all__ = ['DatabricksSecretsClient', 'SecretScope']

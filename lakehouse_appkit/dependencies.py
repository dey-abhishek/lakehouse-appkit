"""
FastAPI dependencies for REST API clients.
"""
from functools import lru_cache
import os
from typing import Optional

from lakehouse_appkit.sql import DatabricksSQLClient
from lakehouse_appkit.dashboards import DatabricksAIBIDashboardClient
from lakehouse_appkit.secrets import DatabricksSecretsClient
from lakehouse_appkit.unity_catalog.rest_client import UnityCatalogRestClient
from lakehouse_appkit.unity_catalog import UnityCatalogManager
from lakehouse_appkit.adapters.databricks import DatabricksAdapter
from lakehouse_appkit.genie import DatabricksGenieClient
from lakehouse_appkit.mlflow import DatabricksMLflowClient
from lakehouse_appkit.connections import DatabricksConnectionsClient
from lakehouse_appkit.functions import DatabricksFunctionsClient
from lakehouse_appkit.auth import DatabricksOAuthClient, DatabricksServicePrincipalClient
from lakehouse_appkit.model_serving import DatabricksModelServingClient
from lakehouse_appkit.jobs import DatabricksJobsClient
from lakehouse_appkit.deployment import DatabricksAppsClient
from lakehouse_appkit.vector_search import DatabricksVectorSearchClient
from lakehouse_appkit.notebooks import DatabricksNotebooksClient
from lakehouse_appkit.delta import DeltaLakeManager


@lru_cache()
def get_databricks_host() -> str:
    """Get Databricks host from environment."""
    host = os.getenv("DATABRICKS_HOST")
    if not host:
        raise ValueError("DATABRICKS_HOST environment variable not set")
    return host


@lru_cache()
def get_databricks_token() -> str:
    """Get Databricks token from environment."""
    token = os.getenv("DATABRICKS_TOKEN")
    if not token:
        raise ValueError("DATABRICKS_TOKEN environment variable not set")
    return token


@lru_cache()
def get_warehouse_id() -> str:
    """Get SQL warehouse ID from environment."""
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    if not warehouse_id:
        raise ValueError("DATABRICKS_WAREHOUSE_ID environment variable not set")
    return warehouse_id


@lru_cache()
def get_sql_client() -> DatabricksSQLClient:
    """
    Get Databricks SQL REST API client (singleton).
    
    Uses REST API for fast SQL execution.
    """
    return DatabricksSQLClient(
        host=get_databricks_host(),
        token=get_databricks_token(),
        warehouse_id=get_warehouse_id()
    )


@lru_cache()
def get_dashboard_client() -> DatabricksAIBIDashboardClient:
    """
    Get AI/BI Dashboard REST API client (singleton).
    
    Uses REST API for dashboard management.
    """
    return DatabricksAIBIDashboardClient(
        host=get_databricks_host(),
        token=get_databricks_token()
    )


@lru_cache()
def get_secrets_client() -> DatabricksSecretsClient:
    """
    Get Databricks Secrets REST API client (singleton).
    
    Uses REST API for secure secret management.
    """
    return DatabricksSecretsClient(
        host=get_databricks_host(),
        token=get_databricks_token()
    )


@lru_cache()
def get_oauth_client() -> DatabricksOAuthClient:
    """
    Get Databricks OAuth 2.0 client (singleton).
    
    Requires OAuth credentials (client_id and client_secret).
    Falls back to environment variables if not in config.
    """
    host = get_databricks_host()
    client_id = os.getenv("DATABRICKS_CLIENT_ID")
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
    
    if not client_id or not client_secret:
        raise ValueError(
            "OAuth credentials not found. Set DATABRICKS_CLIENT_ID and "
            "DATABRICKS_CLIENT_SECRET environment variables."
        )
    
    return DatabricksOAuthClient(
        host=host,
        client_id=client_id,
        client_secret=client_secret
    )


@lru_cache()
def get_service_principal_client() -> DatabricksServicePrincipalClient:
    """
    Get Databricks Service Principal client (singleton).
    
    Uses token-based authentication.
    """
    return DatabricksServicePrincipalClient(
        host=get_databricks_host(),
        token=get_databricks_token()
    )


@lru_cache()
def get_model_serving_client() -> DatabricksModelServingClient:
    """
    Get Databricks Model Serving client (singleton).
    
    Uses REST API for model deployment and predictions.
    """
    return DatabricksModelServingClient(
        host=get_databricks_host(),
        token=get_databricks_token()
    )


@lru_cache()
def get_jobs_client() -> DatabricksJobsClient:
    """
    Get Databricks Jobs (Lakeflow) client (singleton).
    
    Uses REST API for job workflow orchestration.
    """
    return DatabricksJobsClient(
        host=get_databricks_host(),
        token=get_databricks_token()
    )


@lru_cache()
def get_apps_client() -> DatabricksAppsClient:
    """
    Get Databricks Apps client (singleton).
    
    Uses REST API for app deployment and lifecycle management.
    """
    return DatabricksAppsClient(
        host=get_databricks_host(),
        token=get_databricks_token()
    )


@lru_cache()
def get_vector_search_client() -> DatabricksVectorSearchClient:
    """
    Get Databricks Vector Search client (singleton).
    
    Uses REST API for vector indexes and similarity search.
    """
    return DatabricksVectorSearchClient(
        host=get_databricks_host(),
        token=get_databricks_token()
    )


@lru_cache()
def get_notebooks_client() -> DatabricksNotebooksClient:
    """
    Get Databricks Notebooks client (singleton).
    
    Uses REST API for notebook operations.
    """
    return DatabricksNotebooksClient(
        host=get_databricks_host(),
        token=get_databricks_token()
    )


@lru_cache()
def get_delta_manager() -> Optional[DeltaLakeManager]:
    """
    Get Delta Lake Manager (singleton).
    
    Requires Databricks adapter for SQL operations.
    """
    adapter = get_databricks_adapter()
    if adapter:
        return DeltaLakeManager(adapter)
    return None


@lru_cache()
def get_unity_catalog_rest_client() -> UnityCatalogRestClient:
    """
    Get Unity Catalog REST API client (singleton).
    
    Uses REST API for fast metadata operations.
    """
    return UnityCatalogRestClient(
        host=get_databricks_host(),
        token=get_databricks_token()
    )


@lru_cache()
def get_unity_catalog_manager() -> UnityCatalogManager:
    """
    Get Unity Catalog Manager in REST API mode (singleton).
    
    This is the FAST mode - 10-100x faster than SQL!
    """
    return UnityCatalogManager(
        host=get_databricks_host(),
        token=get_databricks_token()
    )


# Legacy SQL-based adapter (for backwards compatibility)
@lru_cache()
def get_databricks_adapter() -> Optional[DatabricksAdapter]:
    """
    Get Databricks adapter (legacy SQL mode).
    
    Only use for actual data queries, not metadata!
    REST API is much faster for metadata operations.
    """
    try:
        adapter = DatabricksAdapter(
            host=get_databricks_host(),
            token=get_databricks_token(),
            warehouse_id=get_warehouse_id(),
            catalog=os.getenv("DATABRICKS_CATALOG", "main"),
            schema=os.getenv("DATABRICKS_SCHEMA", "default")
        )
        return adapter
    except Exception:
        return None


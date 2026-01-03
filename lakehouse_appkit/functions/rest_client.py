"""
Databricks Unity Catalog Functions (UDFs) REST API client.

Functions allow you to create and manage user-defined functions in Unity Catalog.

API Documentation: https://docs.databricks.com/api/workspace/functions
"""
from typing import Dict, Any, List, Optional
from lakehouse_appkit.resilience import ResilientClient
from lakehouse_appkit.sdk.exceptions import DataAppError


class DatabricksFunctionsClient(ResilientClient):
    """Client for Databricks UC Functions API."""
    
    def __init__(self, host: str, token: str):
        """
        Initialize Functions client.
        
        Args:
            host: Databricks workspace URL
            token: Personal access token
        """
        super().__init__()
        self.host = host.replace('https://', '').replace('http://', '').rstrip('/')
        self.token = token
        self.base_url = f"https://{self.host}/api/2.1/unity-catalog/functions"
    
    async def _request(self, method: str, url: str, **kwargs) -> Dict[str, Any]:
        """Make HTTP request with resilience."""
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        return await self._resilient_request(method, url, headers=headers, **kwargs)
    
    async def list_functions(
        self,
        catalog_name: str,
        schema_name: str,
        max_results: int = 100
    ) -> List[Dict[str, Any]]:
        """List functions."""
        url = self.base_url
        params = {
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            "max_results": max_results
        }
        result = await self._request("GET", url, params=params)
        return result.get("functions", [])
    
    async def get_function(self, full_name: str) -> Dict[str, Any]:
        """Get function details."""
        url = f"{self.base_url}/{full_name}"
        return await self._request("GET", url)
    
    async def delete_function(self, full_name: str) -> Dict[str, Any]:
        """Delete a function."""
        url = f"{self.base_url}/{full_name}"
        return await self._request("DELETE", url)

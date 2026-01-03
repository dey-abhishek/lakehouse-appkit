"""
Databricks Unity Catalog Connections REST API client.

Connections allow you to connect to external data sources like MySQL, PostgreSQL, etc.

API Documentation: https://docs.databricks.com/api/workspace/connections
"""
from typing import Dict, Any, List, Optional
from lakehouse_appkit.resilience import ResilientClient
from lakehouse_appkit.sdk.exceptions import DataAppError


class DatabricksConnectionsClient(ResilientClient):
    """Client for Databricks UC Connections API."""
    
    def __init__(self, host: str, token: str):
        """
        Initialize Connections client.
        
        Args:
            host: Databricks workspace URL
            token: Personal access token
        """
        super().__init__()
        self.host = host.replace('https://', '').replace('http://', '').rstrip('/')
        self.token = token
        self.base_url = f"https://{self.host}/api/2.1/unity-catalog/connections"
    
    async def _request(self, method: str, url: str, **kwargs) -> Dict[str, Any]:
        """Make HTTP request with resilience."""
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        return await self._resilient_request(method, url, headers=headers, **kwargs)
    
    async def create_connection(
        self,
        name: str,
        connection_type: str,
        options: Dict[str, str],
        comment: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a connection."""
        url = self.base_url
        payload = {
            "name": name,
            "connection_type": connection_type,
            "options": options
        }
        if comment:
            payload["comment"] = comment
        return await self._request("POST", url, json=payload)
    
    async def list_connections(
        self,
        max_results: int = 100
    ) -> List[Dict[str, Any]]:
        """List connections."""
        url = self.base_url
        params = {"max_results": max_results}
        result = await self._request("GET", url, params=params)
        return result.get("connections", [])
    
    async def get_connection(self, name: str) -> Dict[str, Any]:
        """Get connection details."""
        url = f"{self.base_url}/{name}"
        return await self._request("GET", url)
    
    async def delete_connection(self, name: str) -> Dict[str, Any]:
        """Delete a connection."""
        url = f"{self.base_url}/{name}"
        return await self._request("DELETE", url)

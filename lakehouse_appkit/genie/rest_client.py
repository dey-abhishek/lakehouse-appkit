"""
Databricks Genie Space REST API client.

Genie is an AI-powered analytics interface that allows users to ask questions
in natural language and get insights from their data.

API Documentation: https://docs.databricks.com/api/workspace/genie
"""
from typing import Dict, Any, List, Optional
from lakehouse_appkit.resilience import ResilientClient
from lakehouse_appkit.sdk.exceptions import DataAppError


class DatabricksGenieClient(ResilientClient):
    """Client for Databricks Genie Space API."""
    
    def __init__(self, host: str, token: str):
        """
        Initialize Genie client.
        
        Args:
            host: Databricks workspace URL
            token: Personal access token
        """
        super().__init__()
        self.host = host.replace('https://', '').replace('http://', '').rstrip('/')
        self.token = token
        self.base_url = f"https://{self.host}/api/2.0/genie/spaces"
    
    async def _request(self, method: str, url: str, **kwargs) -> Dict[str, Any]:
        """Make HTTP request with resilience."""
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        return await self._resilient_request(method, url, headers=headers, **kwargs)
    
    async def create_space(
        self,
        display_name: str,
        description: Optional[str] = None,
        sql_warehouse_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a Genie space."""
        url = self.base_url
        payload = {"display_name": display_name}
        if description:
            payload["description"] = description
        if sql_warehouse_id:
            payload["sql_warehouse_id"] = sql_warehouse_id
        return await self._request("POST", url, json=payload)
    
    async def list_spaces(self) -> List[Dict[str, Any]]:
        """List all Genie spaces."""
        url = self.base_url
        result = await self._request("GET", url)
        return result.get("spaces", [])
    
    async def get_space(self, space_id: str) -> Dict[str, Any]:
        """Get Genie space details."""
        url = f"{self.base_url}/{space_id}"
        return await self._request("GET", url)
    
    async def update_space(
        self,
        space_id: str,
        display_name: Optional[str] = None,
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """Update a Genie space."""
        url = f"{self.base_url}/{space_id}"
        payload = {}
        if display_name:
            payload["display_name"] = display_name
        if description:
            payload["description"] = description
        return await self._request("PATCH", url, json=payload)
    
    async def delete_space(self, space_id: str) -> Dict[str, Any]:
        """Delete a Genie space."""
        url = f"{self.base_url}/{space_id}"
        return await self._request("DELETE", url)
    
    async def ask_question(
        self,
        space_id: str,
        question: str,
        context: Optional[str] = None
    ) -> Dict[str, Any]:
        """Ask a natural language question in a Genie space."""
        url = f"{self.base_url}/{space_id}/start-conversation"
        payload = {"content": question}
        if context:
            payload["context"] = context
        return await self._request("POST", url, json=payload)

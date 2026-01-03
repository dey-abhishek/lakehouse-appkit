"""Databricks Vector Search REST API client."""
from typing import List, Dict, Any, Optional
from lakehouse_appkit.resilience import ResilientClient


class DatabricksVectorSearchClient(ResilientClient):
    """Client for Databricks Vector Search API."""
    
    def __init__(self, host: str, token: str, **kwargs):
        """
        Initialize Vector Search client.
        
        Args:
            host: Databricks workspace host
            token: Databricks access token
            **kwargs: Additional arguments for ResilientClient
        """
        super().__init__(**kwargs)
        self.host = host.replace("https://", "").rstrip("/")
        self.token = token
        self.base_url = f"https://{self.host}/api/2.0/vector-search"
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers."""
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
    
    async def _request(self, method: str, endpoint: str, **kwargs) -> Any:
        """Make HTTP request with resilience."""
        session = await self.get_session()
        url = f"{self.base_url}{endpoint}"
        headers = self._get_headers()
        
        async def _call():
            async with session.request(method, url, headers=headers, **kwargs) as resp:
                resp.raise_for_status()
                if resp.status == 204:  # No content
                    return {}
                text = await resp.text()
                return await resp.json() if text else {}
        
        return await self._resilient_request(_call)
    
    # Endpoint Management
    async def create_endpoint(self, name: str, endpoint_type: str = "STANDARD") -> Dict[str, Any]:
        """Create vector search endpoint."""
        data = {"name": name, "endpoint_type": endpoint_type}
        return await self._request("POST", "/endpoints", json=data)
    
    async def list_endpoints(self) -> List[Dict[str, Any]]:
        """List all vector search endpoints."""
        result = await self._request("GET", "/endpoints")
        return result.get("endpoints", []) if isinstance(result, dict) else result
    
    async def get_endpoint(self, name: str) -> Dict[str, Any]:
        """Get endpoint details."""
        return await self._request("GET", f"/endpoints/{name}")
    
    async def delete_endpoint(self, name: str) -> Dict[str, Any]:
        """Delete vector search endpoint."""
        return await self._request("DELETE", f"/endpoints/{name}")
    
    # Index Management
    async def create_index(
        self,
        name: str,
        endpoint_name: str,
        primary_key: str,
        index_type: str,
        delta_sync_index_spec: Optional[Dict[str, Any]] = None,
        direct_access_index_spec: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create vector index."""
        data = {
            "name": name,
            "endpoint_name": endpoint_name,
            "primary_key": primary_key,
            "index_type": index_type
        }
        if delta_sync_index_spec:
            data["delta_sync_index_spec"] = delta_sync_index_spec
        if direct_access_index_spec:
            data["direct_access_index_spec"] = direct_access_index_spec
        return await self._request("POST", "/indexes", json=data)
    
    async def list_indexes(self, endpoint_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """List vector indexes."""
        params = f"?endpoint_name={endpoint_name}" if endpoint_name else ""
        result = await self._request("GET", f"/indexes{params}")
        return result.get("indexes", []) if isinstance(result, dict) else result
    
    async def get_index(self, name: str) -> Dict[str, Any]:
        """Get index details."""
        return await self._request("GET", f"/indexes/{name}")
    
    async def delete_index(self, name: str) -> Dict[str, Any]:
        """Delete vector index."""
        return await self._request("DELETE", f"/indexes/{name}")
    
    async def sync_index(self, name: str) -> Dict[str, Any]:
        """Trigger index sync."""
        return await self._request("POST", f"/indexes/{name}/sync")
    
    # Query & Data Operations
    async def query_index(
        self,
        index_name: str,
        query_vector: Optional[List[float]] = None,
        query_text: Optional[str] = None,
        num_results: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Query vector index."""
        data = {"num_results": num_results}
        if query_vector:
            data["query_vector"] = query_vector
        if query_text:
            data["query_text"] = query_text
        if filters:
            data["filters"] = filters
        if columns:
            data["columns"] = columns
        return await self._request("POST", f"/indexes/{index_name}/query", json=data)
    
    async def upsert_data(self, index_name: str, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Upsert data into direct access index."""
        return await self._request("POST", f"/indexes/{index_name}/upsert", json={"data": data})
    
    async def delete_data(self, index_name: str, primary_keys: List[str]) -> Dict[str, Any]:
        """Delete data from direct access index."""
        return await self._request("POST", f"/indexes/{index_name}/delete", json={"primary_keys": primary_keys})


__all__ = ['DatabricksVectorSearchClient']

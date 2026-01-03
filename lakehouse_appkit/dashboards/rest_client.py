"""Databricks AI/BI Dashboards REST API client."""
from typing import List, Dict, Any, Optional
from lakehouse_appkit.resilience import ResilientClient


class DashboardLifecycleState:
    """Dashboard lifecycle states."""
    ACTIVE = "ACTIVE"
    TRASHED = "TRASHED"


class DatabricksAIBIDashboardClient(ResilientClient):
    """REST API client for Databricks AI/BI Dashboards (Lakeview)."""
    
    def __init__(self, host: str, token: str, **kwargs):
        """
        Initialize AI/BI Dashboard client.
        
        Args:
            host: Databricks workspace host
            token: Databricks access token
            **kwargs: Additional arguments for ResilientClient
        """
        super().__init__(**kwargs)
        self.host = host.replace("https://", "").rstrip("/")
        self.token = token
        self.base_url = f"https://{self.host}/api/2.0/lakeview/dashboards"
    
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
                if resp.status == 204:
                    return {}
                text = await resp.text()
                return await resp.json() if text else {}
        
        return await self._resilient_request(_call)
    
    # Dashboard Management
    async def create_dashboard(
        self,
        display_name: str,
        warehouse_id: str,
        parent_path: Optional[str] = None,
        serialized_dashboard: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a new dashboard."""
        data = {
            "display_name": display_name,
            "warehouse_id": warehouse_id
        }
        if parent_path:
            data["parent_path"] = parent_path
        if serialized_dashboard:
            data["serialized_dashboard"] = serialized_dashboard
        return await self._request("POST", "", json=data)
    
    async def list_dashboards(
        self,
        page_size: int = 100,
        page_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """List dashboards."""
        params = {"page_size": page_size}
        if page_token:
            params["page_token"] = page_token
        return await self._request("GET", "", params=params)
    
    async def get_dashboard(self, dashboard_id: str) -> Dict[str, Any]:
        """Get dashboard details."""
        return await self._request("GET", f"/{dashboard_id}")
    
    async def update_dashboard(
        self,
        dashboard_id: str,
        display_name: Optional[str] = None,
        warehouse_id: Optional[str] = None,
        serialized_dashboard: Optional[str] = None,
        etag: Optional[str] = None
    ) -> Dict[str, Any]:
        """Update a dashboard."""
        data = {}
        if display_name:
            data["display_name"] = display_name
        if warehouse_id:
            data["warehouse_id"] = warehouse_id
        if serialized_dashboard:
            data["serialized_dashboard"] = serialized_dashboard
        if etag:
            data["etag"] = etag
        return await self._request("PATCH", f"/{dashboard_id}", json=data)
    
    async def publish_dashboard(self, dashboard_id: str) -> Dict[str, Any]:
        """Publish a dashboard."""
        return await self._request("POST", f"/{dashboard_id}/published", json={})
    
    async def unpublish_dashboard(self, dashboard_id: str) -> Dict[str, Any]:
        """Unpublish a dashboard."""
        return await self._request("DELETE", f"/{dashboard_id}/published")
    
    async def trash_dashboard(self, dashboard_id: str) -> Dict[str, Any]:
        """Move dashboard to trash."""
        return await self._request("DELETE", f"/{dashboard_id}")
    
    async def get_published_dashboard(self, dashboard_id: str) -> Dict[str, Any]:
        """Get published dashboard."""
        return await self._request("GET", f"/{dashboard_id}/published")


__all__ = ['DatabricksAIBIDashboardClient', 'DashboardLifecycleState']

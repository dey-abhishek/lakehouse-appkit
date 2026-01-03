"""Databricks Deployment/App REST API client."""
from typing import List, Dict, Any, Optional
from lakehouse_appkit.resilience import ResilientClient


class DatabricksDeploymentClient(ResilientClient):
    """REST API client for Databricks App Deployment."""
    
    def __init__(self, host: str, token: str, **kwargs):
        """
        Initialize Deployment client.
        
        Args:
            host: Databricks workspace host
            token: Databricks access token
            **kwargs: Additional arguments for ResilientClient
        """
        super().__init__(**kwargs)
        self.host = host.replace("https://", "").rstrip("/")
        self.token = token
        self.base_url = f"https://{self.host}/api/2.0/apps"
    
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
    
    # App Management
    async def create_app(
        self,
        name: str,
        source_code_path: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Create a new app."""
        data = {"name": name, **kwargs}
        if source_code_path:
            data["source_code_path"] = source_code_path
        return await self._request("POST", "", json=data)
    
    async def list_apps(self) -> List[Dict[str, Any]]:
        """List all apps."""
        result = await self._request("GET", "")
        # Return the apps list if present, otherwise return empty list
        if isinstance(result, dict) and "apps" in result:
            return result["apps"]
        return []
    
    async def get_app(self, name: str) -> Dict[str, Any]:
        """Get app details."""
        return await self._request("GET", f"/{name}")
    
    async def update_app(self, name: str, **kwargs) -> Dict[str, Any]:
        """Update an app."""
        return await self._request("PATCH", f"/{name}", json=kwargs)
    
    async def delete_app(self, name: str) -> Dict[str, Any]:
        """Delete an app."""
        return await self._request("DELETE", f"/{name}")
    
    async def start_app(self, name: str) -> Dict[str, Any]:
        """Start an app."""
        return await self._request("POST", f"/{name}/start")
    
    async def stop_app(self, name: str) -> Dict[str, Any]:
        """Stop an app."""
        return await self._request("POST", f"/{name}/stop")
    
    async def get_app_environment(self, name: str) -> Dict[str, Any]:
        """Get app environment variables."""
        return await self._request("GET", f"/{name}/environment")
    
    async def set_app_environment(self, name: str, env_vars: Dict[str, str]) -> Dict[str, Any]:
        """Set app environment variables."""
        return await self._request("PUT", f"/{name}/environment", json=env_vars)
    
    async def deploy_app(self, name: str, source_code_path: Optional[str] = None) -> Dict[str, Any]:
        """Deploy an app."""
        data = {}
        if source_code_path:
            data["source_code_path"] = source_code_path
        return await self._request("POST", f"/{name}/deployments", json=data)
    
    async def get_deployment(self, name: str, deployment_id: str) -> Dict[str, Any]:
        """Get deployment details."""
        return await self._request("GET", f"/{name}/deployments/{deployment_id}")
    
    async def set_environment(self, name: str, env_vars: Dict[str, str]) -> Dict[str, Any]:
        """Set environment variables (alias for set_app_environment)."""
        return await self.set_app_environment(name, env_vars)
    
    async def get_app_logs(self, name: str, lines: int = 100) -> Dict[str, Any]:
        """Get app logs."""
        params = {"lines": lines} if lines else {}
        return await self._request("GET", f"/{name}/logs", params=params)


__all__ = ['DatabricksDeploymentClient', 'DatabricksAppsClient']

# Alias for backwards compatibility
DatabricksAppsClient = DatabricksDeploymentClient

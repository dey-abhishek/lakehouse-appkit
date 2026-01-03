"""Databricks Model Serving REST API client."""
from typing import List, Dict, Any, Optional
from lakehouse_appkit.resilience import ResilientClient


class DatabricksModelServingClient(ResilientClient):
    """REST API client for Databricks Model Serving."""
    
    def __init__(self, host: str, token: str, **kwargs):
        """Initialize Model Serving client."""
        super().__init__(**kwargs)
        self.host = host.replace("https://", "").rstrip("/")
        self.token = token
        self.base_url = f"https://{self.host}/api/2.0/serving-endpoints"
    
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
    
    # Endpoint Management - Match test expectations
    async def create_endpoint(
        self,
        name: str,
        model_name: str,
        model_version: str,
        workload_size: str = "Small",
        scale_to_zero_enabled: bool = True,
        **kwargs
    ) -> Dict[str, Any]:
        """Create a serving endpoint."""
        config = {
            "served_models": [{
                "model_name": model_name,
                "model_version": model_version,
                "workload_size": workload_size,
                "scale_to_zero_enabled": scale_to_zero_enabled
            }]
        }
        config.update(kwargs.get('config', {}))
        data = {"name": name, "config": config}
        return await self._request("POST", "", json=data)
    
    async def list_endpoints(self) -> List[Dict[str, Any]]:
        """List serving endpoints."""
        result = await self._request("GET", "")
        # Return the endpoints list if present, otherwise return empty list
        if isinstance(result, dict) and "endpoints" in result:
            return result["endpoints"]
        return []
    
    async def get_endpoint(self, name: str) -> Dict[str, Any]:
        """Get endpoint details."""
        return await self._request("GET", f"/{name}")
    
    async def update_endpoint(
        self,
        name: str,
        model_name: Optional[str] = None,
        model_version: Optional[str] = None,
        workload_size: Optional[str] = None,
        scale_to_zero_enabled: Optional[bool] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Update a serving endpoint."""
        config = {}
        served_models = []
        
        if model_name and model_version:
            served_model = {
                "model_name": model_name,
                "model_version": model_version
            }
            if workload_size:
                served_model["workload_size"] = workload_size
            if scale_to_zero_enabled is not None:
                served_model["scale_to_zero_enabled"] = scale_to_zero_enabled
            served_models.append(served_model)
            config["served_models"] = served_models
        
        config.update(kwargs.get('config', {}))
        data = {"config": config}
        return await self._request("PATCH", f"/{name}/config", json=data)
    
    async def update_traffic_config(
        self,
        name: str,
        routes: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Update endpoint traffic configuration."""
        config = {"traffic_config": {"routes": routes}}
        return await self._request("PUT", f"/{name}/config", json={"config": config})
    
    async def delete_endpoint(self, name: str) -> Dict[str, Any]:
        """Delete a serving endpoint."""
        return await self._request("DELETE", f"/{name}")
    
    async def query_endpoint(
        self,
        name: str,
        inputs: Any
    ) -> Dict[str, Any]:
        """Query a serving endpoint."""
        # Use the serving-endpoints URL format (not api/2.0 format)
        session = await self.get_session()
        url = f"https://{self.host}/serving-endpoints/{name}/invocations"
        headers = self._get_headers()
        
        data = {"inputs": inputs} if isinstance(inputs, dict) else {"dataframe_records": inputs}
        
        async def _call():
            async with session.request("POST", url, headers=headers, json=data) as resp:
                resp.raise_for_status()
                if resp.status == 204:
                    return {}
                text = await resp.text()
                return await resp.json() if text else {}
        
        return await self._resilient_request(_call)
    
    async def get_endpoint_metrics(
        self,
        name: str
    ) -> Dict[str, Any]:
        """Get endpoint metrics."""
        return await self._request("GET", f"/{name}/metrics")
    
    async def get_endpoint_logs(
        self,
        name: str,
        served_model_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get endpoint logs."""
        params = {}
        if served_model_name:
            params["served_model_name"] = served_model_name
        return await self._request("GET", f"/{name}/logs", params=params)


__all__ = ['DatabricksModelServingClient']

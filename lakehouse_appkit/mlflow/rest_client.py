"""
Databricks MLflow Experiments REST API client.

MLflow is an open source platform for managing the ML lifecycle, including
experimentation, reproducibility, deployment, and a central model registry.

API Documentation: https://docs.databricks.com/api/workspace/experiments
"""
from typing import Dict, Any, List, Optional
from lakehouse_appkit.resilience import ResilientClient
from lakehouse_appkit.sdk.exceptions import DataAppError


class DatabricksMLflowClient(ResilientClient):
    """Client for Databricks MLflow Experiments API."""
    
    def __init__(self, host: str, token: str):
        """
        Initialize MLflow client.
        
        Args:
            host: Databricks workspace URL
            token: Personal access token
        """
        super().__init__()
        self.host = host.replace('https://', '').replace('http://', '').rstrip('/')
        self.token = token
        self.base_url = f"https://{self.host}/api/2.0/mlflow"
    
    async def _request(self, method: str, url: str, **kwargs) -> Dict[str, Any]:
        """Make HTTP request with resilience."""
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        return await self._resilient_request(method, url, headers=headers, **kwargs)
    
    async def create_experiment(
        self,
        name: str,
        artifact_location: Optional[str] = None,
        tags: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """Create an MLflow experiment."""
        url = f"{self.base_url}/experiments/create"
        payload = {"name": name}
        if artifact_location:
            payload["artifact_location"] = artifact_location
        if tags:
            payload["tags"] = tags
        return await self._request("POST", url, json=payload)
    
    async def list_experiments(
        self,
        max_results: int = 100,
        view_type: str = "ACTIVE_ONLY"
    ) -> List[Dict[str, Any]]:
        """List MLflow experiments."""
        url = f"{self.base_url}/experiments/list"
        params = {"max_results": max_results, "view_type": view_type}
        result = await self._request("GET", url, params=params)
        return result.get("experiments", [])
    
    async def get_experiment(self, experiment_id: str) -> Dict[str, Any]:
        """Get experiment details."""
        url = f"{self.base_url}/experiments/get"
        params = {"experiment_id": experiment_id}
        return await self._request("GET", url, params=params)
    
    async def create_run(
        self,
        experiment_id: str,
        tags: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """Create an MLflow run."""
        url = f"{self.base_url}/runs/create"
        payload = {"experiment_id": experiment_id}
        if tags:
            payload["tags"] = tags
        return await self._request("POST", url, json=payload)
    
    async def log_metric(
        self,
        run_id: str,
        key: str,
        value: float,
        timestamp: Optional[int] = None,
        step: Optional[int] = None
    ) -> Dict[str, Any]:
        """Log a metric to an MLflow run."""
        url = f"{self.base_url}/runs/log-metric"
        payload = {"run_id": run_id, "key": key, "value": value}
        if timestamp:
            payload["timestamp"] = timestamp
        if step:
            payload["step"] = step
        return await self._request("POST", url, json=payload)
    
    async def search_runs(
        self,
        experiment_ids: List[str],
        filter_string: Optional[str] = None,
        max_results: int = 100
    ) -> List[Dict[str, Any]]:
        """Search MLflow runs."""
        url = f"{self.base_url}/runs/search"
        payload = {"experiment_ids": experiment_ids, "max_results": max_results}
        if filter_string:
            payload["filter"] = filter_string
        result = await self._request("POST", url, json=payload)
        return result.get("runs", [])

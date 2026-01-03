"""Databricks Jobs REST API client."""
from typing import List, Dict, Any, Optional
from lakehouse_appkit.resilience import ResilientClient


class DatabricksJobsClient(ResilientClient):
    """REST API client for Databricks Jobs."""
    
    def __init__(self, host: str, token: str, **kwargs):
        """
        Initialize Jobs client.
        
        Args:
            host: Databricks workspace host
            token: Databricks access token
            **kwargs: Additional arguments for ResilientClient
        """
        super().__init__(**kwargs)
        self.host = host.replace("https://", "").rstrip("/")
        self.token = token
        self.base_url = f"https://{self.host}/api/2.1/jobs"
    
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
    
    # Job Management
    async def create_job(self, name: str, tasks: List[Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        """Create a new job."""
        data = {"name": name, "tasks": tasks, **kwargs}
        return await self._request("POST", "/create", json=data)
    
    async def list_jobs(
        self,
        limit: int = 25,
        offset: int = 0,
        expand_tasks: bool = False
    ) -> Dict[str, Any]:
        """List jobs."""
        params = {
            "limit": limit,
            "offset": offset,
            "expand_tasks": "true" if expand_tasks else "false"
        }
        return await self._request("GET", "/list", params=params)
    
    async def get_job(self, job_id: int) -> Dict[str, Any]:
        """Get job details."""
        return await self._request("GET", "/get", params={"job_id": job_id})
    
    async def update_job(self, job_id: int, new_settings: Dict[str, Any]) -> Dict[str, Any]:
        """Update a job."""
        data = {"job_id": job_id, "new_settings": new_settings}
        return await self._request("POST", "/update", json=data)
    
    async def delete_job(self, job_id: int) -> Dict[str, Any]:
        """Delete a job."""
        return await self._request("POST", "/delete", json={"job_id": job_id})
    
    async def run_now(self, job_id: int, **kwargs) -> Dict[str, Any]:
        """Trigger a job run."""
        data = {"job_id": job_id, **kwargs}
        return await self._request("POST", "/run-now", json=data)
    
    async def cancel_run(self, run_id: int) -> Dict[str, Any]:
        """Cancel a job run."""
        return await self._request("POST", "/runs/cancel", json={"run_id": run_id})
    
    async def get_run(self, run_id: int) -> Dict[str, Any]:
        """Get run details."""
        return await self._request("GET", "/runs/get", params={"run_id": run_id})
    
    async def list_runs(
        self,
        job_id: Optional[int] = None,
        active_only: bool = False,
        limit: int = 25,
        offset: int = 0
    ) -> Dict[str, Any]:
        """List job runs."""
        params = {
            "limit": limit,
            "offset": offset,
            "active_only": "true" if active_only else "false"  # Convert bool to string for API
        }
        if job_id:
            params["job_id"] = job_id
        return await self._request("GET", "/runs/list", params=params)
    
    async def get_run_output(self, run_id: int) -> Dict[str, Any]:
        """Get run output."""
        return await self._request("GET", "/runs/get-output", params={"run_id": run_id})
    
    async def repair_run(
        self,
        run_id: int,
        rerun_tasks: Optional[List[str]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Repair a failed job run.
        
        Args:
            run_id: ID of the run to repair
            rerun_tasks: List of task keys to rerun
            **kwargs: Additional parameters
        
        Returns:
            Repair response with repair_id
        """
        data = {"run_id": run_id}
        if rerun_tasks:
            data["rerun_tasks"] = rerun_tasks
        data.update(kwargs)
        return await self._request("POST", "/runs/repair", json=data)


__all__ = ['DatabricksJobsClient']

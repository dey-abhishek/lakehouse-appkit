"""Databricks Delta Lake REST API client."""
from typing import List, Dict, Any, Optional
from lakehouse_appkit.resilience import ResilientClient


class DatabricksDeltaClient(ResilientClient):
    """REST API client for Delta Lake operations."""
    
    def __init__(self, host: str, token: str, **kwargs):
        """
        Initialize Delta Lake client.
        
        Args:
            host: Databricks workspace host
            token: Databricks access token
            **kwargs: Additional arguments for ResilientClient
        """
        super().__init__(**kwargs)
        # Ensure host is a string and handle Mock objects
        if not isinstance(host, str):
            host = str(host) if host else "localhost"
        if not isinstance(token, str):
            token = str(token) if token else "token"
        
        self.host = host.replace("https://", "").rstrip("/")
        self.token = token
        self.base_url = f"https://{self.host}/api/2.0"
    
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
    
    # Delta Lake Operations via SQL
    async def optimize_table(
        self,
        table_name: str,
        where: Optional[str] = None,
        zorder_by: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Optimize a Delta table."""
        sql = f"OPTIMIZE {table_name}"
        if where:
            sql += f" WHERE {where}"
        if zorder_by:
            sql += f" ZORDER BY ({', '.join(zorder_by)})"
        
        return await self._request("POST", "/sql/statements", json={
            "statement": sql,
            "wait_timeout": "30s"
        })
    
    async def vacuum_table(
        self,
        table_name: str,
        retention_hours: int = 168
    ) -> Dict[str, Any]:
        """Vacuum a Delta table."""
        sql = f"VACUUM {table_name} RETAIN {retention_hours} HOURS"
        return await self._request("POST", "/sql/statements", json={
            "statement": sql,
            "wait_timeout": "30s"
        })
    
    async def describe_history(
        self,
        table_name: str,
        limit: int = 10
    ) -> Dict[str, Any]:
        """Get table history."""
        sql = f"DESCRIBE HISTORY {table_name} LIMIT {limit}"
        return await self._request("POST", "/sql/statements", json={
            "statement": sql,
            "wait_timeout": "30s"
        })
    
    async def describe_detail(self, table_name: str) -> Dict[str, Any]:
        """Get table details."""
        sql = f"DESCRIBE DETAIL {table_name}"
        return await self._request("POST", "/sql/statements", json={
            "statement": sql,
            "wait_timeout": "30s"
        })
    
    async def restore_table(
        self,
        table_name: str,
        version: Optional[int] = None,
        timestamp: Optional[str] = None
    ) -> Dict[str, Any]:
        """Restore table to previous version."""
        if version is not None:
            sql = f"RESTORE TABLE {table_name} TO VERSION AS OF {version}"
        elif timestamp:
            sql = f"RESTORE TABLE {table_name} TO TIMESTAMP AS OF '{timestamp}'"
        else:
            raise ValueError("Must provide either version or timestamp")
        
        return await self._request("POST", "/sql/statements", json={
            "statement": sql,
            "wait_timeout": "30s"
        })


__all__ = ['DatabricksDeltaClient']


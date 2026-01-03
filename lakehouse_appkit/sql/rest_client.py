"""
Databricks SQL Execution API Client - REST API Mode (Fast!)

Uses the Databricks SQL Statement Execution API for direct query execution.
Reference: https://docs.databricks.com/api/workspace/statementexecution
"""
import aiohttp
import asyncio
from typing import Optional, Dict, Any, List
from enum import Enum

from lakehouse_appkit.sdk.exceptions import ConnectionError, QueryError


class StatementState(str, Enum):
    """SQL statement execution states."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"
    CLOSED = "CLOSED"


class DatabricksSQLClient:
    """
    REST API client for Databricks SQL Statement Execution.
    
    Much faster than databricks-sql-connector for simple queries!
    Reference: https://docs.databricks.com/api/workspace/statementexecution
    """
    
    def __init__(self, host: str, token: str, warehouse_id: Optional[str] = None):
        """
        Initialize Databricks SQL API client.
        
        Args:
            host: Databricks workspace URL (e.g., https://xxx.cloud.databricks.com)
            token: Databricks personal access token
            warehouse_id: SQL warehouse ID for query execution (optional)
        """
        self.host = host.rstrip('/')
        self.token = token
        self.warehouse_id = warehouse_id or ""
        self.base_url = f"{self.host}/api/2.0/sql/statements"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
    
    async def execute_statement(
        self,
        statement: str,
        warehouse_id: Optional[str] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        parameters: Optional[List[Dict[str, Any]]] = None,
        wait_timeout: str = "30s"
    ) -> Dict[str, Any]:
        """
        Execute a SQL statement and return results.
        
        API: POST /api/2.0/sql/statements
        Reference: https://docs.databricks.com/api/workspace/statementexecution/executestatement
        
        Args:
            statement: SQL statement to execute
            warehouse_id: SQL warehouse ID (uses default if not specified)
            catalog: Optional catalog to use
            schema: Optional schema to use
            parameters: Optional query parameters
            wait_timeout: How long to wait for results (default: 30s)
            
        Returns:
            Statement execution result with data
        """
        payload = {
            "statement": statement,
            "warehouse_id": warehouse_id or self.warehouse_id,
            "wait_timeout": wait_timeout,
            "on_wait_timeout": "CONTINUE"
        }
        
        if catalog:
            payload["catalog"] = catalog
        if schema:
            payload["schema"] = schema
        if parameters:
            payload["parameters"] = parameters
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.base_url,
                    headers=self.headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=60)
                ) as response:
                    if response.status >= 400:
                        error_text = await response.text()
                        raise QueryError(
                            f"SQL execution failed: {response.status} - {error_text}"
                        )
                    
                    result = await response.json()
                    
                    # If statement is still running, poll for results
                    if result.get("status", {}).get("state") == StatementState.PENDING:
                        return await self._poll_statement(result["statement_id"])
                    
                    return result
        except aiohttp.ClientError as e:
            raise ConnectionError(f"Failed to execute SQL statement: {str(e)}")
    
    async def _poll_statement(
        self,
        statement_id: str,
        max_attempts: int = 60,
        poll_interval: float = 1.0
    ) -> Dict[str, Any]:
        """
        Poll a statement until it completes.
        
        Args:
            statement_id: Statement ID to poll
            max_attempts: Maximum polling attempts
            poll_interval: Seconds between polls
            
        Returns:
            Statement result
        """
        url = f"{self.base_url}/{statement_id}"
        
        for attempt in range(max_attempts):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        url,
                        headers=self.headers,
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        if response.status >= 400:
                            error_text = await response.text()
                            raise QueryError(
                                f"Failed to get statement status: {response.status} - {error_text}"
                            )
                        
                        result = await response.json()
                        state = result.get("status", {}).get("state")
                        
                        if state == StatementState.SUCCEEDED:
                            return result
                        elif state in [StatementState.FAILED, StatementState.CANCELED]:
                            error = result.get("status", {}).get("error", {})
                            raise QueryError(f"Statement failed: {error}")
                        
                        # Still running, wait and retry
                        await asyncio.sleep(poll_interval)
            except aiohttp.ClientError as e:
                if attempt == max_attempts - 1:
                    raise ConnectionError(f"Failed to poll statement: {str(e)}")
                await asyncio.sleep(poll_interval)
        
        raise QueryError(f"Statement timed out after {max_attempts} attempts")
    
    async def cancel_statement(self, statement_id: str) -> Dict[str, Any]:
        """
        Cancel a running statement.
        
        API: POST /api/2.0/sql/statements/{statement_id}/cancel
        Reference: https://docs.databricks.com/api/workspace/statementexecution/cancelexecution
        
        Args:
            statement_id: Statement ID to cancel
            
        Returns:
            Cancellation result
        """
        url = f"{self.base_url}/{statement_id}/cancel"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    headers=self.headers,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status >= 400:
                        error_text = await response.text()
                        raise QueryError(
                            f"Failed to cancel statement: {response.status} - {error_text}"
                        )
                    
                    return await response.json()
        except aiohttp.ClientError as e:
            raise ConnectionError(f"Failed to cancel statement: {str(e)}")
    
    async def execute_and_fetch(
        self,
        statement: str,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Execute a statement and return results as list of dicts.
        
        Convenience method that extracts data from the API response.
        
        Args:
            statement: SQL statement
            **kwargs: Additional arguments for execute_statement
            
        Returns:
            List of row dictionaries
        """
        result = await self.execute_statement(statement, **kwargs)
        
        # Extract results
        manifest = result.get("manifest", {})
        chunks = manifest.get("chunks", [])
        
        if not chunks:
            return []
        
        # Get column names
        schema = result.get("manifest", {}).get("schema", {})
        columns = [col["name"] for col in schema.get("columns", [])]
        
        # Parse data from chunks
        rows = []
        for chunk in chunks:
            chunk_data = chunk.get("data_array", [])
            for row_data in chunk_data:
                row_dict = dict(zip(columns, row_data))
                rows.append(row_dict)
        
        return rows
    
    async def list_warehouses(self) -> List[Dict[str, Any]]:
        """
        List SQL warehouses.
        
        API: GET /api/2.0/sql/warehouses
        Reference: https://docs.databricks.com/api/workspace/warehouses/list
        
        Returns:
            List of warehouse objects
        """
        url = f"{self.host}/api/2.0/sql/warehouses"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    headers=self.headers,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status >= 400:
                        error_text = await response.text()
                        raise QueryError(
                            f"Failed to list warehouses: {response.status} - {error_text}"
                        )
                    
                    result = await response.json()
                    return result.get("warehouses", [])
        except aiohttp.ClientError as e:
            raise ConnectionError(f"Failed to list warehouses: {str(e)}")
    
    async def get_warehouse(self, warehouse_id: str) -> Dict[str, Any]:
        """
        Get warehouse details.
        
        API: GET /api/2.0/sql/warehouses/{id}
        Reference: https://docs.databricks.com/api/workspace/warehouses/get
        
        Args:
            warehouse_id: Warehouse ID
            
        Returns:
            Warehouse object
        """
        url = f"{self.host}/api/2.0/sql/warehouses/{warehouse_id}"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    headers=self.headers,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status >= 400:
                        error_text = await response.text()
                        raise QueryError(
                            f"Failed to get warehouse: {response.status} - {error_text}"
                        )
                    
                    return await response.json()
        except aiohttp.ClientError as e:
            raise ConnectionError(f"Failed to get warehouse: {str(e)}")


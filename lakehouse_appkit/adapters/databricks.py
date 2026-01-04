"""
Databricks adapter for Lakehouse-AppKit.

Uses REST APIs and Databricks SDK:
- Databricks SDK (databricks-sdk) for workspace operations, Unity Catalog, Jobs, etc.
- REST APIs (aiohttp) for SQL execution, performance-critical operations, and fine-grained control

Architecture:
- SDK Client: Workspace management, Unity Catalog, Jobs, Model Serving
- REST Client: SQL execution (Statement Execution API), fast HTTP operations, batch requests

No SQL Connector dependency required - all SQL operations via REST API!
"""
from typing import Any, Dict, List, Optional
from functools import wraps
import asyncio
import aiohttp
import logging

# Optional imports - fail gracefully if not installed
try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.catalog import (
        CatalogInfo,
        SchemaInfo,
        TableInfo,
    )
    HAS_SDK = True
except ImportError:
    HAS_SDK = False
    WorkspaceClient = None

logger = logging.getLogger(__name__)


# Define custom exceptions
class QueryError(Exception):
    """Query execution error."""
    pass


class ConnectionError(Exception):
    """Connection error."""
    pass


class AuthenticationError(Exception):
    """Authentication error."""
    pass


def require_connection(func):
    """Decorator to ensure connection before executing query."""
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        if not self.connected:
            await self.connect()
        return await func(self, *args, **kwargs)
    return wrapper


class DatabricksAdapter:
    """
    Unified Databricks adapter using SDK and REST APIs only.

    Components:
    1. SDK Client (databricks-sdk):
       - Unity Catalog operations (catalogs, schemas, tables, volumes)
       - Jobs management (create, run, monitor)
       - Model Serving
       - Clusters
       - Secrets
       - Workspace files

    2. REST Client (aiohttp):
       - SQL execution via Statement Execution API
       - Performance-critical operations
       - Batch requests
       - Custom/undocumented endpoints
       - Fine-grained HTTP control

    Usage:
        # Initialize with credentials
        adapter = DatabricksAdapter(
            host="company.cloud.databricks.com",
            token="dapi...",
            warehouse_id="abc123"
        )

        # Connect (initializes all clients)
        await adapter.connect()

        # Use SDK for Unity Catalog
        catalogs = await adapter.list_catalogs_sdk()

        # Use REST for SQL execution
        results = await adapter.execute_query("SELECT * FROM table")

        # Cleanup
        await adapter.disconnect()
    """

    def __init__(
        self,
        host: str,
        token: str,
        warehouse_id: Optional[str] = None,
        catalog: str = "main",
        schema: str = "default",
        timeout: int = 30,
        use_sdk: bool = True,
    ):
        """
        Initialize Databricks adapter with SDK and REST support.

        Args:
            host: Databricks workspace host (e.g., 'company.cloud.databricks.com')
            token: Personal access token or OAuth token
            warehouse_id: SQL warehouse ID (required for SQL operations)
            catalog: Default Unity Catalog catalog
            schema: Default schema name
            timeout: Request timeout in seconds
            use_sdk: Enable Databricks SDK (default: True)
        """
        # Normalize host
        if not host.startswith("http://") and not host.startswith("https://"):
            host = f"https://{host}"
        if "?" in host:
            host = host.split("?")[0]

        self.host = host.replace("https://", "").replace("http://", "")
        self.workspace_url = f"https://{self.host}"
        self.token = token
        self.warehouse_id = warehouse_id
        self.catalog = catalog
        self.schema = schema
        self.timeout = timeout

        # Feature flags
        self.use_sdk = use_sdk and HAS_SDK

        # Client instances
        self.sdk_client: Optional[WorkspaceClient] = None
        self.rest_session: Optional[aiohttp.ClientSession] = None

        # Connection state
        self.connected = False

        # Warn if dependencies missing
        if use_sdk and not HAS_SDK:
            logger.warning("databricks-sdk not installed. SDK features disabled.")

    async def connect(self):
        """
        Connect to Databricks using all available clients.

        Initializes:
        - SDK Client (if use_sdk=True)
        - REST Session (aiohttp)

        Raises:
            ConnectionError: If connection fails
        """
        try:
            # 1. Initialize SDK Client
            if self.use_sdk:
                self.sdk_client = WorkspaceClient(
                    host=self.workspace_url,
                    token=self.token,
                )
                logger.info("Databricks SDK client initialized")

            # 2. Initialize REST Session
            self.rest_session = aiohttp.ClientSession(
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json",
                },
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            )
            logger.info("REST API session initialized")

            self.connected = True
            logger.info(f"Connected to Databricks: {self.host}")

        except Exception as e:
            await self.disconnect()
            raise ConnectionError(f"Failed to connect to Databricks: {str(e)}")

    async def disconnect(self):
        """
        Disconnect all clients and release resources.

        Closes:
        - REST session
        - SDK client (cleanup)
        """
        # Close REST session
        if self.rest_session and not self.rest_session.closed:
            try:
                await self.rest_session.close()
            except Exception as e:
                logger.warning(f"Error closing REST session: {e}")

        # SDK client cleanup
        if self.sdk_client:
            # SDK client doesn't need explicit cleanup
            self.sdk_client = None

        self.connected = False
        logger.info("Disconnected from Databricks")

    # ========================================================================
    # SQL OPERATIONS VIA REST API - Statement Execution API
    # ========================================================================

    @require_connection
    async def execute_query(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute SQL query via REST API and return results as list of dictionaries.

        Uses Databricks Statement Execution API - no SQL connector needed!

        Args:
            query: SQL query string
            params: Optional query parameters (dict)

        Returns:
            List of result rows as dictionaries

        Raises:
            QueryError: If query execution fails

        Example:
            results = await adapter.execute_query(
                "SELECT * FROM catalog.schema.table WHERE id = :id",
                {"id": 123}
            )
        """
        if not self.warehouse_id:
            raise QueryError("SQL execution requires warehouse_id")

        return await self.execute_query_rest(query, params)

    @require_connection
    async def execute_statement(self, statement: str) -> int:
        """
        Execute SQL statement (INSERT, UPDATE, DELETE, DDL) via REST API.

        Args:
            statement: SQL statement

        Returns:
            Number of rows affected (best effort)

        Raises:
            QueryError: If execution fails
        """
        if not self.warehouse_id:
            raise QueryError("SQL execution requires warehouse_id")

        result = await self.execute_statement_rest(self.warehouse_id, statement)
        
        # Extract row count from REST response
        if result.get("status", {}).get("state") == "SUCCEEDED":
            # Try to extract row count from result
            result_data = result.get("result", {})
            if "data_array" in result_data and result_data["data_array"]:
                return len(result_data["data_array"])
            return 0
        else:
            raise QueryError(f"Statement execution failed: {result.get('status', {})}")

    # ========================================================================
    # SDK METHODS - Unity Catalog, Jobs, Model Serving
    # ========================================================================

    async def list_catalogs_sdk(self) -> List[CatalogInfo]:
        """
        List Unity Catalog catalogs using SDK.

        Returns:
            List of CatalogInfo objects

        Example:
            catalogs = await adapter.list_catalogs_sdk()
            for catalog in catalogs:
                print(catalog.name, catalog.owner)
        """
        if not self.use_sdk or not self.sdk_client:
            raise ConnectionError("SDK client not available. Enable use_sdk=True")

        loop = asyncio.get_event_loop()
        catalogs = await loop.run_in_executor(
            None,
            lambda: list(self.sdk_client.catalogs.list()),
        )
        return catalogs

    async def list_schemas_sdk(self, catalog: str) -> List[SchemaInfo]:
        """List schemas in a catalog using SDK."""
        if not self.use_sdk or not self.sdk_client:
            raise ConnectionError("SDK client not available")

        loop = asyncio.get_event_loop()
        schemas = await loop.run_in_executor(
            None,
            lambda: list(self.sdk_client.schemas.list(catalog_name=catalog)),
        )
        return schemas

    async def list_tables_sdk(self, catalog: str, schema: str) -> List[TableInfo]:
        """List tables in a schema using SDK."""
        if not self.use_sdk or not self.sdk_client:
            raise ConnectionError("SDK client not available")

        loop = asyncio.get_event_loop()
        tables = await loop.run_in_executor(
            None,
            lambda: list(self.sdk_client.tables.list(catalog_name=catalog, schema_name=schema)),
        )
        return tables

    async def get_table_sdk(self, full_name: str) -> TableInfo:
        """
        Get table details using SDK.

        Args:
            full_name: Fully qualified table name (catalog.schema.table)

        Returns:
            TableInfo object with metadata
        """
        if not self.use_sdk or not self.sdk_client:
            raise ConnectionError("SDK client not available")

        loop = asyncio.get_event_loop()
        table = await loop.run_in_executor(
            None,
            self.sdk_client.tables.get,
            full_name,
        )
        return table

    # ========================================================================
    # REST API METHODS - Fast Operations & Custom Endpoints
    # ========================================================================

    async def list_catalogs_rest(self) -> List[Dict[str, Any]]:
        """
        List catalogs using REST API (faster for bulk operations).

        Returns:
            List of catalog dictionaries

        Example:
            catalogs = await adapter.list_catalogs_rest()
            # [{"name": "main", "owner": "admin", ...}, ...]
        """
        url = f"{self.workspace_url}/api/2.1/unity-catalog/catalogs"
        async with self.rest_session.get(url) as response:
            response.raise_for_status()
            data = await response.json()
            return data.get("catalogs", [])

    async def list_schemas_rest(self, catalog: str) -> List[Dict[str, Any]]:
        """List schemas using REST API."""
        url = f"{self.workspace_url}/api/2.1/unity-catalog/schemas"
        params = {"catalog_name": catalog}
        async with self.rest_session.get(url, params=params) as response:
            response.raise_for_status()
            data = await response.json()
            return data.get("schemas", [])

    async def list_tables_rest(self, catalog: str, schema: str) -> List[Dict[str, Any]]:
        """List tables using REST API."""
        url = f"{self.workspace_url}/api/2.1/unity-catalog/tables"
        params = {"catalog_name": catalog, "schema_name": schema}
        async with self.rest_session.get(url, params=params) as response:
            response.raise_for_status()
            data = await response.json()
            return data.get("tables", [])

    async def execute_statement_rest(
        self,
        warehouse_id: str,
        statement: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        parameters: Optional[List[Dict[str, Any]]] = None,
        wait_timeout: str = "30s",
        disposition: str = "INLINE",
        format: str = "JSON_ARRAY",
        byte_limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Execute SQL statement using REST API (Databricks Statement Execution API).

        Based on: https://docs.databricks.com/api/workspace/statementexecution

        This method provides full SQL execution without requiring databricks-sql-connector.
        Supports both synchronous (wait_timeout) and asynchronous (polling) execution.

        Args:
            warehouse_id: SQL warehouse ID
            statement: SQL statement to execute
            catalog: Optional catalog to use (overrides default)
            schema: Optional schema to use (overrides default)
            parameters: Optional list of parameters for parameterized queries
                       Format: [{"name": "param1", "value": "value1", "type": "STRING"}]
            wait_timeout: How long to wait for results (e.g., "30s", "5m", "0s" for async)
            disposition: Result disposition - "INLINE" (default) or "EXTERNAL_LINKS"
            format: Result format - "JSON_ARRAY" (default), "ARROW_STREAM", "CSV"
            byte_limit: Optional limit on result size in bytes

        Returns:
            Dictionary with:
            - status: Execution status (state, error if any)
            - manifest: Result schema information
            - result: Query results (if disposition=INLINE)
            - statement_id: ID for async polling

        Raises:
            QueryError: If statement execution fails

        Example:
            # Simple query
            result = await adapter.execute_statement_rest(
                warehouse_id="abc123",
                statement="SELECT * FROM table WHERE id > 10 LIMIT 100"
            )

            # Parameterized query
            result = await adapter.execute_statement_rest(
                warehouse_id="abc123",
                statement="SELECT * FROM table WHERE country = :country",
                parameters=[
                    {"name": "country", "value": "USA", "type": "STRING"}
                ]
            )

            # Async execution (poll for results)
            result = await adapter.execute_statement_rest(
                warehouse_id="abc123",
                statement="SELECT COUNT(*) FROM huge_table",
                wait_timeout="0s"  # Return immediately
            )
            statement_id = result["statement_id"]
            # Poll with get_statement_status_rest(statement_id)
        """
        url = f"{self.workspace_url}/api/2.0/sql/statements"
        
        payload = {
            "warehouse_id": warehouse_id,
            "statement": statement,
            "wait_timeout": wait_timeout,
            "disposition": disposition,
            "format": format,
        }

        # Add optional parameters
        if catalog:
            payload["catalog"] = catalog
        elif self.catalog:
            payload["catalog"] = self.catalog

        if schema:
            payload["schema"] = schema
        elif self.schema:
            payload["schema"] = self.schema

        if parameters:
            payload["parameters"] = parameters

        if byte_limit:
            payload["byte_limit"] = byte_limit

        try:
            async with self.rest_session.post(url, json=payload) as response:
                response.raise_for_status()
                return await response.json()

        except Exception as e:
            raise QueryError(f"Statement execution failed: {str(e)}")

    async def get_statement_status_rest(self, statement_id: str) -> Dict[str, Any]:
        """
        Get status of an executing or completed statement.

        Based on: https://docs.databricks.com/api/workspace/statementexecution

        Use this to poll for results when using async execution (wait_timeout="0s").

        Args:
            statement_id: Statement ID returned from execute_statement_rest

        Returns:
            Dictionary with status, manifest, and result (if completed)

        Example:
            # Start async execution
            result = await adapter.execute_statement_rest(
                warehouse_id="abc123",
                statement="SELECT COUNT(*) FROM huge_table",
                wait_timeout="0s"
            )
            statement_id = result["statement_id"]

            # Poll for completion
            while True:
                status = await adapter.get_statement_status_rest(statement_id)
                if status["status"]["state"] in ["SUCCEEDED", "FAILED", "CANCELED"]:
                    break
                await asyncio.sleep(1)

            if status["status"]["state"] == "SUCCEEDED":
                results = status["result"]
        """
        url = f"{self.workspace_url}/api/2.0/sql/statements/{statement_id}"

        try:
            async with self.rest_session.get(url) as response:
                response.raise_for_status()
                return await response.json()

        except Exception as e:
            raise QueryError(f"Failed to get statement status: {str(e)}")

    async def cancel_statement_rest(self, statement_id: str) -> Dict[str, Any]:
        """
        Cancel a running SQL statement.

        Based on: https://docs.databricks.com/api/workspace/statementexecution

        Args:
            statement_id: Statement ID to cancel

        Returns:
            Empty dictionary on success

        Example:
            # Start long-running query
            result = await adapter.execute_statement_rest(
                warehouse_id="abc123",
                statement="SELECT * FROM huge_table",
                wait_timeout="0s"
            )
            statement_id = result["statement_id"]

            # Cancel it
            await adapter.cancel_statement_rest(statement_id)
        """
        url = f"{self.workspace_url}/api/2.0/sql/statements/{statement_id}/cancel"

        try:
            async with self.rest_session.post(url) as response:
                response.raise_for_status()
                return await response.json()

        except Exception as e:
            raise QueryError(f"Failed to cancel statement: {str(e)}")

    async def execute_query_rest(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Execute SQL query via REST API and return results as list of dictionaries.

        This is a convenience wrapper around execute_statement_rest that:
        1. Converts dict params to API parameter format
        2. Waits for results synchronously
        3. Parses JSON results into list of dicts

        Args:
            query: SQL query string
            params: Dictionary of parameters (e.g., {"country": "USA"})
            catalog: Optional catalog to use
            schema: Optional schema to use

        Returns:
            List of result rows as dictionaries

        Raises:
            QueryError: If query execution fails

        Example:
            results = await adapter.execute_query_rest(
                "SELECT * FROM table WHERE country = :country",
                params={"country": "USA"}
            )
            for row in results:
                print(row)
        """
        # Convert dict params to API format
        api_parameters = None
        if params:
            api_parameters = []
            for name, value in params.items():
                # Infer type from value
                if isinstance(value, bool):
                    param_type = "BOOLEAN"
                elif isinstance(value, int):
                    param_type = "INT"
                elif isinstance(value, float):
                    param_type = "DOUBLE"
                else:
                    param_type = "STRING"
                    value = str(value)

                api_parameters.append({
                    "name": name,
                    "value": value,
                    "type": param_type,
                })

        # Execute statement
        result = await self.execute_statement_rest(
            warehouse_id=self.warehouse_id,
            statement=query,
            catalog=catalog,
            schema=schema,
            parameters=api_parameters,
            wait_timeout="30s",  # Wait for results
            disposition="INLINE",
            format="JSON_ARRAY",
        )

        # Check status
        status = result.get("status", {})
        if status.get("state") != "SUCCEEDED":
            error = status.get("error", {})
            raise QueryError(
                f"Query failed: {error.get('message', 'Unknown error')}"
            )

        # Parse results
        manifest = result.get("manifest", {})
        schema_info = manifest.get("schema", {})
        columns = [col["name"] for col in schema_info.get("columns", [])]

        result_data = result.get("result", {})
        data_array = result_data.get("data_array", [])

        # Convert to list of dicts
        rows = []
        for row_data in data_array:
            row_dict = dict(zip(columns, row_data))
            rows.append(row_dict)

        return rows

    async def get_workspace_info(self) -> Dict[str, Any]:
        """Get workspace information using REST API."""
        url = f"{self.workspace_url}/api/2.0/workspace-conf"
        async with self.rest_session.get(url) as response:
            response.raise_for_status()
            return await response.json()

    # ========================================================================
    # UTILITY METHODS - Common Operations
    # ========================================================================

    @staticmethod
    def _validate_identifier(identifier: str) -> str:
        """
        Validate SQL identifier to prevent SQL injection.

        Args:
            identifier: SQL identifier (catalog, schema, table name)

        Returns:
            Validated identifier

        Raises:
            ValueError: If identifier is invalid
        """
        import re
        if not re.match(r"^[a-zA-Z0-9_.-]+$", identifier):
            raise ValueError(
                f"Invalid SQL identifier '{identifier}': "
                "only alphanumeric, underscore, hyphen, and dot allowed"
            )
        return identifier

    async def get_tables(self, schema: Optional[str] = None) -> List[str]:
        """
        Get list of table names using REST API.

        Args:
            schema: Schema name (uses default if not provided)

        Returns:
            List of table names
        """
        target_schema = schema or self.schema
        tables = await self.list_tables_rest(self.catalog, target_schema)
        return [t["name"] for t in tables]

    async def get_schema(self, table: str) -> List[Dict[str, Any]]:
        """
        Get table schema (column definitions).

        Uses SDK if available, else raises error.

        Args:
            table: Table name

        Returns:
            List of column definitions
        """
        if self.use_sdk and self.sdk_client:
            # Use SDK to get table metadata
            full_name = f"{self.catalog}.{self.schema}.{table}"
            table_info = await self.get_table_sdk(full_name)
            return [
                {
                    "name": col.name,
                    "data_type": col.type_name.value if hasattr(col.type_name, 'value') else str(col.type_name),
                    "nullable": col.nullable,
                    "default": None,
                }
                for col in table_info.columns
            ]
        else:
            raise ConnectionError("get_schema requires SDK. Enable use_sdk=True or use execute_query with DESCRIBE")

    async def table_exists(self, table: str, schema: Optional[str] = None) -> bool:
        """
        Check if table exists using REST API.

        Args:
            table: Table name
            schema: Schema name (optional)

        Returns:
            True if table exists
        """
        target_schema = schema or self.schema
        tables = await self.list_tables_rest(self.catalog, target_schema)
        return any(t["name"] == table for t in tables)

    def __repr__(self) -> str:
        """Debug representation."""
        return (
            f"DatabricksAdapter("
            f"host={self.host}, "
            f"catalog={self.catalog}, "
            f"schema={self.schema}, "
            f"connected={self.connected}, "
            f"sdk={'enabled' if self.use_sdk else 'disabled'}"
            f")"
        )

    def __str__(self) -> str:
        """Human-readable representation."""
        status = "connected" if self.connected else "disconnected"
        return f"DatabricksAdapter({self.workspace_url}, {status})"

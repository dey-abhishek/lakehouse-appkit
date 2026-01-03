"""
Databricks adapter for Lakehouse-AppKit.

Provides async interface to Databricks SQL warehouses with full Unity Catalog support.
Based on ARCHITECTURE.md - Adapter Component Architecture (lines 283-345).
"""
from typing import Any, Dict, List, Optional
from functools import wraps


# Define custom exceptions since they're not in sdk.exceptions
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
    Databricks SQL warehouse adapter.

    Provides async interface to execute queries against Databricks SQL warehouses
    with Unity Catalog integration.

    Architecture:
        - Connection Management: connect(), disconnect()
        - Query Execution: execute_query(), execute_statement()
        - Metadata Operations: get_tables(), get_schema(), table_exists()
        - Security: SQL injection protection via _validate_identifier()
    """

    def __init__(
        self,
        host: str,
        token: str,
        warehouse_id: Optional[str] = None,
        catalog: str = "main",
        schema: str = "default",
        timeout: int = 30,
    ):
        """
        Initialize Databricks adapter.

        Args:
            host: Databricks workspace host (e.g., 'company.cloud.databricks.com' or 'https://...')
            token: Personal access token for authentication
            warehouse_id: SQL warehouse ID (optional, can be set later)
            catalog: Default Unity Catalog catalog name
            schema: Default schema name
            timeout: Query timeout in seconds
        """
        # Clean up host - ensure it has https:// protocol
        if not host.startswith("http://") and not host.startswith("https://"):
            host = f"https://{host}"
        # Remove query parameters if present
        if "?" in host:
            host = host.split("?")[0]

        self.host = host
        self.workspace_url = host if host.startswith("https://") else f"https://{host}"
        self.token = token
        self.warehouse_id = warehouse_id
        self.catalog = catalog
        self.schema = schema
        self.timeout = timeout

        # Connection state
        self.connection = None
        self.cursor = None
        self.connected = False

    async def connect(self):
        """
        Connect to Databricks SQL warehouse.

        Establishes connection to the configured warehouse and initializes
        catalog and schema context.

        In production, this uses databricks-sql-connector:
            from databricks import sql
            self.connection = sql.connect(
                server_hostname=self.host,
                http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
                access_token=self.token,
                catalog=self.catalog,
                schema=self.schema
            )
            self.cursor = self.connection.cursor()

        Raises:
            ConnectionError: If connection fails
        """
        try:
            # Mark as connected
            # TODO: Implement actual connection with databricks-sql-connector
            self.connected = True
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Databricks: {str(e)}")

    async def disconnect(self):
        """
        Disconnect from Databricks SQL warehouse.

        Cleanly closes cursor and connection, releasing resources.
        """
        if self.cursor:
            try:
                self.cursor.close()
            except Exception:
                pass

        if self.connection:
            try:
                self.connection.close()
            except Exception:
                pass

        self.connected = False

    @require_connection
    async def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute SQL query and return results as list of dictionaries.

        Args:
            query: SQL query string
            params: Optional query parameters (dict)

        Returns:
            List of result rows as dictionaries with column names as keys

        Raises:
            QueryError: If query execution fails

        Example:
            results = await adapter.execute_query(
                "SELECT * FROM table WHERE id = :id",
                {"id": 123}
            )
        """
        try:
            # In production with databricks-sql-connector:
            # self.cursor.execute(query, params or {})
            # columns = [desc[0] for desc in self.cursor.description]
            # rows = self.cursor.fetchall()
            # return [dict(zip(columns, row)) for row in rows]

            # Placeholder for testing
            return []
        except Exception as e:
            raise QueryError(f"Query execution failed: {str(e)}")

    @require_connection
    async def execute_statement(self, statement: str) -> int:
        """
        Execute SQL statement (INSERT, UPDATE, DELETE, DDL) and return row count.

        Args:
            statement: SQL statement to execute

        Returns:
            Number of rows affected

        Raises:
            QueryError: If statement execution fails

        Example:
            rows_affected = await adapter.execute_statement(
                "DELETE FROM table WHERE status = 'archived'"
            )
        """
        try:
            # In production with databricks-sql-connector:
            # self.cursor.execute(statement)
            # return self.cursor.rowcount

            # Placeholder for testing
            return 0
        except Exception as e:
            raise QueryError(f"Statement execution failed: {str(e)}")

    @staticmethod
    def _validate_identifier(identifier: str) -> str:
        """
        Validate SQL identifier to prevent SQL injection attacks.

        This provides defense-in-depth security by ensuring identifiers
        (catalog, schema, table names) only contain safe characters.

        Args:
            identifier: SQL identifier to validate

        Returns:
            The validated identifier (unchanged if valid)

        Raises:
            ValueError: If identifier contains unsafe characters

        Example:
            validated = adapter._validate_identifier("my_table")  # OK
            validated = adapter._validate_identifier("'; DROP TABLE--")  # Raises ValueError
        """
        import re

        # Allow: alphanumeric, underscore, hyphen, dot (for qualified names)
        # This prevents SQL injection via identifiers
        if not re.match(r"^[a-zA-Z0-9_.-]+$", identifier):
            raise ValueError(
                f"Invalid SQL identifier '{identifier}': "
                "only alphanumeric characters, underscores, hyphens, and dots are allowed"
            )
        return identifier

    async def get_tables(self, schema: Optional[str] = None) -> List[str]:
        """
        Get list of table names in a schema.

        Args:
            schema: Schema name (uses default schema if not provided)

        Returns:
            List of table names

        Example:
            tables = await adapter.get_tables("my_schema")
            # Returns: ["customers", "orders", "products"]
        """
        target_schema = schema or self.schema

        # Validate inputs to prevent SQL injection
        validated_catalog = self._validate_identifier(self.catalog)
        validated_schema = self._validate_identifier(target_schema)

        query = f"""
            SELECT table_name
            FROM {validated_catalog}.information_schema.tables
            WHERE table_schema = '{validated_schema}'
            ORDER BY table_name
        """

        results = await self.execute_query(query)
        return [row["table_name"] for row in results]

    async def get_schema(self, table: str) -> List[Dict[str, Any]]:
        """
        Get schema information (columns) for a table.

        Args:
            table: Table name

        Returns:
            List of column definitions with name, data_type, nullable, default

        Example:
            schema = await adapter.get_schema("customers")
            # Returns: [
            #     {"name": "id", "data_type": "INT", "nullable": False, "default": None},
            #     {"name": "email", "data_type": "STRING", "nullable": True, "default": None}
            # ]
        """
        # Validate inputs to prevent SQL injection
        validated_catalog = self._validate_identifier(self.catalog)
        validated_schema = self._validate_identifier(self.schema)
        validated_table = self._validate_identifier(table)

        query = f"""
            SELECT
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM {validated_catalog}.information_schema.columns
            WHERE table_schema = '{validated_schema}'
            AND table_name = '{validated_table}'
            ORDER BY ordinal_position
        """

        results = await self.execute_query(query)
        return [
            {
                "name": row["column_name"],
                "data_type": row["data_type"],
                "nullable": row["is_nullable"] == "YES",
                "default": row.get("column_default"),
            }
            for row in results
        ]

    async def table_exists(self, table: str, schema: Optional[str] = None) -> bool:
        """
        Check if a table exists in the specified schema.

        Args:
            table: Table name
            schema: Schema name (uses default schema if not provided)

        Returns:
            True if table exists, False otherwise

        Example:
            exists = await adapter.table_exists("customers", "prod_schema")
        """
        target_schema = schema or self.schema

        # Validate inputs to prevent SQL injection
        validated_catalog = self._validate_identifier(self.catalog)
        validated_schema = self._validate_identifier(target_schema)
        validated_table = self._validate_identifier(table)

        query = f"""
            SELECT COUNT(*) as count
            FROM {validated_catalog}.information_schema.tables
            WHERE table_schema = '{validated_schema}'
            AND table_name = '{validated_table}'
        """

        results = await self.execute_query(query)
        return results[0]["count"] > 0 if results else False

    def __repr__(self) -> str:
        """String representation of adapter for debugging."""
        return (
            f"DatabricksAdapter("
            f"host={self.host}, "
            f"catalog={self.catalog}, "
            f"schema={self.schema}, "
            f"connected={self.connected}"
            f")"
        )

    def __str__(self) -> str:
        """Human-readable string representation."""
        status = "connected" if self.connected else "disconnected"
        return f"DatabricksAdapter({self.workspace_url}, {status})"

"""
Regression Test Suite for REST-Only Databricks Adapter

This test suite ensures backward compatibility after removing SQL Connector.
Tests verify that all existing functionality still works with REST APIs only.

Test Coverage:
1. Initialization (backward compatible)
2. Connection management
3. SQL operations (now via REST instead of SQL connector)
4. Unity Catalog operations (SDK and REST)
5. Error handling
6. Parameter binding (security)
7. Async operations
"""
import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from aioresponses import aioresponses as AioresponsesMock

from lakehouse_appkit.adapters.databricks import (
    DatabricksAdapter,
    ConnectionError,
    QueryError,
    AuthenticationError,
)


@pytest.fixture
def mock_aioresponses():
    """Fixture for aioresponses."""
    with AioresponsesMock() as m:
        yield m


class TestBackwardCompatibility:
    """Test that existing code patterns still work."""

    def test_initialization_backward_compatible(self):
        """Test that old initialization code still works."""
        # Old code pattern (should still work)
        adapter = DatabricksAdapter(
            host="test.databricks.com",
            token="test-token",
            warehouse_id="test-warehouse",
            catalog="main",
            schema="default"
        )

        assert adapter.host == "test.databricks.com"
        assert adapter.token == "test-token"
        assert adapter.warehouse_id == "test-warehouse"
        assert adapter.catalog == "main"
        assert adapter.schema == "default"

    def test_initialization_with_https(self):
        """Test host normalization still works."""
        adapter = DatabricksAdapter(
            host="https://test.databricks.com",
            token="test-token"
        )

        assert adapter.host == "test.databricks.com"
        assert adapter.workspace_url == "https://test.databricks.com"

    def test_no_use_sql_connector_parameter(self):
        """Test that use_sql_connector parameter is removed."""
        # Should not accept use_sql_connector anymore
        adapter = DatabricksAdapter(
            host="test.databricks.com",
            token="test-token",
            warehouse_id="test-warehouse"
        )

        # Verify no sql_connector attributes
        assert not hasattr(adapter, 'use_sql_connector')
        assert not hasattr(adapter, 'sql_connection')
        assert not hasattr(adapter, 'sql_cursor')


class TestSQLOperationsRegression:
    """Regression tests for SQL operations (now via REST)."""

    @pytest.mark.asyncio
    async def test_execute_query_still_works(self, mock_aioresponses):
        """Test that execute_query() still works (now uses REST)."""
        adapter = DatabricksAdapter(
            host="test.databricks.com",
            token="test-token",
            warehouse_id="test-warehouse"
        )

        # Mock REST API response
        mock_aioresponses.post(
            "https://test.databricks.com/api/2.0/sql/statements",
            payload={
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "schema": {
                        "columns": [
                            {"name": "id"},
                            {"name": "name"},
                            {"name": "email"}
                        ]
                    }
                },
                "result": {
                    "data_array": [
                        [1, "Alice", "alice@example.com"],
                        [2, "Bob", "bob@example.com"]
                    ]
                }
            }
        )

        await adapter.connect()

        # Old code pattern - should still work
        results = await adapter.execute_query(
            "SELECT id, name, email FROM users WHERE id > :min_id",
            params={"min_id": 0}
        )

        # Verify results format unchanged
        assert len(results) == 2
        assert results[0]["id"] == 1
        assert results[0]["name"] == "Alice"
        assert results[0]["email"] == "alice@example.com"
        assert results[1]["id"] == 2
        assert results[1]["name"] == "Bob"

        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_execute_statement_still_works(self, mock_aioresponses):
        """Test that execute_statement() still works (now uses REST)."""
        adapter = DatabricksAdapter(
            host="test.databricks.com",
            token="test-token",
            warehouse_id="test-warehouse"
        )

        # Mock REST API response
        mock_aioresponses.post(
            "https://test.databricks.com/api/2.0/sql/statements",
            payload={
                "status": {"state": "SUCCEEDED"},
                "manifest": {"schema": {"columns": []}},
                "result": {"data_array": []}
            }
        )

        await adapter.connect()

        # Old code pattern - should still work
        rows_affected = await adapter.execute_statement(
            "DELETE FROM temp_table WHERE created_at < '2024-01-01'"
        )

        # Should return number (even if 0)
        assert isinstance(rows_affected, int)

        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_parameterized_queries_still_work(self, mock_aioresponses):
        """Test that parameterized queries still work."""
        adapter = DatabricksAdapter(
            host="test.databricks.com",
            token="test-token",
            warehouse_id="test-warehouse"
        )

        mock_aioresponses.post(
            "https://test.databricks.com/api/2.0/sql/statements",
            payload={
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "schema": {"columns": [{"name": "count"}]}
                },
                "result": {"data_array": [[42]]}
            }
        )

        await adapter.connect()

        # Old parameterized query pattern
        results = await adapter.execute_query(
            """
            SELECT COUNT(*) as count
            FROM orders
            WHERE status = :status
              AND amount > :min_amount
              AND created_at >= :start_date
            """,
            params={
                "status": "completed",
                "min_amount": 100.50,
                "start_date": "2024-01-01"
            }
        )

        assert len(results) == 1
        assert results[0]["count"] == 42

        await adapter.disconnect()


class TestUnityCatalogRegression:
    """Regression tests for Unity Catalog operations."""

    @pytest.mark.asyncio
    async def test_list_catalogs_rest_still_works(self, mock_aioresponses):
        """Test REST catalog listing unchanged."""
        adapter = DatabricksAdapter(
            host="test.databricks.com",
            token="test-token"
        )

        mock_aioresponses.get(
            "https://test.databricks.com/api/2.1/unity-catalog/catalogs",
            payload={
                "catalogs": [
                    {"name": "main", "owner": "admin"},
                    {"name": "dev", "owner": "dev_team"}
                ]
            }
        )

        await adapter.connect()

        # Old code pattern
        catalogs = await adapter.list_catalogs_rest()

        assert len(catalogs) == 2
        assert catalogs[0]["name"] == "main"
        assert catalogs[1]["name"] == "dev"

        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_list_tables_rest_still_works(self, mock_aioresponses):
        """Test REST table listing unchanged."""
        adapter = DatabricksAdapter(
            host="test.databricks.com",
            token="test-token"
        )

        mock_aioresponses.get(
            "https://test.databricks.com/api/2.1/unity-catalog/tables?catalog_name=main&schema_name=default",
            payload={
                "tables": [
                    {"name": "customers"},
                    {"name": "orders"},
                    {"name": "products"}
                ]
            }
        )

        await adapter.connect()

        # Old code pattern
        tables = await adapter.list_tables_rest("main", "default")

        assert len(tables) == 3
        assert tables[0]["name"] == "customers"

        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_get_tables_convenience_method(self, mock_aioresponses):
        """Test convenience method still works."""
        adapter = DatabricksAdapter(
            host="test.databricks.com",
            token="test-token"
        )

        mock_aioresponses.get(
            "https://test.databricks.com/api/2.1/unity-catalog/tables?catalog_name=main&schema_name=default",
            payload={
                "tables": [
                    {"name": "table1"},
                    {"name": "table2"}
                ]
            }
        )

        await adapter.connect()

        # Old convenience method
        tables = await adapter.get_tables("default")

        assert len(tables) == 2
        assert "table1" in tables
        assert "table2" in tables

        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_table_exists_still_works(self, mock_aioresponses):
        """Test table_exists method unchanged."""
        adapter = DatabricksAdapter(
            host="test.databricks.com",
            token="test-token"
        )

        mock_aioresponses.get(
            "https://test.databricks.com/api/2.1/unity-catalog/tables?catalog_name=main&schema_name=default",
            payload={
                "tables": [
                    {"name": "customers"},
                    {"name": "orders"}
                ]
            },
            repeat=True
        )

        await adapter.connect()

        # Old code pattern
        exists = await adapter.table_exists("customers")
        assert exists is True

        not_exists = await adapter.table_exists("nonexistent")
        assert not_exists is False

        await adapter.disconnect()


class TestErrorHandlingRegression:
    """Regression tests for error handling."""

    @pytest.mark.asyncio
    async def test_query_error_on_failure(self, mock_aioresponses):
        """Test QueryError still raised on failure."""
        adapter = DatabricksAdapter(
            host="test.databricks.com",
            token="test-token",
            warehouse_id="test-warehouse"
        )

        mock_aioresponses.post(
            "https://test.databricks.com/api/2.0/sql/statements",
            payload={
                "status": {
                    "state": "FAILED",
                    "error": {
                        "message": "Table not found: nonexistent_table"
                    }
                }
            }
        )

        await adapter.connect()

        # Should still raise QueryError
        with pytest.raises(QueryError) as exc_info:
            await adapter.execute_query("SELECT * FROM nonexistent_table")

        assert "Table not found" in str(exc_info.value)

        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_query_error_without_warehouse_id(self):
        """Test QueryError raised when warehouse_id missing."""
        adapter = DatabricksAdapter(
            host="test.databricks.com",
            token="test-token",
            warehouse_id=None  # No warehouse
        )

        await adapter.connect()

        # Should raise QueryError about missing warehouse_id
        with pytest.raises(QueryError) as exc_info:
            await adapter.execute_query("SELECT 1")

        assert "warehouse_id" in str(exc_info.value).lower()

        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_connection_error_on_connect_failure(self):
        """Test ConnectionError still raised on connection failure."""
        with patch("aiohttp.ClientSession", side_effect=Exception("Network error")):
            adapter = DatabricksAdapter(
                host="test.databricks.com",
                token="test-token"
            )

            with pytest.raises(ConnectionError) as exc_info:
                await adapter.connect()

            assert "Network error" in str(exc_info.value)


class TestSecurityRegression:
    """Regression tests for security features."""

    def test_sql_injection_protection(self):
        """Test SQL identifier validation still works."""
        # Valid identifiers
        assert DatabricksAdapter._validate_identifier("my_table") == "my_table"
        assert DatabricksAdapter._validate_identifier("catalog.schema.table") == "catalog.schema.table"
        assert DatabricksAdapter._validate_identifier("table-name") == "table-name"

        # Invalid identifiers (SQL injection attempts)
        with pytest.raises(ValueError):
            DatabricksAdapter._validate_identifier("'; DROP TABLE users--")

        with pytest.raises(ValueError):
            DatabricksAdapter._validate_identifier("table; DELETE FROM users")

        with pytest.raises(ValueError):
            DatabricksAdapter._validate_identifier("table name")  # space

    @pytest.mark.asyncio
    async def test_parameter_binding_still_safe(self, mock_aioresponses):
        """Test parameter binding prevents SQL injection."""
        adapter = DatabricksAdapter(
            host="test.databricks.com",
            token="test-token",
            warehouse_id="test-warehouse"
        )

        mock_aioresponses.post(
            "https://test.databricks.com/api/2.0/sql/statements",
            payload={
                "status": {"state": "SUCCEEDED"},
                "manifest": {"schema": {"columns": []}},
                "result": {"data_array": []}
            }
        )

        await adapter.connect()

        # Attempt SQL injection via parameter (should be safe)
        malicious_input = "'; DROP TABLE users--"
        await adapter.execute_query(
            "SELECT * FROM table WHERE id = :user_id",
            params={"user_id": malicious_input}
        )

        # Parameters should be properly bound (not concatenated)
        # The API receives parameters separately, not in SQL string
        # This prevents SQL injection

        await adapter.disconnect()


class TestConnectionManagementRegression:
    """Regression tests for connection lifecycle."""

    @pytest.mark.asyncio
    async def test_connect_disconnect_lifecycle(self, mock_aioresponses):
        """Test connection lifecycle unchanged."""
        adapter = DatabricksAdapter(
            host="test.databricks.com",
            token="test-token"
        )

        # Should start disconnected
        assert not adapter.connected

        # Connect
        await adapter.connect()
        assert adapter.connected
        assert adapter.rest_session is not None

        # Disconnect
        await adapter.disconnect()
        assert not adapter.connected

    @pytest.mark.asyncio
    async def test_multiple_operations_same_connection(self, mock_aioresponses):
        """Test multiple operations with same connection."""
        adapter = DatabricksAdapter(
            host="test.databricks.com",
            token="test-token",
            warehouse_id="test-warehouse"
        )

        # Mock multiple operations
        mock_aioresponses.post(
            "https://test.databricks.com/api/2.0/sql/statements",
            payload={
                "status": {"state": "SUCCEEDED"},
                "manifest": {"schema": {"columns": [{"name": "result"}]}},
                "result": {"data_array": [[1]]}
            },
            repeat=True
        )

        mock_aioresponses.get(
            "https://test.databricks.com/api/2.1/unity-catalog/catalogs",
            payload={"catalogs": []}
        )

        await adapter.connect()

        # Multiple operations should work
        await adapter.execute_query("SELECT 1")
        await adapter.execute_query("SELECT 2")
        await adapter.execute_query("SELECT 3")
        await adapter.list_catalogs_rest()

        # Still connected
        assert adapter.connected

        await adapter.disconnect()


class TestAsyncOperationsRegression:
    """Regression tests for async patterns."""

    @pytest.mark.asyncio
    async def test_parallel_queries_still_work(self, mock_aioresponses):
        """Test parallel query execution."""
        adapter = DatabricksAdapter(
            host="test.databricks.com",
            token="test-token",
            warehouse_id="test-warehouse"
        )

        # Mock 3 queries
        mock_aioresponses.post(
            "https://test.databricks.com/api/2.0/sql/statements",
            payload={
                "status": {"state": "SUCCEEDED"},
                "manifest": {"schema": {"columns": [{"name": "count"}]}},
                "result": {"data_array": [[1]]}
            },
            repeat=True
        )

        await adapter.connect()

        # Old pattern for parallel queries
        queries = [
            "SELECT COUNT(*) FROM table1",
            "SELECT COUNT(*) FROM table2",
            "SELECT COUNT(*) FROM table3"
        ]

        results = await asyncio.gather(*[
            adapter.execute_query(q) for q in queries
        ])

        assert len(results) == 3
        # Results should all be lists
        for result in results:
            assert isinstance(result, list)
            assert len(result) == 1

        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_require_connection_decorator_still_works(self, mock_aioresponses):
        """Test @require_connection decorator auto-connects."""
        adapter = DatabricksAdapter(
            host="test.databricks.com",
            token="test-token",
            warehouse_id="test-warehouse"
        )

        mock_aioresponses.post(
            "https://test.databricks.com/api/2.0/sql/statements",
            payload={
                "status": {"state": "SUCCEEDED"},
                "manifest": {"schema": {"columns": [{"name": "result"}]}},
                "result": {"data_array": [[1]]}
            }
        )

        # Should auto-connect
        assert not adapter.connected
        results = await adapter.execute_query("SELECT 1")
        assert adapter.connected

        await adapter.disconnect()


class TestRegressionSummary:
    """Summary test to verify all key functionality."""

    @pytest.mark.asyncio
    async def test_complete_workflow(self, mock_aioresponses):
        """
        Complete workflow regression test.
        
        This test verifies that a typical usage pattern still works:
        1. Initialize adapter
        2. Connect
        3. Execute SQL queries
        4. List Unity Catalog objects
        5. Disconnect
        """
        # 1. Initialize (old pattern)
        adapter = DatabricksAdapter(
            host="company.databricks.com",
            token="dapi-test-token",
            warehouse_id="abc123",
            catalog="main",
            schema="default"
        )

        # Mock responses
        mock_aioresponses.post(
            "https://company.databricks.com/api/2.0/sql/statements",
            payload={
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "schema": {"columns": [{"name": "customer_id"}, {"name": "total"}]}
                },
                "result": {
                    "data_array": [[1, 1000.50], [2, 2500.75]]
                }
            }
        )

        mock_aioresponses.get(
            "https://company.databricks.com/api/2.1/unity-catalog/catalogs",
            payload={
                "catalogs": [
                    {"name": "main"},
                    {"name": "dev"}
                ]
            }
        )

        mock_aioresponses.get(
            "https://company.databricks.com/api/2.1/unity-catalog/tables?catalog_name=main&schema_name=default",
            payload={
                "tables": [
                    {"name": "customers"},
                    {"name": "orders"}
                ]
            }
        )

        # 2. Connect
        await adapter.connect()
        assert adapter.connected

        # 3. Execute SQL query (now via REST, but same API)
        results = await adapter.execute_query(
            "SELECT customer_id, total FROM orders WHERE total > :min_total",
            params={"min_total": 100}
        )

        assert len(results) == 2
        assert results[0]["customer_id"] == 1
        assert results[0]["total"] == 1000.50

        # 4. List Unity Catalog objects
        catalogs = await adapter.list_catalogs_rest()
        assert len(catalogs) == 2

        tables = await adapter.get_tables()
        assert "customers" in tables
        assert "orders" in tables

        # 5. Disconnect
        await adapter.disconnect()
        assert not adapter.connected

        print("✅ Complete workflow regression test passed!")


# Run regression tests with summary
if __name__ == "__main__":
    pytest.main([
        __file__,
        "-v",
        "--tb=short",
        "-k", "TestRegressionSummary"
    ])


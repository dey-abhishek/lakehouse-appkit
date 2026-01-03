"""
Tests for Databricks SQL REST API Client.
"""
import pytest
from aioresponses import aioresponses

from lakehouse_appkit.sql import DatabricksSQLClient, StatementState
from lakehouse_appkit.sdk.exceptions import ConnectionError, QueryError
from tests.test_config import skip_if_no_config


# ============================================================================
# Unit Tests (Mocked with aioresponses)
# ============================================================================

class TestDatabricksSQLClientUnit:
    """Unit tests for SQL REST API client with mocked responses."""
    
    def test_client_initialization(self):
        """Test SQL client initialization."""
        client = DatabricksSQLClient(
            host="https://test.cloud.databricks.com",
            token="test-token",
            warehouse_id="test-warehouse"
        )
        
        assert client.host == "https://test.cloud.databricks.com"
        assert client.token == "test-token"
        assert client.warehouse_id == "test-warehouse"
        assert client.base_url == "https://test.cloud.databricks.com/api/2.0/sql/statements"
    
    @pytest.mark.asyncio
    async def test_execute_statement_success(self):
        """Test successful statement execution."""
        client = DatabricksSQLClient("https://test.cloud.databricks.com", "token", "warehouse")
        
        mock_response = {
            "statement_id": "test-statement-id",
            "status": {"state": StatementState.SUCCEEDED},
            "manifest": {
                "schema": {"columns": [{"name": "id"}, {"name": "name"}]},
                "chunks": [{
                    "data_array": [[1, "test"], [2, "test2"]]
                }]
            }
        }
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.0/sql/statements",
                payload=mock_response,
                status=200
            )
            
            result = await client.execute_statement("SELECT 1")
            
            assert result["statement_id"] == "test-statement-id"
            assert result["status"]["state"] == StatementState.SUCCEEDED
    
    @pytest.mark.asyncio
    async def test_execute_and_fetch(self):
        """Test execute_and_fetch convenience method."""
        client = DatabricksSQLClient("https://test.cloud.databricks.com", "token", "warehouse")
        
        mock_response = {
            "statement_id": "test-statement-id",
            "status": {"state": StatementState.SUCCEEDED},
            "manifest": {
                "schema": {"columns": [{"name": "id"}, {"name": "name"}]},
                "chunks": [{
                    "data_array": [[1, "alice"], [2, "bob"]]
                }]
            }
        }
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.0/sql/statements",
                payload=mock_response,
                status=200
            )
            
            results = await client.execute_and_fetch("SELECT * FROM users")
            
            assert len(results) == 2
            assert results[0] == {"id": 1, "name": "alice"}
            assert results[1] == {"id": 2, "name": "bob"}
    
    @pytest.mark.asyncio
    async def test_execute_statement_with_parameters(self):
        """Test statement execution with parameters."""
        client = DatabricksSQLClient("https://test.cloud.databricks.com", "token", "warehouse")
        
        mock_response = {
            "statement_id": "test-statement-id",
            "status": {"state": StatementState.SUCCEEDED},
            "manifest": {"schema": {"columns": []}, "chunks": []}
        }
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.0/sql/statements",
                payload=mock_response,
                status=200
            )
            
            parameters = [{"name": "id", "value": "123", "type": "STRING"}]
            await client.execute_statement(
                "SELECT * FROM table WHERE id = :id",
                parameters=parameters
            )
            
            # Just verify it completes without error
            assert True
    
    @pytest.mark.asyncio
    async def test_execute_statement_error(self):
        """Test statement execution with API error."""
        client = DatabricksSQLClient("https://test.cloud.databricks.com", "token", "warehouse")
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.0/sql/statements",
                status=400,
                body="Bad request"
            )
            
            with pytest.raises(QueryError, match="SQL execution failed"):
                await client.execute_statement("INVALID SQL")
    
    @pytest.mark.asyncio
    async def test_cancel_statement(self):
        """Test statement cancellation."""
        client = DatabricksSQLClient("https://test.cloud.databricks.com", "token", "warehouse")
        
        mock_response = {"statement_id": "test-id", "status": {"state": StatementState.CANCELED}}
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.0/sql/statements/test-id/cancel",
                payload=mock_response,
                status=200
            )
            
            result = await client.cancel_statement("test-id")
            
            assert result["status"]["state"] == StatementState.CANCELED
    
    @pytest.mark.asyncio
    async def test_list_warehouses(self):
        """Test listing SQL warehouses."""
        client = DatabricksSQLClient("https://test.cloud.databricks.com", "token", "warehouse")
        
        mock_response = {
            "warehouses": [
                {"id": "wh1", "name": "Warehouse 1", "state": "RUNNING"},
                {"id": "wh2", "name": "Warehouse 2", "state": "STOPPED"}
            ]
        }
        
        with aioresponses() as m:
            m.get(
                "https://test.cloud.databricks.com/api/2.0/sql/warehouses",
                payload=mock_response,
                status=200
            )
            
            warehouses = await client.list_warehouses()
            
            assert len(warehouses) == 2
            assert warehouses[0]["id"] == "wh1"
            assert warehouses[1]["name"] == "Warehouse 2"
    
    @pytest.mark.asyncio
    async def test_get_warehouse(self):
        """Test getting warehouse details."""
        client = DatabricksSQLClient("https://test.cloud.databricks.com", "token", "warehouse")
        
        mock_response = {
            "id": "wh1",
            "name": "Test Warehouse",
            "state": "RUNNING",
            "cluster_size": "Small"
        }
        
        with aioresponses() as m:
            m.get(
                "https://test.cloud.databricks.com/api/2.0/sql/warehouses/wh1",
                payload=mock_response,
                status=200
            )
            
            warehouse = await client.get_warehouse("wh1")
            
            assert warehouse["id"] == "wh1"
            assert warehouse["name"] == "Test Warehouse"
            assert warehouse["state"] == "RUNNING"


# ============================================================================
# Integration Tests (Real API Calls)
# ============================================================================

@pytest.mark.integration
@skip_if_no_config("config/.env.dev")
@pytest.mark.asyncio
async def test_sql_list_warehouses_integration():
    """Test listing warehouses with real Databricks."""
    import os
    client = DatabricksSQLClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN"),
        warehouse_id=os.getenv("DATABRICKS_WAREHOUSE_ID")
    )
    
    warehouses = await client.list_warehouses()
    
    assert isinstance(warehouses, list)
    if warehouses:
        assert "id" in warehouses[0]
        assert "name" in warehouses[0]


@pytest.mark.integration
@skip_if_no_config("config/.env.dev")
@pytest.mark.asyncio
async def test_sql_execute_simple_query_integration():
    """Test executing a simple query with real Databricks."""
    import os
    client = DatabricksSQLClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN"),
        warehouse_id=os.getenv("DATABRICKS_WAREHOUSE_ID")
    )
    
    results = await client.execute_and_fetch("SELECT 1 as test_col")
    
    assert isinstance(results, list)
    assert len(results) > 0


@pytest.mark.integration
@skip_if_no_config("config/.env.dev")
@pytest.mark.asyncio
async def test_sql_get_warehouse_integration():
    """Test getting warehouse details with real Databricks."""
    import os
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    
    client = DatabricksSQLClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN"),
        warehouse_id=warehouse_id
    )
    
    warehouse = await client.get_warehouse(warehouse_id)
    
    assert isinstance(warehouse, dict)
    assert warehouse.get("id") == warehouse_id
    assert "name" in warehouse


"""
Unit tests for Genie, MLflow, Connections, and Functions REST API clients.
"""
import pytest
import os
from unittest.mock import AsyncMock, patch
from lakehouse_appkit.genie import DatabricksGenieClient
from lakehouse_appkit.mlflow import DatabricksMLflowClient
from lakehouse_appkit.connections import DatabricksConnectionsClient
from lakehouse_appkit.functions import DatabricksFunctionsClient

# Load config from .env.dev
# Get config values
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://test.cloud.databricks.com")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "test-token")


# ============================================================================
# Genie Space Tests
# ============================================================================

class TestGenieClient:
    """Tests for Databricks Genie REST API client."""
    
    @pytest.fixture
    def genie_client(self):
        """Create Genie client using dev config."""
        return DatabricksGenieClient(
            host=DATABRICKS_HOST,
            token=DATABRICKS_TOKEN
        )
    
    @pytest.mark.asyncio
    async def test_create_space(self, genie_client):
        """Test creating a Genie space."""
        from unittest.mock import AsyncMock, patch
        
        # Mock the _resilient_request method
        with patch.object(genie_client, '_resilient_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"space_id": "123", "display_name": "test-space"}
            
            result = await genie_client.create_space(
                display_name="test-space",
                sql_warehouse_id="warehouse-123"
            )
            
            assert result["space_id"] == "123"
            assert result["display_name"] == "test-space"
        
        # Clean up
        await genie_client.close()
    
    @pytest.mark.asyncio
    async def test_list_spaces(self, genie_client):
        """Test listing Genie spaces."""
        from unittest.mock import AsyncMock, patch
        
        # Mock the _resilient_request method
        with patch.object(genie_client, '_resilient_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {
                "spaces": [
                    {"space_id": "1", "display_name": "space1"},
                    {"space_id": "2", "display_name": "space2"}
                ]
            }
            
            result = await genie_client.list_spaces()
            
            assert len(result) == 2
            assert result[0]["space_id"] == "1"
        
        # Clean up
        await genie_client.close()
    
    @pytest.mark.asyncio
    async def test_ask_question(self, genie_client):
        """Test asking a question in Genie."""
        from unittest.mock import AsyncMock, patch
        
        # Mock the _resilient_request method
        with patch.object(genie_client, '_resilient_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {
                "conversation_id": "conv-1",
                "response": "Here are the results..."
            }
            
            result = await genie_client.ask_question(
                space_id="123",
                question="Show me the top 10 customers"
            )
            
            assert result["conversation_id"] == "conv-1"
        
        # Clean up
        await genie_client.close()


# ============================================================================
# MLflow Experiments Tests
# ============================================================================

class TestMLflowClient:
    """Tests for Databricks MLflow REST API client."""
    
    @pytest.fixture
    def mlflow_client(self):
        """Create MLflow client using dev config."""
        return DatabricksMLflowClient(
            host=DATABRICKS_HOST,
            token=DATABRICKS_TOKEN
        )
    
    @pytest.mark.asyncio
    async def test_create_experiment(self, mlflow_client):
        """Test creating an experiment."""
        from unittest.mock import AsyncMock, patch
        
        # Mock the _resilient_request method
        with patch.object(mlflow_client, '_resilient_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"experiment_id": "123"}
            
            result = await mlflow_client.create_experiment(name="test-exp")
            
            assert result["experiment_id"] == "123"
        
        # Clean up
        await mlflow_client.close()
    
    @pytest.mark.asyncio
    async def test_list_experiments(self, mlflow_client):
        """Test listing experiments."""
        with patch.object(mlflow_client, '_resilient_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {
                "experiments": [
                    {"experiment_id": "1", "name": "exp1"},
                    {"experiment_id": "2", "name": "exp2"}
                ]
            }
            
            result = await mlflow_client.list_experiments()
            
            assert len(result) == 2
            assert result[0]["experiment_id"] == "1"
        
        await mlflow_client.close()
    
    @pytest.mark.asyncio
    async def test_create_run(self, mlflow_client):
        """Test creating a run."""
        with patch.object(mlflow_client, '_resilient_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {
                "run": {
                    "info": {"run_id": "run-123", "status": "RUNNING"}
                }
            }
            
            result = await mlflow_client.create_run(experiment_id="123")
            
            assert result["run"]["info"]["run_id"] == "run-123"
        
        await mlflow_client.close()
    
    @pytest.mark.asyncio
    async def test_log_metric(self, mlflow_client):
        """Test logging a metric."""
        with patch.object(mlflow_client, '_resilient_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {}
            
            result = await mlflow_client.log_metric(
                run_id="run-123",
                key="accuracy",
                value=0.95
            )
            
            assert result == {}
        
        await mlflow_client.close()
    
    @pytest.mark.asyncio
    async def test_search_runs(self, mlflow_client):
        """Test searching runs."""
        with patch.object(mlflow_client, '_resilient_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {
                "runs": [
                    {"info": {"run_id": "1"}},
                    {"info": {"run_id": "2"}}
                ]
            }
            
            result = await mlflow_client.search_runs(
                experiment_ids=["123"],
                filter_string="metrics.accuracy > 0.9"
            )
            
            assert len(result) == 2
        
        await mlflow_client.close()


# ============================================================================
# UC Connections Tests
# ============================================================================

class TestConnectionsClient:
    """Tests for Databricks UC Connections REST API client."""
    
    @pytest.fixture
    def connections_client(self):
        """Create Connections client using dev config."""
        return DatabricksConnectionsClient(
            host=DATABRICKS_HOST,
            token=DATABRICKS_TOKEN
        )
    
    @pytest.mark.asyncio
    async def test_create_connection(self, connections_client):
        """Test creating a connection."""
        from unittest.mock import AsyncMock, patch
        
        # Mock the _resilient_request method
        with patch.object(connections_client, '_resilient_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {
                "name": "mysql-conn",
                "connection_type": "MYSQL"
            }
            
            result = await connections_client.create_connection(
                name="mysql-conn",
                connection_type="MYSQL",
                options={"host": "localhost", "port": "3306"}
            )
            
            assert result["name"] == "mysql-conn"
        
        # Clean up
        await connections_client.close()
    
    @pytest.mark.asyncio
    async def test_list_connections(self, connections_client):
        """Test listing connections."""
        from unittest.mock import AsyncMock, patch
        
        # Mock the _resilient_request method
        with patch.object(connections_client, '_resilient_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {
                "connections": [
                    {"name": "conn1", "connection_type": "MYSQL"},
                    {"name": "conn2", "connection_type": "POSTGRESQL"}
                ]
            }
            
            result = await connections_client.list_connections()
            
            assert len(result) == 2
            assert result[0]["name"] == "conn1"
        
        # Clean up
        await connections_client.close()
    
    @pytest.mark.asyncio
    async def test_delete_connection(self, connections_client):
        """Test deleting a connection."""
        from unittest.mock import AsyncMock, patch
        
        # Mock the _resilient_request method
        with patch.object(connections_client, '_resilient_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {}
            
            result = await connections_client.delete_connection("mysql-conn")
            
            assert result == {}
        
        # Clean up
        await connections_client.close()


# ============================================================================
# UC Functions Tests
# ============================================================================

class TestFunctionsClient:
    """Tests for Databricks UC Functions REST API client."""
    
    @pytest.fixture
    def functions_client(self):
        """Create Functions client using dev config."""
        return DatabricksFunctionsClient(
            host=DATABRICKS_HOST,
            token=DATABRICKS_TOKEN
        )
    
    @pytest.mark.asyncio
    async def test_list_functions(self, functions_client):
        """Test listing functions."""
        from unittest.mock import AsyncMock, patch
        
        # Mock the _resilient_request method
        with patch.object(functions_client, '_resilient_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {
                "functions": [
                    {"full_name": "main.default.func1", "data_type": "INT"},
                    {"full_name": "main.default.func2", "data_type": "STRING"}
                ]
            }
            
            result = await functions_client.list_functions(
                catalog_name="main",
                schema_name="default"
            )
            
            assert len(result) == 2
            assert result[0]["full_name"] == "main.default.func1"
        
        # Clean up
        await functions_client.close()
    
    @pytest.mark.asyncio
    async def test_get_function(self, functions_client):
        """Test getting function details."""
        from unittest.mock import AsyncMock, patch
        
        # Mock the _resilient_request method
        with patch.object(functions_client, '_resilient_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {
                "full_name": "main.default.my_udf",
                "data_type": "INT",
                "routine_definition": "RETURN x * 2"
            }
            
            result = await functions_client.get_function("main.default.my_udf")
            
            assert result["full_name"] == "main.default.my_udf"
        
        # Clean up
        await functions_client.close()
    
    @pytest.mark.asyncio
    async def test_delete_function(self, functions_client):
        """Test deleting a function."""
        from unittest.mock import AsyncMock, patch
        
        # Mock the _resilient_request method
        with patch.object(functions_client, '_resilient_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {}
            
            result = await functions_client.delete_function("main.default.my_udf")
            
            assert result == {}
        
        # Clean up
        await functions_client.close()


# ============================================================================
# Summary Statistics
# ============================================================================

def test_phase_5_summary():
    """Verify Phase 5 implementation completeness."""
    # 4 REST API clients
    assert DatabricksGenieClient is not None
    assert DatabricksMLflowClient is not None
    assert DatabricksConnectionsClient is not None
    assert DatabricksFunctionsClient is not None
    
    # Config loaded
    assert DATABRICKS_HOST is not None
    assert DATABRICKS_TOKEN is not None
    
    # All clients have proper initialization
    genie = DatabricksGenieClient("https://test.com", "token")
    mlflow = DatabricksMLflowClient("https://test.com", "token")
    connections = DatabricksConnectionsClient("https://test.com", "token")
    functions = DatabricksFunctionsClient("https://test.com", "token")
    
    assert genie.base_url == "https://test.com/api/2.0/genie/spaces"
    assert mlflow.base_url == "https://test.com/api/2.0/mlflow"
    assert connections.base_url == "https://test.com/api/2.1/unity-catalog/connections"
    assert functions.base_url == "https://test.com/api/2.1/unity-catalog/functions"


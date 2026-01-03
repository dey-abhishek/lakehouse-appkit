"""
Unit and integration tests for Databricks Vector Search.
"""
import pytest
from aioresponses import aioresponses

from lakehouse_appkit.vector_search import DatabricksVectorSearchClient
from tests.test_config import skip_if_no_config


# ============================================================================
# Unit Tests
# ============================================================================

class TestVectorSearchClientUnit:
    """Unit tests for DatabricksVectorSearchClient."""
    
    @pytest.mark.asyncio
    async def test_create_endpoint(self):
        """Test creating vector search endpoint."""
        client = DatabricksVectorSearchClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.0/vector-search/endpoints",
                payload={"name": "test-endpoint", "status": "ONLINE"}
            )
            
            result = await client.create_endpoint(name="test-endpoint")
            
            assert result["name"] == "test-endpoint"
            assert result["status"] == "ONLINE"
    
    @pytest.mark.asyncio
    async def test_list_endpoints(self):
        """Test listing endpoints."""
        client = DatabricksVectorSearchClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.get(
                "https://test.cloud.databricks.com/api/2.0/vector-search/endpoints",
                payload={
                    "endpoints": [
                        {"name": "endpoint1", "status": "ONLINE"},
                        {"name": "endpoint2", "status": "ONLINE"}
                    ]
                }
            )
            
            endpoints = await client.list_endpoints()
            
            assert len(endpoints) == 2
            assert endpoints[0]["name"] == "endpoint1"
    
    @pytest.mark.asyncio
    async def test_create_index(self):
        """Test creating vector index."""
        client = DatabricksVectorSearchClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.0/vector-search/indexes",
                payload={
                    "name": "main.default.test_idx",
                    "status": "PROVISIONING"
                }
            )
            
            result = await client.create_index(
                name="main.default.test_idx",
                endpoint_name="test-endpoint",
                primary_key="id",
                index_type="DELTA_SYNC",
                delta_sync_index_spec={
                    "source_table": "main.default.source",
                    "embedding_source_columns": [{
                        "name": "text",
                        "embedding_model_endpoint_name": "embeddings"
                    }]
                }
            )
            
            assert result["name"] == "main.default.test_idx"
            assert result["status"] == "PROVISIONING"
    
    @pytest.mark.asyncio
    async def test_list_indexes(self):
        """Test listing indexes."""
        client = DatabricksVectorSearchClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        from unittest.mock import patch, AsyncMock
        with patch.object(client, '_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {
                "indexes": [
                    {"name": "idx1", "status": "ONLINE"},
                    {"name": "idx2", "status": "ONLINE"}
                ]
            }
            
            indexes = await client.list_indexes()
            
            # list_indexes returns just the list (extracts from response)
            assert len(indexes) == 2
            assert indexes[0]["name"] == "idx1"
        
        await client.close()
    
    @pytest.mark.asyncio
    async def test_get_index(self):
        """Test getting index details."""
        client = DatabricksVectorSearchClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.get(
                "https://test.cloud.databricks.com/api/2.0/vector-search/indexes/main.default.test_idx",
                payload={
                    "name": "main.default.test_idx",
                    "status": "ONLINE",
                    "index_type": "DELTA_SYNC"
                }
            )
            
            index = await client.get_index("main.default.test_idx")
            
            assert index["name"] == "main.default.test_idx"
            assert index["status"] == "ONLINE"
    
    @pytest.mark.asyncio
    async def test_delete_index(self):
        """Test deleting index."""
        client = DatabricksVectorSearchClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.delete(
                "https://test.cloud.databricks.com/api/2.0/vector-search/indexes/main.default.test_idx",
                payload={}
            )
            
            result = await client.delete_index("main.default.test_idx")
            
            assert result == {}
    
    @pytest.mark.asyncio
    async def test_sync_index(self):
        """Test syncing index."""
        client = DatabricksVectorSearchClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.0/vector-search/indexes/main.default.test_idx/sync",
                payload={"status": "SYNCING"}
            )
            
            result = await client.sync_index("main.default.test_idx")
            
            assert result["status"] == "SYNCING"
    
    @pytest.mark.asyncio
    async def test_query_index(self):
        """Test querying index."""
        client = DatabricksVectorSearchClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.0/vector-search/indexes/main.default.test_idx/query",
                payload={
                    "manifest": {
                        "columns": ["id", "text"],
                        "count": 5
                    },
                    "result": {
                        "data_array": [
                            ["1", "result 1"],
                            ["2", "result 2"]
                        ]
                    }
                }
            )
            
            result = await client.query_index(
                index_name="main.default.test_idx",
                query_text="test query",
                num_results=5
            )
            
            assert "manifest" in result
            assert result["manifest"]["count"] == 5
    
    @pytest.mark.asyncio
    async def test_upsert_data(self):
        """Test upserting data."""
        client = DatabricksVectorSearchClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.0/vector-search/indexes/main.default.test_idx/upsert",
                payload={"status": "SUCCESS"}
            )
            
            result = await client.upsert_data(
                index_name="main.default.test_idx",
                data=[
                    {"id": "1", "vector": [0.1, 0.2], "text": "test"}
                ]
            )
            
            assert result["status"] == "SUCCESS"
    
    @pytest.mark.asyncio
    async def test_delete_data(self):
        """Test deleting data."""
        client = DatabricksVectorSearchClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.0/vector-search/indexes/main.default.test_idx/delete",
                payload={"status": "SUCCESS"}
            )
            
            result = await client.delete_data(
                index_name="main.default.test_idx",
                primary_keys=["1", "2", "3"]
            )
            
            assert result["status"] == "SUCCESS"


# ============================================================================
# Integration Tests
# ============================================================================

@pytest.mark.asyncio
@skip_if_no_config
async def test_vector_list_endpoints_integration():
    """Integration test: List vector search endpoints."""
    from tests.test_config import get_test_config
    
    config = get_test_config()
    assert config is not None
    assert config.databricks is not None
    
    client = DatabricksVectorSearchClient(
        host=config.databricks.host,
        token=config.databricks.token
    )
    
    try:
        # Should not raise an error
        endpoints = await client.list_endpoints()
        assert isinstance(endpoints, list)
    finally:
        await client.close()


@pytest.mark.asyncio
@skip_if_no_config
async def test_vector_list_indexes_integration():
    """Integration test: List vector indexes."""
    from tests.test_config import get_test_config
    
    config = get_test_config()
    assert config is not None
    assert config.databricks is not None
    
    client = DatabricksVectorSearchClient(
        host=config.databricks.host,
        token=config.databricks.token
    )
    
    try:
        # First, get available endpoints
        endpoints = await client.list_endpoints()
        
        if endpoints and len(endpoints) > 0:
            # Use the first endpoint to list indexes
            endpoint_name = endpoints[0].get("name")
            if endpoint_name:
                indexes = await client.list_indexes(endpoint_name=endpoint_name)
                assert isinstance(indexes, list)
        else:
            # No endpoints available, skip the test
            import pytest
            pytest.skip("No vector search endpoints available")
    finally:
        await client.close()


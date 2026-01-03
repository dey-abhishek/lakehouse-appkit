"""
Unit tests for Vector Search FastAPI routes.
"""
import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock
import os

# Set dummy env vars for testing
os.environ["DATABRICKS_HOST"] = "https://test.cloud.databricks.com"
os.environ["DATABRICKS_TOKEN"] = "test-token"

from lakehouse_appkit.vector_search.routes import router, get_vector_search_client
from lakehouse_appkit.vector_search import DatabricksVectorSearchClient


# Create mock client
def get_mock_client():
    """Override dependency with mock client."""
    # Don't use spec to allow any attribute access
    mock = AsyncMock()
    # Set default return values
    mock.create_endpoint.return_value = {"name": "test", "status": "ONLINE"}
    mock.list_endpoints.return_value = []
    mock.get_endpoint.return_value = {"name": "test"}
    mock.delete_endpoint.return_value = {}
    mock.create_index.return_value = {"name": "test", "status": "ONLINE"}
    mock.list_indexes.return_value = []
    mock.get_index.return_value = {"name": "test"}
    mock.delete_index.return_value = {}
    mock.sync_index.return_value = {"status": "SYNCING"}
    mock.query_index.return_value = {"manifest": {}, "result": {}}
    mock.upsert_data.return_value = {"status": "SUCCESS"}
    mock.delete_data.return_value = {"status": "SUCCESS"}
    return mock


# Create test client with dependency override
from fastapi import FastAPI
app = FastAPI()
app.include_router(router)
app.dependency_overrides[get_vector_search_client] = get_mock_client
client = TestClient(app)


# ============================================================================
# Endpoint Management Route Tests
# ============================================================================

class TestVectorSearchEndpointRoutes:
    """Tests for vector search endpoint routes."""
    
    def test_create_endpoint(self):
        """Test creating vector search endpoint."""
        response = client.post(
            "/api/vector-search/endpoints",
            json={
                "name": "test-endpoint",
                "endpoint_type": "STANDARD"
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "endpoint" in data
    
    def test_list_endpoints(self):
        """Test listing endpoints."""
        response = client.get("/api/vector-search/endpoints")
        
        assert response.status_code == 200
        data = response.json()
        assert "endpoints" in data
        assert "count" in data
    
    def test_get_endpoint(self):
        """Test getting endpoint details."""
        response = client.get("/api/vector-search/endpoints/test-endpoint")
        
        assert response.status_code == 200
        data = response.json()
        assert "name" in data
    
    def test_delete_endpoint(self):
        """Test deleting endpoint."""
        response = client.delete("/api/vector-search/endpoints/test-endpoint")
        
        assert response.status_code == 200
        data = response.json()
        assert "message" in data


# ============================================================================
# Index Management Route Tests
# ============================================================================

class TestVectorSearchIndexRoutes:
    """Tests for vector search index routes."""
    
    def test_create_index_delta_sync(self):
        """Test creating delta sync index."""
        response = client.post(
            "/api/vector-search/indexes",
            json={
                "name": "main.default.test_idx",
                "endpoint_name": "test-endpoint",
                "primary_key": "id",
                "index_type": "DELTA_SYNC",
                "delta_sync_index_spec": {
                    "source_table": "main.default.source",
                    "embedding_source_columns": [{
                        "name": "text",
                        "embedding_model_endpoint_name": "embeddings"
                    }],
                    "pipeline_type": "TRIGGERED"
                }
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "index" in data
    
    def test_list_indexes(self):
        """Test listing indexes."""
        response = client.get("/api/vector-search/indexes")
        
        assert response.status_code == 200
        data = response.json()
        assert "indexes" in data
        assert "count" in data
    
    def test_get_index(self):
        """Test getting index details."""
        response = client.get("/api/vector-search/indexes/main.default.test_idx")
        
        assert response.status_code == 200
        data = response.json()
        assert "name" in data
    
    def test_delete_index(self):
        """Test deleting index."""
        response = client.delete("/api/vector-search/indexes/main.default.test_idx")
        
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
    
    def test_sync_index(self):
        """Test syncing index."""
        response = client.post("/api/vector-search/indexes/main.default.test_idx/sync")
        
        assert response.status_code == 200
        data = response.json()
        assert "message" in data


# ============================================================================
# Query & Data Operations Route Tests
# ============================================================================

class TestVectorSearchQueryRoutes:
    """Tests for vector search query and data routes."""
    
    def test_query_index_with_text(self):
        """Test querying index with text."""
        response = client.post(
            "/api/vector-search/indexes/main.default.test_idx/query",
            json={
                "query_text": "wireless headphones",
                "num_results": 5
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "index" in data
        assert "results" in data
    
    def test_upsert_data(self):
        """Test upserting data."""
        response = client.post(
            "/api/vector-search/indexes/main.default.test_idx/upsert",
            json={
                "data": [
                    {
                        "id": "item1",
                        "vector": [0.1, 0.2, 0.3],
                        "text": "test item"
                    }
                ]
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert data["count"] == 1
    
    def test_delete_data(self):
        """Test deleting data."""
        response = client.post(
            "/api/vector-search/indexes/main.default.test_idx/delete",
            json={
                "primary_keys": ["item1", "item2", "item3"]
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert data["count"] == 3


# ============================================================================
# Health Check Route Tests
# ============================================================================

class TestVectorSearchHealthRoutes:
    """Tests for vector search health routes."""
    
    def test_health_check(self):
        """Test health check endpoint."""
        response = client.get("/api/vector-search/health/status")
        
        assert response.status_code == 200
        data = response.json()
        assert data["service"] == "vector-search"
        assert data["status"] == "healthy"
        assert "features" in data

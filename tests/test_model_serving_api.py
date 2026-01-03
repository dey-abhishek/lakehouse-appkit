"""
Tests for Model Serving API.
"""
import pytest
from aioresponses import aioresponses
from unittest.mock import AsyncMock, patch

from lakehouse_appkit.model_serving import DatabricksModelServingClient
from lakehouse_appkit.sdk.exceptions import ConnectionError, QueryError


# ============================================================================
# Model Serving Client Tests
# ============================================================================

class TestModelServingClient:
    """Unit tests for Model Serving client."""
    
    @pytest.mark.asyncio
    async def test_create_endpoint(self):
        """Test creating serving endpoint."""
        client = DatabricksModelServingClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.0/serving-endpoints",
                payload={
                    "name": "fraud-detection",
                    "state": "READY",
                    "config": {
                        "served_models": [{
                            "model_name": "main.models.fraud",
                            "model_version": "1"
                        }]
                    }
                },
                status=200
            )
            
            endpoint = await client.create_endpoint(
                name="fraud-detection",
                model_name="main.models.fraud",
                model_version="1",
                workload_size="Small"
            )
            
            assert endpoint["name"] == "fraud-detection"
            assert endpoint["state"] == "READY"
    
    @pytest.mark.asyncio
    async def test_list_endpoints(self):
        """Test listing serving endpoints."""
        client = DatabricksModelServingClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.get(
                "https://test.cloud.databricks.com/api/2.0/serving-endpoints",
                payload={
                    "endpoints": [
                        {"name": "endpoint1", "state": "READY"},
                        {"name": "endpoint2", "state": "READY"}
                    ]
                },
                status=200
            )
            
            endpoints = await client.list_endpoints()
            
            assert len(endpoints) == 2
            assert endpoints[0]["name"] == "endpoint1"
    
    @pytest.mark.asyncio
    async def test_get_endpoint(self):
        """Test getting endpoint details."""
        client = DatabricksModelServingClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.get(
                "https://test.cloud.databricks.com/api/2.0/serving-endpoints/test-endpoint",
                payload={
                    "name": "test-endpoint",
                    "state": "READY",
                    "config": {"served_models": []}
                },
                status=200
            )
            
            endpoint = await client.get_endpoint("test-endpoint")
            
            assert endpoint["name"] == "test-endpoint"
            assert endpoint["state"] == "READY"
    
    @pytest.mark.asyncio
    async def test_delete_endpoint(self):
        """Test deleting endpoint."""
        client = DatabricksModelServingClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.delete(
                "https://test.cloud.databricks.com/api/2.0/serving-endpoints/test-endpoint",
                payload={},
                status=200
            )
            
            result = await client.delete_endpoint("test-endpoint")
            assert result == {}
    
    @pytest.mark.asyncio
    async def test_update_traffic_config(self):
        """Test updating traffic configuration."""
        client = DatabricksModelServingClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.put(
                "https://test.cloud.databricks.com/api/2.0/serving-endpoints/test-endpoint/config",
                payload={
                    "traffic_config": {
                        "routes": [
                            {"served_model_name": "v1", "traffic_percentage": 90},
                            {"served_model_name": "v2", "traffic_percentage": 10}
                        ]
                    }
                },
                status=200
            )
            
            result = await client.update_traffic_config(
                name="test-endpoint",
                routes=[
                    {"served_model_name": "v1", "traffic_percentage": 90},
                    {"served_model_name": "v2", "traffic_percentage": 10}
                ]
            )
            
            assert "traffic_config" in result
    
    @pytest.mark.asyncio
    async def test_query_endpoint_single(self):
        """Test single prediction."""
        client = DatabricksModelServingClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/serving-endpoints/test-endpoint/invocations",
                payload={
                    "predictions": [0.95]
                },
                status=200
            )
            
            result = await client.query_endpoint(
                name="test-endpoint",
                inputs={"amount": 100.0, "merchant": "ABC"}
            )
            
            assert "predictions" in result
            assert result["predictions"] == [0.95]
    
    @pytest.mark.asyncio
    async def test_query_endpoint_batch(self):
        """Test batch prediction."""
        client = DatabricksModelServingClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/serving-endpoints/test-endpoint/invocations",
                payload={
                    "predictions": [0.95, 0.12]
                },
                status=200
            )
            
            result = await client.query_endpoint(
                name="test-endpoint",
                inputs=[
                    {"amount": 100.0},
                    {"amount": 50.0}
                ]
            )
            
            assert len(result["predictions"]) == 2
    
    @pytest.mark.asyncio
    async def test_get_endpoint_metrics(self):
        """Test getting endpoint metrics."""
        client = DatabricksModelServingClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.get(
                "https://test.cloud.databricks.com/api/2.0/serving-endpoints/test-endpoint/metrics",
                payload={
                    "request_count": 1000,
                    "latency_p50": 100
                },
                status=200
            )
            
            metrics = await client.get_endpoint_metrics("test-endpoint")
            
            assert "request_count" in metrics
    
    @pytest.mark.asyncio
    async def test_get_endpoint_logs(self):
        """Test getting endpoint logs."""
        client = DatabricksModelServingClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.get(
                "https://test.cloud.databricks.com/api/2.0/serving-endpoints/test-endpoint/logs",
                payload={
                    "logs": [
                        {"timestamp": 123456, "message": "Test log"}
                    ]
                },
                status=200
            )
            
            logs = await client.get_endpoint_logs("test-endpoint")
            
            assert "logs" in logs
    
    @pytest.mark.asyncio
    async def test_prediction_error_handling(self):
        """Test prediction error handling."""
        client = DatabricksModelServingClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with patch.object(client, '_resilient_request', new_callable=AsyncMock) as mock_request:
            from lakehouse_appkit.sdk.exceptions import QueryError
            mock_request.side_effect = QueryError("Model prediction failed")
            
            with pytest.raises(QueryError, match="Model prediction failed"):
                await client.query_endpoint(
                    name="test-endpoint",
                    inputs={"test": 1}
                )
            
            mock_request.assert_called_once()
        
        await client.close()


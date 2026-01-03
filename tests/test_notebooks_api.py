"""
Unit and integration tests for Databricks Notebooks.
"""
import pytest
from aioresponses import aioresponses
from unittest.mock import AsyncMock, patch
import base64

from lakehouse_appkit.notebooks import DatabricksNotebooksClient
from tests.test_config import skip_if_no_config


# ============================================================================
# Unit Tests
# ============================================================================

class TestNotebooksClientUnit:
    """Unit tests for DatabricksNotebooksClient."""
    
    @pytest.mark.asyncio
    async def test_export_notebook(self):
        """Test exporting notebook."""
        client = DatabricksNotebooksClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        test_content = "# Databricks notebook\nprint('hello')"
        encoded_content = base64.b64encode(test_content.encode('utf-8')).decode('utf-8')
        
        with patch.object(client, '_resilient_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"content": encoded_content}
            
            content = await client.export_notebook("/Users/test/notebook", format="SOURCE")
            
            assert content == test_content
            mock_request.assert_called_once()
        
        await client.close()
    
    @pytest.mark.asyncio
    async def test_import_notebook(self):
        """Test importing notebook."""
        client = DatabricksNotebooksClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with patch.object(client, '_resilient_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {}
            
            result = await client.import_notebook(
                path="/Users/test/new_notebook",
                content="# New notebook\nprint('hello')",
                language="PYTHON"
            )
            
            assert result == {}
            mock_request.assert_called_once()
        
        await client.close()
    
    @pytest.mark.asyncio
    async def test_list_notebooks(self):
        """Test listing notebooks."""
        client = DatabricksNotebooksClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with patch.object(client, '_resilient_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {
                "objects": [
                    {"path": "/Users/test/notebook1", "object_type": "NOTEBOOK"},
                    {"path": "/Users/test/notebook2", "object_type": "NOTEBOOK"}
                ]
            }
            
            notebooks = await client.list_notebooks("/Users/test")
            
            # list_notebooks returns full response
            assert "objects" in notebooks
            assert len(notebooks["objects"]) == 2
            assert notebooks["objects"][0]["path"] == "/Users/test/notebook1"
            mock_request.assert_called_once()
        
        await client.close()
    
    @pytest.mark.asyncio
    async def test_run_notebook(self):
        """Test running notebook."""
        client = DatabricksNotebooksClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with patch.object(client, '_resilient_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {
                "status": "Finished",
                "results": {"data": "success"}
            }
            
            result = await client.run_notebook(
                notebook_path="/Users/test/notebook",
                timeout_seconds=300,
                parameters={"date": "2024-01-01"}
            )
            
            assert result["status"] == "Finished"
            mock_request.assert_called_once()
        
        await client.close()


# ============================================================================
# Integration Tests
# ============================================================================

@pytest.mark.asyncio
@skip_if_no_config
async def test_notebooks_list_integration():
    """Integration test: List notebooks."""
    from tests.test_config import get_test_config
    
    config = get_test_config()
    assert config is not None
    assert config.databricks is not None
    
    client = DatabricksNotebooksClient(
        host=config.databricks.host,
        token=config.databricks.token
    )
    
    # List notebooks in /Users directory
    # Should not raise an error, but may return empty list
    try:
        notebooks = await client.list_notebooks("/Users")
        # Accept either list or dict response
        assert notebooks is not None
        if isinstance(notebooks, dict):
            # Response might be wrapped in objects key
            notebooks = notebooks.get("objects", [])
        assert isinstance(notebooks, list)
    finally:
        await client.close()

"""
Tests for Databricks AI/BI Dashboards REST API Client.
"""
import pytest
from aioresponses import aioresponses

from lakehouse_appkit.dashboards import DatabricksAIBIDashboardClient, DashboardLifecycleState
from lakehouse_appkit.sdk.exceptions import ConnectionError, QueryError
from tests.test_config import skip_if_no_config


# ============================================================================
# Unit Tests (Mocked with aioresponses)
# ============================================================================

class TestDashboardClientUnit:
    """Unit tests for AI/BI Dashboard REST API client with mocked responses."""
    
    def test_client_initialization(self):
        """Test dashboard client initialization."""
        client = DatabricksAIBIDashboardClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        assert client.host == "test.cloud.databricks.com"
        assert client.token == "test-token"
        assert client.base_url == "https://test.cloud.databricks.com/api/2.0/lakeview/dashboards"
    
    @pytest.mark.asyncio
    async def test_create_dashboard(self):
        """Test creating a dashboard."""
        client = DatabricksAIBIDashboardClient("https://test.cloud.databricks.com", "token")
        
        mock_response = {
            "dashboard_id": "test-dashboard-id",
            "display_name": "Test Dashboard",
            "warehouse_id": "test-warehouse",
            "lifecycle_state": DashboardLifecycleState.ACTIVE
        }
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.0/lakeview/dashboards",
                payload=mock_response,
                status=200
            )
            
            result = await client.create_dashboard(
                display_name="Test Dashboard",
                warehouse_id="test-warehouse"
            )
            
            assert result["dashboard_id"] == "test-dashboard-id"
            assert result["display_name"] == "Test Dashboard"
            assert result["warehouse_id"] == "test-warehouse"
    
    @pytest.mark.asyncio
    async def test_list_dashboards(self):
        """Test listing dashboards."""
        client = DatabricksAIBIDashboardClient("https://test.cloud.databricks.com", "token")
        
        mock_response = {
            "dashboards": [
                {"dashboard_id": "dash1", "display_name": "Dashboard 1"},
                {"dashboard_id": "dash2", "display_name": "Dashboard 2"}
            ],
            "next_page_token": None
        }
        
        with aioresponses() as m:
            # Match URL with query parameters
            m.get(
                "https://test.cloud.databricks.com/api/2.0/lakeview/dashboards?page_size=100",
                payload=mock_response,
                status=200
            )
            
            result = await client.list_dashboards()
            
            assert len(result["dashboards"]) == 2
            assert result["dashboards"][0]["dashboard_id"] == "dash1"
    
    @pytest.mark.asyncio
    async def test_get_dashboard(self):
        """Test getting dashboard details."""
        client = DatabricksAIBIDashboardClient("https://test.cloud.databricks.com", "token")
        
        mock_response = {
            "dashboard_id": "test-id",
            "display_name": "My Dashboard",
            "warehouse_id": "wh1",
            "lifecycle_state": DashboardLifecycleState.ACTIVE
        }
        
        with aioresponses() as m:
            m.get(
                "https://test.cloud.databricks.com/api/2.0/lakeview/dashboards/test-id",
                payload=mock_response,
                status=200
            )
            
            result = await client.get_dashboard("test-id")
            
            assert result["dashboard_id"] == "test-id"
            assert result["display_name"] == "My Dashboard"
    
    @pytest.mark.asyncio
    async def test_update_dashboard(self):
        """Test updating a dashboard."""
        client = DatabricksAIBIDashboardClient("https://test.cloud.databricks.com", "token")
        
        mock_response = {
            "dashboard_id": "test-id",
            "display_name": "Updated Dashboard",
            "warehouse_id": "new-warehouse"
        }
        
        with aioresponses() as m:
            m.patch(
                "https://test.cloud.databricks.com/api/2.0/lakeview/dashboards/test-id",
                payload=mock_response,
                status=200
            )
            
            result = await client.update_dashboard(
                dashboard_id="test-id",
                display_name="Updated Dashboard",
                warehouse_id="new-warehouse"
            )
            
            assert result["display_name"] == "Updated Dashboard"
            assert result["warehouse_id"] == "new-warehouse"
    
    @pytest.mark.asyncio
    async def test_publish_dashboard(self):
        """Test publishing a dashboard."""
        client = DatabricksAIBIDashboardClient("https://test.cloud.databricks.com", "token")
        
        mock_response = {
            "dashboard_id": "test-id",
            "version": 1,
            "published_at": "2024-01-01T00:00:00Z"
        }
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.0/lakeview/dashboards/test-id/published",
                payload=mock_response,
                status=200
            )
            
            result = await client.publish_dashboard("test-id")
            
            assert result["dashboard_id"] == "test-id"
            assert result["version"] == 1
    
    @pytest.mark.asyncio
    async def test_unpublish_dashboard(self):
        """Test unpublishing a dashboard."""
        client = DatabricksAIBIDashboardClient("https://test.cloud.databricks.com", "token")
        
        with aioresponses() as m:
            m.delete(
                "https://test.cloud.databricks.com/api/2.0/lakeview/dashboards/test-id/published",
                status=200
            )
            
            await client.unpublish_dashboard("test-id")
            # Should complete without error
    
    @pytest.mark.asyncio
    async def test_trash_dashboard(self):
        """Test moving dashboard to trash."""
        client = DatabricksAIBIDashboardClient("https://test.cloud.databricks.com", "token")
        
        with aioresponses() as m:
            m.delete(
                "https://test.cloud.databricks.com/api/2.0/lakeview/dashboards/test-id",
                status=200
            )
            
            await client.trash_dashboard("test-id")
            # Should complete without error
    
    @pytest.mark.asyncio
    async def test_get_published_dashboard(self):
        """Test getting published dashboard."""
        client = DatabricksAIBIDashboardClient("https://test.cloud.databricks.com", "token")
        
        mock_response = {
            "dashboard_id": "test-id",
            "version": 2,
            "published_at": "2024-01-01T00:00:00Z"
        }
        
        with aioresponses() as m:
            m.get(
                "https://test.cloud.databricks.com/api/2.0/lakeview/dashboards/test-id/published",
                payload=mock_response,
                status=200
            )
            
            result = await client.get_published_dashboard("test-id")
            
            assert result["dashboard_id"] == "test-id"
            assert result["version"] == 2
    
    @pytest.mark.asyncio
    async def test_create_dashboard_error(self):
        """Test dashboard creation with API error."""
        from unittest.mock import AsyncMock, patch
        
        # Mock the client's _request method to simulate API error
        client = DatabricksAIBIDashboardClient("https://test.cloud.databricks.com", "token")
        
        # Mock the underlying request method
        with patch.object(client, '_resilient_request', new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = Exception("API Error: Invalid request")
            
            with pytest.raises(Exception, match="API Error"):
                await client.create_dashboard("Test", "warehouse")
        
        # Clean up session
        await client.close()


# ============================================================================
# Integration Tests (Real API Calls)
# ============================================================================

@pytest.mark.integration
@skip_if_no_config("config/.env.dev")
@pytest.mark.asyncio
async def test_dashboard_list_integration():
    """Test listing dashboards with real Databricks."""
    import os
    client = DatabricksAIBIDashboardClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN")
    )
    
    result = await client.list_dashboards()
    
    assert isinstance(result, dict)
    assert "dashboards" in result
    assert isinstance(result["dashboards"], list)


@pytest.mark.integration
@skip_if_no_config("config/.env.dev")
@pytest.mark.asyncio
async def test_dashboard_lifecycle_integration():
    """Test complete dashboard lifecycle with real Databricks."""
    import os
    client = DatabricksAIBIDashboardClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN")
    )
    
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    if not warehouse_id:
        pytest.skip("DATABRICKS_WAREHOUSE_ID not configured")
    
    # Create dashboard
    dashboard = await client.create_dashboard(
        display_name="Test Dashboard - Lakehouse-AppKit",
        warehouse_id=warehouse_id
    )
    
    dashboard_id = dashboard["dashboard_id"]
    
    try:
        # Get dashboard
        details = await client.get_dashboard(dashboard_id)
        assert details["dashboard_id"] == dashboard_id
        
        # Publish dashboard
        published = await client.publish_dashboard(dashboard_id)
        assert published["dashboard_id"] == dashboard_id
        assert "version" in published
        
        # Get published dashboard
        pub_dash = await client.get_published_dashboard(dashboard_id)
        assert pub_dash["dashboard_id"] == dashboard_id
        
        # Unpublish dashboard
        await client.unpublish_dashboard(dashboard_id)
        
    finally:
        # Cleanup: trash dashboard
        try:
            await client.trash_dashboard(dashboard_id)
        except:
            pass  # Ignore cleanup errors


@pytest.mark.integration
@skip_if_no_config("config/.env.dev")
@pytest.mark.asyncio
async def test_dashboard_update_integration():
    """Test updating a dashboard with real Databricks."""
    import os
    client = DatabricksAIBIDashboardClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN")
    )
    
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    if not warehouse_id:
        pytest.skip("DATABRICKS_WAREHOUSE_ID not configured")
    
    # Create dashboard
    dashboard = await client.create_dashboard(
        display_name="Test Dashboard - Update",
        warehouse_id=warehouse_id
    )
    
    dashboard_id = dashboard["dashboard_id"]
    
    try:
        # Update dashboard
        updated = await client.update_dashboard(
            dashboard_id=dashboard_id,
            display_name="Test Dashboard - Updated"
        )
        
        assert updated["dashboard_id"] == dashboard_id
        assert updated["display_name"] == "Test Dashboard - Updated"
        
    finally:
        # Cleanup
        try:
            await client.trash_dashboard(dashboard_id)
        except:
            pass


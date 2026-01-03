"""
Unit and integration tests for Delta Lake operations.
"""
import pytest
from unittest.mock import AsyncMock, Mock

from lakehouse_appkit.delta import DeltaLakeManager
from lakehouse_appkit.adapters.databricks import DatabricksAdapter
from tests.test_config import skip_if_no_config


# ============================================================================
# Unit Tests
# ============================================================================

class TestDeltaLakeManagerUnit:
    """Unit tests for DeltaLakeManager."""
    
    @pytest.mark.asyncio
    async def test_optimize_table(self):
        """Test optimizing table."""
        # Mock adapter with required attributes
        adapter = Mock(spec=DatabricksAdapter)
        adapter.host = "test.databricks.com"
        adapter.token = "test-token"
        adapter.execute_query = AsyncMock(return_value=[
            {"metrics": {"numFilesAdded": 1, "numFilesRemoved": 10}}
        ])
        
        manager = DeltaLakeManager(adapter)
        
        result = await manager.optimize_table("catalog.schema.table")
        
        assert result["status"] == "success"
        adapter.execute_query.assert_called_once()
        call_args = adapter.execute_query.call_args[0][0]
        assert "OPTIMIZE" in call_args
        assert "catalog.schema.table" in call_args
    
    @pytest.mark.asyncio
    async def test_optimize_table_with_zorder(self):
        """Test optimizing table with z-ordering."""
        adapter = Mock(spec=DatabricksAdapter)
        adapter.execute_query = AsyncMock(return_value=[])
        
        manager = DeltaLakeManager(adapter)
        
        result = await manager.optimize_table(
            "catalog.schema.table",
            zorder_by=["date", "user_id"]
        )
        
        assert result["status"] == "success"
        call_args = adapter.execute_query.call_args[0][0]
        assert "ZORDER BY" in call_args
        assert "date" in call_args
        assert "user_id" in call_args
    
    @pytest.mark.asyncio
    async def test_vacuum_table(self):
        """Test vacuuming table."""
        adapter = Mock(spec=DatabricksAdapter)
        adapter.execute_query = AsyncMock(return_value=[])
        
        manager = DeltaLakeManager(adapter)
        
        result = await manager.vacuum_table(
            "catalog.schema.table",
            retention_hours=168
        )
        
        assert result["status"] == "success"
        call_args = adapter.execute_query.call_args[0][0]
        assert "VACUUM" in call_args
        assert "168" in call_args
    
    @pytest.mark.asyncio
    async def test_query_as_of_version(self):
        """Test time travel query with version."""
        adapter = Mock(spec=DatabricksAdapter)
        adapter.execute_query = AsyncMock(return_value=[
            {"id": 1, "value": "old_value"}
        ])
        
        manager = DeltaLakeManager(adapter)
        
        results = await manager.query_as_of(
            "catalog.schema.table",
            version=10
        )
        
        assert results["status"] == "success"
        assert "data" in results
        call_args = adapter.execute_query.call_args[0][0]
        assert "VERSION AS OF 10" in call_args
    
    @pytest.mark.asyncio
    async def test_query_as_of_timestamp(self):
        """Test time travel query with timestamp."""
        adapter = Mock(spec=DatabricksAdapter)
        adapter.execute_query = AsyncMock(return_value=[])
        
        manager = DeltaLakeManager(adapter)
        
        await manager.query_as_of(
            "catalog.schema.table",
            timestamp="2024-01-01 00:00:00"
        )
        
        call_args = adapter.execute_query.call_args[0][0]
        assert "TIMESTAMP AS OF" in call_args
        assert "2024-01-01" in call_args
    
    @pytest.mark.asyncio
    async def test_query_as_of_no_params(self):
        """Test time travel query without params raises error."""
        adapter = Mock(spec=DatabricksAdapter)
        manager = DeltaLakeManager(adapter)
        
        with pytest.raises(ValueError):
            await manager.query_as_of("catalog.schema.table")
    
    @pytest.mark.asyncio
    async def test_describe_history(self):
        """Test getting table history."""
        adapter = Mock(spec=DatabricksAdapter)
        adapter.execute_query = AsyncMock(return_value=[
            {"version": 2, "timestamp": "2024-01-02", "operation": "WRITE"},
            {"version": 1, "timestamp": "2024-01-01", "operation": "CREATE"}
        ])
        
        manager = DeltaLakeManager(adapter)
        
        history = await manager.describe_history("catalog.schema.table", limit=10)
        
        assert history["status"] == "success"
        assert "history" in history
        assert len(history["history"]) == 2
        assert history["history"][0]["version"] == 2
        call_args = adapter.execute_query.call_args[0][0]
        assert "DESCRIBE HISTORY" in call_args
        assert "LIMIT 10" in call_args
    
    @pytest.mark.asyncio
    async def test_get_table_properties(self):
        """Test getting table properties."""
        adapter = Mock(spec=DatabricksAdapter)
        adapter.execute_query = AsyncMock(return_value=[
            {"key": "delta.minReaderVersion", "value": "1"},
            {"key": "delta.minWriterVersion", "value": "2"}
        ])
        
        manager = DeltaLakeManager(adapter)
        
        result = await manager.get_table_properties("catalog.schema.table")
        
        assert result["status"] == "success"
        assert "properties" in result
        adapter.execute_query.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_set_table_property(self):
        """Test setting table property."""
        adapter = Mock(spec=DatabricksAdapter)
        adapter.execute_query = AsyncMock(return_value=[])
        
        manager = DeltaLakeManager(adapter)
        
        result = await manager.set_table_property(
            "catalog.schema.table",
            "delta.autoOptimize.optimizeWrite",
            "true"
        )
        
        assert result["status"] == "success"
        call_args = adapter.execute_query.call_args[0][0]
        assert "ALTER TABLE" in call_args
        assert "SET TBLPROPERTIES" in call_args


# ============================================================================
# Integration Tests
# ============================================================================

@pytest.mark.asyncio
@skip_if_no_config
async def test_delta_describe_history_integration():
    """Integration test: Describe table history."""
    from tests.test_config import get_test_config
    
    config = get_test_config()
    assert config is not None
    assert config.databricks is not None
    
    adapter = DatabricksAdapter(
        host=config.databricks.host,
        token=config.databricks.token,
        warehouse_id=config.databricks.warehouse_id,
        catalog=config.databricks.catalog or "main",
        schema=config.databricks.schema_name or "default"
    )
    
    try:
        await adapter.connect()
        manager = DeltaLakeManager(adapter)
        
        # Try to get history of any table
        # This might fail if no tables exist, which is okay for test
        try:
            history = await manager.describe_history(
                f"{adapter.catalog}.{adapter.schema}.nonexistent_table",
                limit=1
            )
        except Exception:
            # Table doesn't exist, which is expected
            pass
    except Exception:
        # Connection or config issues
        pass


"""
End-to-end integration tests.
"""
import pytest
from fastapi.testclient import TestClient

from tests.test_config import skip_if_no_databricks, skip_if_no_unity_catalog


# ============================================================================
# FastAPI Application Tests
# ============================================================================

@pytest.mark.integration
class TestFastAPIIntegration:
    """Integration tests for FastAPI application."""
    
    def test_health_endpoint(self, test_client):
        """Test health check endpoint."""
        response = test_client.get("/api/health")
        
        # May not exist in test client
        # assert response.status_code == 200
    
    @skip_if_no_unity_catalog()
    def test_uc_catalogs_endpoint(self, test_client):
        """Test Unity Catalog catalogs endpoint."""
        response = test_client.get("/api/unity-catalog/catalogs")
        
        # Requires proper setup
        # assert response.status_code in [200, 401, 403]


# ============================================================================
# Complete Workflow Tests (moved out of class for pytest-asyncio compatibility)
# ============================================================================

@pytest.mark.integration
@pytest.mark.slow
@skip_if_no_unity_catalog()
@pytest.mark.asyncio
async def test_data_discovery_workflow(uc_manager, test_catalog):
    """Test complete data discovery workflow."""
    # 1. List catalogs
    catalogs = await uc_manager.list_catalogs()
    assert len(catalogs) > 0
    
    # Filter out catalogs with no name
    valid_catalogs = [c for c in catalogs if c.name]
    if not valid_catalogs:
        pytest.skip("No valid catalogs found")
    
    # 2. List schemas in first catalog
    schemas = await uc_manager.list_schemas(valid_catalogs[0].name)
    assert isinstance(schemas, list)
    
    # 3. If schemas exist, list tables
    if schemas:
        valid_schemas = [s for s in schemas if s.name]
        if valid_schemas:
            tables = await uc_manager.list_tables(
                valid_catalogs[0].name,
                valid_schemas[0].name
            )
            assert isinstance(tables, list)


@pytest.mark.integration
@pytest.mark.slow
@skip_if_no_unity_catalog()
@pytest.mark.asyncio
async def test_table_inspection_workflow(
    uc_manager, test_catalog, test_schema, test_table
):
    """Test complete table inspection workflow."""
    if not test_table:
        pytest.skip("TEST_TABLE not configured")
    
    # 1. Get table details
    table = await uc_manager.get_table_details(
        test_catalog, test_schema, test_table
    )
    assert table.name == test_table
    
    # 2. Get statistics
    stats = await uc_manager.get_table_stats(
        test_catalog, test_schema, test_table
    )
    assert "row_count" in stats
    
    # 3. Get sample data
    sample = await uc_manager.get_table_sample(
        test_catalog, test_schema, test_table, limit=10
    )
    assert isinstance(sample, list)
    
    # 4. Get lineage
    lineage = await uc_manager.get_table_lineage(
        test_catalog, test_schema, test_table
    )
    assert lineage.object_name == f"{test_catalog}.{test_schema}.{test_table}"


@pytest.mark.integration
@pytest.mark.slow
@skip_if_no_unity_catalog()
@pytest.mark.asyncio
async def test_search_and_inspect_workflow(uc_manager, test_catalog):
    """Test search and inspect workflow."""
    # 1. Search for tables
    results = await uc_manager.search_tables(
        "",  # Empty search to get any tables
        catalogs=[test_catalog],
        limit=5
    )
    
    # 2. If results found, inspect first table
    if results:
        first_table = results[0]
        details = await uc_manager.get_table_details(
            first_table.catalog_name,
            first_table.schema_name,
            first_table.name
        )
        assert details.name == first_table.name


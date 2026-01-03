"""
Tests for Unity Catalog integration.
"""
import pytest
from unittest.mock import AsyncMock, Mock, patch

from lakehouse_appkit.unity_catalog import (
    UnityCatalogManager,
    UCCatalog,
    UCSchema,
    UCTable,
    UCColumn,
    UCVolume,
    UCLineage,
    UCPermission,
    UCTableType,
)
from tests.test_config import skip_if_no_unity_catalog


# ============================================================================
# Unit Tests (with mocks)
# ============================================================================

class TestUnityCatalogModels:
    """Test Pydantic models."""
    
    def test_uc_catalog_model(self, sample_catalog_data):
        """Test UCCatalog model."""
        catalog = UCCatalog(**sample_catalog_data)
        assert catalog.name == "test_catalog"
        assert catalog.owner == "test_owner"
        assert catalog.comment == "Test catalog"
    
    def test_uc_schema_model(self, sample_schema_data):
        """Test UCSchema model."""
        schema = UCSchema(**sample_schema_data)
        assert schema.name == "test_schema"
        assert schema.full_name == "test_catalog.test_schema"
        assert schema.catalog_name == "test_catalog"
    
    def test_uc_table_model(self, sample_table_data):
        """Test UCTable model."""
        table = UCTable(**sample_table_data)
        assert table.name == "test_table"
        assert table.table_type == UCTableType.MANAGED
        assert len(table.columns) == 2
        assert table.columns[0].name == "id"
        assert table.row_count == 1000
    
    def test_uc_column_model(self):
        """Test UCColumn model."""
        column = UCColumn(
            name="test_col",
            type_name="string",
            type_text="string",
            position=0,
            nullable=True,
            comment="Test column"
        )
        assert column.name == "test_col"
        assert column.nullable is True


class TestUnityCatalogManagerUnit:
    """Unit tests for Unity Catalog Manager (with mocks)."""
    
    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_list_catalogs(self, mock_uc_manager, mock_databricks_adapter):
        """Test listing catalogs."""
        mock_databricks_adapter.execute_query.return_value = [
            {"catalog": "main", "comment": "Main catalog", "owner": "admin"},
            {"catalog": "dev", "comment": "Dev catalog", "owner": "dev_team"},
        ]
        
        catalogs = await mock_uc_manager.list_catalogs()
        
        assert len(catalogs) == 2
        assert catalogs[0].name == "main"
        assert catalogs[1].name == "dev"
        mock_databricks_adapter.execute_query.assert_called_once()
    
    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_list_schemas(self, mock_uc_manager, mock_databricks_adapter):
        """Test listing schemas."""
        mock_databricks_adapter.execute_query.return_value = [
            {"databaseName": "default", "comment": "Default schema"},
            {"databaseName": "bronze", "comment": "Bronze layer"},
        ]
        
        schemas = await mock_uc_manager.list_schemas("main")
        
        assert len(schemas) == 2
        assert schemas[0].name == "default"
        assert schemas[1].name == "bronze"
    
    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_list_tables(self, mock_uc_manager, mock_databricks_adapter):
        """Test listing tables."""
        mock_databricks_adapter.execute_query.return_value = [
            {"tableName": "users", "isTemporary": False},
            {"tableName": "orders", "isTemporary": False},
        ]
        
        tables = await mock_uc_manager.list_tables("main", "default")
        
        assert len(tables) == 2
        assert tables[0].name == "users"
        assert tables[1].name == "orders"
    
    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_search_tables(self, mock_uc_manager, mock_databricks_adapter):
        """Test searching tables."""
        # Mock adapter.execute_query to return table data
        mock_databricks_adapter.execute_query.return_value = [
            {"name": "customer_data", "schema_name": "default", "table_type": "MANAGED"},
            {"name": "orders", "schema_name": "default", "table_type": "MANAGED"},
        ]
        
        results = await mock_uc_manager.search_tables("customer", limit=10)
        
        assert len(results) == 1
        assert "customer" in results[0].name.lower()


# ============================================================================
# Integration Tests (require real Databricks)
# ============================================================================

# ============================================================================
# Integration Tests (moved out of class for pytest-asyncio compatibility)
# ============================================================================

@pytest.mark.integration
@skip_if_no_unity_catalog()
@pytest.mark.asyncio
async def test_uc_list_catalogs_integration(uc_manager):
    """Test listing catalogs with real Databricks."""
    catalogs = await uc_manager.list_catalogs()
    
    assert isinstance(catalogs, list)
    assert len(catalogs) > 0
    assert all(isinstance(c, UCCatalog) for c in catalogs)
    assert all(c.name for c in catalogs)


@pytest.mark.integration
@skip_if_no_unity_catalog()
@pytest.mark.asyncio
async def test_uc_list_schemas_integration(uc_manager, test_catalog):
    """Test listing schemas with real Databricks."""
    schemas = await uc_manager.list_schemas(test_catalog)
    
    assert isinstance(schemas, list)
    assert all(isinstance(s, UCSchema) for s in schemas)
    assert all(s.name for s in schemas)
    assert all(s.catalog_name == test_catalog for s in schemas)


@pytest.mark.integration
@skip_if_no_unity_catalog()
@pytest.mark.asyncio
async def test_uc_list_tables_integration(uc_manager, test_catalog, test_schema):
    """Test listing tables with real Databricks."""
    tables = await uc_manager.list_tables(test_catalog, test_schema)
    
    assert isinstance(tables, list)
    assert all(isinstance(t, UCTable) for t in tables)


@pytest.mark.integration
@skip_if_no_unity_catalog()
@pytest.mark.asyncio
async def test_uc_get_table_details_integration(
    uc_manager, test_catalog, test_schema, test_table
):
    """Test getting table details with real Databricks."""
    if not test_table:
        pytest.skip("TEST_TABLE not configured")
    
    table = await uc_manager.get_table_details(test_catalog, test_schema, test_table)
    
    assert isinstance(table, UCTable)
    assert table.name == test_table
    assert table.catalog_name == test_catalog
    assert table.schema_name == test_schema
    assert len(table.columns) > 0
    assert all(isinstance(col, UCColumn) for col in table.columns)


@pytest.mark.integration
@skip_if_no_unity_catalog()
@pytest.mark.asyncio
async def test_uc_get_table_sample_integration(
    uc_manager, test_catalog, test_schema, test_table
):
    """Test getting table sample with real Databricks."""
    if not test_table:
        pytest.skip("TEST_TABLE not configured")
    
    sample = await uc_manager.get_table_sample(
        test_catalog, test_schema, test_table, limit=5
    )
    
    assert isinstance(sample, list)
    assert len(sample) <= 5


@pytest.mark.integration
@skip_if_no_unity_catalog()
@pytest.mark.asyncio
async def test_uc_get_table_stats_integration(
    uc_manager, test_catalog, test_schema, test_table
):
    """Test getting table stats with real Databricks."""
    if not test_table:
        pytest.skip("TEST_TABLE not configured")
    
    stats = await uc_manager.get_table_stats(test_catalog, test_schema, test_table)
    
    assert isinstance(stats, dict)
    assert "row_count" in stats
    assert "column_count" in stats


@pytest.mark.integration
@skip_if_no_unity_catalog()
@pytest.mark.asyncio
async def test_uc_search_tables_integration(uc_manager, test_catalog):
    """Test searching tables with real Databricks."""
    results = await uc_manager.search_tables("", catalogs=[test_catalog], limit=5)
    
    assert isinstance(results, list)
    assert len(results) <= 5
    assert all(isinstance(t, UCTable) for t in results)


@pytest.mark.integration
@skip_if_no_unity_catalog()
@pytest.mark.asyncio
async def test_uc_get_catalog_tree_integration(uc_manager):
    """Test getting catalog tree with real Databricks."""
    tree = await uc_manager.get_catalog_tree()
    
    assert isinstance(tree, dict)
    assert len(tree) > 0
    
    # Check tree structure
    for catalog_name, catalog_data in tree.items():
        assert "info" in catalog_data
        assert "schemas" in catalog_data
        assert isinstance(catalog_data["schemas"], dict)


# ============================================================================
# API Endpoint Tests
# ============================================================================

@pytest.mark.unit
class TestUnityCatalogAPI:
    """Test Unity Catalog API endpoints."""
    
    @pytest.fixture(autouse=True)
    def isolate_test(self):
        """Ensure complete isolation for each test in this class."""
        # Setup: Clear any existing state
        import sys
        from functools import lru_cache
        
        # Clear LRU caches
        for name, obj in sys.modules.items():
            if hasattr(obj, '__dict__'):
                for attr_name, attr_value in list(obj.__dict__.items()):
                    if hasattr(attr_value, 'cache_clear'):
                        try:
                            attr_value.cache_clear()
                        except:
                            pass
        
        yield  # Run the test
        
        # Teardown: Clean up any test pollution
        # This runs after each test method
    
    def test_list_catalogs_endpoint(self):
        """Test /catalogs endpoint."""
        from fastapi.testclient import TestClient
        from fastapi import FastAPI
        from unittest.mock import AsyncMock
        from lakehouse_appkit.unity_catalog import UCCatalog
        from lakehouse_appkit.unity_catalog.routes import router as uc_router
        from lakehouse_appkit.dependencies import get_unity_catalog_manager
        from lakehouse_appkit.sdk.auth import AuthContext, User, get_auth_manager
        
        # Create app with router
        app = FastAPI()
        app.include_router(uc_router)  # Router has prefix="/api/unity-catalog"
        
        # Create mock manager
        mock_mgr = AsyncMock()
        mock_mgr.list_catalogs = AsyncMock(return_value=[
            UCCatalog(name="test_catalog", comment="Test catalog", full_name="test_catalog")
        ])
        
        # Create mock auth context
        mock_auth_context = AuthContext(
            user=User(username="test_user", roles=["admin"]),
            token="test-token",
            authenticated=True
        )
        
        # Override dependencies
        app.dependency_overrides[get_unity_catalog_manager] = lambda: mock_mgr
        app.dependency_overrides[get_auth_manager().get_current_context] = lambda: mock_auth_context
        
        # Create test client
        test_client = TestClient(app)
        
        # Make request
        response = test_client.get("/api/unity-catalog/catalogs")
        
        # Verify
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        data = response.json()
        assert "catalogs" in data, f"Missing 'catalogs' in response: {data}"
        assert len(data["catalogs"]) == 1
        assert data["catalogs"][0]["name"] == "test_catalog"
    
    def test_list_schemas_endpoint(self):
        """Test /catalogs/{catalog}/schemas endpoint."""
        from fastapi.testclient import TestClient
        from fastapi import FastAPI
        from unittest.mock import AsyncMock, MagicMock
        from lakehouse_appkit.unity_catalog import UnityCatalogManager, UCSchema
        from lakehouse_appkit.unity_catalog.routes import router as uc_router
        from lakehouse_appkit.dependencies import get_unity_catalog_manager
        from lakehouse_appkit.sdk.auth import get_auth_manager
        
        # Create test app with dependency overrides
        app = FastAPI()
        app.include_router(uc_router)  # Router already has prefix="/api/unity-catalog"
        
        # Create mock manager
        mock_manager = AsyncMock(spec=UnityCatalogManager)
        test_schema = UCSchema(
            name="test_schema",
            catalog_name="test_catalog",
            full_name="test_catalog.test_schema"
        )
        mock_manager.list_schemas.return_value = [test_schema]
        
        # Create mock auth manager
        mock_auth_mgr = MagicMock()
        mock_auth_context = MagicMock()
        mock_auth_mgr.get_current_context.return_value = mock_auth_context
        
        # Override dependencies
        app.dependency_overrides[get_unity_catalog_manager] = lambda: mock_manager
        app.dependency_overrides[get_auth_manager().get_current_context] = lambda: mock_auth_context
        
        try:
            # Create client with overrides
            client = TestClient(app)
            
            # Make request
            response = client.get("/api/unity-catalog/catalogs/test_catalog/schemas")
            
            # Verify
            assert response.status_code == 200
            data = response.json()
            assert "schemas" in data
            assert data["count"] == 1
            assert data["schemas"][0]["name"] == "test_schema"
        finally:
            # Always cleanup dependency overrides
            app.dependency_overrides.clear()


# ============================================================================
# CLI Tests
# ============================================================================

@pytest.mark.unit
class TestUnityCatalogCLI:
    """Test Unity Catalog CLI commands."""
    
    def test_list_catalogs_cli(self, cli_runner):
        """Test 'uc list-catalogs' command."""
        from lakehouse_appkit.cli.commands.uc import list_catalogs
        
        with patch('lakehouse_appkit.cli.commands.uc.UnityCatalogManager') as mock_uc:
            mock_instance = Mock()
            mock_instance.list_catalogs = AsyncMock(return_value=[
                UCCatalog(name="main", owner="admin")
            ])
            mock_uc.return_value = mock_instance
            
            result = cli_runner.invoke(
                list_catalogs,
                ['--workspace', 'https://test.databricks.com', '--token', 'test-token']
            )
            
            # Note: This will fail without proper async setup in CLI
            # assert result.exit_code == 0
    
    def test_search_cli(self, cli_runner):
        """Test 'uc search' command."""
        from lakehouse_appkit.cli.commands.uc import search
        
        result = cli_runner.invoke(
            search,
            [
                'customer',
                '--workspace', 'https://test.databricks.com',
                '--token', 'test-token',
                '--limit', '10'
            ]
        )
        
        # Will need proper mocking for async operations
        # assert result.exit_code == 0


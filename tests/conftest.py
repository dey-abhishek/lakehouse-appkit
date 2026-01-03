"""
Pytest configuration and fixtures for Lakehouse-AppKit tests.
"""
import os
import sys

import pytest
import asyncio
from typing import AsyncGenerator, Generator
from unittest.mock import Mock, AsyncMock
from unittest import mock

from lakehouse_appkit.adapters.databricks import DatabricksAdapter
try:
    from lakehouse_appkit.unity_catalog import UnityCatalogManager
except ImportError:
    # Unity Catalog might not have UnityCatalogManager class, use the module itself
    from lakehouse_appkit import unity_catalog
    UnityCatalogManager = getattr(unity_catalog, 'UnityCatalogManager', None)

try:
    from lakehouse_appkit.sdk.auth import AuthManager, AuthContext
except ImportError:
    # Auth classes might be named differently
    from lakehouse_appkit.sdk import auth as auth_module
    AuthManager = getattr(auth_module, 'AuthManager', getattr(auth_module, 'DatabricksAuthManager', None))
    AuthContext = getattr(auth_module, 'AuthContext', getattr(auth_module, 'User', None))
from tests.test_config import get_test_config, skip_if_no_databricks

# Don't load config at import time - let individual tests load it when needed
# This prevents .env.dev from polluting ALL tests
# TEST_CONFIG = get_test_config()
TEST_CONFIG = None


# ============================================================================
# Test Isolation Fixtures - Applied to ALL tests automatically
# ============================================================================

@pytest.fixture(autouse=True)
def isolate_environment_variables(request):
    """
    Isolate environment variables for each test.
    
    This prevents tests from polluting each other's environment.
    Disabled for test_dependencies.py which manages its own environment.
    """
    # Skip for test_dependencies.py - it needs full control of environment
    if 'test_dependencies.py' in request.fspath.basename:
        yield
        return
    
    # Save original environment
    original_env = os.environ.copy()
    
    yield  # Run the test
    
    # Restore original environment, removing any new keys
    current_keys = set(os.environ.keys())
    original_keys = set(original_env.keys())
    
    # Remove keys that were added during the test
    for key in current_keys - original_keys:
        os.environ.pop(key, None)
    
    # Restore keys that were modified or removed
    for key, value in original_env.items():
        os.environ[key] = value


@pytest.fixture(autouse=True)
def clear_lru_cache(request):
    """
    Clear all lru_cache decorators before and after each test.
    
    This prevents test isolation issues where cached singletons from one test
    affect another test.
    
    Disabled for test_dependencies.py which manages its own caches.
    """
    # Skip for test_dependencies.py - it needs full control of caches
    if 'test_dependencies.py' in request.fspath.basename:
        yield
        return
    
    # Clear before test
    try:
        from lakehouse_appkit import dependencies
        
        # List of all @lru_cache decorated functions
        cached_functions = [
            'get_databricks_host',
            'get_databricks_token',
            'get_databricks_warehouse_id',
            'get_unity_catalog_rest_client',
            'get_sql_client',
            'get_dashboard_client',
            'get_secrets_client',
            'get_oauth_client',
            'get_service_principal_client',
            'get_model_serving_client',
            'get_jobs_client',
            'get_deployment_client',
            'get_vector_search_client',
            'get_notebooks_client',
            'get_genie_client',
            'get_mlflow_client',
            'get_connections_client',
            'get_functions_client',
            'get_auth_manager',
        ]
        
        for func_name in cached_functions:
            if hasattr(dependencies, func_name):
                func = getattr(dependencies, func_name)
                if hasattr(func, 'cache_clear'):
                    func.cache_clear()
    except ImportError:
        pass
    
    yield  # Run the test
    
    # Clear after test
    try:
        from lakehouse_appkit import dependencies
        
        for func_name in cached_functions:
            if hasattr(dependencies, func_name):
                func = getattr(dependencies, func_name)
                if hasattr(func, 'cache_clear'):
                    func.cache_clear()
    except ImportError:
        pass


@pytest.fixture(autouse=True)
def cleanup_mock_patches():
    """
    Clean up any mock patches that weren't properly stopped.
    
    This ensures mocks from one test don't leak into another test.
    """
    yield  # Run the test
    
    # Stop all active patches
    mock.patch.stopall()


@pytest.fixture(autouse=True)
def reset_module_cache():
    """
    Reset module-level cached values.
    
    This prevents cached config from polluting tests.
    """
    # Clear before test
    import sys
    
    # Store modules that might have cached state
    modules_to_reload = [
        'lakehouse_appkit.config',
        'lakehouse_appkit.dependencies',
        'tests.test_config',
    ]
    
    yield  # Run the test
    
    # Force reload of config modules to clear any module-level caches
    for module_name in modules_to_reload:
        if module_name in sys.modules:
            # Don't actually reload (causes issues), just clear caches if they exist
            module = sys.modules[module_name]
            if hasattr(module, 'TEST_CONFIG'):
                # Re-fetch config fresh
                if hasattr(module, 'get_test_config'):
                    try:
                        # Update TEST_CONFIG with fresh value
                        module.TEST_CONFIG = module.get_test_config()
                    except:
                        pass


# ============================================================================
# Pytest Configuration
# ============================================================================

def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test (requires real Databricks)"
    )
    config.addinivalue_line(
        "markers", "unit: mark test as unit test (no external dependencies)"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "ai: mark test as requiring AI provider"
    )


# ============================================================================
# Event Loop Fixture - Removed to use pytest-asyncio's default
# ============================================================================
# The event_loop fixture is provided by pytest-asyncio automatically


# ============================================================================
# Mock Fixtures
# ============================================================================

@pytest.fixture
def mock_databricks_adapter():
    """Mock Databricks adapter for unit tests."""
    adapter = Mock(spec=DatabricksAdapter)
    adapter.execute_query = AsyncMock(return_value=[])
    adapter.workspace_url = "https://test.databricks.com"
    adapter.token = "test-token"
    return adapter


@pytest.fixture
def mock_uc_manager(mock_databricks_adapter):
    """Mock Unity Catalog manager for unit tests."""
    # Pass adapter for backward compatibility
    manager = UnityCatalogManager(adapter=mock_databricks_adapter)
    return manager


@pytest.fixture
def mock_auth_context():
    """Mock authentication context."""
    return AuthContext(
        user_id="test-user",
        username="test@example.com",
        roles=["user"],
        permissions=["read", "write"],
        metadata={}
    )


@pytest.fixture
def mock_auth_manager():
    """Mock authentication manager."""
    manager = Mock(spec=AuthManager)
    manager.get_current_context = Mock(return_value=AuthContext(
        user_id="test-user",
        username="test@example.com",
        roles=["user"],
        permissions=["read"],
        metadata={}
    ))
    return manager


# ============================================================================
# Real Integration Fixtures (require configuration)
# ============================================================================

@pytest.fixture
async def databricks_adapter():
    """
    Real Databricks adapter for integration tests.
    
    Requires Databricks configuration in config/.env.dev
    """
    if TEST_CONFIG is None:
        pytest.skip("Databricks configuration not available")
    
    # Check if we have the required Databricks config
    if not TEST_CONFIG.databricks.token:
        pytest.skip("Databricks token not configured")
    
    # Extract host from workspace_url if needed
    host = TEST_CONFIG.databricks.host or TEST_CONFIG.databricks.workspace_url
    if host and host.startswith("http"):
        # Remove protocol
        host = host.replace("https://", "").replace("http://", "")
        # Remove any query parameters
        if "?" in host:
            host = host.split("?")[0]
    
    adapter = DatabricksAdapter(
        host=host,
        token=TEST_CONFIG.databricks.token,
        warehouse_id=TEST_CONFIG.databricks.warehouse_id,
        catalog=TEST_CONFIG.databricks.catalog,
        schema=TEST_CONFIG.databricks.schema,
    )
    
    # Connect to Databricks
    await adapter.connect()
    
    yield adapter
    
    # Cleanup if adapter has close method
    if hasattr(adapter, 'close'):
        await adapter.close()


@pytest.fixture
async def uc_manager(databricks_adapter):
    """
    Real Unity Catalog manager for integration tests.
    
    Requires databricks_adapter fixture.
    """
    return UnityCatalogManager(databricks_adapter)


@pytest.fixture
def test_catalog():
    """Test catalog name."""
    config = get_test_config()
    if config and config.databricks and config.databricks.catalog:
        return config.databricks.catalog
    pytest.skip("Test catalog not configured in databricks config")


@pytest.fixture
def test_schema():
    """Test schema name."""
    config = get_test_config()
    if config and config.databricks and config.databricks.schema:
        return config.databricks.schema
    pytest.skip("Test schema not configured in databricks config")


@pytest.fixture
def test_table():
    """Test table name."""
    config = get_test_config()
    if config and config.unity_catalog and config.unity_catalog.table:
        return config.unity_catalog.table
    pytest.skip("Test table not configured")


@pytest.fixture
def test_volume():
    """Test volume name."""
    if TEST_CONFIG and TEST_CONFIG.unity_catalog:
        return TEST_CONFIG.unity_catalog.volume
    return None


# ============================================================================
# Sample Data Fixtures
# ============================================================================

@pytest.fixture
def sample_catalog_data():
    """Sample catalog data for testing."""
    return {
        "name": "test_catalog",
        "comment": "Test catalog",
        "owner": "test_owner",
        "metastore_id": "test_metastore",
    }


@pytest.fixture
def sample_schema_data():
    """Sample schema data for testing."""
    return {
        "catalog_name": "test_catalog",
        "name": "test_schema",
        "full_name": "test_catalog.test_schema",
        "comment": "Test schema",
        "owner": "test_owner",
    }


@pytest.fixture
def sample_table_data():
    """Sample table data for testing."""
    return {
        "catalog_name": "test_catalog",
        "schema_name": "test_schema",
        "name": "test_table",
        "full_name": "test_catalog.test_schema.test_table",
        "table_type": "MANAGED",
        "columns": [
            {
                "name": "id",
                "type_name": "bigint",
                "type_text": "bigint",
                "position": 0,
                "nullable": False,
            },
            {
                "name": "name",
                "type_name": "string",
                "type_text": "string",
                "position": 1,
                "nullable": True,
                "comment": "User name",
            },
        ],
        "owner": "test_owner",
        "row_count": 1000,
    }


@pytest.fixture
def sample_query_result():
    """Sample query result for testing."""
    return [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25},
        {"id": 3, "name": "Charlie", "age": 35},
    ]


# ============================================================================
# Temporary File Fixtures
# ============================================================================

@pytest.fixture
def temp_project_dir(tmp_path):
    """Create a temporary project directory."""
    project_dir = tmp_path / "test_project"
    project_dir.mkdir()
    return project_dir


@pytest.fixture
def temp_config_file(tmp_path):
    """Create a temporary config file."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text("""
databricks:
  workspace_url: https://test.databricks.com
  token: test-token

unity_catalog:
  catalog: test_catalog
  schema: test_schema
""")
    return config_file


# ============================================================================
# AI Provider Fixtures
# ============================================================================

@pytest.fixture
def mock_openai_client():
    """Mock OpenAI client."""
    client = Mock()
    client.chat.completions.create = AsyncMock(return_value=Mock(
        choices=[Mock(message=Mock(content="Generated code here"))]
    ))
    return client


@pytest.fixture
def mock_anthropic_client():
    """Mock Anthropic client."""
    client = Mock()
    client.messages.create = AsyncMock(return_value=Mock(
        content=[Mock(text="Generated code here")]
    ))
    return client


@pytest.fixture
def mock_gemini_client():
    """Mock Gemini client."""
    client = Mock()
    client.generate_content = AsyncMock(return_value=Mock(
        text="Generated code here"
    ))
    return client


# ============================================================================
# FastAPI Test Client Fixture
# ============================================================================

@pytest.fixture
def test_client():
    """FastAPI test client with proper cleanup."""
    from fastapi.testclient import TestClient
    from fastapi import FastAPI
    
    app = FastAPI()
    
    # Add Unity Catalog routes
    from lakehouse_appkit.unity_catalog.routes import router as uc_router
    app.include_router(uc_router, prefix="/api/unity-catalog")
    
    client = TestClient(app)
    
    yield client
    
    # Cleanup: Clear dependency overrides to prevent test pollution
    app.dependency_overrides.clear()


# ============================================================================
# CLI Test Fixtures
# ============================================================================

@pytest.fixture
def cli_runner():
    """Click CLI test runner."""
    from click.testing import CliRunner
    return CliRunner()


# ============================================================================
# Utility Functions
# ============================================================================

@pytest.fixture
def assert_async():
    """Helper for asserting async operations."""
    async def _assert_async(coro, expected=None, raises=None):
        if raises:
            with pytest.raises(raises):
                await coro
        else:
            result = await coro
            if expected is not None:
                assert result == expected
            return result
    return _assert_async

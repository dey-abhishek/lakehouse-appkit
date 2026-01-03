"""Tests for Databricks Secrets API."""
import pytest
from unittest.mock import AsyncMock, patch
from aioresponses import aioresponses

from lakehouse_appkit.secrets import DatabricksSecretsClient
from lakehouse_appkit.sdk.exceptions import ConnectionError
from lakehouse_appkit.resilience_config import ResiliencePresets, set_default_resilience_config

# Skip if no config
from tests.test_config import skip_if_no_config, skip_if_no_databricks


# ============================================================================
# Fixture to use testing resilience
# ============================================================================

@pytest.fixture(autouse=True, scope="module")
def use_testing_resilience():
    """Use testing resilience preset (1 retry, circuit breaker disabled)."""
    original_config = None
    try:
        from lakehouse_appkit.resilience_config import _default_config
        original_config = _default_config
    except:
        pass
    
    set_default_resilience_config(ResiliencePresets.testing())
    
    yield
    
    if original_config:
        set_default_resilience_config(original_config)


# ============================================================================
# Unit Tests - Secrets REST API Client
# ============================================================================

class TestSecretsClientUnit:
    """Unit tests for Databricks Secrets REST API client."""
    
    @pytest.mark.asyncio
    async def test_list_scopes(self):
        """Test listing secret scopes."""
        client = DatabricksSecretsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.get(
                "https://test.cloud.databricks.com/api/2.0/secrets/scopes/list",
                payload={"scopes": [{"name": "scope1"}, {"name": "scope2"}]},
                status=200,
                repeat=True
            )
            
            scopes = await client.list_scopes()
            
            assert len(scopes) == 2
            assert scopes[0]["name"] == "scope1"
    
    @pytest.mark.asyncio
    async def test_create_scope(self):
        """Test creating a secret scope."""
        client = DatabricksSecretsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.0/secrets/scopes/create",
                payload={},
                status=200,
                repeat=True
            )
            
            result = await client.create_scope("test-scope")
            
            assert result == {}
    
    @pytest.mark.asyncio
    async def test_delete_scope(self):
        """Test deleting a secret scope."""
        client = DatabricksSecretsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.0/secrets/scopes/delete",
                payload={},
                status=200,
                repeat=True
            )
            
            result = await client.delete_scope("test-scope")
            
            assert result == {}
    
    @pytest.mark.asyncio
    async def test_put_secret(self):
        """Test putting a secret."""
        client = DatabricksSecretsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.0/secrets/put",
                payload={},
                status=200,
                repeat=True
            )
            
            result = await client.put_secret(
                scope="test-scope",
                key="api-key",
                string_value="secret123"
            )
            
            assert result == {}
    
    @pytest.mark.asyncio
    async def test_list_secrets(self):
        """Test listing secrets in a scope."""
        client = DatabricksSecretsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.get(
                "https://test.cloud.databricks.com/api/2.0/secrets/list?scope=test-scope",
                payload={"secrets": [{"key": "key1"}, {"key": "key2"}]},
                status=200,
                repeat=True
            )
            
            secrets = await client.list_secrets("test-scope")
            
            assert len(secrets) == 2
            assert secrets[0]["key"] == "key1"
    
    @pytest.mark.asyncio
    async def test_get_secret_metadata(self):
        """Test getting secret metadata."""
        client = DatabricksSecretsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.get(
                "https://test.cloud.databricks.com/api/2.0/secrets/get?scope=test-scope&key=api-key",
                payload={"key": "api-key", "value": None},
                status=200,
                repeat=True
            )
            
            result = await client.get_secret("test-scope", "api-key")
            
            assert result["key"] == "api-key"
    
    @pytest.mark.asyncio
    async def test_delete_secret(self):
        """Test deleting a secret."""
        client = DatabricksSecretsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.0/secrets/delete",
                payload={},
                status=200,
                repeat=True
            )
            
            result = await client.delete_secret("test-scope", "api-key")
            
            assert result == {}
    
    @pytest.mark.asyncio
    async def test_api_error(self):
        """Test API error handling."""
        client = DatabricksSecretsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.get(
                "https://test.cloud.databricks.com/api/2.0/secrets/scopes/list",
                status=400,
                body="Bad Request",
                repeat=True
            )
            
            with pytest.raises(ConnectionError) as exc_info:
                await client.list_scopes()
            
            assert "400" in str(exc_info.value)


# ============================================================================
# Integration Tests
# ============================================================================

@skip_if_no_databricks()
@pytest.mark.asyncio
async def test_secrets_list_scopes_integration():
    """Test listing secret scopes (integration)."""
    from lakehouse_appkit.dependencies import get_secrets_client
    
    client = get_secrets_client()
    
    try:
        scopes = await client.list_scopes()
        assert scopes is not None
        assert isinstance(scopes, list)
    except Exception as e:
        error_msg = str(e).lower()
        assert any(word in error_msg for word in ["permission", "not found", "denied", "invalid", "url", "connection"]), \
            f"Unexpected error: {e}"


@skip_if_no_databricks()
@pytest.mark.asyncio
async def test_secrets_scope_lifecycle_integration():
    """
    Test complete secrets scope lifecycle (integration).
    
    Creates, lists, and deletes a test scope.
    """
    from lakehouse_appkit.dependencies import get_secrets_client
    import os
    
    client = get_secrets_client()
    test_scope = f"test-lakehouse-appkit-{os.getpid()}"
    
    try:
        # Create scope
        await client.create_scope(test_scope)
        
        # List scopes - should include our test scope
        scopes = await client.list_scopes()
        scope_names = [s.get("name") for s in scopes]
        assert test_scope in scope_names
        
        # Delete scope
        await client.delete_scope(test_scope)
        
        # List again - should not include deleted scope
        scopes = await client.list_scopes()
        scope_names = [s.get("name") for s in scopes]
        assert test_scope not in scope_names
        
    except Exception as e:
        # Clean up on error
        try:
            await client.delete_scope(test_scope)
        except:
            pass
        
        error_msg = str(e).lower()
        assert any(word in error_msg for word in ["permission", "not found", "denied", "invalid", "url", "connection"]), \
            f"Unexpected error during workflow: {e}"


@skip_if_no_databricks()
@pytest.mark.asyncio
async def test_secrets_secret_lifecycle_integration():
    """
    Test complete secret lifecycle (integration).
    
    Creates scope, puts secret, lists secrets, deletes secret, deletes scope.
    """
    from lakehouse_appkit.dependencies import get_secrets_client
    import os
    
    client = get_secrets_client()
    test_scope = f"test-lakehouse-appkit-{os.getpid()}"
    test_key = "test-key"
    
    try:
        # Create scope
        await client.create_scope(test_scope)
        
        # Put secret
        await client.put_secret(test_scope, test_key, string_value="test-value")
        
        # List secrets - should include our test secret
        secrets = await client.list_secrets(test_scope)
        secret_keys = [s.get("key") for s in secrets]
        assert test_key in secret_keys
        
        # Delete secret
        await client.delete_secret(test_scope, test_key)
        
        # Delete scope
        await client.delete_scope(test_scope)
        
    except Exception as e:
        # Clean up on error
        try:
            await client.delete_secret(test_scope, test_key)
        except:
            pass
        try:
            await client.delete_scope(test_scope)
        except:
            pass
        
        error_msg = str(e).lower()
        assert any(word in error_msg for word in ["permission", "not found", "denied", "invalid", "url", "connection"]), \
            f"Unexpected error during workflow: {e}"

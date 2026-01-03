"""
Tests for OAuth 2.0 and Service Principals.
"""
import pytest
import time
from unittest.mock import patch
from aioresponses import aioresponses

from lakehouse_appkit.auth import DatabricksOAuthClient, DatabricksServicePrincipalClient
from lakehouse_appkit.auth.oauth_client import OAuthToken, OAuthTokenManager
from lakehouse_appkit.sdk.exceptions import ConnectionError, DataAppError


# ============================================================================
# OAuth Client Tests
# ============================================================================

class TestOAuthClient:
    """Unit tests for OAuth 2.0 client."""
    
    @pytest.mark.asyncio
    async def test_get_token_success(self):
        """Test getting OAuth token."""
        client = DatabricksOAuthClient(
            host="https://test.cloud.databricks.com",
            client_id="test-client-id",
            client_secret="test-secret"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/oidc/v1/token",
                payload={
                    "access_token": "test-token-12345",
                    "token_type": "Bearer",
                    "expires_in": 3600
                },
                status=200
            )
            
            token = await client.get_token(scopes=["all-apis"])
            
            assert token.access_token == "test-token-12345"
            assert token.token_type == "Bearer"
            assert token.expires_in == 3600
            assert not token.is_expired
    
    @pytest.mark.asyncio
    async def test_token_caching(self):
        """Test that tokens are cached."""
        client = DatabricksOAuthClient(
            host="https://test.cloud.databricks.com",
            client_id="test-client-id",
            client_secret="test-secret"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/oidc/v1/token",
                payload={
                    "access_token": "test-token",
                    "token_type": "Bearer",
                    "expires_in": 3600
                },
                status=200
            )
            
            # First call
            token1 = await client.get_token()
            # Second call should return cached token
            token2 = await client.get_token()
            
            assert token1 is token2
            assert client.has_valid_token
    
    @pytest.mark.asyncio
    async def test_token_refresh(self):
        """Test token refresh."""
        client = DatabricksOAuthClient(
            host="https://test.cloud.databricks.com",
            client_id="test-client-id",
            client_secret="test-secret"
        )
        
        with aioresponses() as m:
            # First token
            m.post(
                "https://test.cloud.databricks.com/oidc/v1/token",
                payload={
                    "access_token": "token1",
                    "token_type": "Bearer",
                    "expires_in": 3600
                },
                status=200
            )
            
            token1 = await client.get_token()
            
            # Refresh
            m.post(
                "https://test.cloud.databricks.com/oidc/v1/token",
                payload={
                    "access_token": "token2",
                    "token_type": "Bearer",
                    "expires_in": 3600
                },
                status=200
            )
            
            token2 = await client.refresh_token()
            
            assert token1.access_token != token2.access_token
            assert token2.access_token == "token2"
    
    def test_token_expiration(self):
        """Test token expiration detection."""
        # Create expired token
        token = OAuthToken(
            access_token="test",
            token_type="Bearer",
            expires_in=60,
            issued_at=time.time() - 120  # 2 minutes ago
        )
        
        assert token.is_expired
        
        # Create valid token
        token2 = OAuthToken(
            access_token="test",
            token_type="Bearer",
            expires_in=3600,
            issued_at=time.time()
        )
        
        assert not token2.is_expired
    
    @pytest.mark.asyncio
    async def test_oauth_error_handling(self):
        """Test OAuth error handling."""
        client = DatabricksOAuthClient(
            host="https://test.cloud.databricks.com",
            client_id="test-client-id",
            client_secret="test-secret"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/oidc/v1/token",
                payload={"error": "invalid_client"},
                status=401
            )
            
            with pytest.raises(ConnectionError, match="401"):
                await client.get_token()


# ============================================================================
# OAuth Token Manager Tests
# ============================================================================

class TestOAuthTokenManager:
    """Unit tests for OAuth token manager."""
    
    def test_register_client(self):
        """Test registering OAuth client."""
        manager = OAuthTokenManager()
        
        manager.register_client(
            name="test-service",
            client_id="test-id",
            client_secret="test-secret",
            host="https://test.cloud.databricks.com"
        )
        
        assert "test-service" in manager._clients
    
    @pytest.mark.asyncio
    async def test_get_token_for_client(self):
        """Test getting token for registered client."""
        manager = OAuthTokenManager()
        
        manager.register_client(
            name="test-service",
            client_id="test-id",
            client_secret="test-secret",
            host="https://test.cloud.databricks.com"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/oidc/v1/token",
                payload={
                    "access_token": "test-token",
                    "token_type": "Bearer",
                    "expires_in": 3600
                },
                status=200
            )
            
            token = await manager.get_token("test-service")
            assert token.access_token == "test-token"
    
    @pytest.mark.asyncio
    async def test_unregistered_client_error(self):
        """Test error for unregistered client."""
        manager = OAuthTokenManager()
        
        with pytest.raises(DataAppError, match="not registered"):
            await manager.get_token("nonexistent")


# ============================================================================
# Service Principal Client Tests
# ============================================================================

class TestServicePrincipalClient:
    """Unit tests for Service Principal client."""
    
    @pytest.mark.asyncio
    async def test_create_service_principal(self):
        """Test creating service principal."""
        client = DatabricksServicePrincipalClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.0/preview/scim/v2/ServicePrincipals",
                payload={
                    "id": "sp-12345",
                    "displayName": "test-sp",
                    "active": True
                },
                status=201
            )
            
            sp = await client.create_service_principal(
                display_name="test-sp",
                application_id="app-123"
            )
            
            assert sp["id"] == "sp-12345"
            assert sp["displayName"] == "test-sp"
    
    @pytest.mark.asyncio
    async def test_list_service_principals(self):
        """Test listing service principals."""
        client = DatabricksServicePrincipalClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            # Must include params in URL matching
            m.get(
                "https://test.cloud.databricks.com/api/2.0/preview/scim/v2/ServicePrincipals?count=100&startIndex=1",
                payload={
                    "Resources": [
                        {"id": "sp1", "displayName": "SP 1"},
                        {"id": "sp2", "displayName": "SP 2"}
                    ],
                    "totalResults": 2
                },
                status=200
            )
            
            sps = await client.list_service_principals()
            
            assert len(sps) == 2
            assert sps[0]["id"] == "sp1"
    
    @pytest.mark.asyncio
    async def test_create_oauth_secret(self):
        """Test creating OAuth secret."""
        client = DatabricksServicePrincipalClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.0/service-principals/sp-123/credentials/secrets",
                payload={
                    "client_id": "client-id-123",
                    "secret": "secret-value-456",
                    "secret_id": "secret-id-789"
                },
                status=200
            )
            
            secret = await client.create_oauth_secret("sp-123")
            
            assert "client_id" in secret
            assert "secret" in secret
    
    @pytest.mark.asyncio
    async def test_delete_service_principal(self):
        """Test deleting service principal."""
        client = DatabricksServicePrincipalClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.delete(
                "https://test.cloud.databricks.com/api/2.0/preview/scim/v2/ServicePrincipals/sp-123",
                payload={},
                status=200
            )
            
            result = await client.delete_service_principal("sp-123")
            assert result == {}


"""
Databricks OAuth 2.0 REST API client.

Supports OAuth 2.0 Machine-to-Machine (M2M) authentication.

API Documentation: https://docs.databricks.com/dev-tools/auth/oauth-m2m.html
"""
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from lakehouse_appkit.resilience import ResilientClient


class DatabricksOAuthClient(ResilientClient):
    """Client for Databricks OAuth 2.0 API."""
    
    def __init__(
        self,
        host: str,
        client_id: str,
        client_secret: str,
        scope: str = "all-apis"
    ):
        """
        Initialize OAuth client.
        
        Args:
            host: Databricks workspace URL
            client_id: OAuth client ID
            client_secret: OAuth client secret
            scope: OAuth scope (default: all-apis)
        """
        super().__init__()
        self.host = host.replace('https://', '').replace('http://', '').rstrip('/')
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope
        self.base_url = f"https://{self.host}/oidc/v1"
        
        # Token caching
        self._token: Optional[Dict[str, Any]] = None
        self._token_expiry: Optional[datetime] = None
    
    async def _request(self, method: str, url: str, **kwargs) -> Dict[str, Any]:
        """Make HTTP request with resilience."""
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        return await self._resilient_request(method, url, headers=headers, **kwargs)
    
    @property
    def has_valid_token(self) -> bool:
        """
        Check if we have a valid cached token.
        
        Returns:
            True if token is valid and not expired
        """
        if not self._token or not self._token_expiry:
            return False
        # Check if token is still valid (with 5 min buffer)
        return datetime.now() < self._token_expiry - timedelta(minutes=5)
    
    async def get_token(self) -> Dict[str, Any]:
        """
        Get OAuth token.
        
        Returns:
            Token response with access_token, token_type, expires_in
        """
        if self.has_valid_token:
            return self._token
        
        url = f"{self.base_url}/token"
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": self.scope
        }
        
        token_response = await self._request("POST", url, data=payload)
        
        # Cache token
        self._token = token_response
        expires_in = token_response.get("expires_in", 3600)
        self._token_expiry = datetime.now() + timedelta(seconds=expires_in)
        
        return token_response
    
    async def refresh_token(self) -> Dict[str, Any]:
        """
        Force refresh of OAuth token.
        
        Returns:
            New token response
        """
        # Clear cached token
        self._token = None
        self._token_expiry = None
        
        # Get new token
        return await self.get_token()
    
    async def revoke_token(self, token: str) -> Dict[str, Any]:
        """
        Revoke an OAuth token.
        
        Args:
            token: Token to revoke
            
        Returns:
            Revocation response
        """
        url = f"{self.base_url}/revoke"
        payload = {
            "token": token,
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        
        return await self._request("POST", url, data=payload)


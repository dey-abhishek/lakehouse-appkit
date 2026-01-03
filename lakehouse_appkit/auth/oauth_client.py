"""
REST API client for Databricks OAuth 2.0 (Machine-to-Machine).

API Reference: https://docs.databricks.com/dev-tools/auth/oauth-m2m.html
"""
import aiohttp
import time
from typing import Dict, Optional, Any
from dataclasses import dataclass
from lakehouse_appkit.sdk.exceptions import ConnectionError, DataAppError


@dataclass
class OAuthToken:
    """OAuth token with metadata."""
    access_token: str
    token_type: str
    expires_in: int
    issued_at: float
    
    @property
    def is_expired(self) -> bool:
        """Check if token is expired (with 60s buffer)."""
        return (time.time() - self.issued_at) >= (self.expires_in - 60)
    
    @property
    def expires_at(self) -> float:
        """Get absolute expiration time."""
        return self.issued_at + self.expires_in


class DatabricksOAuthClient:
    """Async REST API client for Databricks OAuth 2.0 (M2M)."""
    
    def __init__(
        self,
        host: str,
        client_id: str,
        client_secret: str,
        token_endpoint: Optional[str] = None
    ):
        """Initialize OAuth client."""
        self.host = host.rstrip('/')
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_endpoint = token_endpoint or f"{self.host}/oidc/v1/token"
        self._token: Optional[OAuthToken] = None
    
    @property
    def has_valid_token(self) -> bool:
        """Check if cached token is valid."""
        return self._token is not None and not self._token.is_expired
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers."""
        return {
            "Content-Type": "application/x-www-form-urlencoded"
        }
    
    async def get_token(self, scopes: Optional[list[str]] = None, force_refresh: bool = False) -> OAuthToken:
        """Get OAuth token."""
        if not force_refresh and self._token and not self._token.is_expired:
            return self._token
        
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        if scopes:
            data["scope"] = " ".join(scopes)
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.token_endpoint,
                    data=data,
                    headers=self._get_headers()
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        self._token = OAuthToken(
                            access_token=result["access_token"],
                            token_type=result.get("token_type", "Bearer"),
                            expires_in=result.get("expires_in", 3600),
                            issued_at=time.time()
                        )
                        return self._token
                    else:
                        error_text = await response.text()
                        raise ConnectionError(f"OAuth failed ({response.status}): {error_text}")
        except aiohttp.ClientError as e:
            raise ConnectionError(f"OAuth connection failed: {str(e)}")
    
    async def refresh_token(self) -> OAuthToken:
        """Refresh token."""
        return await self.get_token(force_refresh=True)
    
    async def validate_token(self, token: str) -> bool:
        """Validate token."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.host}/api/2.0/clusters/list",
                    headers={"Authorization": f"Bearer {token}"}
                ) as response:
                    return response.status in [200, 403]
        except:
            return False
    
    def clear_cache(self):
        """Clear cached token."""
        self._token = None


class OAuthTokenManager:
    """Token manager for multiple OAuth clients."""
    
    def __init__(self):
        """Initialize token manager."""
        self._clients: Dict[str, DatabricksOAuthClient] = {}
        self.tokens: Dict[str, OAuthToken] = {}
    
    def register_client(
        self,
        name: str,
        client_id: str,
        client_secret: str,
        host: str
    ):
        """Register OAuth client."""
        self._clients[name] = DatabricksOAuthClient(
            host=host,
            client_id=client_id,
            client_secret=client_secret
        )
    
    async def get_token(
        self,
        client_name: str,
        scopes: Optional[list[str]] = None
    ) -> OAuthToken:
        """Get token for registered client."""
        if client_name not in self._clients:
            raise DataAppError(f"OAuth client '{client_name}' not registered")
        
        # Check cache
        if client_name in self.tokens:
            token = self.tokens[client_name]
            if not token.is_expired:
                return token
        
        # Get new token
        token = await self._clients[client_name].get_token(scopes=scopes)
        self.tokens[client_name] = token
        return token
    
    def store_token(self, client_name: str, token: OAuthToken):
        """Store token."""
        self.tokens[client_name] = token
    
    def clear_token(self, client_name: str):
        """Clear token."""
        if client_name in self.tokens:
            del self.tokens[client_name]


__all__ = ['DatabricksOAuthClient', 'OAuthToken', 'OAuthTokenManager']

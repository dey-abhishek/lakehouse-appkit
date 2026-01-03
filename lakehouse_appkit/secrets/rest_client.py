"""Databricks Secrets REST API client."""
from typing import List, Dict, Any, Optional
from lakehouse_appkit.resilience import ResilientClient


class DatabricksSecretsClient(ResilientClient):
    """REST API client for Databricks Secrets."""
    
    def __init__(self, host: str, token: str, **kwargs):
        """
        Initialize Secrets client.
        
        Args:
            host: Databricks workspace host
            token: Databricks access token
            **kwargs: Additional arguments for ResilientClient
        """
        super().__init__(**kwargs)
        self.host = host.replace("https://", "").rstrip("/")
        self.token = token
        self.base_url = f"https://{self.host}/api/2.0"
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers."""
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
    
    async def _request(self, method: str, endpoint: str, **kwargs) -> Any:
        """Make HTTP request with resilience."""
        session = await self.get_session()
        url = f"{self.base_url}{endpoint}"
        headers = self._get_headers()
        
        async def _call():
            async with session.request(method, url, headers=headers, **kwargs) as resp:
                resp.raise_for_status()
                if resp.status == 204:
                    return {}
                text = await resp.text()
                return await resp.json() if text else {}
        
        return await self._resilient_request(_call)
    
    # Scope Management
    async def create_scope(self, scope: str, initial_manage_principal: str = "users") -> Dict[str, Any]:
        """Create a secret scope."""
        data = {
            "scope": scope,
            "initial_manage_principal": initial_manage_principal
        }
        return await self._request("POST", "/secrets/scopes/create", json=data)
    
    async def list_scopes(self) -> List[Dict[str, Any]]:
        """List all secret scopes."""
        result = await self._request("GET", "/secrets/scopes/list")
        return result.get("scopes", []) if isinstance(result, dict) else result
    
    async def delete_scope(self, scope: str) -> Dict[str, Any]:
        """Delete a secret scope."""
        data = {"scope": scope}
        return await self._request("POST", "/secrets/scopes/delete", json=data)
    
    # Secret Management
    async def put_secret(self, scope: str, key: str, string_value: str) -> Dict[str, Any]:
        """Put a secret in a scope."""
        data = {
            "scope": scope,
            "key": key,
            "string_value": string_value
        }
        return await self._request("POST", "/secrets/put", json=data)
    
    async def list_secrets(self, scope: str) -> List[Dict[str, Any]]:
        """List secrets in a scope."""
        result = await self._request("GET", f"/secrets/list?scope={scope}")
        return result.get("secrets", []) if isinstance(result, dict) else result
    
    async def get_secret(self, scope: str, key: str) -> Dict[str, Any]:
        """Get a secret value (metadata only via API)."""
        return await self._request("GET", f"/secrets/get?scope={scope}&key={key}")
    
    async def delete_secret(self, scope: str, key: str) -> Dict[str, Any]:
        """Delete a secret."""
        data = {
            "scope": scope,
            "key": key
        }
        return await self._request("POST", "/secrets/delete", json=data)
    
    # ACL Management
    async def put_acl(self, scope: str, principal: str, permission: str) -> Dict[str, Any]:
        """Put ACL for a scope."""
        data = {
            "scope": scope,
            "principal": principal,
            "permission": permission
        }
        return await self._request("POST", "/secrets/acls/put", json=data)
    
    async def get_acl(self, scope: str, principal: str) -> Dict[str, Any]:
        """Get ACL for a principal in a scope."""
        return await self._request("GET", f"/secrets/acls/get?scope={scope}&principal={principal}")
    
    async def list_acls(self, scope: str) -> List[Dict[str, Any]]:
        """List ACLs for a scope."""
        result = await self._request("GET", f"/secrets/acls/list?scope={scope}")
        return result.get("items", []) if isinstance(result, dict) else result
    
    async def delete_acl(self, scope: str, principal: str) -> Dict[str, Any]:
        """Delete ACL for a principal in a scope."""
        data = {
            "scope": scope,
            "principal": principal
        }
        return await self._request("POST", "/secrets/acls/delete", json=data)


__all__ = ['DatabricksSecretsClient']

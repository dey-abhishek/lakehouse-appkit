"""
Unity Catalog REST API client.
"""
from typing import List, Dict, Any, Optional
from lakehouse_appkit.resilience import ResilientClient

class UnityCatalogRestClient(ResilientClient):
    """REST API client for Unity Catalog."""
    
    def __init__(self, host: str, token: str):
        """Initialize client."""
        super().__init__()
        self.host = host.rstrip("/")
        self.token = token
        self.base_url = f"https://{self.host}/api/2.0"
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers."""
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
    
    async def _request(self, method: str, endpoint: str, **kwargs) -> Any:
        """Make HTTP request."""
        import aiohttp
        session = await self.get_session()
        url = f"{self.base_url}{endpoint}"
        headers = self._get_headers()
        
        async def _call():
            async with session.request(method, url, headers=headers, **kwargs) as resp:
                resp.raise_for_status()
                return await resp.json()
        
        return await self._resilient_request(_call)

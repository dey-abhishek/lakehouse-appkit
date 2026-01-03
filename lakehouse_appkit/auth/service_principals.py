"""
REST API client for Databricks Service Principals.

API Reference: https://docs.databricks.com/api/workspace/serviceprincipal
"""
import aiohttp
from typing import Dict, List, Optional, Any
from lakehouse_appkit.sdk.exceptions import ConnectionError


class ServicePrincipalsClient:
    """
    Async REST API client for Databricks Service Principals.
    
    Service principals are non-human identities for automated authentication.
    Used for CI/CD, automation, and application authentication.
    
    Features:
    - Create/read/update/delete service principals
    - Manage OAuth secrets
    - Assign to groups
    - Set permissions
    
    Example:
        ```python
        client = DatabricksServicePrincipalClient(
            host="https://xxx.cloud.databricks.com",
            token="dapi..."
        )
        
        # Create service principal
        sp = await client.create_service_principal(
            display_name="my-app",
            application_id="app-id-12345"
        )
        
        # Generate OAuth secret
        secret = await client.create_oauth_secret(sp["id"])
        
        # List all service principals
        sps = await client.list_service_principals()
        ```
    
    API Reference:
        https://docs.databricks.com/api/workspace/serviceprincipal
    """
    
    def __init__(self, host: str, token: str):
        """
        Initialize Service Principal client.
        
        Args:
            host: Databricks workspace URL
            token: Databricks personal access token
        """
        self.host = host.rstrip('/')
        self.token = token
        self.base_url = f"{self.host}/api/2.0/account/scim/v2/ServicePrincipals"
        self.workspace_base_url = f"{self.host}/api/2.0/preview/scim/v2/ServicePrincipals"
    
    async def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        use_workspace_api: bool = True
    ) -> Dict[str, Any]:
        """Make authenticated request to Service Principals API."""
        base = self.workspace_base_url if use_workspace_api else self.base_url
        url = f"{base}/{endpoint}" if endpoint else base
        
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    json=data,
                    params=params
                ) as response:
                    if response.status in [200, 201]:
                        text = await response.text()
                        return {} if not text else await response.json()
                    else:
                        error_text = await response.text()
                        raise ConnectionError(
                            f"Service Principal API error ({response.status}): {error_text}"
                        )
        except aiohttp.ClientError as e:
            raise ConnectionError(f"Failed to connect to Databricks: {str(e)}")
    
    # ========================================================================
    # Service Principal Management
    # ========================================================================
    
    async def create_service_principal(
        self,
        display_name: str,
        application_id: Optional[str] = None,
        active: bool = True
    ) -> Dict[str, Any]:
        """
        Create a new service principal.
        
        Args:
            display_name: Display name for the service principal
            application_id: Application ID (auto-generated if not provided)
            active: Whether the service principal is active
            
        Returns:
            Service principal object with ID
            
        Example:
            ```python
            sp = await client.create_service_principal(
                display_name="data-pipeline-service",
                application_id="app-12345"
            )
            print(sp["id"])  # Service principal ID
            ```
        """
        data = {
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"],
            "displayName": display_name,
            "active": active
        }
        
        if application_id:
            data["applicationId"] = application_id
        
        return await self._request("POST", "", data=data)
    
    async def get_service_principal(self, sp_id: str) -> Dict[str, Any]:
        """
        Get service principal by ID.
        
        Args:
            sp_id: Service principal ID
            
        Returns:
            Service principal object
        """
        return await self._request("GET", sp_id)
    
    async def list_service_principals(
        self,
        filter_query: Optional[str] = None,
        start_index: int = 1,
        count: int = 100
    ) -> List[Dict[str, Any]]:
        """
        List all service principals.
        
        Args:
            filter_query: SCIM filter query (e.g., 'displayName eq "my-app"')
            start_index: Starting index for pagination
            count: Number of results per page
            
        Returns:
            List of service principals
            
        Example:
            ```python
            # List all
            sps = await client.list_service_principals()
            
            # Filter by name
            sps = await client.list_service_principals(
                filter_query='displayName eq "my-app"'
            )
            ```
        """
        params = {"startIndex": start_index, "count": count}
        if filter_query:
            params["filter"] = filter_query
        
        result = await self._request("GET", "", params=params)
        return result.get("Resources", [])
    
    async def update_service_principal(
        self,
        sp_id: str,
        display_name: Optional[str] = None,
        active: Optional[bool] = None
    ) -> Dict[str, Any]:
        """
        Update service principal.
        
        Args:
            sp_id: Service principal ID
            display_name: New display name
            active: New active status
            
        Returns:
            Updated service principal
        """
        # Get current SP
        sp = await self.get_service_principal(sp_id)
        
        # Update fields
        if display_name:
            sp["displayName"] = display_name
        if active is not None:
            sp["active"] = active
        
        return await self._request("PUT", sp_id, data=sp)
    
    async def delete_service_principal(self, sp_id: str) -> Dict[str, Any]:
        """
        Delete service principal.
        
        Args:
            sp_id: Service principal ID
            
        Returns:
            Empty dict on success
        """
        return await self._request("DELETE", sp_id)
    
    # ========================================================================
    # OAuth Secret Management
    # ========================================================================
    
    async def create_oauth_secret(
        self,
        sp_id: str,
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create OAuth client secret for service principal.
        
        Args:
            sp_id: Service principal ID
            description: Secret description
            
        Returns:
            Secret object with client_id and client_secret
            
        Example:
            ```python
            secret = await client.create_oauth_secret(
                sp_id="sp-12345",
                description="Production secret"
            )
            
            # Store these securely!
            client_id = secret["client_id"]
            client_secret = secret["secret"]
            ```
        
        Note:
            The secret value is only returned once and cannot be retrieved later!
        """
        url = f"{self.host}/api/2.0/service-principals/{sp_id}/credentials/secrets"
        
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        data = {}
        if description:
            data["description"] = description
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=data) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        error_text = await response.text()
                        raise ConnectionError(
                            f"Failed to create OAuth secret ({response.status}): {error_text}"
                        )
        except aiohttp.ClientError as e:
            raise ConnectionError(f"Failed to connect to Databricks: {str(e)}")
    
    async def list_oauth_secrets(self, sp_id: str) -> List[Dict[str, Any]]:
        """
        List OAuth secrets for service principal.
        
        Args:
            sp_id: Service principal ID
            
        Returns:
            List of secret metadata (not the actual secrets)
        """
        url = f"{self.host}/api/2.0/service-principals/{sp_id}/credentials/secrets"
        
        headers = {
            "Authorization": f"Bearer {self.token}"
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result.get("secrets", [])
                    else:
                        error_text = await response.text()
                        raise ConnectionError(
                            f"Failed to list OAuth secrets ({response.status}): {error_text}"
                        )
        except aiohttp.ClientError as e:
            raise ConnectionError(f"Failed to connect to Databricks: {str(e)}")
    
    async def delete_oauth_secret(self, sp_id: str, secret_id: str) -> Dict[str, Any]:
        """
        Delete OAuth secret.
        
        Args:
            sp_id: Service principal ID
            secret_id: Secret ID
            
        Returns:
            Empty dict on success
        """
        url = f"{self.host}/api/2.0/service-principals/{sp_id}/credentials/secrets/{secret_id}"
        
        headers = {
            "Authorization": f"Bearer {self.token}"
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.delete(url, headers=headers) as response:
                    if response.status == 200:
                        return {}
                    else:
                        error_text = await response.text()
                        raise ConnectionError(
                            f"Failed to delete OAuth secret ({response.status}): {error_text}"
                        )
        except aiohttp.ClientError as e:
            raise ConnectionError(f"Failed to connect to Databricks: {str(e)}")


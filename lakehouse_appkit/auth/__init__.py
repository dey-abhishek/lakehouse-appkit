"""Databricks OAuth and Service Principals."""
from enum import Enum
from lakehouse_appkit.auth.oauth_client import DatabricksOAuthClient
from lakehouse_appkit.auth.service_principals import ServicePrincipalsClient

class OAuthGrantType(str, Enum):
    """OAuth grant types."""
    CLIENT_CREDENTIALS = "client_credentials"
    AUTHORIZATION_CODE = "authorization_code"

class ServicePrincipalState(str, Enum):
    """Service principal states."""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"

# Alias for backward compatibility
DatabricksServicePrincipalClient = ServicePrincipalsClient

__all__ = [
    'DatabricksOAuthClient',
    'ServicePrincipalsClient', 
    'DatabricksServicePrincipalClient',
    'OAuthGrantType',
    'ServicePrincipalState'
]

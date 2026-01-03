"""SDK package for Lakehouse-AppKit."""

from lakehouse_appkit.sdk.exceptions import (
    DataAppError,
    AdapterError,
    ConnectionError,
    QueryError,
    AuthenticationError,
    ConfigurationError,
    DatabricksError,
    APIError,
    ResourceNotFoundError,
    PermissionError,
    ValidationError,
)

from lakehouse_appkit.sdk.auth import (
    User,
    AuthContext,
    AuthManager,
    DatabricksAuthManager,
    get_auth_manager,
)

__all__ = [
    # Exceptions
    "DataAppError",
    "AdapterError",
    "ConnectionError",
    "QueryError",
    "AuthenticationError",
    "ConfigurationError",
    "DatabricksError",
    "APIError",
    "ResourceNotFoundError",
    "PermissionError",
    "ValidationError",
    # Auth
    "User",
    "AuthContext",
    "AuthManager",
    "DatabricksAuthManager",
    "get_auth_manager",
]


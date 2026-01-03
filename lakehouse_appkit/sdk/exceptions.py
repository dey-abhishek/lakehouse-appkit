"""
Custom exceptions for Lakehouse-AppKit.

Provides a hierarchy of exceptions for better error handling.
"""


class DataAppError(Exception):
    """Base exception for all Lakehouse-AppKit errors."""

    pass


class AdapterError(DataAppError):
    """Base exception for adapter-related errors."""

    pass


class ConnectionError(AdapterError):
    """Connection to data platform failed."""

    pass


class QueryError(AdapterError):
    """Query execution failed."""

    pass


class AuthenticationError(DataAppError):
    """Authentication failed."""

    pass


class ConfigurationError(DataAppError):
    """Configuration is invalid or missing."""

    pass


class DatabricksError(DataAppError):
    """General Databricks API error."""

    pass


class APIError(DatabricksError):
    """Databricks API request failed."""

    def __init__(self, message: str, status_code: int = None, response: dict = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class ResourceNotFoundError(DatabricksError):
    """Requested resource was not found."""

    pass


class PermissionError(DatabricksError):
    """Insufficient permissions for operation."""

    pass


class ValidationError(DataAppError):
    """Input validation failed."""

    pass


class AIProviderError(DataAppError):
    """AI provider (OpenAI, Claude, etc.) error."""

    pass


class UnityCatalogError(DatabricksError):
    """Unity Catalog operation failed."""

    pass


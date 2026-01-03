"""Utility functions for SDK."""
import re
from typing import Any
from datetime import datetime, date


def sanitize_sql(query: str) -> str:
    """Sanitize SQL query."""
    return query.strip()


def sanitize_sql_identifier(identifier: str) -> str:
    """
    Sanitize SQL identifier (table, column, schema names).
    
    Args:
        identifier: SQL identifier to sanitize
    
    Returns:
        Sanitized identifier
    
    Raises:
        ValueError: If identifier contains unsafe characters
    """
    if not re.match(r'^[a-zA-Z0-9_.-]+$', identifier):
        raise ValueError(
            f"Invalid SQL identifier '{identifier}': "
            "only alphanumeric, underscore, hyphen, and dot allowed"
        )
    # Replace hyphens with underscores
    sanitized = identifier.replace('-', '_')
    # If starts with number, prepend underscore
    if sanitized and sanitized[0].isdigit():
        sanitized = '_' + sanitized
    return sanitized


def quote_identifier(identifier: str) -> str:
    """Quote SQL identifier."""
    return f'`{identifier}`'


def format_sql_value(value: Any) -> str:
    """
    Format Python value for SQL.
    
    Args:
        value: Python value to format
    
    Returns:
        SQL-formatted string
    """
    if value is None:
        return "NULL"
    elif isinstance(value, str):
        # Escape single quotes
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    elif isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, (datetime, date)):
        return f"'{value.isoformat()}'"
    else:
        return f"'{str(value)}'"


def serialize_value(value: Any) -> Any:
    """Serialize Python value for JSON."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def format_query_for_logging(query: str, max_length: int = 100) -> str:
    """
    Format query for logging (truncate if too long).
    
    Args:
        query: SQL query
        max_length: Maximum length
    
    Returns:
        Formatted query string
    """
    query = query.strip()
    if len(query) > max_length:
        return query[:max_length] + "..."
    return query

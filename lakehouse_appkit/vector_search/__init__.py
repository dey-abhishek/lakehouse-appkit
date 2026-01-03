"""Databricks Vector Search."""
from enum import Enum
from lakehouse_appkit.vector_search.rest_client import DatabricksVectorSearchClient

class VectorIndexType(str, Enum):
    """Vector index types."""
    DELTA_SYNC = "DELTA_SYNC"
    DIRECT_ACCESS = "DIRECT_ACCESS"

__all__ = ['DatabricksVectorSearchClient', 'VectorIndexType']

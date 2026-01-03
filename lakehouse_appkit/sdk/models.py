"""Data models for Lakehouse-AppKit."""
from typing import List, Dict, Any, Optional
from pydantic import BaseModel

class QueryResult(BaseModel):
    """Query result model."""
    data: List[Dict[str, Any]]
    row_count: int
    
class TableSchema(BaseModel):
    """Table schema model."""
    table_name: str
    columns: List[Dict[str, Any]]
    
class ColumnSchema(BaseModel):
    """Column schema model."""
    name: str
    data_type: str
    nullable: bool = True
    default: Optional[Any] = None

"""Query builder for SQL."""
from typing import List, Optional, Dict, Any

class QueryBuilder:
    """Fluent SQL query builder."""
    
    def __init__(self):
        self._select: List[str] = []
        self._from: Optional[str] = None
        self._joins: List[str] = []
        self._where: List[str] = []
        self._group_by: List[str] = []
        self._having: List[str] = []
        self._order_by: List[str] = []
        self._limit: Optional[int] = None
        self._offset: Optional[int] = None
        self._params: Dict[str, Any] = {}
    
    def select(self, *columns: str) -> "QueryBuilder":
        """Add SELECT columns."""
        self._select.extend(columns)
        return self
    
    def from_table(self, table: str) -> "QueryBuilder":
        """Set FROM table."""
        self._from = table
        return self
    
    def join(self, table: str, condition: str, join_type: str = "INNER") -> "QueryBuilder":
        """Add JOIN clause."""
        self._joins.append(f"{join_type} JOIN {table} ON {condition}")
        return self
    
    def where(self, condition: str) -> "QueryBuilder":
        """Add WHERE condition."""
        self._where.append(condition)
        return self
    
    def group_by(self, *columns: str) -> "QueryBuilder":
        """Add GROUP BY columns."""
        self._group_by.extend(columns)
        return self
    
    def having(self, condition: str) -> "QueryBuilder":
        """Add HAVING condition."""
        self._having.append(condition)
        return self
    
    def order_by(self, column: str, direction: str = "ASC") -> "QueryBuilder":
        """Add ORDER BY clause."""
        self._order_by.append(f"{column} {direction}")
        return self
    
    def limit(self, n: int) -> "QueryBuilder":
        """Set LIMIT."""
        self._limit = n
        return self
    
    def offset(self, n: int) -> "QueryBuilder":
        """Set OFFSET."""
        self._offset = n
        return self
    
    def param(self, name: str, value: Any) -> "QueryBuilder":
        """Add query parameter."""
        self._params[name] = value
        return self
    
    def reset(self) -> "QueryBuilder":
        """Reset the query builder."""
        self._select = []
        self._from = None
        self._joins = []
        self._where = []
        self._group_by = []
        self._having = []
        self._order_by = []
        self._limit = None
        self._offset = None
        self._params = {}
        return self
    
    def build(self) -> str:
        """Build SQL query string."""
        if not self._from:
            raise ValueError("FROM clause is required")
        
        parts = []
        parts.append(f"SELECT {', '.join(self._select) if self._select else '*'}")
        parts.append(f"FROM {self._from}")
        
        if self._joins:
            parts.extend(self._joins)
        
        if self._where:
            parts.append(f"WHERE {' AND '.join(self._where)}")
        
        if self._group_by:
            parts.append(f"GROUP BY {', '.join(self._group_by)}")
        
        if self._having:
            parts.append(f"HAVING {' AND '.join(self._having)}")
        
        if self._order_by:
            parts.append(f"ORDER BY {', '.join(self._order_by)}")
        
        if self._limit:
            parts.append(f"LIMIT {self._limit}")
        
        if self._offset:
            parts.append(f"OFFSET {self._offset}")
        
        return " ".join(parts)
    
    def get_params(self) -> Dict[str, Any]:
        """Get query parameters."""
        return self._params.copy()

"""Base adapter interface."""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

class BaseAdapter(ABC):
    """Base adapter interface for data platforms."""
    
    @abstractmethod
    async def connect(self):
        """Connect to data platform."""
        pass
    
    @abstractmethod
    async def disconnect(self):
        """Disconnect from data platform."""
        pass
    
    @abstractmethod
    async def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute query and return results."""
        pass

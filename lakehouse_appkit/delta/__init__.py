"""Delta Lake operations for Databricks."""
from typing import Optional, Dict, Any, List


class DeltaLakeManager:
    """
    Delta Lake Manager for table operations.
    
    Can be initialized with either:
    1. An adapter object (uses adapter's execute_query)
    2. host and token directly (creates REST client)
    3. No args (gets from config)
    """
    
    def __init__(self, adapter_or_host=None, token=None, **kwargs):
        """
        Initialize Delta Lake Manager.
        
        Args:
            adapter_or_host: Either a Databricks adapter or host string
            token: Databricks token (if adapter_or_host is a string)
            **kwargs: Additional arguments
        """
        # Check if it's an adapter (has execute_query method)
        if hasattr(adapter_or_host, 'execute_query'):
            # Use adapter directly (for legacy/mock compatibility)
            self.adapter = adapter_or_host
            self.client = None
        elif adapter_or_host and token:
            # Create REST client
            from .rest_client import DatabricksDeltaClient
            self.client = DatabricksDeltaClient(
                host=adapter_or_host,
                token=token,
                **kwargs
            )
            self.adapter = None
        elif adapter_or_host is None:
            # Get from config
            from lakehouse_appkit.config import get_config
            config = get_config()
            if config.databricks:
                from .rest_client import DatabricksDeltaClient
                self.client = DatabricksDeltaClient(
                    host=config.databricks.host,
                    token=config.databricks.token,
                    **kwargs
                )
                self.adapter = None
            else:
                raise ValueError("No Databricks config available")
        else:
            raise ValueError("Must provide either adapter or host+token")
    
    async def optimize_table(self, table_name: str, where: Optional[str] = None, zorder_by: Optional[List[str]] = None) -> Dict[str, Any]:
        """Optimize table."""
        if self.client:
            return await self.client.optimize_table(table_name, where, zorder_by)
        else:
            # Use adapter (SQL execution)
            sql = f"OPTIMIZE {table_name}"
            if where:
                sql += f" WHERE {where}"
            if zorder_by:
                sql += f" ZORDER BY ({', '.join(zorder_by)})"
            await self.adapter.execute_query(sql)
            return {"status": "success"}
    
    async def vacuum_table(self, table_name: str, retention_hours: Optional[int] = None) -> Dict[str, Any]:
        """Vacuum table."""
        if self.client:
            return await self.client.vacuum_table(table_name, retention_hours)
        else:
            sql = f"VACUUM {table_name}"
            if retention_hours is not None:
                sql += f" RETAIN {retention_hours} HOURS"
            await self.adapter.execute_query(sql)
            return {"status": "success"}
    
    async def query_as_of(self, table_name: str, version: Optional[int] = None, timestamp: Optional[str] = None) -> Dict[str, Any]:
        """Query table as of version or timestamp."""
        if self.client:
            return await self.client.query_as_of(table_name, version, timestamp)
        else:
            if version is not None:
                sql = f"SELECT * FROM {table_name} VERSION AS OF {version}"
            elif timestamp:
                sql = f"SELECT * FROM {table_name} TIMESTAMP AS OF '{timestamp}'"
            else:
                raise ValueError("Must provide either version or timestamp")
            result = await self.adapter.execute_query(sql)
            return {"status": "success", "data": result}
    
    async def describe_history(self, table_name: str, limit: Optional[int] = None) -> Dict[str, Any]:
        """Get table history."""
        if self.client:
            return await self.client.describe_history(table_name, limit)
        else:
            sql = f"DESCRIBE HISTORY {table_name}"
            if limit:
                sql += f" LIMIT {limit}"
            result = await self.adapter.execute_query(sql)
            return {"status": "success", "history": result}
    
    async def get_table_properties(self, table_name: str) -> Dict[str, Any]:
        """Get table properties."""
        if self.client:
            return await self.client.get_table_properties(table_name)
        else:
            sql = f"SHOW TBLPROPERTIES {table_name}"
            result = await self.adapter.execute_query(sql)
            return {"status": "success", "properties": result}
    
    async def set_table_property(self, table_name: str, property_name: str, property_value: str) -> Dict[str, Any]:
        """Set table property."""
        if self.client:
            return await self.client.set_table_property(table_name, property_name, property_value)
        else:
            sql = f"ALTER TABLE {table_name} SET TBLPROPERTIES ('{property_name}' = '{property_value}')"
            await self.adapter.execute_query(sql)
            return {"status": "success"}


__all__ = ['DeltaLakeManager']

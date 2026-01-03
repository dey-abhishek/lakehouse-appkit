"""Unity Catalog integration for Lakehouse-AppKit."""
from typing import List, Dict, Any, Optional
from enum import Enum
from pydantic import BaseModel
from lakehouse_appkit.unity_catalog.rest_client import UnityCatalogRestClient


class UCTableType(str, Enum):
    """Unity Catalog table types."""
    MANAGED = "MANAGED"
    EXTERNAL = "EXTERNAL"
    VIEW = "VIEW"
    MATERIALIZED_VIEW = "MATERIALIZED_VIEW"


class UCColumn(BaseModel):
    """Unity Catalog column model."""
    name: str
    type_name: str
    nullable: bool = True
    comment: Optional[str] = None
    position: Optional[int] = None


class UCCatalog(BaseModel):
    """Unity Catalog catalog model."""
    name: str
    owner: Optional[str] = None
    comment: Optional[str] = None
    created_at: Optional[int] = None
    updated_at: Optional[int] = None
    metastore_id: Optional[str] = None


class UCSchema(BaseModel):
    """Unity Catalog schema model."""
    name: str
    catalog_name: str
    full_name: str
    owner: Optional[str] = None
    comment: Optional[str] = None
    created_at: Optional[int] = None
    updated_at: Optional[int] = None


class UCTable(BaseModel):
    """Unity Catalog table model."""
    name: str
    catalog_name: str
    schema_name: str
    table_type: UCTableType
    full_name: str
    columns: List[UCColumn] = []
    owner: Optional[str] = None
    comment: Optional[str] = None
    created_at: Optional[int] = None
    updated_at: Optional[int] = None
    data_source_format: Optional[str] = None
    storage_location: Optional[str] = None
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None


class UCVolume(BaseModel):
    """Unity Catalog volume model."""
    name: str
    catalog_name: str
    schema_name: str
    full_name: str
    volume_type: str
    owner: Optional[str] = None
    comment: Optional[str] = None
    storage_location: Optional[str] = None


class UCLineage(BaseModel):
    """Unity Catalog lineage model."""
    object_type: str
    object_name: str
    upstream: List[str] = []
    downstream: List[str] = []


class UCPermission(BaseModel):
    """Unity Catalog permission model."""
    principal: str
    privileges: List[str] = []


class UnityCatalogManager:
    """Unity Catalog manager for catalog, schema, and table operations."""
    
    def __init__(self, host: Optional[str] = None, token: Optional[str] = None, adapter=None):
        """
        Initialize Unity Catalog manager.
        
        Args:
            host: Databricks workspace host
            token: Databricks access token
            adapter: Optional Databricks adapter (for testing/backward compatibility)
        """
        if adapter is not None:
            # Use adapter if provided (for testing/backward compatibility)
            self.adapter = adapter
            self.client = None
            self.host = getattr(adapter, 'host', 'test.databricks.com')
            self.token = getattr(adapter, 'token', 'test-token')
        elif host and token:
            # Create REST client
            self.client = UnityCatalogRestClient(host, token)
            self.adapter = None
            self.host = host
            self.token = token
        else:
            raise ValueError("Must provide either (host, token) or adapter")
    
    async def list_catalogs(self) -> List[UCCatalog]:
        """List all catalogs."""
        if self.adapter:
            # Use adapter for testing
            response = await self.adapter.execute_query("SHOW CATALOGS")
            # Convert SHOW CATALOGS format to UCCatalog format
            catalogs = []
            for c in response:
                if isinstance(c, dict):
                    # Handle both formats: {'catalog': 'main'} and {'name': 'main'}
                    catalog_data = {
                        'name': c.get('name') or c.get('catalog'),
                        'owner': c.get('owner'),
                        'comment': c.get('comment'),
                        'created_at': c.get('created_at')
                    }
                    catalogs.append(UCCatalog(**catalog_data))
                else:
                    catalogs.append(c)
            return catalogs
        else:
            response = await self.client._request("GET", "/unity-catalog/catalogs")
            catalogs = response.get('catalogs', []) if isinstance(response, dict) else response
            return [UCCatalog(**c) if isinstance(c, dict) else c for c in catalogs]
    
    async def list_schemas(self, catalog: str) -> List[UCSchema]:
        """List schemas in a catalog."""
        if self.adapter:
            response = await self.adapter.execute_query(f"SHOW SCHEMAS IN {catalog}")
            # Convert SHOW SCHEMAS format to UCSchema format
            schemas = []
            for s in response:
                if isinstance(s, dict):
                    # Handle both formats
                    name = s.get('name') or s.get('databaseName')
                    schema_data = {
                        'name': name,
                        'catalog_name': s.get('catalog_name', catalog),
                        'full_name': f"{catalog}.{name}",
                        'owner': s.get('owner'),
                        'comment': s.get('comment')
                    }
                    schemas.append(UCSchema(**schema_data))
                else:
                    schemas.append(s)
            return schemas
        else:
            response = await self.client._request("GET", f"/unity-catalog/schemas?catalog_name={catalog}")
            schemas = response.get('schemas', []) if isinstance(response, dict) else response
            return [UCSchema(**s) if isinstance(s, dict) else s for s in schemas]
    
    async def list_tables(self, catalog: str, schema: str) -> List[UCTable]:
        """List tables in a schema."""
        if self.adapter:
            response = await self.adapter.execute_query(f"SHOW TABLES IN {catalog}.{schema}")
            # Convert SHOW TABLES format to UCTable format
            tables = []
            for t in response:
                if isinstance(t, dict):
                    # Handle both formats
                    name = t.get('name') or t.get('tableName')
                    raw_type = t.get('table_type') or t.get('tableType', 'TABLE')
                    # Map TABLE -> MANAGED for compatibility
                    if raw_type == 'TABLE':
                        raw_type = 'MANAGED'
                    table_data = {
                        'name': name,
                        'catalog_name': t.get('catalog_name', catalog),
                        'schema_name': t.get('schema_name', schema),
                        'full_name': f"{catalog}.{schema}.{name}",
                        'table_type': raw_type,
                        'data_source_format': t.get('data_source_format') or t.get('format', 'DELTA'),
                        'comment': t.get('comment'),
                        'owner': t.get('owner'),
                        'created_at': t.get('created_at')
                    }
                    tables.append(UCTable(**table_data))
                else:
                    tables.append(t)
            return tables
        else:
            response = await self.client._request("GET", f"/unity-catalog/tables?catalog_name={catalog}&schema_name={schema}")
            tables = response.get('tables', []) if isinstance(response, dict) else response
            return [UCTable(**t) if isinstance(t, dict) else t for t in tables]
    
    async def get_table(self, full_name: str) -> UCTable:
        """Get table details."""
        if self.adapter:
            # Mock implementation
            parts = full_name.split('.')
            if len(parts) == 3:
                catalog, schema, name = parts
                return UCTable(
                    name=name,
                    catalog_name=catalog,
                    schema_name=schema,
                    full_name=full_name,
                    table_type='MANAGED',
                    data_source_format='DELTA'
                )
        else:
            response = await self.client._request("GET", f"/unity-catalog/tables/{full_name}")
            return UCTable(**response) if isinstance(response, dict) else response
    
    async def search_tables(self, query: str, catalog: Optional[str] = None, limit: int = 100) -> List[UCTable]:
        """Search tables by name or description."""
        if self.adapter:
            # Simple mock search using SHOW TABLES
            if catalog:
                response = await self.adapter.execute_query(f"SHOW TABLES IN {catalog}")
            else:
                response = await self.adapter.execute_query("SHOW TABLES")
            # Filter by query
            filtered = []
            for t in response[:limit]:  # Apply limit
                if isinstance(t, dict):
                    name = t.get('name') or t.get('tableName', '')
                    if query.lower() in name.lower():
                        raw_type = t.get('table_type') or t.get('tableType', 'TABLE')
                        if raw_type == 'TABLE':
                            raw_type = 'MANAGED'
                        table_data = {
                            'name': name,
                            'catalog_name': catalog or 'main',
                            'schema_name': t.get('schema_name', 'default'),
                            'full_name': f"{catalog or 'main'}.{t.get('schema_name', 'default')}.{name}",
                            'table_type': raw_type,
                            'data_source_format': t.get('data_source_format') or t.get('format', 'DELTA'),
                        }
                        filtered.append(UCTable(**table_data))
                elif query.lower() in str(t).lower():
                    filtered.append(t)
            return filtered[:limit]
        else:
            params = f"query={query}&limit={limit}"
            if catalog:
                params += f"&catalog_name={catalog}"
            response = await self.client._request("GET", f"/unity-catalog/tables/search?{params}")
            tables = response.get('tables', []) if isinstance(response, dict) else response
            return [UCTable(**t) if isinstance(t, dict) else t for t in tables]
    
    async def get_lineage(self, full_name: str) -> UCLineage:
        """Get table lineage."""
        response = await self.client._request("GET", f"/unity-catalog/lineage?table_name={full_name}")
        return UCLineage(
            object_type="table",
            object_name=full_name,
            upstream=response.get('upstreams', []),
            downstream=response.get('downstreams', [])
        )
    
    async def list_volumes(self, catalog: str, schema: str) -> List[UCVolume]:
        """List volumes in a schema."""
        response = await self.client._request("GET", f"/unity-catalog/volumes?catalog_name={catalog}&schema_name={schema}")
        volumes = response.get('volumes', []) if isinstance(response, dict) else response
        return [UCVolume(**v) if isinstance(v, dict) else v for v in volumes]
    
    async def get_permissions(self, securable_type: str, full_name: str) -> List[UCPermission]:
        """Get permissions for a securable object."""
        response = await self.client._request("GET", f"/unity-catalog/permissions/{securable_type}/{full_name}")
        permissions = response.get('privilege_assignments', []) if isinstance(response, dict) else []
        return [UCPermission(**p) if isinstance(p, dict) else p for p in permissions]
    
    async def close(self):
        """Close connections."""
        await self.client.close()


__all__ = [
    'UnityCatalogManager',
    'UCCatalog',
    'UCSchema',
    'UCTable',
    'UCColumn',
    'UCVolume',
    'UCLineage',
    'UCPermission',
    'UCTableType',
]

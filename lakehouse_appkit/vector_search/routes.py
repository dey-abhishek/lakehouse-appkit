"""
FastAPI routes for Databricks Vector Search.
"""
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from lakehouse_appkit.dependencies import get_vector_search_client
from lakehouse_appkit.vector_search import DatabricksVectorSearchClient


router = APIRouter(prefix="/api/vector-search", tags=["vector-search"])


# ============================================================================
# Request/Response Models
# ============================================================================

class EndpointCreate(BaseModel):
    """Request model for creating vector search endpoint."""
    name: str = Field(..., description="Endpoint name")
    endpoint_type: str = Field("STANDARD", description="Endpoint type")


class DeltaSyncIndexSpec(BaseModel):
    """Delta sync index specification."""
    source_table: str = Field(..., description="Source Delta table")
    embedding_source_columns: List[Dict[str, str]] = Field(..., description="Columns to embed")
    pipeline_type: str = Field("TRIGGERED", description="Pipeline type (TRIGGERED or CONTINUOUS)")


class DirectAccessIndexSpec(BaseModel):
    """Direct access index specification."""
    embedding_dimension: int = Field(..., description="Embedding vector dimension")
    schema_json: Dict[str, Any] = Field(..., description="Schema definition")


class IndexCreate(BaseModel):
    """Request model for creating vector index."""
    name: str = Field(..., description="Index name (catalog.schema.index)")
    endpoint_name: str = Field(..., description="Vector search endpoint name")
    primary_key: str = Field(..., description="Primary key column")
    index_type: str = Field(..., description="DELTA_SYNC or DIRECT_ACCESS")
    delta_sync_index_spec: Optional[DeltaSyncIndexSpec] = Field(None, description="Delta sync config")
    direct_access_index_spec: Optional[DirectAccessIndexSpec] = Field(None, description="Direct access config")


class QueryRequest(BaseModel):
    """Request model for querying vector index."""
    query_vector: Optional[List[float]] = Field(None, description="Query vector")
    query_text: Optional[str] = Field(None, description="Query text")
    num_results: int = Field(10, description="Number of results")
    filters: Optional[Dict[str, Any]] = Field(None, description="Metadata filters")
    columns: Optional[List[str]] = Field(None, description="Columns to return")


class UpsertRequest(BaseModel):
    """Request model for upserting data."""
    data: List[Dict[str, Any]] = Field(..., description="Data to upsert")


class DeleteRequest(BaseModel):
    """Request model for deleting data."""
    primary_keys: List[str] = Field(..., description="Primary keys to delete")


# ============================================================================
# Endpoint Management Routes
# ============================================================================

@router.post("/endpoints")
async def create_endpoint(
    endpoint: EndpointCreate,
    client: DatabricksVectorSearchClient = Depends(get_vector_search_client)
):
    """
    Create vector search endpoint.
    
    **Example:**
    ```json
    {
        "name": "my-vector-endpoint",
        "endpoint_type": "STANDARD"
    }
    ```
    """
    try:
        result = await client.create_endpoint(
            name=endpoint.name,
            endpoint_type=endpoint.endpoint_type
        )
        
        return {
            "message": "Endpoint created successfully",
            "endpoint": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create endpoint: {str(e)}")


@router.get("/endpoints")
async def list_endpoints(
    client: DatabricksVectorSearchClient = Depends(get_vector_search_client)
):
    """
    List all vector search endpoints.
    """
    try:
        endpoints = await client.list_endpoints()
        
        return {
            "endpoints": endpoints,
            "count": len(endpoints)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list endpoints: {str(e)}")


@router.get("/endpoints/{name}")
async def get_endpoint(
    name: str,
    client: DatabricksVectorSearchClient = Depends(get_vector_search_client)
):
    """
    Get endpoint details.
    """
    try:
        endpoint = await client.get_endpoint(name)
        return endpoint
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Endpoint not found: {str(e)}")


@router.delete("/endpoints/{name}")
async def delete_endpoint(
    name: str,
    client: DatabricksVectorSearchClient = Depends(get_vector_search_client)
):
    """
    Delete vector search endpoint.
    """
    try:
        await client.delete_endpoint(name)
        return {
            "message": "Endpoint deleted successfully",
            "name": name
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete endpoint: {str(e)}")


# ============================================================================
# Index Management Routes
# ============================================================================

@router.post("/indexes")
async def create_index(
    index: IndexCreate,
    client: DatabricksVectorSearchClient = Depends(get_vector_search_client)
):
    """
    Create vector index.
    
    Supports both DELTA_SYNC (auto-updates from Delta table) and 
    DIRECT_ACCESS (manual updates) indexes.
    
    **Example (Delta Sync):**
    ```json
    {
        "name": "main.default.product_embeddings",
        "endpoint_name": "my-endpoint",
        "primary_key": "id",
        "index_type": "DELTA_SYNC",
        "delta_sync_index_spec": {
            "source_table": "main.default.products",
            "embedding_source_columns": [{
                "name": "description",
                "embedding_model_endpoint_name": "bge_large"
            }],
            "pipeline_type": "TRIGGERED"
        }
    }
    ```
    
    **Example (Direct Access):**
    ```json
    {
        "name": "main.default.custom_vectors",
        "endpoint_name": "my-endpoint",
        "primary_key": "id",
        "index_type": "DIRECT_ACCESS",
        "direct_access_index_spec": {
            "embedding_dimension": 1536,
            "schema_json": {
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "vector", "type": "array<float>"},
                    {"name": "text", "type": "string"}
                ]
            }
        }
    }
    ```
    """
    try:
        delta_spec = index.delta_sync_index_spec.dict() if index.delta_sync_index_spec else None
        direct_spec = index.direct_access_index_spec.dict() if index.direct_access_index_spec else None
        
        result = await client.create_index(
            name=index.name,
            endpoint_name=index.endpoint_name,
            primary_key=index.primary_key,
            index_type=index.index_type,
            delta_sync_index_spec=delta_spec,
            direct_access_index_spec=direct_spec
        )
        
        return {
            "message": "Index created successfully",
            "index": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create index: {str(e)}")


@router.get("/indexes")
async def list_indexes(
    endpoint_name: Optional[str] = None,
    client: DatabricksVectorSearchClient = Depends(get_vector_search_client)
):
    """
    List vector indexes.
    
    Optionally filter by endpoint name.
    """
    try:
        indexes = await client.list_indexes(endpoint_name=endpoint_name)
        
        return {
            "indexes": indexes,
            "count": len(indexes),
            "endpoint_filter": endpoint_name
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list indexes: {str(e)}")


@router.get("/indexes/{name}")
async def get_index(
    name: str,
    client: DatabricksVectorSearchClient = Depends(get_vector_search_client)
):
    """
    Get index details.
    """
    try:
        index = await client.get_index(name)
        return index
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Index not found: {str(e)}")


@router.delete("/indexes/{name}")
async def delete_index(
    name: str,
    client: DatabricksVectorSearchClient = Depends(get_vector_search_client)
):
    """
    Delete vector index.
    """
    try:
        await client.delete_index(name)
        return {
            "message": "Index deleted successfully",
            "name": name
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete index: {str(e)}")


@router.post("/indexes/{name}/sync")
async def sync_index(
    name: str,
    client: DatabricksVectorSearchClient = Depends(get_vector_search_client)
):
    """
    Trigger index sync.
    
    Only applicable for DELTA_SYNC indexes.
    """
    try:
        result = await client.sync_index(name)
        
        return {
            "message": "Index sync triggered",
            "name": name,
            "sync_status": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to sync index: {str(e)}")


# ============================================================================
# Query & Data Operations Routes
# ============================================================================

@router.post("/indexes/{name}/query")
async def query_index(
    name: str,
    query: QueryRequest,
    client: DatabricksVectorSearchClient = Depends(get_vector_search_client)
):
    """
    Query vector index for similar items.
    
    Supports both vector-based and text-based queries.
    
    **Example (Vector Query):**
    ```json
    {
        "query_vector": [0.1, 0.2, ..., 0.9],
        "num_results": 10,
        "filters": {"category": "electronics"},
        "columns": ["id", "name", "price"]
    }
    ```
    
    **Example (Text Query):**
    ```json
    {
        "query_text": "wireless headphones",
        "num_results": 5
    }
    ```
    """
    try:
        results = await client.query_index(
            index_name=name,
            query_vector=query.query_vector,
            query_text=query.query_text,
            num_results=query.num_results,
            filters=query.filters,
            columns=query.columns
        )
        
        return {
            "index": name,
            "num_results": query.num_results,
            "results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query index: {str(e)}")


@router.post("/indexes/{name}/upsert")
async def upsert_data(
    name: str,
    request: UpsertRequest,
    client: DatabricksVectorSearchClient = Depends(get_vector_search_client)
):
    """
    Upsert data into direct access index.
    
    **Example:**
    ```json
    {
        "data": [
            {
                "id": "item1",
                "vector": [0.1, 0.2, ..., 0.9],
                "text": "product description"
            },
            {
                "id": "item2",
                "vector": [0.2, 0.3, ..., 0.8],
                "text": "another product"
            }
        ]
    }
    ```
    """
    try:
        result = await client.upsert_data(
            index_name=name,
            data=request.data
        )
        
        return {
            "message": "Data upserted successfully",
            "index": name,
            "count": len(request.data),
            "result": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upsert data: {str(e)}")


@router.post("/indexes/{name}/delete")
async def delete_data(
    name: str,
    request: DeleteRequest,
    client: DatabricksVectorSearchClient = Depends(get_vector_search_client)
):
    """
    Delete data from direct access index.
    
    **Example:**
    ```json
    {
        "primary_keys": ["item1", "item2", "item3"]
    }
    ```
    """
    try:
        result = await client.delete_data(
            index_name=name,
            primary_keys=request.primary_keys
        )
        
        return {
            "message": "Data deleted successfully",
            "index": name,
            "count": len(request.primary_keys),
            "result": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete data: {str(e)}")


# ============================================================================
# Health Check
# ============================================================================

@router.get("/health")
async def health_check():
    """Health check for vector search service."""
    return {
        "service": "vector-search",
        "status": "healthy",
        "features": [
            "vector_indexes",
            "similarity_search",
            "delta_sync",
            "direct_access"
        ]
    }


@router.get("/health/status")
async def health_check_status():
    """Health check for vector search service (alias)."""
    return await health_check()


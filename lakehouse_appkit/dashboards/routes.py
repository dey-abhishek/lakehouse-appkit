"""
FastAPI routes for Databricks AI/BI Dashboards (Lakeview).
"""
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from lakehouse_appkit.dependencies import get_dashboard_client
from lakehouse_appkit.dashboards import DatabricksAIBIDashboardClient


router = APIRouter(prefix="/api/dashboards", tags=["dashboards"])


# ============================================================================
# Request/Response Models
# ============================================================================


# ============================================================================
# Request/Response Models
# ============================================================================

class DashboardCreate(BaseModel):
    """Request model for creating a dashboard."""
    display_name: str = Field(..., description="Display name for the dashboard")
    warehouse_id: Optional[str] = Field(None, description="SQL warehouse ID")
    parent_path: Optional[str] = Field(None, description="Parent folder path")
    serialized_dashboard: Optional[str] = Field(None, description="Dashboard JSON")


class DashboardUpdate(BaseModel):
    """Request model for updating a dashboard."""
    display_name: Optional[str] = Field(None, description="New display name")
    serialized_dashboard: Optional[str] = Field(None, description="Updated dashboard JSON")
    etag: Optional[str] = Field(None, description="ETag for optimistic concurrency")


class DashboardPublish(BaseModel):
    """Request model for publishing a dashboard."""
    embed_credentials: bool = Field(False, description="Embed credentials in published version")
    warehouse_id: Optional[str] = Field(None, description="SQL warehouse ID")


class DashboardResponse(BaseModel):
    """Response model for dashboard operations."""
    dashboard_id: str
    display_name: Optional[str] = None
    warehouse_id: Optional[str] = None
    create_time: Optional[str] = None
    update_time: Optional[str] = None
    path: Optional[str] = None
    etag: Optional[str] = None
    lifecycle_state: Optional[str] = None
    message: Optional[str] = None


class DashboardListResponse(BaseModel):
    """Response model for listing dashboards."""
    dashboards: List[Dict[str, Any]]
    next_page_token: Optional[str] = None
    total_count: Optional[int] = None


# ============================================================================
# Dashboard CRUD Routes
# ============================================================================

@router.post("/", response_model=DashboardResponse)
async def create_dashboard(
    dashboard: DashboardCreate,
    client: DatabricksAIBIDashboardClient = Depends(get_dashboard_client)
):
    """
    Create a new AI/BI dashboard.
    
    Creates a new Lakeview dashboard in Databricks workspace.
    
    **Example:**
    ```json
    {
        "display_name": "Sales Analytics",
        "warehouse_id": "abc123",
        "parent_path": "/Users/user@company.com/"
    }
    ```
    """
    try:
        result = await client.create_dashboard(
            display_name=dashboard.display_name,
            warehouse_id=dashboard.warehouse_id,
            parent_path=dashboard.parent_path,
            serialized_dashboard=dashboard.serialized_dashboard
        )
        
        return DashboardResponse(
            dashboard_id=result.get("dashboard_id", ""),
            display_name=result.get("display_name"),
            warehouse_id=result.get("warehouse_id"),
            create_time=result.get("create_time"),
            path=result.get("path"),
            etag=result.get("etag"),
            lifecycle_state=result.get("lifecycle_state"),
            message="Dashboard created successfully"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create dashboard: {str(e)}")


@router.get("/", response_model=DashboardListResponse)
async def list_dashboards(
    page_size: int = Query(100, ge=1, le=1000, description="Number of dashboards per page"),
    page_token: Optional[str] = Query(None, description="Token for pagination"),
    view: Optional[str] = Query("DASHBOARD_VIEW_BASIC", description="View level (BASIC/FULL)"),
    client: DatabricksAIBIDashboardClient = Depends(get_dashboard_client)
):
    """
    List all AI/BI dashboards in the workspace.
    
    Returns a paginated list of dashboards with their metadata.
    
    **Parameters:**
    - **page_size**: Number of results per page (1-1000)
    - **page_token**: Token for next page (from previous response)
    - **view**: Detail level (DASHBOARD_VIEW_BASIC or DASHBOARD_VIEW_FULL)
    """
    try:
        result = await client.list_dashboards(
            page_size=page_size,
            page_token=page_token,
            view=view
        )
        
        dashboards = result.get("dashboards", [])
        next_token = result.get("next_page_token")
        
        return DashboardListResponse(
            dashboards=dashboards,
            next_page_token=next_token,
            total_count=len(dashboards)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list dashboards: {str(e)}")


@router.get("/{dashboard_id}", response_model=DashboardResponse)
async def get_dashboard(
    dashboard_id: str,
    client: DatabricksAIBIDashboardClient = Depends(get_dashboard_client)
):
    """
    Get details of a specific dashboard.
    
    Returns full metadata and configuration for a dashboard.
    
    **Parameters:**
    - **dashboard_id**: UUID of the dashboard
    """
    try:
        result = await client.get_dashboard(dashboard_id)
        
        return DashboardResponse(
            dashboard_id=result.get("dashboard_id", dashboard_id),
            display_name=result.get("display_name"),
            warehouse_id=result.get("warehouse_id"),
            create_time=result.get("create_time"),
            update_time=result.get("update_time"),
            path=result.get("path"),
            etag=result.get("etag"),
            lifecycle_state=result.get("lifecycle_state")
        )
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Dashboard not found: {str(e)}")


@router.patch("/{dashboard_id}", response_model=DashboardResponse)
async def update_dashboard(
    dashboard_id: str,
    update: DashboardUpdate,
    client: DatabricksAIBIDashboardClient = Depends(get_dashboard_client)
):
    """
    Update an existing dashboard.
    
    Updates dashboard metadata or configuration. Use ETag for optimistic concurrency.
    
    **Parameters:**
    - **dashboard_id**: UUID of the dashboard
    - **etag**: Optional ETag for conflict detection
    
    **Example:**
    ```json
    {
        "display_name": "Updated Sales Analytics",
        "etag": "abc123"
    }
    ```
    """
    try:
        result = await client.update_dashboard(
            dashboard_id=dashboard_id,
            display_name=update.display_name,
            serialized_dashboard=update.serialized_dashboard,
            etag=update.etag
        )
        
        return DashboardResponse(
            dashboard_id=result.get("dashboard_id", dashboard_id),
            display_name=result.get("display_name"),
            warehouse_id=result.get("warehouse_id"),
            update_time=result.get("update_time"),
            etag=result.get("etag"),
            lifecycle_state=result.get("lifecycle_state"),
            message="Dashboard updated successfully"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update dashboard: {str(e)}")


@router.delete("/{dashboard_id}")
async def delete_dashboard(
    dashboard_id: str,
    etag: Optional[str] = Query(None, description="ETag for optimistic concurrency"),
    client: DatabricksAIBIDashboardClient = Depends(get_dashboard_client)
):
    """
    Delete a dashboard.
    
    Permanently deletes a dashboard. Use ETag to prevent accidental deletion.
    
    **Parameters:**
    - **dashboard_id**: UUID of the dashboard
    - **etag**: Optional ETag for conflict detection
    """
    try:
        await client.delete_dashboard(dashboard_id, etag=etag)
        return {"message": "Dashboard deleted successfully", "dashboard_id": dashboard_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete dashboard: {str(e)}")


# ============================================================================
# Dashboard Publishing Routes
# ============================================================================

@router.post("/{dashboard_id}/publish", response_model=DashboardResponse)
async def publish_dashboard(
    dashboard_id: str,
    publish_config: DashboardPublish,
    client: DatabricksAIBIDashboardClient = Depends(get_dashboard_client)
):
    """
    Publish a dashboard.
    
    Creates a published version of the dashboard that can be shared.
    
    **Parameters:**
    - **dashboard_id**: UUID of the dashboard
    - **embed_credentials**: Whether to embed credentials (default: false)
    - **warehouse_id**: SQL warehouse ID for queries
    
    **Example:**
    ```json
    {
        "embed_credentials": true,
        "warehouse_id": "abc123"
    }
    ```
    """
    try:
        result = await client.publish_dashboard(
            dashboard_id=dashboard_id,
            embed_credentials=publish_config.embed_credentials,
            warehouse_id=publish_config.warehouse_id
        )
        
        return DashboardResponse(
            dashboard_id=result.get("dashboard_id", dashboard_id),
            display_name=result.get("display_name"),
            warehouse_id=result.get("warehouse_id"),
            update_time=result.get("update_time"),
            lifecycle_state=result.get("lifecycle_state"),
            message="Dashboard published successfully"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to publish dashboard: {str(e)}")


@router.delete("/{dashboard_id}/publish")
async def unpublish_dashboard(
    dashboard_id: str,
    etag: Optional[str] = Query(None, description="ETag for optimistic concurrency"),
    client: DatabricksAIBIDashboardClient = Depends(get_dashboard_client)
):
    """
    Unpublish a dashboard.
    
    Removes the published version of the dashboard.
    
    **Parameters:**
    - **dashboard_id**: UUID of the dashboard
    - **etag**: Optional ETag for conflict detection
    """
    try:
        await client.unpublish_dashboard(dashboard_id, etag=etag)
        return {"message": "Dashboard unpublished successfully", "dashboard_id": dashboard_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to unpublish dashboard: {str(e)}")


# ============================================================================
# Health Check
# ============================================================================

@router.get("/health/status")
async def dashboard_health():
    """Health check for dashboard service."""
    return {
        "service": "dashboards",
        "status": "healthy",
        "features": [
            "create_dashboard",
            "list_dashboards",
            "get_dashboard",
            "update_dashboard",
            "delete_dashboard",
            "publish_dashboard",
            "unpublish_dashboard"
        ]
    }

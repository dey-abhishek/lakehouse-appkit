"""
FastAPI routes for YAML workflow management.
"""
from fastapi import APIRouter, HTTPException, UploadFile, File, Depends
from typing import Dict, Any, Optional, List
from pydantic import BaseModel
from pathlib import Path
import yaml

from lakehouse_appkit.workflows import WorkflowEngine, Workflow
from lakehouse_appkit.dependencies import get_databricks_adapter


router = APIRouter(prefix="/api/workflows", tags=["workflows"])


class WorkflowExecutionRequest(BaseModel):
    """Request to execute a workflow."""
    workflow_name: str
    user: str
    parameters: Optional[Dict[str, Any]] = None


class WorkflowExecutionResponse(BaseModel):
    """Response from workflow execution."""
    execution_id: str
    workflow_name: str
    status: str
    step_results: Dict[str, Any]
    error: Optional[str] = None


class WorkflowValidationResponse(BaseModel):
    """Response from workflow validation."""
    is_valid: bool
    error: Optional[str] = None
    workflow_name: Optional[str] = None


class WorkflowListResponse(BaseModel):
    """List of available workflows."""
    workflows: List[Dict[str, Any]]


@router.get("/", response_model=WorkflowListResponse)
async def list_workflows():
    """
    List all available workflow definitions.
    
    Returns:
        List of workflow metadata
    """
    workflows_dir = Path("workflows")
    if not workflows_dir.exists():
        return WorkflowListResponse(workflows=[])
    
    workflows = []
    for yaml_file in workflows_dir.glob("*.yaml"):
        try:
            with open(yaml_file, 'r') as f:
                data = yaml.safe_load(f)
                workflows.append({
                    "name": data.get("name"),
                    "version": data.get("version"),
                    "description": data.get("description"),
                    "file": yaml_file.name,
                    "owner": data.get("owner"),
                    "tags": data.get("tags", [])
                })
        except Exception as e:
            # Skip invalid files
            continue
    
    return WorkflowListResponse(workflows=workflows)


@router.get("/{workflow_name}", response_model=Dict[str, Any])
async def get_workflow(workflow_name: str):
    """
    Get workflow definition.
    
    Args:
        workflow_name: Name of the workflow
        
    Returns:
        Workflow definition
    """
    yaml_path = Path(f"workflows/{workflow_name}.yaml")
    if not yaml_path.exists():
        raise HTTPException(status_code=404, detail=f"Workflow '{workflow_name}' not found")
    
    try:
        workflow = WorkflowEngine.load_workflow(str(yaml_path))
        return workflow.dict()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error loading workflow: {str(e)}")


@router.post("/validate", response_model=WorkflowValidationResponse)
async def validate_workflow(file: UploadFile = File(...)):
    """
    Validate a workflow YAML file.
    
    Args:
        file: YAML file to validate
        
    Returns:
        Validation result
    """
    try:
        # Save uploaded file temporarily
        temp_path = Path(f"/tmp/{file.filename}")
        with open(temp_path, 'wb') as f:
            content = await file.read()
            f.write(content)
        
        # Validate
        is_valid, error = WorkflowEngine.validate_workflow(str(temp_path))
        
        # Load workflow name if valid
        workflow_name = None
        if is_valid:
            with open(temp_path, 'r') as f:
                data = yaml.safe_load(f)
                workflow_name = data.get("name")
        
        # Cleanup
        temp_path.unlink(missing_ok=True)
        
        return WorkflowValidationResponse(
            is_valid=is_valid,
            error=error,
            workflow_name=workflow_name
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Validation error: {str(e)}")


@router.post("/execute", response_model=WorkflowExecutionResponse)
async def execute_workflow(
    request: WorkflowExecutionRequest,
    adapter = Depends(get_databricks_adapter)
):
    """
    Execute a workflow.
    
    Args:
        request: Workflow execution request
        adapter: Databricks adapter (injected)
        
    Returns:
        Execution results
    """
    yaml_path = Path(f"workflows/{request.workflow_name}.yaml")
    if not yaml_path.exists():
        raise HTTPException(status_code=404, detail=f"Workflow '{request.workflow_name}' not found")
    
    try:
        # Load workflow
        workflow = WorkflowEngine.load_workflow(str(yaml_path))
        
        # Create engine
        engine = WorkflowEngine(adapter)
        
        # Execute
        context = await engine.execute_workflow(
            workflow=workflow,
            user=request.user,
            parameters=request.parameters
        )
        
        return WorkflowExecutionResponse(
            execution_id=context.execution_id,
            workflow_name=context.workflow_name,
            status=context.status,
            step_results=context.step_results,
            error=context.error
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Execution error: {str(e)}")


@router.post("/upload")
async def upload_workflow(file: UploadFile = File(...)):
    """
    Upload a new workflow definition.
    
    Args:
        file: YAML workflow file
        
    Returns:
        Upload confirmation
    """
    if not file.filename.endswith('.yaml'):
        raise HTTPException(status_code=400, detail="File must be a YAML file")
    
    try:
        # Validate first
        content = await file.read()
        temp_path = Path(f"/tmp/{file.filename}")
        with open(temp_path, 'wb') as f:
            f.write(content)
        
        is_valid, error = WorkflowEngine.validate_workflow(str(temp_path))
        if not is_valid:
            temp_path.unlink(missing_ok=True)
            raise HTTPException(status_code=400, detail=f"Invalid workflow: {error}")
        
        # Save to workflows directory
        workflows_dir = Path("workflows")
        workflows_dir.mkdir(exist_ok=True)
        
        workflow_path = workflows_dir / file.filename
        workflow_path.write_bytes(content)
        
        # Cleanup
        temp_path.unlink(missing_ok=True)
        
        return {
            "message": "Workflow uploaded successfully",
            "filename": file.filename,
            "path": str(workflow_path)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload error: {str(e)}")


@router.delete("/{workflow_name}")
async def delete_workflow(workflow_name: str):
    """
    Delete a workflow definition.
    
    Args:
        workflow_name: Name of the workflow to delete
        
    Returns:
        Deletion confirmation
    """
    yaml_path = Path(f"workflows/{workflow_name}.yaml")
    if not yaml_path.exists():
        raise HTTPException(status_code=404, detail=f"Workflow '{workflow_name}' not found")
    
    try:
        yaml_path.unlink()
        return {
            "message": f"Workflow '{workflow_name}' deleted successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete error: {str(e)}")


"""
Workflow engine for declarative YAML-based workflows.

Enables governed, auditable data workflows defined in YAML.
"""
from typing import Dict, List, Any, Optional, Callable
from pathlib import Path
import yaml
from pydantic import BaseModel, Field, validator
from enum import Enum
import asyncio
from datetime import datetime


class WorkflowStepType(str, Enum):
    """Type of workflow step."""
    SQL = "sql"
    PYTHON = "python"
    APPROVAL = "approval"
    VALIDATION = "validation"
    NOTIFY = "notify"
    DELTA = "delta"
    JOB = "job"


class WorkflowStep(BaseModel):
    """A single step in a workflow."""
    name: str = Field(..., description="Step name")
    type: WorkflowStepType = Field(..., description="Step type")
    description: Optional[str] = Field(None, description="Step description")
    
    # SQL step fields
    query: Optional[str] = Field(None, description="SQL query to execute")
    catalog: Optional[str] = Field(None, description="Unity Catalog catalog")
    schema_: Optional[str] = Field(None, alias="schema", description="Unity Catalog schema")
    
    # Python step fields
    function: Optional[str] = Field(None, description="Python function to execute")
    params: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Function parameters")
    
    # Approval step fields
    approver: Optional[str] = Field(None, description="Required approver")
    approval_message: Optional[str] = Field(None, description="Approval prompt")
    
    # Validation step fields
    validation_query: Optional[str] = Field(None, description="Validation SQL query")
    expected_result: Optional[Any] = Field(None, description="Expected validation result")
    
    # Delta step fields
    operation: Optional[str] = Field(None, description="Delta operation (optimize, vacuum)")
    table: Optional[str] = Field(None, description="Fully qualified table name")
    
    # Job step fields
    job_id: Optional[int] = Field(None, description="Databricks job ID")
    
    # Common fields
    depends_on: Optional[List[str]] = Field(default_factory=list, description="Dependencies")
    on_failure: Optional[str] = Field(default="fail", description="Failure behavior: fail, continue, retry")
    retry_count: Optional[int] = Field(default=0, description="Number of retries")
    timeout_seconds: Optional[int] = Field(default=300, description="Step timeout")
    
    # Audit fields
    log_result: bool = Field(default=True, description="Log step result")
    require_approval: bool = Field(default=False, description="Require approval before execution")

    class Config:
        use_enum_values = True


class WorkflowSchedule(BaseModel):
    """Workflow schedule configuration."""
    enabled: bool = Field(default=False, description="Enable scheduling")
    cron: Optional[str] = Field(None, description="Cron expression")
    timezone: str = Field(default="UTC", description="Timezone")


class WorkflowGovernance(BaseModel):
    """Governance settings for workflow."""
    require_approval: bool = Field(default=False, description="Require approval to run")
    approvers: List[str] = Field(default_factory=list, description="List of approvers")
    audit_table: Optional[str] = Field(None, description="Audit log table")
    notification_emails: List[str] = Field(default_factory=list, description="Notification emails")


class Workflow(BaseModel):
    """Complete workflow definition."""
    name: str = Field(..., description="Workflow name")
    version: str = Field(default="1.0", description="Workflow version")
    description: Optional[str] = Field(None, description="Workflow description")
    
    # Workflow settings
    catalog: str = Field(..., description="Default Unity Catalog catalog")
    schema_: str = Field(..., alias="schema", description="Default Unity Catalog schema")
    warehouse_id: Optional[str] = Field(None, description="SQL Warehouse ID")
    
    # Steps
    steps: List[WorkflowStep] = Field(..., description="Workflow steps")
    
    # Governance
    governance: WorkflowGovernance = Field(default_factory=WorkflowGovernance)
    
    # Schedule
    schedule: Optional[WorkflowSchedule] = Field(None, description="Workflow schedule")
    
    # Metadata
    owner: Optional[str] = Field(None, description="Workflow owner")
    tags: List[str] = Field(default_factory=list, description="Workflow tags")
    created_at: Optional[datetime] = Field(None, description="Creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Update timestamp")

    @validator("steps")
    def validate_dependencies(cls, steps):
        """Validate step dependencies."""
        step_names = {step.name for step in steps}
        for step in steps:
            for dep in step.depends_on:
                if dep not in step_names:
                    raise ValueError(f"Step '{step.name}' depends on unknown step '{dep}'")
        return steps


class WorkflowExecutionContext(BaseModel):
    """Context for workflow execution."""
    workflow_name: str
    execution_id: str
    user: str
    started_at: datetime
    parameters: Dict[str, Any] = Field(default_factory=dict)
    step_results: Dict[str, Any] = Field(default_factory=dict)
    status: str = "running"
    error: Optional[str] = None


class WorkflowEngine:
    """Engine for executing YAML-defined workflows."""
    
    def __init__(self, adapter):
        """
        Initialize workflow engine.
        
        Args:
            adapter: DatabricksAdapter instance
        """
        self.adapter = adapter
        self.step_handlers: Dict[WorkflowStepType, Callable] = {
            WorkflowStepType.SQL: self._execute_sql_step,
            WorkflowStepType.PYTHON: self._execute_python_step,
            WorkflowStepType.APPROVAL: self._execute_approval_step,
            WorkflowStepType.VALIDATION: self._execute_validation_step,
            WorkflowStepType.NOTIFY: self._execute_notify_step,
            WorkflowStepType.DELTA: self._execute_delta_step,
            WorkflowStepType.JOB: self._execute_job_step,
        }
    
    @staticmethod
    def load_workflow(yaml_path: str) -> Workflow:
        """
        Load workflow from YAML file.
        
        Args:
            yaml_path: Path to YAML file
            
        Returns:
            Workflow object
        """
        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)
        return Workflow(**data)
    
    @staticmethod
    def validate_workflow(yaml_path: str) -> tuple[bool, Optional[str]]:
        """
        Validate workflow YAML without executing.
        
        Args:
            yaml_path: Path to YAML file
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            workflow = WorkflowEngine.load_workflow(yaml_path)
            return True, None
        except Exception as e:
            return False, str(e)
    
    async def execute_workflow(
        self,
        workflow: Workflow,
        user: str,
        parameters: Dict[str, Any] = None
    ) -> WorkflowExecutionContext:
        """
        Execute a workflow.
        
        Args:
            workflow: Workflow to execute
            user: User executing the workflow
            parameters: Runtime parameters
            
        Returns:
            WorkflowExecutionContext with results
        """
        import uuid
        
        # Create execution context
        context = WorkflowExecutionContext(
            workflow_name=workflow.name,
            execution_id=str(uuid.uuid4()),
            user=user,
            started_at=datetime.utcnow(),
            parameters=parameters or {}
        )
        
        # Check approval requirements
        if workflow.governance.require_approval:
            # In production, this would check approval status
            print(f"⚠️  Workflow requires approval from: {workflow.governance.approvers}")
        
        # Connect to Databricks
        await self.adapter.connect()
        
        try:
            # Execute steps in order, respecting dependencies
            executed_steps = set()
            
            for step in workflow.steps:
                # Check dependencies
                if not all(dep in executed_steps for dep in step.depends_on):
                    context.status = "failed"
                    context.error = f"Step '{step.name}' dependencies not met"
                    break
                
                # Execute step
                print(f"\n🔄 Executing step: {step.name} ({step.type})")
                
                try:
                    result = await self._execute_step(step, workflow, context)
                    context.step_results[step.name] = result
                    executed_steps.add(step.name)
                    print(f"✅ Step completed: {step.name}")
                    
                except Exception as e:
                    print(f"❌ Step failed: {step.name} - {e}")
                    
                    if step.on_failure == "fail":
                        context.status = "failed"
                        context.error = f"Step '{step.name}' failed: {str(e)}"
                        break
                    elif step.on_failure == "continue":
                        context.step_results[step.name] = {"error": str(e)}
                        executed_steps.add(step.name)
                        continue
                    elif step.on_failure == "retry" and step.retry_count > 0:
                        # Retry logic
                        for attempt in range(step.retry_count):
                            print(f"🔁 Retrying step {step.name} (attempt {attempt + 1}/{step.retry_count})")
                            try:
                                result = await self._execute_step(step, workflow, context)
                                context.step_results[step.name] = result
                                executed_steps.add(step.name)
                                print(f"✅ Step completed on retry: {step.name}")
                                break
                            except Exception as retry_e:
                                if attempt == step.retry_count - 1:
                                    context.status = "failed"
                                    context.error = f"Step '{step.name}' failed after {step.retry_count} retries"
                                    raise
            
            # Mark as completed if all steps succeeded
            if context.status == "running":
                context.status = "completed"
                print(f"\n🎉 Workflow completed successfully!")
            
        finally:
            await self.adapter.disconnect()
        
        return context
    
    async def _execute_step(
        self,
        step: WorkflowStep,
        workflow: Workflow,
        context: WorkflowExecutionContext
    ) -> Any:
        """Execute a single workflow step."""
        handler = self.step_handlers.get(step.type)
        if not handler:
            raise ValueError(f"Unknown step type: {step.type}")
        
        return await handler(step, workflow, context)
    
    async def _execute_sql_step(
        self,
        step: WorkflowStep,
        workflow: Workflow,
        context: WorkflowExecutionContext
    ) -> Any:
        """Execute SQL step."""
        if not step.query:
            raise ValueError(f"SQL step '{step.name}' missing query")
        
        # Replace parameters in query
        query = step.query
        for key, value in context.parameters.items():
            query = query.replace(f"${{{key}}}", str(value))
        
        print(f"   Query: {query[:100]}...")
        result = await self.adapter.execute_query(query)
        print(f"   Rows affected: {len(result) if result else 0}")
        
        return result
    
    async def _execute_python_step(
        self,
        step: WorkflowStep,
        workflow: Workflow,
        context: WorkflowExecutionContext
    ) -> Any:
        """Execute Python function step."""
        if not step.function:
            raise ValueError(f"Python step '{step.name}' missing function")
        
        # In production, this would securely execute registered functions
        print(f"   Function: {step.function}")
        print(f"   Params: {step.params}")
        
        return {"status": "executed", "function": step.function}
    
    async def _execute_approval_step(
        self,
        step: WorkflowStep,
        workflow: Workflow,
        context: WorkflowExecutionContext
    ) -> Any:
        """Execute approval step."""
        print(f"   Approver: {step.approver}")
        print(f"   Message: {step.approval_message}")
        
        # In production, this would check approval status
        return {"status": "approved", "approver": step.approver}
    
    async def _execute_validation_step(
        self,
        step: WorkflowStep,
        workflow: Workflow,
        context: WorkflowExecutionContext
    ) -> Any:
        """Execute validation step."""
        if not step.validation_query:
            raise ValueError(f"Validation step '{step.name}' missing validation_query")
        
        result = await self.adapter.execute_query(step.validation_query)
        
        # Check expected result
        if step.expected_result is not None:
            actual = result[0] if result else None
            if actual != step.expected_result:
                raise ValueError(f"Validation failed: expected {step.expected_result}, got {actual}")
        
        print(f"   ✅ Validation passed")
        return result
    
    async def _execute_notify_step(
        self,
        step: WorkflowStep,
        workflow: Workflow,
        context: WorkflowExecutionContext
    ) -> Any:
        """Execute notification step."""
        print(f"   📧 Sending notifications...")
        
        # In production, this would send actual notifications
        return {"status": "sent"}
    
    async def _execute_delta_step(
        self,
        step: WorkflowStep,
        workflow: Workflow,
        context: WorkflowExecutionContext
    ) -> Any:
        """Execute Delta Lake operation."""
        if not step.table or not step.operation:
            raise ValueError(f"Delta step '{step.name}' missing table or operation")
        
        if step.operation == "optimize":
            query = f"OPTIMIZE {step.table}"
        elif step.operation == "vacuum":
            query = f"VACUUM {step.table}"
        else:
            raise ValueError(f"Unknown Delta operation: {step.operation}")
        
        print(f"   Operation: {step.operation} on {step.table}")
        result = await self.adapter.execute_query(query)
        
        return result
    
    async def _execute_job_step(
        self,
        step: WorkflowStep,
        workflow: Workflow,
        context: WorkflowExecutionContext
    ) -> Any:
        """Execute Databricks job."""
        if not step.job_id:
            raise ValueError(f"Job step '{step.name}' missing job_id")
        
        print(f"   Job ID: {step.job_id}")
        
        # In production, this would trigger the job and wait for completion
        return {"status": "completed", "job_id": step.job_id}


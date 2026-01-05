"""
CLI commands for YAML workflow management.
"""
import click
import yaml
import asyncio
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich import print as rprint

from lakehouse_appkit.workflows import WorkflowEngine
from lakehouse_appkit.adapters.databricks import DatabricksAdapter
from lakehouse_appkit.config import get_config


console = Console()


@click.group(name="workflow")
def workflow_group():
    """Manage YAML-based workflows."""
    pass


@workflow_group.command(name="list")
def list_workflows():
    """List all available workflows."""
    workflows_dir = Path("workflows")
    if not workflows_dir.exists():
        console.print("[yellow]No workflows directory found[/yellow]")
        return
    
    yaml_files = list(workflows_dir.glob("*.yaml"))
    if not yaml_files:
        console.print("[yellow]No workflows found[/yellow]")
        return
    
    table = Table(title="Available Workflows")
    table.add_column("Name", style="cyan")
    table.add_column("Version", style="green")
    table.add_column("Description", style="white")
    table.add_column("Owner", style="yellow")
    table.add_column("Tags", style="magenta")
    
    for yaml_file in yaml_files:
        try:
            with open(yaml_file, 'r') as f:
                data = yaml.safe_load(f)
                table.add_row(
                    data.get("name", "Unknown"),
                    data.get("version", "N/A"),
                    data.get("description", "")[:50] + "..." if len(data.get("description", "")) > 50 else data.get("description", ""),
                    data.get("owner", "N/A"),
                    ", ".join(data.get("tags", []))
                )
        except Exception as e:
            console.print(f"[red]Error reading {yaml_file.name}: {e}[/red]")
    
    console.print(table)


@workflow_group.command(name="show")
@click.argument("workflow_name")
def show_workflow(workflow_name):
    """Show workflow definition."""
    yaml_path = Path(f"workflows/{workflow_name}.yaml")
    if not yaml_path.exists():
        console.print(f"[red]Workflow '{workflow_name}' not found[/red]")
        return
    
    try:
        workflow = WorkflowEngine.load_workflow(str(yaml_path))
        
        console.print(f"\n[bold cyan]Workflow: {workflow.name}[/bold cyan]")
        console.print(f"[dim]Version: {workflow.version}[/dim]")
        console.print(f"[dim]Description: {workflow.description}[/dim]")
        console.print(f"\n[bold]Catalog:[/bold] {workflow.catalog}")
        console.print(f"[bold]Schema:[/bold] {workflow.schema_}")
        
        # Governance
        console.print(f"\n[bold yellow]Governance:[/bold yellow]")
        console.print(f"  Require Approval: {workflow.governance.require_approval}")
        if workflow.governance.approvers:
            console.print(f"  Approvers: {', '.join(workflow.governance.approvers)}")
        
        # Steps
        console.print(f"\n[bold green]Steps ({len(workflow.steps)}):[/bold green]")
        for i, step in enumerate(workflow.steps, 1):
            console.print(f"  {i}. [cyan]{step.name}[/cyan] ([dim]{step.type}[/dim])")
            if step.description:
                console.print(f"     {step.description}")
            if step.depends_on:
                console.print(f"     [dim]Depends on: {', '.join(step.depends_on)}[/dim]")
        
        # Schedule
        if workflow.schedule and workflow.schedule.enabled:
            console.print(f"\n[bold]Schedule:[/bold] {workflow.schedule.cron} ({workflow.schedule.timezone})")
        
        # Metadata
        if workflow.tags:
            console.print(f"\n[bold]Tags:[/bold] {', '.join(workflow.tags)}")
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")


@workflow_group.command(name="validate")
@click.argument("workflow_file")
def validate_workflow(workflow_file):
    """Validate a workflow YAML file."""
    if not Path(workflow_file).exists():
        console.print(f"[red]File '{workflow_file}' not found[/red]")
        return
    
    console.print(f"Validating [cyan]{workflow_file}[/cyan]...")
    
    is_valid, error = WorkflowEngine.validate_workflow(workflow_file)
    
    if is_valid:
        console.print("[green]✓ Workflow is valid[/green]")
        
        # Show workflow info
        workflow = WorkflowEngine.load_workflow(workflow_file)
        console.print(f"\n[bold]Workflow:[/bold] {workflow.name}")
        console.print(f"[bold]Steps:[/bold] {len(workflow.steps)}")
        console.print(f"[bold]Governance:[/bold] {'Required' if workflow.governance.require_approval else 'Optional'}")
    else:
        console.print(f"[red]✗ Validation failed:[/red]\n{error}")


@workflow_group.command(name="execute")
@click.argument("workflow_name")
@click.option("--user", default="cli_user", help="User executing the workflow")
@click.option("--param", multiple=True, help="Parameters as key=value pairs")
def execute_workflow(workflow_name, user, param):
    """Execute a workflow."""
    yaml_path = Path(f"workflows/{workflow_name}.yaml")
    if not yaml_path.exists():
        console.print(f"[red]Workflow '{workflow_name}' not found[/red]")
        return
    
    # Parse parameters
    parameters = {}
    for p in param:
        if '=' in p:
            key, value = p.split('=', 1)
            parameters[key] = value
    
    async def run():
        try:
            # Load workflow
            workflow = WorkflowEngine.load_workflow(str(yaml_path))
            
            console.print(f"\n[bold cyan]Executing workflow: {workflow.name}[/bold cyan]")
            console.print(f"[dim]User: {user}[/dim]")
            if parameters:
                console.print(f"[dim]Parameters: {parameters}[/dim]\n")
            
            # Create adapter
            config = get_config()
            adapter = DatabricksAdapter(
                host=config.databricks.host,
                token=config.databricks.token,
                warehouse_id=config.databricks.warehouse_id
            )
            
            # Create engine
            engine = WorkflowEngine(adapter)
            
            # Execute
            context = await engine.execute_workflow(
                workflow=workflow,
                user=user,
                parameters=parameters
            )
            
            # Show results
            console.print(f"\n[bold]Execution Results:[/bold]")
            console.print(f"Execution ID: {context.execution_id}")
            console.print(f"Status: [{'green' if context.status == 'completed' else 'red'}]{context.status}[/]")
            
            if context.error:
                console.print(f"[red]Error: {context.error}[/red]")
            
            if context.step_results:
                console.print(f"\n[bold]Step Results:[/bold]")
                for step_name, result in context.step_results.items():
                    console.print(f"  [cyan]{step_name}[/cyan]: {result}")
            
        except Exception as e:
            console.print(f"[red]Execution failed: {e}[/red]")
    
    asyncio.run(run())


@workflow_group.command(name="create")
@click.argument("workflow_name")
def create_workflow(workflow_name):
    """Create a new workflow template."""
    yaml_path = Path(f"workflows/{workflow_name}.yaml")
    
    if yaml_path.exists():
        console.print(f"[red]Workflow '{workflow_name}' already exists[/red]")
        return
    
    # Create workflows directory if it doesn't exist
    yaml_path.parent.mkdir(exist_ok=True)
    
    # Template
    template = f"""# {workflow_name} Workflow
name: {workflow_name}
version: "1.0"
description: Description of your workflow

catalog: main
schema: default
warehouse_id: ${{DATABRICKS_SQL_WAREHOUSE_ID}}

governance:
  require_approval: false
  approvers: []
  audit_table: main.audit.workflow_runs

steps:
  - name: step_1
    type: sql
    description: First step
    query: >
      SELECT 1 as result

owner: your_email@company.com
tags:
  - custom
"""
    
    yaml_path.write_text(template)
    console.print(f"[green]✓ Created workflow template: {yaml_path}[/green]")
    console.print(f"\nEdit the file and then validate with:")
    console.print(f"  [cyan]lakehouse-appkit workflow validate {yaml_path}[/cyan]")


@workflow_group.command(name="test")
@click.argument("workflow_file")
def test_workflow(workflow_file):
    """Test workflow without executing (dry-run)."""
    if not Path(workflow_file).exists():
        console.print(f"[red]File '{workflow_file}' not found[/red]")
        return
    
    try:
        workflow = WorkflowEngine.load_workflow(workflow_file)
        
        console.print(f"\n[bold]Dry-run for workflow: {workflow.name}[/bold]\n")
        
        # Show execution plan
        console.print("[bold green]Execution Plan:[/bold green]")
        executed = set()
        
        for i, step in enumerate(workflow.steps, 1):
            # Check dependencies
            deps_met = all(dep in executed for dep in step.depends_on)
            status = "✓" if deps_met else "⚠"
            
            console.print(f"{status} Step {i}: [cyan]{step.name}[/cyan] ({step.type})")
            
            if step.depends_on:
                console.print(f"   Dependencies: {', '.join(step.depends_on)}")
            
            if step.query:
                console.print(f"   Query: {step.query[:60]}...")
            
            if step.require_approval:
                console.print(f"   [yellow]⚠ Requires approval[/yellow]")
            
            executed.add(step.name)
            console.print()
        
        console.print(f"[green]✓ Workflow structure is valid[/green]")
        console.print(f"[dim]Total steps: {len(workflow.steps)}[/dim]")
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")


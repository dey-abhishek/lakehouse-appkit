"""
Main CLI entry point for Lakehouse-AppKit.
"""
import click
from rich.console import Console

from lakehouse_appkit import __version__

console = Console()


@click.group()
@click.version_option(version=__version__, prog_name="lakehouse-appkit")
def cli():
    """
    Lakehouse-AppKit: CLI-driven app builder for FastAPI data applications.
    
    Build production-ready lakehouse data apps with Databricks integration.
    """
    pass


@cli.command()
@click.argument("app_name")
@click.option(
    "--template",
    "-t",
    type=click.Choice(["dashboard", "api", "admin", "analytics"]),
    default="dashboard",
    help="Application template to use",
)
@click.option(
    "--adapter",
    "-a",
    type=click.Choice(["databricks"]),
    default="databricks",
    help="Data platform adapter",
)
@click.option(
    "--path",
    "-p",
    default=".",
    help="Path where the app should be created",
)
def create(app_name: str, template: str, adapter: str, path: str):
    """Create a new Lakehouse-AppKit application."""
    from lakehouse_appkit.cli.commands.create import create_app
    
    console.print(f"[bold green]Creating new app:[/bold green] {app_name}")
    console.print(f"[cyan]Template:[/cyan] {template}")
    console.print(f"[cyan]Adapter:[/cyan] {adapter}")
    
    create_app(app_name, template, adapter, path)
    
    console.print(f"\n[bold green]✓[/bold green] App created successfully!")
    console.print(f"\n[bold]Next steps:[/bold]")
    console.print(f"  1. cd {app_name}")
    console.print(f"  2. lakehouse-appkit run")


@cli.command()
@click.option(
    "--host",
    default="0.0.0.0",
    help="Host to bind the server to",
)
@click.option(
    "--port",
    default=8000,
    type=int,
    help="Port to bind the server to",
)
@click.option(
    "--reload",
    is_flag=True,
    default=False,
    help="Enable auto-reload on code changes",
)
def run(host: str, port: int, reload: bool):
    """Run the Lakehouse-AppKit application locally."""
    from lakehouse_appkit.cli.commands.run import run_app
    
    console.print(f"[bold green]Starting Lakehouse-AppKit server...[/bold green]")
    console.print(f"[cyan]Host:[/cyan] {host}")
    console.print(f"[cyan]Port:[/cyan] {port}")
    
    run_app(host, port, reload)


@cli.command()
@click.option(
    "--target",
    "-t",
    type=click.Choice(["databricks", "kubernetes"]),
    required=True,
    help="Deployment target",
)
@click.option(
    "--config",
    "-c",
    help="Path to deployment configuration file",
)
def deploy(target: str, config: str):
    """Deploy the Lakehouse-AppKit application."""
    from lakehouse_appkit.cli.commands.deploy import deploy_app
    
    console.print(f"[bold green]Deploying to {target}...[/bold green]")
    
    deploy_app(target, config)
    
    console.print(f"\n[bold green]✓[/bold green] Deployment successful!")


@cli.command()
def init():
    """Initialize Lakehouse-AppKit configuration in the current directory."""
    from lakehouse_appkit.cli.commands.init import init_config
    
    console.print("[bold green]Initializing Lakehouse-AppKit configuration...[/bold green]")
    
    init_config()
    
    console.print("\n[bold green]✓[/bold green] Configuration initialized!")


@cli.command()
@click.argument("component")
def add(component: str):
    """Add a component to the current application."""
    from lakehouse_appkit.cli.commands.add import add_component
    
    console.print(f"[bold green]Adding component:[/bold green] {component}")
    
    add_component(component)
    
    console.print(f"\n[bold green]✓[/bold green] Component added successfully!")


# Register AI scaffolding commands
from lakehouse_appkit.cli.commands.ai import ai
cli.add_command(ai)

# Register Unity Catalog commands
from lakehouse_appkit.cli.commands.uc import uc
cli.add_command(uc)

# Register Secrets Management commands
from lakehouse_appkit.cli.commands.secrets import secrets
cli.add_command(secrets)

# Register OAuth & Service Principal commands
from lakehouse_appkit.cli.commands.oauth_sp import oauth, sp
cli.add_command(oauth)
cli.add_command(sp)

# Register Model Serving commands
from lakehouse_appkit.cli.commands.model_serving import model
cli.add_command(model)

# Register Jobs (Lakeflow) commands
from lakehouse_appkit.cli.commands.jobs import jobs
cli.add_command(jobs)

# Register Deployment commands
from lakehouse_appkit.cli.commands.deployment import deploy as deploy_cmd
cli.add_command(deploy_cmd)

# Register Vector Search commands
from lakehouse_appkit.cli.commands.vector_search import vector
cli.add_command(vector)

# Register Notebooks commands
from lakehouse_appkit.cli.commands.notebooks import notebook
cli.add_command(notebook)

# Register Delta Lake commands
from lakehouse_appkit.cli.commands.delta import delta as delta_cmd
cli.add_command(delta_cmd)

# Register Genie commands
from lakehouse_appkit.cli.commands.genie import genie
cli.add_command(genie)

# Register MLflow commands
from lakehouse_appkit.cli.commands.mlflow_cmd import mlflow_cmd
cli.add_command(mlflow_cmd)

# Register Connections commands
from lakehouse_appkit.cli.commands.connections import connections
cli.add_command(connections)

# Register Functions commands
from lakehouse_appkit.cli.commands.functions_cmd import functions_cmd
cli.add_command(functions_cmd)

# Register AI Providers commands
from lakehouse_appkit.cli.commands.ai_providers_cmd import ai_providers_cmd
cli.add_command(ai_providers_cmd)


if __name__ == "__main__":
    cli()

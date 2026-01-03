"""CLI commands for mlflow_cmd."""
import click
from rich.console import Console

console = Console()

@click.group(name="mlflow_cmd")
def mlflow_cmd():
    """mlflow_cmd commands."""
    pass

@mlflow_cmd.command("list")
def list_items():
    """List items."""
    console.print("[yellow]mlflow_cmd list - not yet implemented[/yellow]")

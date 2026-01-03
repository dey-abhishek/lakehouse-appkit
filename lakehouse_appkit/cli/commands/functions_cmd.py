"""CLI commands for functions_cmd."""
import click
from rich.console import Console

console = Console()

@click.group(name="functions_cmd")
def functions_cmd():
    """functions_cmd commands."""
    pass

@functions_cmd.command("list")
def list_items():
    """List items."""
    console.print("[yellow]functions_cmd list - not yet implemented[/yellow]")

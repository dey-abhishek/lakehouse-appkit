"""CLI commands for connections."""
import click
from rich.console import Console

console = Console()

@click.group(name="connections")
def connections():
    """connections commands."""
    pass

@connections.command("list")
def list_items():
    """List items."""
    console.print("[yellow]connections list - not yet implemented[/yellow]")

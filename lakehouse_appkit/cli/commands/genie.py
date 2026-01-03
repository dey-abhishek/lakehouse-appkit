"""CLI commands for genie."""
import click
from rich.console import Console

console = Console()

@click.group(name="genie")
def genie():
    """genie commands."""
    pass

@genie.command("list")
def list_items():
    """List items."""
    console.print("[yellow]genie list - not yet implemented[/yellow]")

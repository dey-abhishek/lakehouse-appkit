"""CLI commands for ai_providers_cmd."""
import click
from rich.console import Console

console = Console()

@click.group(name="ai_providers_cmd")
def ai_providers_cmd():
    """ai_providers_cmd commands."""
    pass

@ai_providers_cmd.command("list")
def list_items():
    """List items."""
    console.print("[yellow]ai_providers_cmd list - not yet implemented[/yellow]")

"""
CLI commands for OAuth and Service Principals.
"""
import click
from rich.console import Console

console = Console()

@click.group(name="oauth")
def oauth():
    """OAuth token management."""
    pass

@oauth.command("get-token")
def get_token():
    """Get OAuth access token."""
    from lakehouse_appkit.oauth import DatabricksOAuthManager
    from lakehouse_appkit.config import get_config
    import asyncio
    
    async def _get():
        config = get_config()
        manager = DatabricksOAuthManager(
            client_id=config.databricks.client_id,
            client_secret=config.databricks.client_secret,
            host=config.databricks.host
        )
        token = await manager.get_token()
        console.print(f"[green]Token:[/green] {token}")
    
    asyncio.run(_get())

@click.group(name="sp")
def sp():
    """Service Principal management."""
    pass

@sp.command("list")
def list_sp():
    """List service principals."""
    console.print("[yellow]Service Principal list - not yet implemented[/yellow]")

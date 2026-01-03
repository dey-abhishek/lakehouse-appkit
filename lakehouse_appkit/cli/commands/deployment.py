"""
CLI commands for Databricks Apps deployment.
"""
import click
import asyncio
from rich.console import Console

console = Console()


@click.group(name="deploy")
def deploy():
    """Deploy and manage Databricks Apps."""
    pass


@deploy.command("list-apps")
def list_apps():
    """List deployed apps."""
    from lakehouse_appkit.deployment import DatabricksAppsClient
    from lakehouse_appkit.config import get_config
    
    async def _list():
        config = get_config()
        client = DatabricksAppsClient(
            host=config.databricks.host,
            token=config.databricks.token
        )
        
        apps = await client.list_apps()
        
        if not apps:
            console.print("[yellow]No apps found[/yellow]")
            await client.close()
            return
        
        console.print("[bold]Deployed Apps:[/bold]")
        for app in apps:
            console.print(f"  • {app.get('name', '')}")
        
        await client.close()
    
    asyncio.run(_list())


@deploy.command("create-app")
@click.argument("name")
@click.option("--description", help="App description")
def create_app(name: str, description: str):
    """Create a new app."""
    from lakehouse_appkit.deployment import DatabricksAppsClient
    from lakehouse_appkit.config import get_config
    
    async def _create():
        config = get_config()
        client = DatabricksAppsClient(
            host=config.databricks.host,
            token=config.databricks.token
        )
        
        app = await client.create_app(name=name, description=description)
        console.print(f"[green]✓[/green] Created app: {app.get('name')}")
        await client.close()
    
    asyncio.run(_create())

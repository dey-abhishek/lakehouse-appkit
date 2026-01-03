"""
CLI commands for Vector Search.
"""
import click
import asyncio
from rich.console import Console
from rich.table import Table

console = Console()


@click.group(name="vector")
def vector():
    """Manage Databricks Vector Search."""
    pass


@vector.command("list-endpoints")
def list_endpoints():
    """List vector search endpoints."""
    from lakehouse_appkit.vector_search import DatabricksVectorSearchClient
    from lakehouse_appkit.config import get_config
    
    async def _list():
        config = get_config()
        client = DatabricksVectorSearchClient(
            host=config.databricks.host,
            token=config.databricks.token
        )
        
        endpoints = await client.list_endpoints()
        
        if not endpoints:
            console.print("[yellow]No vector search endpoints found[/yellow]")
            await client.close()
            return
        
        table = Table(title="Vector Search Endpoints")
        table.add_column("Name", style="cyan")
        table.add_column("Type", style="green")
        table.add_column("Creator", style="yellow")
        
        for endpoint in endpoints:
            table.add_row(
                endpoint.get("name", ""),
                endpoint.get("endpoint_type", ""),
                endpoint.get("creator", "")
            )
        
        console.print(table)
        await client.close()
    
    asyncio.run(_list())


@vector.command("list-indexes")
@click.option("--endpoint", required=True, help="Endpoint name")
def list_indexes(endpoint: str):
    """List vector indexes."""
    from lakehouse_appkit.vector_search import DatabricksVectorSearchClient
    from lakehouse_appkit.config import get_config
    
    async def _list():
        config = get_config()
        client = DatabricksVectorSearchClient(
            host=config.databricks.host,
            token=config.databricks.token
        )
        
        indexes = await client.list_indexes(endpoint_name=endpoint)
        
        if not indexes:
            console.print(f"[yellow]No indexes found for endpoint: {endpoint}[/yellow]")
            await client.close()
            return
        
        table = Table(title=f"Indexes on {endpoint}")
        table.add_column("Name", style="cyan")
        table.add_column("Status", style="green")
        
        for index in indexes:
            table.add_row(
                index.get("name", ""),
                index.get("status", {}).get("state", "")
            )
        
        console.print(table)
        await client.close()
    
    asyncio.run(_list())

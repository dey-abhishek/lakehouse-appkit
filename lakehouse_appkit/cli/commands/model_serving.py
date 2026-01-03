"""
CLI commands for Model Serving.
"""
import click
import asyncio
from rich.console import Console
from rich.table import Table
import json

console = Console()


@click.group(name="model")
def model():
    """Manage Databricks Model Serving."""
    pass


@model.command("list-endpoints")
def list_endpoints():
    """List serving endpoints."""
    from lakehouse_appkit.model_serving import DatabricksModelServingClient
    from lakehouse_appkit.config import get_config
    
    async def _list():
        config = get_config()
        client = DatabricksModelServingClient(
            host=config.databricks.host,
            token=config.databricks.token
        )
        
        endpoints = await client.list_endpoints()
        
        if not endpoints:
            console.print("[yellow]No serving endpoints found[/yellow]")
            await client.close()
            return
        
        table = Table(title="Model Serving Endpoints")
        table.add_column("Name", style="cyan")
        table.add_column("State", style="green")
        table.add_column("Creator", style="yellow")
        
        for endpoint in endpoints:
            table.add_row(
                endpoint.get("name", ""),
                endpoint.get("state", {}).get("ready", ""),
                endpoint.get("creator", "")
            )
        
        console.print(table)
        await client.close()
    
    asyncio.run(_list())


@model.command("predict")
@click.argument("endpoint")
@click.option("--data", required=True, help="JSON input data")
def predict(endpoint: str, data: str):
    """Make a prediction."""
    from lakehouse_appkit.model_serving import DatabricksModelServingClient
    from lakehouse_appkit.config import get_config
    
    async def _predict():
        config = get_config()
        client = DatabricksModelServingClient(
            host=config.databricks.host,
            token=config.databricks.token
        )
        
        input_data = json.loads(data)
        result = await client.predict(endpoint, input_data)
        
        console.print("[bold]Prediction result:[/bold]")
        console.print(json.dumps(result, indent=2))
        await client.close()
    
    asyncio.run(_predict())

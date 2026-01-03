"""
CLI commands for Notebooks.
"""
import click
import asyncio
from rich.console import Console

console = Console()


@click.group(name="notebook")
def notebook():
    """Manage Databricks Notebooks."""
    pass


@notebook.command("list")
@click.argument("path", default="/Users")
def list_notebooks(path: str):
    """List notebooks in a path."""
    from lakehouse_appkit.notebooks import DatabricksNotebooksClient
    from lakehouse_appkit.config import get_config
    
    async def _list():
        config = get_config()
        client = DatabricksNotebooksClient(
            host=config.databricks.host,
            token=config.databricks.token
        )
        
        notebooks = await client.list_notebooks(path)
        
        if isinstance(notebooks, dict):
            notebooks = notebooks.get("objects", [])
        
        if not notebooks:
            console.print(f"[yellow]No notebooks found in: {path}[/yellow]")
            await client.close()
            return
        
        console.print(f"[bold]Notebooks in {path}:[/bold]")
        for nb in notebooks:
            console.print(f"  • {nb.get('path', '')}")
        
        await client.close()
    
    asyncio.run(_list())


@notebook.command("export")
@click.argument("path")
@click.option("--format", default="SOURCE", help="Export format")
@click.option("--output", help="Output file")
def export_notebook(path: str, format: str, output: str):
    """Export a notebook."""
    from lakehouse_appkit.notebooks import DatabricksNotebooksClient
    from lakehouse_appkit.config import get_config
    
    async def _export():
        config = get_config()
        client = DatabricksNotebooksClient(
            host=config.databricks.host,
            token=config.databricks.token
        )
        
        content = await client.export_notebook(path, format=format)
        
        if output:
            with open(output, 'w') as f:
                f.write(content)
            console.print(f"[green]✓[/green] Exported to: {output}")
        else:
            console.print(content)
        
        await client.close()
    
    asyncio.run(_export())

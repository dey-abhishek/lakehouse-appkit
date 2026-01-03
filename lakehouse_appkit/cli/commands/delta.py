"""
CLI commands for Delta Lake operations.
"""
import click
import asyncio
from rich.console import Console

console = Console()


@click.group(name="delta")
def delta():
    """Delta Lake operations."""
    pass


@delta.command("optimize")
@click.argument("table")
def optimize(table: str):
    """Optimize a Delta table."""
    from lakehouse_appkit.delta import DeltaLakeManager
    from lakehouse_appkit.config import get_config
    
    async def _optimize():
        config = get_config()
        manager = DeltaLakeManager(
            host=config.databricks.host,
            token=config.databricks.token,
            warehouse_id=config.databricks.warehouse_id
        )
        
        await manager.optimize_table(table)
        console.print(f"[green]✓[/green] Optimized table: {table}")
    
    asyncio.run(_optimize())


@delta.command("vacuum")
@click.argument("table")
@click.option("--retention-hours", default=168, help="Retention hours")
def vacuum(table: str, retention_hours: int):
    """Vacuum a Delta table."""
    from lakehouse_appkit.delta import DeltaLakeManager
    from lakehouse_appkit.config import get_config
    
    async def _vacuum():
        config = get_config()
        manager = DeltaLakeManager(
            host=config.databricks.host,
            token=config.databricks.token,
            warehouse_id=config.databricks.warehouse_id
        )
        
        await manager.vacuum_table(table, retention_hours=retention_hours)
        console.print(f"[green]✓[/green] Vacuumed table: {table}")
    
    asyncio.run(_vacuum())


@delta.command("history")
@click.argument("table")
@click.option("--limit", default=10, help="Number of entries")
def history(table: str, limit: int):
    """Show Delta table history."""
    from lakehouse_appkit.delta import DeltaLakeManager
    from lakehouse_appkit.config import get_config
    import json
    
    async def _history():
        config = get_config()
        manager = DeltaLakeManager(
            host=config.databricks.host,
            token=config.databricks.token,
            warehouse_id=config.databricks.warehouse_id
        )
        
        hist = await manager.describe_history(table, limit=limit)
        console.print(json.dumps(hist, indent=2))
    
    asyncio.run(_history())

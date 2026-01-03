"""
Unity Catalog CLI commands.
"""
import asyncio
import click
import json
from typing import Optional
from rich.console import Console
from rich.table import Table
from rich.tree import Tree

from lakehouse_appkit.adapters.databricks import DatabricksAdapter
from lakehouse_appkit.unity_catalog import UnityCatalogManager

console = Console()


@click.group()
def uc():
    """Unity Catalog commands."""
    pass


@uc.command()
@click.option('--workspace', '-w', required=True, help='Databricks workspace URL')
@click.option('--token', '-t', required=True, help='Databricks access token')
def list_catalogs(workspace: str, token: str):
    """List all catalogs in the metastore."""
    async def _run():
        adapter = DatabricksAdapter(workspace_url=workspace, token=token)
        uc_manager = UnityCatalogManager(adapter)
        
        console.print("\n[bold]📁 Catalogs[/bold]\n")
        
        catalogs = await uc_manager.list_catalogs()
        
        if not catalogs:
            console.print("[dim]No catalogs found[/dim]")
            return
        
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Name", style="cyan")
        table.add_column("Owner", style="green")
        table.add_column("Comment", style="dim")
        
        for catalog in catalogs:
            table.add_row(
                catalog.name,
                catalog.owner or "-",
                catalog.comment or "-"
            )
        
        console.print(table)
        console.print(f"\n[dim]Total: {len(catalogs)} catalogs[/dim]")
    
    asyncio.run(_run())


@uc.command()
@click.option('--workspace', '-w', required=True, help='Databricks workspace URL')
@click.option('--token', '-t', required=True, help='Databricks access token')
@click.option('--catalog', '-c', required=True, help='Catalog name')
def list_schemas(workspace: str, token: str, catalog: str):
    """List all schemas in a catalog."""
    async def _run():
        adapter = DatabricksAdapter(workspace_url=workspace, token=token)
        uc_manager = UnityCatalogManager(adapter)
        
        console.print(f"\n[bold]📂 Schemas in {catalog}[/bold]\n")
        
        schemas = await uc_manager.list_schemas(catalog)
        
        if not schemas:
            console.print("[dim]No schemas found[/dim]")
            return
        
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Name", style="cyan")
        table.add_column("Full Name", style="blue")
        table.add_column("Comment", style="dim")
        
        for schema in schemas:
            table.add_row(
                schema.name,
                schema.full_name,
                schema.comment or "-"
            )
        
        console.print(table)
        console.print(f"\n[dim]Total: {len(schemas)} schemas[/dim]")
    
    asyncio.run(_run())


@uc.command()
@click.option('--workspace', '-w', required=True, help='Databricks workspace URL')
@click.option('--token', '-t', required=True, help='Databricks access token')
@click.option('--catalog', '-c', required=True, help='Catalog name')
@click.option('--schema', '-s', required=True, help='Schema name')
@click.option('--include-views/--no-views', default=True, help='Include views')
def list_tables(workspace: str, token: str, catalog: str, schema: str, include_views: bool):
    """List all tables in a schema."""
    async def _run():
        adapter = DatabricksAdapter(workspace_url=workspace, token=token)
        uc_manager = UnityCatalogManager(adapter)
        
        console.print(f"\n[bold]📊 Tables in {catalog}.{schema}[/bold]\n")
        
        tables = await uc_manager.list_tables(catalog, schema, include_views)
        
        if not tables:
            console.print("[dim]No tables found[/dim]")
            return
        
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Name", style="cyan")
        table.add_column("Type", style="yellow")
        table.add_column("Full Name", style="blue")
        
        for tbl in tables:
            table.add_row(
                tbl.name,
                tbl.table_type.value,
                tbl.full_name
            )
        
        console.print(table)
        console.print(f"\n[dim]Total: {len(tables)} tables[/dim]")
    
    asyncio.run(_run())


@uc.command()
@click.option('--workspace', '-w', required=True, help='Databricks workspace URL')
@click.option('--token', '-t', required=True, help='Databricks access token')
@click.option('--catalog', '-c', required=True, help='Catalog name')
@click.option('--schema', '-s', required=True, help='Schema name')
@click.option('--table', '-t', 'table_name', required=True, help='Table name')
def describe(workspace: str, token: str, catalog: str, schema: str, table_name: str):
    """Get detailed table information."""
    async def _run():
        adapter = DatabricksAdapter(workspace_url=workspace, token=token)
        uc_manager = UnityCatalogManager(adapter)
        
        console.print(f"\n[bold]📊 Table Details: {catalog}.{schema}.{table_name}[/bold]\n")
        
        table = await uc_manager.get_table_details(catalog, schema, table_name)
        
        # Table info
        console.print(f"[bold]Type:[/bold] {table.table_type.value}")
        console.print(f"[bold]Owner:[/bold] {table.owner or 'Unknown'}")
        console.print(f"[bold]Location:[/bold] {table.storage_location or 'N/A'}")
        if table.comment:
            console.print(f"[bold]Comment:[/bold] {table.comment}")
        console.print()
        
        # Columns
        console.print("[bold]Columns:[/bold]\n")
        col_table = Table(show_header=True, header_style="bold magenta")
        col_table.add_column("Name", style="cyan")
        col_table.add_column("Type", style="yellow")
        col_table.add_column("Nullable", style="green")
        col_table.add_column("Comment", style="dim")
        
        for col in table.columns:
            col_table.add_row(
                col.name,
                col.type_name,
                "Yes" if col.nullable else "No",
                col.comment or "-"
            )
        
        console.print(col_table)
        
        # Stats
        if table.row_count:
            console.print(f"\n[dim]Row count: {table.row_count:,}[/dim]")
    
    asyncio.run(_run())


@uc.command()
@click.option('--workspace', '-w', required=True, help='Databricks workspace URL')
@click.option('--token', '-t', required=True, help='Databricks access token')
@click.option('--catalog', '-c', required=True, help='Catalog name')
@click.option('--schema', '-s', required=True, help='Schema name')
@click.option('--table', '-t', 'table_name', required=True, help='Table name')
@click.option('--limit', '-l', default=10, help='Number of rows to show')
def sample(workspace: str, token: str, catalog: str, schema: str, table_name: str, limit: int):
    """Show sample data from a table."""
    async def _run():
        adapter = DatabricksAdapter(workspace_url=workspace, token=token)
        uc_manager = UnityCatalogManager(adapter)
        
        console.print(f"\n[bold]🔍 Sample Data: {catalog}.{schema}.{table_name}[/bold]\n")
        
        sample_data = await uc_manager.get_table_sample(catalog, schema, table_name, limit)
        
        if not sample_data:
            console.print("[dim]No data in table[/dim]")
            return
        
        # Create table
        table = Table(show_header=True, header_style="bold magenta")
        
        # Add columns
        for col_name in sample_data[0].keys():
            table.add_column(col_name, style="cyan")
        
        # Add rows
        for row in sample_data:
            table.add_row(*[str(v) if v is not None else "[dim]NULL[/dim]" for v in row.values()])
        
        console.print(table)
        console.print(f"\n[dim]Showing {len(sample_data)} of {limit} rows[/dim]")
    
    asyncio.run(_run())


@uc.command()
@click.option('--workspace', '-w', required=True, help='Databricks workspace URL')
@click.option('--token', '-t', required=True, help='Databricks access token')
@click.argument('search_term')
@click.option('--catalog', '-c', multiple=True, help='Catalogs to search (can specify multiple)')
@click.option('--limit', '-l', default=20, help='Maximum results')
def search(workspace: str, token: str, search_term: str, catalog: tuple, limit: int):
    """Search for tables across catalogs."""
    async def _run():
        adapter = DatabricksAdapter(workspace_url=workspace, token=token)
        uc_manager = UnityCatalogManager(adapter)
        
        console.print(f"\n[bold]🔍 Searching for: '{search_term}'[/bold]\n")
        
        catalogs = list(catalog) if catalog else None
        results = await uc_manager.search_tables(search_term, catalogs, limit)
        
        if not results:
            console.print("[dim]No results found[/dim]")
            return
        
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Full Name", style="cyan")
        table.add_column("Type", style="yellow")
        table.add_column("Catalog", style="blue")
        table.add_column("Schema", style="green")
        
        for tbl in results:
            table.add_row(
                tbl.full_name,
                tbl.table_type.value,
                tbl.catalog_name,
                tbl.schema_name
            )
        
        console.print(table)
        console.print(f"\n[dim]Found {len(results)} results[/dim]")
    
    asyncio.run(_run())


@uc.command()
@click.option('--workspace', '-w', required=True, help='Databricks workspace URL')
@click.option('--token', '-t', required=True, help='Databricks access token')
@click.option('--catalog', '-c', required=True, help='Catalog name')
@click.option('--schema', '-s', required=True, help='Schema name')
@click.option('--table', '-t', 'table_name', required=True, help='Table name')
def lineage(workspace: str, token: str, catalog: str, schema: str, table_name: str):
    """Show lineage for a table."""
    async def _run():
        adapter = DatabricksAdapter(workspace_url=workspace, token=token)
        uc_manager = UnityCatalogManager(adapter)
        
        console.print(f"\n[bold]🔗 Lineage: {catalog}.{schema}.{table_name}[/bold]\n")
        
        lineage_info = await uc_manager.get_table_lineage(catalog, schema, table_name)
        
        if lineage_info.upstream:
            console.print("[bold green]Upstream (Sources):[/bold green]")
            for table in lineage_info.upstream:
                console.print(f"  ← {table}")
            console.print()
        
        console.print(f"[bold yellow]Current:[/bold yellow] {lineage_info.object_name}\n")
        
        if lineage_info.downstream:
            console.print("[bold red]Downstream (Consumers):[/bold red]")
            for table in lineage_info.downstream:
                console.print(f"  → {table}")
        
        if not lineage_info.upstream and not lineage_info.downstream:
            console.print("[dim]No lineage information available[/dim]")
    
    asyncio.run(_run())


@uc.command()
@click.option('--workspace', '-w', required=True, help='Databricks workspace URL')
@click.option('--token', '-t', required=True, help='Databricks access token')
def tree(workspace: str, token: str):
    """Show complete catalog tree."""
    async def _run():
        adapter = DatabricksAdapter(workspace_url=workspace, token=token)
        uc_manager = UnityCatalogManager(adapter)
        
        console.print("\n[bold]🌳 Catalog Tree[/bold]\n")
        
        with console.status("[bold green]Loading catalog tree..."):
            catalog_tree = await uc_manager.get_catalog_tree()
        
        tree = Tree("📁 Unity Catalog")
        
        for catalog_name, catalog_data in catalog_tree.items():
            catalog_node = tree.add(f"📁 [cyan]{catalog_name}[/cyan]")
            
            for schema_name, schema_data in catalog_data['schemas'].items():
                schema_node = catalog_node.add(f"📂 [blue]{schema_name}[/blue]")
                
                # Add tables
                if schema_data['tables']:
                    tables_node = schema_node.add(f"Tables ({len(schema_data['tables'])})")
                    for table in schema_data['tables'][:5]:  # Show first 5
                        tables_node.add(f"📊 [green]{table.name}[/green]")
                    if len(schema_data['tables']) > 5:
                        tables_node.add(f"[dim]... and {len(schema_data['tables']) - 5} more[/dim]")
                
                # Add volumes
                if schema_data['volumes']:
                    volumes_node = schema_node.add(f"Volumes ({len(schema_data['volumes'])})")
                    for volume in schema_data['volumes'][:3]:  # Show first 3
                        volumes_node.add(f"💾 [yellow]{volume.name}[/yellow]")
                    if len(schema_data['volumes']) > 3:
                        volumes_node.add(f"[dim]... and {len(schema_data['volumes']) - 3} more[/dim]")
        
        console.print(tree)
    
    asyncio.run(_run())


@uc.command()
@click.option('--workspace', '-w', required=True, help='Databricks workspace URL')
@click.option('--token', '-t', required=True, help='Databricks access token')
@click.option('--catalog', '-c', required=True, help='Catalog name')
@click.option('--schema', '-s', required=True, help='Schema name')
def volumes(workspace: str, token: str, catalog: str, schema: str):
    """List volumes in a schema."""
    async def _run():
        adapter = DatabricksAdapter(workspace_url=workspace, token=token)
        uc_manager = UnityCatalogManager(adapter)
        
        console.print(f"\n[bold]💾 Volumes in {catalog}.{schema}[/bold]\n")
        
        volumes = await uc_manager.list_volumes(catalog, schema)
        
        if not volumes:
            console.print("[dim]No volumes found[/dim]")
            return
        
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Name", style="cyan")
        table.add_column("Type", style="yellow")
        table.add_column("Storage Location", style="dim")
        
        for volume in volumes:
            table.add_row(
                volume.name,
                volume.volume_type,
                volume.storage_location or "-"
            )
        
        console.print(table)
        console.print(f"\n[dim]Total: {len(volumes)} volumes[/dim]")
    
    asyncio.run(_run())


# Export functions for testing
__all__ = ['uc', 'list_catalogs', 'list_schemas', 'list_tables', 'describe', 'sample', 'search', 'lineage', 'tree', 'volumes']


if __name__ == '__main__':
    uc()


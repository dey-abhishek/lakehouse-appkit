"""
CLI commands for Databricks Secrets Management.
"""
import click
import asyncio
from rich.console import Console
from rich.table import Table

console = Console()


@click.group(name="secrets")
def secrets():
    """Manage Databricks secrets (scopes, secrets, ACLs)."""
    pass


@secrets.command("list-scopes")
def list_scopes():
    """List all secret scopes."""
    from lakehouse_appkit.secrets import DatabricksSecretsClient
    from lakehouse_appkit.config import get_config
    
    async def _list():
        config = get_config()
        client = DatabricksSecretsClient(
            host=config.databricks.host,
            token=config.databricks.token
        )
        
        scopes = await client.list_scopes()
        
        if not scopes:
            console.print("[yellow]No secret scopes found[/yellow]")
            return
        
        table = Table(title="Secret Scopes")
        table.add_column("Scope", style="cyan")
        table.add_column("Backend Type", style="green")
        
        for scope in scopes:
            table.add_row(scope.get("name", ""), scope.get("backend_type", ""))
        
        console.print(table)
        await client.close()
    
    asyncio.run(_list())


@secrets.command("create-scope")
@click.argument("scope")
@click.option("--backend", default="DATABRICKS", help="Backend type")
def create_scope(scope: str, backend: str):
    """Create a new secret scope."""
    from lakehouse_appkit.secrets import DatabricksSecretsClient
    from lakehouse_appkit.config import get_config
    
    async def _create():
        config = get_config()
        client = DatabricksSecretsClient(
            host=config.databricks.host,
            token=config.databricks.token
        )
        
        await client.create_scope(scope, backend_type=backend)
        console.print(f"[green]✓[/green] Created scope: {scope}")
        await client.close()
    
    asyncio.run(_create())


@secrets.command("delete-scope")
@click.argument("scope")
@click.confirmation_option(prompt="Are you sure you want to delete this scope?")
def delete_scope(scope: str):
    """Delete a secret scope."""
    from lakehouse_appkit.secrets import DatabricksSecretsClient
    from lakehouse_appkit.config import get_config
    
    async def _delete():
        config = get_config()
        client = DatabricksSecretsClient(
            host=config.databricks.host,
            token=config.databricks.token
        )
        
        await client.delete_scope(scope)
        console.print(f"[green]✓[/green] Deleted scope: {scope}")
        await client.close()
    
    asyncio.run(_delete())


@secrets.command("list-secrets")
@click.argument("scope")
def list_secrets(scope: str):
    """List secrets in a scope."""
    from lakehouse_appkit.secrets import DatabricksSecretsClient
    from lakehouse_appkit.config import get_config
    
    async def _list():
        config = get_config()
        client = DatabricksSecretsClient(
            host=config.databricks.host,
            token=config.databricks.token
        )
        
        secrets = await client.list_secrets(scope)
        
        if not secrets:
            console.print(f"[yellow]No secrets found in scope: {scope}[/yellow]")
            await client.close()
            return
        
        table = Table(title=f"Secrets in {scope}")
        table.add_column("Key", style="cyan")
        table.add_column("Last Updated", style="yellow")
        
        for secret in secrets:
            table.add_row(
                secret.get("key", ""),
                str(secret.get("last_updated_timestamp", ""))
            )
        
        console.print(table)
        await client.close()
    
    asyncio.run(_list())


@secrets.command("put-secret")
@click.argument("scope")
@click.argument("key")
@click.option("--value", prompt=True, hide_input=True, help="Secret value")
def put_secret(scope: str, key: str, value: str):
    """Put a secret in a scope."""
    from lakehouse_appkit.secrets import DatabricksSecretsClient
    from lakehouse_appkit.config import get_config
    
    async def _put():
        config = get_config()
        client = DatabricksSecretsClient(
            host=config.databricks.host,
            token=config.databricks.token
        )
        
        await client.put_secret(scope, key, value)
        console.print(f"[green]✓[/green] Secret '{key}' saved in scope '{scope}'")
        await client.close()
    
    asyncio.run(_put())


@secrets.command("delete-secret")
@click.argument("scope")
@click.argument("key")
@click.confirmation_option(prompt="Are you sure you want to delete this secret?")
def delete_secret(scope: str, key: str):
    """Delete a secret from a scope."""
    from lakehouse_appkit.secrets import DatabricksSecretsClient
    from lakehouse_appkit.config import get_config
    
    async def _delete():
        config = get_config()
        client = DatabricksSecretsClient(
            host=config.databricks.host,
            token=config.databricks.token
        )
        
        await client.delete_secret(scope, key)
        console.print(f"[green]✓[/green] Deleted secret '{key}' from scope '{scope}'")
        await client.close()
    
    asyncio.run(_delete())

"""
CLI commands for AI scaffolding.
"""
import click
import asyncio
from rich.console import Console
from rich.table import Table

console = Console()


@click.group(name="ai")
def ai():
    """AI-assisted app scaffolding commands."""
    pass


@ai.command("generate-endpoint")
@click.argument("name")
@click.option("--method", default="GET", help="HTTP method (GET, POST, PUT, DELETE)")
@click.option("--path", default=None, help="API path (default: /{name})")
@click.option("--provider", default="claude", help="AI provider (openai, claude, gemini)")
def generate_endpoint(name: str, method: str, path: str, provider: str):
    """Generate a FastAPI endpoint using AI."""
    from lakehouse_appkit.sdk.ai_scaffolding import AIScaffolder
    from lakehouse_appkit.config import get_config
    
    console.print(f"[bold green]Generating endpoint:[/bold green] {name}")
    console.print(f"[cyan]Method:[/cyan] {method}")
    console.print(f"[cyan]Provider:[/cyan] {provider}")
    
    async def _generate():
        config = get_config()
        scaffolder = AIScaffolder(config)
        
        prompt = f"Create a FastAPI {method} endpoint named {name}"
        if path:
            prompt += f" at path {path}"
        
        code = await scaffolder.generate_endpoint(
            name=name,
            method=method,
            prompt=prompt,
            provider=provider
        )
        
        console.print("\n[bold]Generated code:[/bold]")
        console.print(code)
        
        # Save to file
        filename = f"{name}_endpoint.py"
        with open(filename, 'w') as f:
            f.write(code)
        console.print(f"\n[green]✓[/green] Saved to {filename}")
    
    asyncio.run(_generate())


@ai.command("endpoint")
@click.argument("name")
@click.option("--method", default="GET", help="HTTP method (GET, POST, PUT, DELETE)")
@click.option("--path", default=None, help="API path (default: /{name})")
@click.option("--provider", default="claude", help="AI provider (openai, claude, gemini)")
def endpoint_alias(name: str, method: str, path: str, provider: str):
    """Generate a FastAPI endpoint using AI (alias for generate-endpoint)."""
    # Just call the main function
    generate_endpoint.callback(name, method, path, provider)


@ai.command("generate-adapter")
@click.argument("name")
@click.option("--platform", default="databricks", help="Data platform")
@click.option("--provider", default="claude", help="AI provider")
def generate_adapter(name: str, platform: str, provider: str):
    """Generate a data adapter using AI."""
    from lakehouse_appkit.sdk.ai_scaffolding import AIScaffolder
    from lakehouse_appkit.config import get_config
    
    console.print(f"[bold green]Generating adapter:[/bold green] {name}")
    console.print(f"[cyan]Platform:[/cyan] {platform}")
    
    async def _generate():
        config = get_config()
        scaffolder = AIScaffolder(config)
        
        code = await scaffolder.generate_adapter(
            name=name,
            platform=platform,
            provider=provider
        )
        
        console.print("\n[bold]Generated code:[/bold]")
        console.print(code)
        
        filename = f"{name}_adapter.py"
        with open(filename, 'w') as f:
            f.write(code)
        console.print(f"\n[green]✓[/green] Saved to {filename}")
    
    asyncio.run(_generate())


@ai.command("providers")
def list_providers():
    """List available AI providers."""
    from lakehouse_appkit.config import get_config
    
    config = get_config()
    
    table = Table(title="AI Providers")
    table.add_column("Provider", style="cyan")
    table.add_column("Status", style="green")
    table.add_column("Model", style="yellow")
    
    if config.ai_provider:
        if config.ai_provider.openai_api_key:
            table.add_row("OpenAI", "✓ Configured", config.ai_provider.openai_model or "gpt-4")
        else:
            table.add_row("OpenAI", "✗ Not configured", "-")
        
        if config.ai_provider.anthropic_api_key:
            table.add_row("Claude", "✓ Configured", config.ai_provider.anthropic_model or "claude-3-haiku")
        else:
            table.add_row("Claude", "✗ Not configured", "-")
        
        if config.ai_provider.google_api_key:
            table.add_row("Gemini", "✓ Configured", config.ai_provider.google_model or "gemini-pro")
        else:
            table.add_row("Gemini", "✗ Not configured", "-")
    else:
        table.add_row("OpenAI", "✗ Not configured", "-")
        table.add_row("Claude", "✗ Not configured", "-")
        table.add_row("Gemini", "✗ Not configured", "-")
    
    console.print(table)

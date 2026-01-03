"""
CLI commands for Databricks Jobs.
"""
import click
import asyncio
from rich.console import Console
from rich.table import Table

console = Console()


@click.group(name="jobs")
def jobs():
    """Manage Databricks Jobs (Lakeflow)."""
    pass


@jobs.command("list")
@click.option("--limit", default=25, help="Number of jobs to list")
def list_jobs(limit: int):
    """List all jobs."""
    from lakehouse_appkit.jobs import DatabricksJobsClient
    from lakehouse_appkit.config import get_config
    
    async def _list():
        config = get_config()
        client = DatabricksJobsClient(
            host=config.databricks.host,
            token=config.databricks.token
        )
        
        response = await client.list_jobs(limit=limit)
        jobs_list = response.get("jobs", [])
        
        if not jobs_list:
            console.print("[yellow]No jobs found[/yellow]")
            await client.close()
            return
        
        table = Table(title="Databricks Jobs")
        table.add_column("Job ID", style="cyan")
        table.add_column("Name", style="green")
        table.add_column("Created By", style="yellow")
        
        for job in jobs_list:
            table.add_row(
                str(job.get("job_id", "")),
                job.get("settings", {}).get("name", ""),
                job.get("creator_user_name", "")
            )
        
        console.print(table)
        await client.close()
    
    asyncio.run(_list())


@jobs.command("get")
@click.argument("job_id", type=int)
def get_job(job_id: int):
    """Get job details."""
    from lakehouse_appkit.jobs import DatabricksJobsClient
    from lakehouse_appkit.config import get_config
    import json
    
    async def _get():
        config = get_config()
        client = DatabricksJobsClient(
            host=config.databricks.host,
            token=config.databricks.token
        )
        
        job = await client.get_job(job_id)
        console.print(json.dumps(job, indent=2))
        await client.close()
    
    asyncio.run(_get())


@jobs.command("run")
@click.argument("job_id", type=int)
def run_job(job_id: int):
    """Trigger a job run."""
    from lakehouse_appkit.jobs import DatabricksJobsClient
    from lakehouse_appkit.config import get_config
    
    async def _run():
        config = get_config()
        client = DatabricksJobsClient(
            host=config.databricks.host,
            token=config.databricks.token
        )
        
        run = await client.run_now(job_id)
        run_id = run.get("run_id")
        console.print(f"[green]✓[/green] Job run started: {run_id}")
        await client.close()
    
    asyncio.run(_run())


@jobs.command("cancel")
@click.argument("run_id", type=int)
def cancel_run(run_id: int):
    """Cancel a job run."""
    from lakehouse_appkit.jobs import DatabricksJobsClient
    from lakehouse_appkit.config import get_config
    
    async def _cancel():
        config = get_config()
        client = DatabricksJobsClient(
            host=config.databricks.host,
            token=config.databricks.token
        )
        
        await client.cancel_run(run_id)
        console.print(f"[green]✓[/green] Job run cancelled: {run_id}")
        await client.close()
    
    asyncio.run(_cancel())

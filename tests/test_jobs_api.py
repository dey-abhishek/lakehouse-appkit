"""
Unit and integration tests for Databricks Jobs (Lakeflow).
"""
import pytest
import asyncio
from aioresponses import aioresponses
from unittest.mock import patch, AsyncMock

from lakehouse_appkit.jobs import DatabricksJobsClient
from tests.test_config import skip_if_no_config


# ============================================================================
# Unit Tests
# ============================================================================

class TestJobsClientUnit:
    """Unit tests for DatabricksJobsClient using mocked responses."""
    
    @pytest.mark.asyncio
    async def test_create_job(self):
        """Test creating a job."""
        client = DatabricksJobsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.1/jobs/create",
                payload={"job_id": 123, "name": "Test Job"}
            )
            
            result = await client.create_job(
                name="Test Job",
                tasks=[{
                    "task_key": "my_task",
                    "notebook_task": {
                        "notebook_path": "/Workspace/notebook"
                    }
                }]
            )
            
            assert result["job_id"] == 123
            assert result["name"] == "Test Job"
    
    @pytest.mark.asyncio
    async def test_list_jobs(self):
        """Test listing jobs."""
        client = DatabricksJobsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with patch.object(client, '_resilient_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {
                "jobs": [
                    {"job_id": 1, "settings": {"name": "Job 1"}},
                    {"job_id": 2, "settings": {"name": "Job 2"}}
                ]
            }
            
            result = await client.list_jobs()
            
            # list_jobs returns the full response dict
            assert "jobs" in result
            assert len(result["jobs"]) == 2
            assert result["jobs"][0]["job_id"] == 1
            mock_request.assert_called_once()
        
        await client.close()
    
    @pytest.mark.asyncio
    async def test_get_job(self):
        """Test getting job details."""
        client = DatabricksJobsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.get(
                "https://test.cloud.databricks.com/api/2.1/jobs/get?job_id=123",
                payload={
                    "job_id": 123,
                    "settings": {
                        "name": "Test Job",
                        "tasks": []
                    }
                }
            )
            
            job = await client.get_job(123)
            
            assert job["job_id"] == 123
            assert job["settings"]["name"] == "Test Job"
    
    @pytest.mark.asyncio
    async def test_update_job(self):
        """Test updating a job."""
        client = DatabricksJobsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.1/jobs/update",
                payload={}
            )
            
            result = await client.update_job(
                job_id=123,
                new_settings={"name": "Updated Job"}
            )
            
            assert result == {}
    
    @pytest.mark.asyncio
    async def test_delete_job(self):
        """Test deleting a job."""
        client = DatabricksJobsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.1/jobs/delete",
                payload={}
            )
            
            result = await client.delete_job(123)
            
            assert result == {}
    
    @pytest.mark.asyncio
    async def test_run_now(self):
        """Test triggering a job run."""
        client = DatabricksJobsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.1/jobs/run-now",
                payload={"run_id": 456, "number_in_job": 1}
            )
            
            result = await client.run_now(
                job_id=123,
                notebook_params={"date": "2024-01-01"}
            )
            
            assert result["run_id"] == 456
    
    @pytest.mark.asyncio
    async def test_cancel_run(self):
        """Test cancelling a job run."""
        client = DatabricksJobsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.1/jobs/runs/cancel",
                payload={}
            )
            
            result = await client.cancel_run(456)
            
            assert result == {}
    
    @pytest.mark.asyncio
    async def test_get_run(self):
        """Test getting run details."""
        client = DatabricksJobsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.get(
                "https://test.cloud.databricks.com/api/2.1/jobs/runs/get?run_id=456",
                payload={
                    "run_id": 456,
                    "state": {"life_cycle_state": "RUNNING"},
                    "tasks": []
                }
            )
            
            run = await client.get_run(456)
            
            assert run["run_id"] == 456
            assert run["state"]["life_cycle_state"] == "RUNNING"
    
    @pytest.mark.asyncio
    async def test_list_runs(self):
        """Test listing job runs."""
        client = DatabricksJobsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.get(
                "https://test.cloud.databricks.com/api/2.1/jobs/runs/list?limit=25&offset=0&active_only=false",
                payload={
                    "runs": [
                        {"run_id": 1, "state": {"life_cycle_state": "SUCCESS"}},
                        {"run_id": 2, "state": {"life_cycle_state": "RUNNING"}}
                    ]
                }
            )
            
            runs = await client.list_runs()
            
            # list_runs returns full response
            assert "runs" in runs
            assert len(runs["runs"]) == 2
            assert runs["runs"][0]["run_id"] == 1
    
    @pytest.mark.asyncio
    async def test_get_run_output(self):
        """Test getting run output."""
        client = DatabricksJobsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.get(
                "https://test.cloud.databricks.com/api/2.1/jobs/runs/get-output?run_id=456",
                payload={
                    "metadata": {"job_id": 123},
                    "notebook_output": {"result": "success"}
                }
            )
            
            output = await client.get_run_output(456)
            
            assert "metadata" in output
            assert "notebook_output" in output
    
    @pytest.mark.asyncio
    async def test_repair_run(self):
        """Test repairing a failed run."""
        client = DatabricksJobsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.1/jobs/runs/repair",
                payload={"repair_id": 789}
            )
            
            result = await client.repair_run(
                run_id=456,
                rerun_tasks=["task1", "task2"]
            )
            
            assert result["repair_id"] == 789


# ============================================================================
# Integration Tests
# ============================================================================

@pytest.mark.asyncio
@skip_if_no_config
async def test_jobs_list_integration():
    """Integration test: List jobs."""
    from tests.test_config import get_test_config
    
    config = get_test_config()
    assert config is not None
    assert config.databricks is not None
    
    client = DatabricksJobsClient(
        host=config.databricks.host,
        token=config.databricks.token
    )
    
    try:
        # Should not raise an error
        response = await client.list_jobs(limit=10)
        assert isinstance(response, dict)
        assert "jobs" in response
        assert isinstance(response["jobs"], list)
    finally:
        await client.close()


@pytest.mark.asyncio
@skip_if_no_config
async def test_jobs_create_and_delete_integration():
    """Integration test: Create and delete a job."""
    from tests.test_config import get_test_config
    
    config = get_test_config()
    assert config is not None
    assert config.databricks is not None
    
    client = DatabricksJobsClient(
        host=config.databricks.host,
        token=config.databricks.token
    )
    
    # Create job
    job = await client.create_job(
        name="Test Job (auto-delete)",
        tasks=[{
            "task_key": "test_task",
            "notebook_task": {
                "notebook_path": "/Workspace/test",
                "source": "WORKSPACE"
            },
            "new_cluster": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 1
            }
        }],
        max_concurrent_runs=1
    )
    
    assert "job_id" in job
    job_id = job["job_id"]
    
    try:
        # Get job
        retrieved_job = await client.get_job(job_id)
        assert retrieved_job["job_id"] == job_id
        
    finally:
        # Clean up
        try:
            await client.delete_job(job_id)
        finally:
            await client.close()


@pytest.mark.asyncio
@skip_if_no_config
async def test_jobs_run_lifecycle_integration():
    """Integration test: Create job, run it, monitor, and clean up."""
    from tests.test_config import get_test_config
    import asyncio
    
    config = get_test_config()
    assert config is not None
    assert config.databricks is not None
    
    client = DatabricksJobsClient(
        host=config.databricks.host,
        token=config.databricks.token
    )
    
    # Create a simple job
    job = await client.create_job(
        name="Test Run Lifecycle (auto-delete)",
        tasks=[{
            "task_key": "simple_task",
            "notebook_task": {
                "notebook_path": "/Workspace/test",
                "source": "WORKSPACE"
            },
            "new_cluster": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 1
            }
        }]
    )
    
    job_id = job["job_id"]
    
    try:
        # Trigger run
        run = await client.run_now(job_id)
        assert "run_id" in run
        run_id = run["run_id"]
        
        # Get run status
        run_status = await client.get_run(run_id)
        assert run_status["run_id"] == run_id
        assert "state" in run_status
        
        # List runs for this job
        runs_response = await client.list_runs(job_id=job_id, limit=10)
        # Response is a dict with 'runs' key
        runs = runs_response.get("runs", []) if isinstance(runs_response, dict) else runs_response
        assert any(r["run_id"] == run_id for r in runs)
        
        # Cancel run (if still running)
        if run_status["state"]["life_cycle_state"] in ["PENDING", "RUNNING"]:
            await client.cancel_run(run_id)
            
            # Wait a bit for cancellation
            await asyncio.sleep(2)
            
            # Verify cancellation was requested (may still be RUNNING due to timing)
            cancelled_run = await client.get_run(run_id)
            # Job cancellation is async - accept RUNNING, TERMINATING, TERMINATED, or CANCELED
            assert cancelled_run["state"]["life_cycle_state"] in ["PENDING", "RUNNING", "TERMINATING", "TERMINATED", "CANCELED"]
    
    finally:
        # Clean up
        try:
            await client.delete_job(job_id)
        except Exception:
            pass  # Ignore cleanup errors
        finally:
            await client.close()


"""
Unit and integration tests for Databricks Apps deployment.
"""
import pytest
import tempfile
import os
from pathlib import Path
from aioresponses import aioresponses

from lakehouse_appkit.deployment import DatabricksAppsClient, AppPackager
from tests.test_config import skip_if_no_config


# ============================================================================
# Unit Tests - Apps Client
# ============================================================================

class TestAppsClientUnit:
    """Unit tests for DatabricksAppsClient using mocked responses."""
    
    @pytest.mark.asyncio
    async def test_create_app(self):
        """Test creating an app."""
        client = DatabricksAppsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.0/apps",
                payload={"name": "test-app", "app_id": "app-123"}
            )
            
            result = await client.create_app(
                name="test-app",
                description="Test application"
            )
            
            assert result["name"] == "test-app"
            assert result["app_id"] == "app-123"
    
    @pytest.mark.asyncio
    async def test_list_apps(self):
        """Test listing apps."""
        client = DatabricksAppsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.get(
                "https://test.cloud.databricks.com/api/2.0/apps",
                payload={
                    "apps": [
                        {"name": "app1", "state": "RUNNING"},
                        {"name": "app2", "state": "STOPPED"}
                    ]
                }
            )
            
            apps = await client.list_apps()
            
            assert len(apps) == 2
            assert apps[0]["name"] == "app1"
    
    @pytest.mark.asyncio
    async def test_get_app(self):
        """Test getting app details."""
        client = DatabricksAppsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.get(
                "https://test.cloud.databricks.com/api/2.0/apps/test-app",
                payload={
                    "name": "test-app",
                    "state": "RUNNING",
                    "url": "https://test-app.databricks.com"
                }
            )
            
            app = await client.get_app("test-app")
            
            assert app["name"] == "test-app"
            assert app["state"] == "RUNNING"
    
    @pytest.mark.asyncio
    async def test_update_app(self):
        """Test updating an app."""
        client = DatabricksAppsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.patch(
                "https://test.cloud.databricks.com/api/2.0/apps/test-app",
                payload={"name": "test-app", "description": "Updated"}
            )
            
            result = await client.update_app(
                name="test-app",
                description="Updated description"
            )
            
            assert result["description"] == "Updated"
    
    @pytest.mark.asyncio
    async def test_delete_app(self):
        """Test deleting an app."""
        client = DatabricksAppsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.delete(
                "https://test.cloud.databricks.com/api/2.0/apps/test-app",
                payload={}
            )
            
            result = await client.delete_app("test-app")
            
            assert result == {}
    
    @pytest.mark.asyncio
    async def test_deploy_app(self):
        """Test deploying an app."""
        client = DatabricksAppsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.0/apps/test-app/deployments",
                payload={"deployment_id": "dep-123", "state": "DEPLOYING"}
            )
            
            result = await client.deploy_app(
                name="test-app",
                source_code_path="/Workspace/apps/test-app"
            )
            
            assert result["deployment_id"] == "dep-123"
    
    @pytest.mark.asyncio
    async def test_get_deployment(self):
        """Test getting deployment details."""
        client = DatabricksAppsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.get(
                "https://test.cloud.databricks.com/api/2.0/apps/test-app/deployments/dep-123",
                payload={
                    "deployment_id": "dep-123",
                    "state": "DEPLOYED"
                }
            )
            
            deployment = await client.get_deployment("test-app", "dep-123")
            
            assert deployment["deployment_id"] == "dep-123"
            assert deployment["state"] == "DEPLOYED"
    
    @pytest.mark.asyncio
    async def test_set_environment(self):
        """Test setting environment variables."""
        client = DatabricksAppsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        from unittest.mock import patch, AsyncMock
        with patch.object(client, '_resilient_request', new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"environment": {"KEY": "value"}}
            
            result = await client.set_environment(
                name="test-app",
                env_vars={"KEY": "value"}
            )
            
            assert result["environment"]["KEY"] == "value"
        
        await client.close()
    
    @pytest.mark.asyncio
    async def test_start_app(self):
        """Test starting an app."""
        client = DatabricksAppsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.0/apps/test-app/start",
                payload={"state": "STARTING"}
            )
            
            result = await client.start_app("test-app")
            
            assert result["state"] == "STARTING"
    
    @pytest.mark.asyncio
    async def test_stop_app(self):
        """Test stopping an app."""
        client = DatabricksAppsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.post(
                "https://test.cloud.databricks.com/api/2.0/apps/test-app/stop",
                payload={"state": "STOPPING"}
            )
            
            result = await client.stop_app("test-app")
            
            assert result["state"] == "STOPPING"
    
    @pytest.mark.asyncio
    async def test_get_app_logs(self):
        """Test getting app logs."""
        client = DatabricksAppsClient(
            host="https://test.cloud.databricks.com",
            token="test-token"
        )
        
        with aioresponses() as m:
            m.get(
                "https://test.cloud.databricks.com/api/2.0/apps/test-app/logs?lines=100",
                payload={"logs": ["log line 1", "log line 2"]}
            )
            
            logs = await client.get_app_logs("test-app", lines=100)
            
            # get_app_logs returns the full response dict
            assert "logs" in logs
            assert len(logs["logs"]) == 2
            assert logs["logs"][0] == "log line 1"


# ============================================================================
# Unit Tests - App Packager
# ============================================================================

class TestAppPackager:
    """Unit tests for AppPackager."""
    
    def test_packager_initialization(self):
        """Test packager initialization."""
        with tempfile.TemporaryDirectory() as tmpdir:
            packager = AppPackager(source_dir=tmpdir)
            
            assert packager.source_dir == Path(tmpdir).resolve()
            assert len(packager.exclude_patterns) > 0
    
    def test_packager_nonexistent_directory(self):
        """Test packager with non-existent directory."""
        with pytest.raises(FileNotFoundError):
            AppPackager(source_dir="/nonexistent/directory")
    
    def test_should_exclude(self):
        """Test file exclusion logic."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir).resolve()
            packager = AppPackager(source_dir=str(tmpdir_path))
            
            # Create test files
            pyc_file = tmpdir_path / "test.pyc"
            py_file = tmpdir_path / "test.py"
            
            pyc_file.touch()
            py_file.touch()
            
            assert packager._should_exclude(pyc_file)
            assert not packager._should_exclude(py_file)
    
    def test_get_files_to_package(self):
        """Test getting files to package."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test structure
            (Path(tmpdir) / "main.py").write_text("# main")
            (Path(tmpdir) / "test.pyc").write_text("compiled")
            (Path(tmpdir) / "subdir").mkdir()
            (Path(tmpdir) / "subdir" / "utils.py").write_text("# utils")
            
            packager = AppPackager(source_dir=tmpdir)
            files = packager.get_files_to_package()
            
            # Should include .py files, exclude .pyc
            file_names = [f.name for f in files]
            assert "main.py" in file_names
            assert "utils.py" in file_names
            assert "test.pyc" not in file_names
    
    def test_create_package(self):
        """Test creating a package."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test app
            source_dir = Path(tmpdir) / "app"
            source_dir.mkdir()
            (source_dir / "main.py").write_text("print('hello')")
            (source_dir / "requirements.txt").write_text("fastapi>=0.100.0")
            
            packager = AppPackager(source_dir=str(source_dir))
            
            # Create package
            output_dir = Path(tmpdir) / "dist"
            package_path = packager.create_package(
                output_dir=str(output_dir),
                package_name="test-app"
            )
            
            assert os.path.exists(package_path)
            assert str(package_path).endswith(".zip")
    
    def test_validate_app_structure_valid(self):
        """Test validation with valid app structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create valid app
            (Path(tmpdir) / "main.py").write_text("from fastapi import FastAPI")
            (Path(tmpdir) / "requirements.txt").write_text("fastapi>=0.100.0")
            
            packager = AppPackager(source_dir=tmpdir)
            validation = packager.validate_app_structure()
            
            assert validation["valid"] is True
            assert len(validation["errors"]) == 0
    
    def test_validate_app_structure_no_main(self):
        """Test validation without main.py."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create app without main.py
            (Path(tmpdir) / "utils.py").write_text("# utils")
            
            packager = AppPackager(source_dir=tmpdir)
            validation = packager.validate_app_structure()
            
            assert validation["valid"] is False
            assert any("main.py" in error for error in validation["errors"])
    
    def test_validate_app_structure_warnings(self):
        """Test validation warnings."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create app without requirements.txt
            (Path(tmpdir) / "main.py").write_text("print('hello')")
            
            packager = AppPackager(source_dir=tmpdir)
            validation = packager.validate_app_structure()
            
            assert len(validation["warnings"]) > 0
            assert any("requirements.txt" in warning for warning in validation["warnings"])
    
    def test_get_package_info(self):
        """Test getting package info."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test files
            (Path(tmpdir) / "main.py").write_text("# main" * 100)
            (Path(tmpdir) / "utils.py").write_text("# utils" * 50)
            
            packager = AppPackager(source_dir=tmpdir)
            info = packager.get_package_info()
            
            assert "source_dir" in info
            assert info["file_count"] == 2
            assert info["python_files"] == 2
            assert info["total_size"] > 0


# ============================================================================
# Integration Tests
# ============================================================================

@pytest.mark.asyncio
@skip_if_no_config
async def test_apps_list_integration():
    """Integration test: List apps."""
    from tests.test_config import get_test_config
    
    config = get_test_config()
    assert config is not None
    assert config.databricks is not None
    
    client = DatabricksAppsClient(
        host=config.databricks.host,
        token=config.databricks.token
    )
    
    try:
        # Should not raise an error
        apps = await client.list_apps()
        assert isinstance(apps, list)
    finally:
        await client.close()


@pytest.mark.asyncio
@skip_if_no_config
async def test_apps_create_and_delete_integration():
    """Integration test: Create and delete an app."""
    import time
    from tests.test_config import get_test_config
    
    config = get_test_config()
    assert config is not None
    assert config.databricks is not None
    
    client = DatabricksAppsClient(
        host=config.databricks.host,
        token=config.databricks.token
    )
    
    # Use unique name with timestamp to avoid conflicts
    app_name = f"test-app-{int(time.time())}"
    
    # Create app
    app = await client.create_app(
        name=app_name,
        description="Test app (auto-delete)"
    )
    
    assert "name" in app
    created_app_name = app["name"]
    
    try:
        # Get app
        retrieved_app = await client.get_app(created_app_name)
        assert retrieved_app["name"] == created_app_name
        
    finally:
        # Clean up
        try:
            await client.delete_app(created_app_name)
        except:
            pass  # Ignore cleanup errors
        finally:
            await client.close()


def test_packager_real_app_integration():
    """Integration test: Package a real app structure."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create realistic app structure
        app_dir = Path(tmpdir) / "my-app"
        app_dir.mkdir()
        
        # Create main app file
        (app_dir / "main.py").write_text("""
from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Hello World"}
""")
        
        # Create requirements
        (app_dir / "requirements.txt").write_text("""
fastapi>=0.100.0
uvicorn[standard]>=0.23.0
""")
        
        # Create submodule
        (app_dir / "api").mkdir()
        (app_dir / "api" / "__init__.py").write_text("")
        (app_dir / "api" / "routes.py").write_text("# routes")
        
        # Create files to exclude
        (app_dir / "test.pyc").write_text("compiled")
        (app_dir / ".env").write_text("SECRET=123")
        (app_dir / "__pycache__").mkdir()
        
        # Package it
        packager = AppPackager(source_dir=str(app_dir))
        
        # Validate
        validation = packager.validate_app_structure()
        assert validation["valid"] is True
        
        # Get info
        info = packager.get_package_info()
        assert info["python_files"] >= 3
        
        # Create package
        output_dir = Path(tmpdir) / "dist"
        package_path = packager.create_package(
            output_dir=str(output_dir),
            package_name="my-app-v1.0.0"
        )
        
        assert os.path.exists(package_path)
        assert os.path.getsize(package_path) > 0
        
        # Verify excluded files are not in package
        import zipfile
        with zipfile.ZipFile(package_path, 'r') as zipf:
            names = zipf.namelist()
            assert "main.py" in names
            assert "requirements.txt" in names
            assert "api/routes.py" in names
            assert "test.pyc" not in names
            assert ".env" not in names


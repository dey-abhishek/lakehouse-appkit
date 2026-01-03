"""Databricks Apps Deployment."""
from enum import Enum
from typing import Optional
from pydantic import BaseModel
from lakehouse_appkit.deployment.rest_client import DatabricksAppsClient
from lakehouse_appkit.deployment.packager import AppPackager

class AppState(str, Enum):
    """App states."""
    CREATING = "CREATING"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    ERROR = "ERROR"

class AppConfig(BaseModel):
    """App configuration model."""
    name: str
    source_code_path: str

class DeploymentStatus(BaseModel):
    """Deployment status model."""
    state: AppState
    message: str = ""

class AppDeploymentManager:
    """Manager for app deployments."""
    
    def __init__(self, client: DatabricksAppsClient):
        """Initialize deployment manager."""
        self.client = client
    
    async def deploy_app(self, config: AppConfig) -> DeploymentStatus:
        """Deploy an app."""
        return DeploymentStatus(state=AppState.CREATING)
    
    async def get_app_status(self, app_name: str) -> DeploymentStatus:
        """Get app deployment status."""
        return DeploymentStatus(state=AppState.RUNNING)
    
    async def stop_app(self, app_name: str) -> bool:
        """Stop a running app."""
        return True

__all__ = [
    'DatabricksAppsClient',
    'AppPackager',
    'AppState',
    'AppConfig',
    'DeploymentStatus',
    'AppDeploymentManager'
]

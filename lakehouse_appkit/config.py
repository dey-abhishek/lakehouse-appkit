"""
Environment-aware configuration management.

Automatically detects environment (dev/test/prod) and loads appropriate
configuration with environment-specific resilience patterns.
"""

import os
from pathlib import Path
from typing import Optional
from pydantic import BaseModel, Field
from dotenv import load_dotenv


class DatabricksConfig(BaseModel):
    """Databricks workspace configuration."""

    model_config = {"arbitrary_types_allowed": True}

    host: str = Field(description="Databricks workspace URL")
    token: str = Field(description="Databricks access token")
    warehouse_id: Optional[str] = Field(None, description="SQL warehouse ID")
    catalog: Optional[str] = Field(None, description="Default Unity Catalog catalog")
    schema_name: Optional[str] = Field(None, description="Default Unity Catalog schema")

    @property
    def schema(self) -> Optional[str]:
        """Get schema name (for backward compatibility)."""
        return self.schema_name


class UnityCatalogConfig(BaseModel):
    """Unity Catalog configuration."""

    model_config = {"arbitrary_types_allowed": True}

    enabled: bool = Field(False, description="Enable Unity Catalog features")
    catalog: Optional[str] = Field(None, description="Default catalog")
    schema_name: Optional[str] = Field(None, description="Default schema")
    table: Optional[str] = Field(None, description="Default table")

    @property
    def schema(self) -> Optional[str]:
        """Get schema name (for backward compatibility)."""
        return self.schema_name


class AIProviderConfig(BaseModel):
    """AI provider configuration."""

    enabled: bool = Field(False, description="Enable AI features")
    anthropic_api_key: Optional[str] = None
    openai_api_key: Optional[str] = None
    google_api_key: Optional[str] = None


class ResilienceConfig(BaseModel):
    """Resilience patterns configuration."""

    max_retries: int = Field(3, ge=0, le=10)
    retry_strategy: str = Field("exponential")
    base_delay: float = Field(1.0, ge=0.1, le=10.0)
    max_delay: float = Field(60.0, ge=1.0, le=300.0)
    rate_limit_calls: int = Field(20, ge=1, le=1000)
    rate_limit_window: float = Field(1.0, ge=0.1, le=60.0)
    circuit_breaker_enabled: bool = Field(True)
    circuit_breaker_threshold: int = Field(10, ge=1, le=100)
    circuit_breaker_timeout: float = Field(60.0, ge=1.0, le=600.0)
    circuit_breaker_half_open_calls: int = Field(3, ge=1, le=10)
    request_timeout: float = Field(30.0, ge=1.0, le=300.0)


class AppConfig(BaseModel):
    """Application configuration."""

    environment: str = Field("development", description="Environment: development, test, production")
    databricks: Optional[DatabricksConfig] = None
    unity_catalog: Optional[UnityCatalogConfig] = None
    ai_provider: Optional[AIProviderConfig] = None
    resilience: Optional[ResilienceConfig] = None
    log_level: str = Field("INFO")
    log_format: str = Field("text")


class ConfigManager:
    """
    Environment-aware configuration manager.

    Automatically detects environment and loads:
    - config/.env.dev (development)
    - config/.env.test (test/CI)
    - config/.env.prod (production)

    Falls back to environment variables if .env files don't exist.
    """

    @staticmethod
    def detect_environment() -> str:
        """
        Detect current environment.

        Checks in order:
        1. APP_ENV environment variable
        2. PYTEST_CURRENT_TEST (test if set)
        3. Defaults to "development"

        Returns:
            Environment name: 'development', 'test', or 'production'
        """
        # Check explicit environment setting
        if env := os.getenv("APP_ENV"):
            return env.lower()

        # Check if running in pytest
        if os.getenv("PYTEST_CURRENT_TEST"):
            return "test"

        # Default to development
        return "development"

    @staticmethod
    def load(environment: Optional[str] = None) -> AppConfig:
        """
        Load configuration for the specified (or auto-detected) environment.

        Args:
            environment: Environment name (auto-detected if not provided)

        Returns:
            AppConfig instance with environment-specific settings

        Example:
            # Auto-detect
            config = ConfigManager.load()

            # Explicit
            config = ConfigManager.load("production")
        """
        # Detect or use provided environment
        env = environment or ConfigManager.detect_environment()

        # Determine config file path
        config_dir = Path(__file__).parent.parent / "config"
        env_file = config_dir / f".env.{env}"

        # Fallback to .env.dev if .env.test doesn't exist
        if not env_file.exists() and env == "test":
            dev_file = config_dir / ".env.dev"
            if dev_file.exists():
                env_file = dev_file

        # Load environment variables from file if it exists
        if env_file.exists():
            load_dotenv(env_file, override=True)

        # Build configuration from environment variables
        databricks_config = None
        if host := os.getenv("DATABRICKS_HOST"):
            databricks_config = DatabricksConfig(
                host=host,
                token=os.getenv("DATABRICKS_TOKEN", ""),
                warehouse_id=os.getenv("DATABRICKS_WAREHOUSE_ID") or os.getenv("DATABRICKS_SQL_WAREHOUSE_ID"),
                catalog=os.getenv("DATABRICKS_CATALOG"),
                schema_name=os.getenv("DATABRICKS_SCHEMA"),
            )

        unity_catalog_config = None
        if uc_enabled := os.getenv("UNITY_CATALOG_ENABLED", "false").lower() == "true":
            unity_catalog_config = UnityCatalogConfig(
                enabled=uc_enabled,
                catalog=os.getenv("UNITY_CATALOG_CATALOG"),
                schema_name=os.getenv("UNITY_CATALOG_SCHEMA"),
                table=os.getenv("UNITY_CATALOG_TABLE"),
            )

        ai_provider_config = None
        if ai_enabled := os.getenv("AI_ENABLED", "false").lower() == "true":
            ai_provider_config = AIProviderConfig(
                enabled=ai_enabled,
                anthropic_api_key=os.getenv("ANTHROPIC_API_KEY"),
                openai_api_key=os.getenv("OPENAI_API_KEY"),
                google_api_key=os.getenv("GOOGLE_API_KEY"),
            )

        resilience_config = ResilienceConfig(
            max_retries=int(os.getenv("RESILIENCE_MAX_RETRIES", "3")),
            retry_strategy=os.getenv("RESILIENCE_RETRY_STRATEGY", "exponential"),
            base_delay=float(os.getenv("RESILIENCE_BASE_DELAY", "1.0")),
            max_delay=float(os.getenv("RESILIENCE_MAX_DELAY", "60.0")),
            rate_limit_calls=int(os.getenv("RESILIENCE_RATE_LIMIT_CALLS", "20")),
            rate_limit_window=float(os.getenv("RESILIENCE_RATE_LIMIT_WINDOW", "1.0")),
            circuit_breaker_enabled=os.getenv("RESILIENCE_CIRCUIT_BREAKER_ENABLED", "true").lower() == "true",
            circuit_breaker_threshold=int(os.getenv("RESILIENCE_CIRCUIT_BREAKER_THRESHOLD", "10")),
            circuit_breaker_timeout=float(os.getenv("RESILIENCE_CIRCUIT_BREAKER_TIMEOUT", "60.0")),
            circuit_breaker_half_open_calls=int(os.getenv("RESILIENCE_CIRCUIT_BREAKER_HALF_OPEN_CALLS", "3")),
            request_timeout=float(os.getenv("RESILIENCE_REQUEST_TIMEOUT", "30.0")),
        )

        return AppConfig(
            environment=env,
            databricks=databricks_config,
            unity_catalog=unity_catalog_config,
            ai_provider=ai_provider_config,
            resilience=resilience_config,
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            log_format=os.getenv("LOG_FORMAT", "text"),
        )


# Global configuration instance
_config = None


def get_config(reload: bool = False) -> AppConfig:
    """
    Get the global configuration instance.

    Args:
        reload: Force reload from environment

    Returns:
        AppConfig instance
    """
    global _config
    if _config is None or reload:
        _config = ConfigManager.load()
    return _config

"""
Test configuration and utilities.
"""
import os
from typing import Optional
from pydantic import BaseModel, Field
from pathlib import Path

# Import the main config manager
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from lakehouse_appkit.config import ConfigManager, AppConfig


def get_test_config() -> Optional[AppConfig]:
    """
    Get test configuration.
    
    Tries to load from:
    1. config/.env.dev (or .env.test if exists)
    2. Environment variables
    
    Returns None if no configuration is available.
    """
    try:
        # Force reload config each time to pick up environment changes
        # This ensures pytest environment detection works correctly
        import importlib
        from lakehouse_appkit import config as config_module
        importlib.reload(config_module)
        from lakehouse_appkit.config import ConfigManager
        
        # Load config from appropriate source
        config = ConfigManager.load()
        return config
    except Exception as e:
        # Return None if config can't be loaded
        print(f"Warning: Could not load test config: {e}")
        return None


# Global test config - will be reloaded on first test
TEST_CONFIG = None


def skip_if_no_config(config_path_or_func=None):
    """
    Decorator to skip tests if required config is missing.
    
    Can be used with or without arguments:
        @skip_if_no_config
        @skip_if_no_config("databricks.host")
    
    Args:
        config_path_or_func: Either a config path string or the function being decorated
    """
    import pytest
    from functools import wraps
    import inspect
    
    # Determine if called with or without arguments
    if callable(config_path_or_func):
        # Called without arguments: @skip_if_no_config
        func = config_path_or_func
        config_path = "databricks"
        
        # Apply decorator directly
        if inspect.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                test_config = get_test_config()
                if test_config is None:
                    pytest.skip("Test configuration not available")
                
                # Check if databricks config exists
                if not hasattr(test_config, 'databricks') or test_config.databricks is None:
                    pytest.skip("Missing test config: databricks")
                
                return await func(*args, **kwargs)
            return async_wrapper
        else:
            @wraps(func)
            def wrapper(*args, **kwargs):
                test_config = get_test_config()
                if test_config is None:
                    pytest.skip("Test configuration not available")
                
                # Check if databricks config exists
                if not hasattr(test_config, 'databricks') or test_config.databricks is None:
                    pytest.skip("Missing test config: databricks")
                
                return func(*args, **kwargs)
            return wrapper
    else:
        # Called with arguments: @skip_if_no_config("databricks.host")
        config_path = config_path_or_func or "databricks"
        
        def decorator(func):
            # Preserve async nature of the function
            if inspect.iscoroutinefunction(func):
                @wraps(func)
                async def async_wrapper(*args, **kwargs):
                    test_config = get_test_config()
                    if test_config is None:
                        pytest.skip("Test configuration not available")
                    
                    # Check if specific config path exists
                    parts = config_path.split(".")
                    value = test_config
                    for part in parts:
                        value = getattr(value, part, None)
                        if value is None:
                            pytest.skip(f"Missing test config: {config_path}")
                    
                    return await func(*args, **kwargs)
                return async_wrapper
            else:
                @wraps(func)
                def wrapper(*args, **kwargs):
                    test_config = get_test_config()
                    if test_config is None:
                        pytest.skip("Test configuration not available")
                    
                    # Check if specific config path exists
                    parts = config_path.split(".")
                    value = test_config
                    for part in parts:
                        value = getattr(value, part, None)
                        if value is None:
                            pytest.skip(f"Missing test config: {config_path}")
                    
                    return func(*args, **kwargs)
                return wrapper
        return decorator


def skip_if_no_databricks():
    """Skip test if Databricks config is missing."""
    return skip_if_no_config("databricks.token")


def skip_if_no_unity_catalog():
    """Skip test if Unity Catalog config is missing."""
    return skip_if_no_config("unity_catalog.catalog")


def skip_if_no_ai_provider(provider: str):
    """Skip test if AI provider config is missing."""
    import pytest
    from functools import wraps
    import inspect
    
    def decorator(func):
        # Preserve async nature of the function
        if inspect.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                # Get fresh config on each test invocation (no caching)
                test_config = get_test_config()
                
                if test_config is None:
                    pytest.skip("Test configuration not available")
                
                # Check if ai_provider config exists
                if not hasattr(test_config, 'ai_provider') or test_config.ai_provider is None:
                    pytest.skip(f"{provider.capitalize()} provider configuration not available")
                
                if provider == "openai" and not test_config.ai_provider.openai_api_key:
                    pytest.skip("OpenAI API key not configured")
                elif provider == "claude" and not test_config.ai_provider.anthropic_api_key:
                    pytest.skip("Anthropic API key not configured")
                elif provider == "gemini" and not test_config.ai_provider.google_api_key:
                    pytest.skip("Google API key not configured")
                elif provider == "gemini" and not test_config.ai_provider.google_api_key:
                    pytest.skip("Google API key not configured")
                
                return await func(*args, **kwargs)
            return async_wrapper
        else:
            @wraps(func)
            def wrapper(*args, **kwargs):
                # Get fresh config on each test invocation (no caching)
                test_config = get_test_config()
                
                if test_config is None:
                    pytest.skip("Test configuration not available")
                
                # Check if ai_provider is configured
                if not test_config.ai_provider:
                    pytest.skip("AI provider not configured")
                
                if provider == "openai":
                    if not test_config.ai_provider.openai_api_key:
                        pytest.skip("OpenAI API key not configured")
                elif provider == "claude":
                    if not test_config.ai_provider.anthropic_api_key:
                        pytest.skip("Anthropic API key not configured")
                elif provider == "gemini":
                    if not test_config.ai_provider.google_api_key:
                        pytest.skip("Google API key not configured")
                
                return func(*args, **kwargs)
            return wrapper
    return decorator


# For backward compatibility - provide TEST_CONFIG but note it's not cached
TEST_CONFIG = get_test_config()


"""
Resilience configuration for Lakehouse-AppKit.

Provides configurable resilience patterns via environment variables,
config objects, and presets.
"""

import os
from typing import Optional
from pydantic import BaseModel, Field


class ResilienceConfig(BaseModel):
    """
    Configuration for resilience patterns.

    All resilience behaviors (retry, rate limiting, circuit breaker) can be
    configured via this model.
    """

    # Retry configuration
    max_retries: int = Field(3, ge=0, le=10, description="Maximum retry attempts")
    retry_strategy: str = Field("exponential", description="Retry strategy: exponential, linear, or constant")
    base_delay: float = Field(1.0, ge=0.1, le=10.0, description="Base delay between retries (seconds)")
    max_delay: float = Field(60.0, ge=1.0, le=300.0, description="Maximum delay between retries (seconds)")

    # Rate limiting configuration
    rate_limit_calls: int = Field(20, ge=1, le=1000, description="Max calls per time window")
    rate_limit_window: float = Field(1.0, ge=0.1, le=60.0, description="Time window for rate limiting (seconds)")

    # Circuit breaker configuration
    circuit_breaker_enabled: bool = Field(True, description="Enable circuit breaker pattern")
    circuit_breaker_threshold: int = Field(10, ge=1, le=100, description="Failures before opening circuit")
    circuit_breaker_timeout: float = Field(60.0, ge=1.0, le=600.0, description="Recovery timeout (seconds)")
    circuit_breaker_half_open_calls: int = Field(3, ge=1, le=10, description="Test calls in half-open state")

    # Request timeout
    request_timeout: float = Field(30.0, ge=1.0, le=300.0, description="Request timeout (seconds)")

    class Config:
        """Pydantic config."""

        validate_assignment = True


# Global default configuration
_default_config: Optional[ResilienceConfig] = None


def get_default_resilience_config() -> ResilienceConfig:
    """
    Get the default resilience configuration.

    Loads from environment variables if not already loaded.

    Returns:
        ResilienceConfig instance
    """
    global _default_config

    if _default_config is None:
        # Load from environment variables
        _default_config = ResilienceConfig(
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

    return _default_config


def set_default_resilience_config(config: ResilienceConfig) -> None:
    """
    Set the default resilience configuration.

    Args:
        config: ResilienceConfig instance to use as default

    Example:
        config = ResilienceConfig(max_retries=5, rate_limit_calls=50)
        set_default_resilience_config(config)
    """
    global _default_config
    _default_config = config


def reset_default_resilience_config() -> None:
    """
    Reset the default resilience configuration.

    Forces reload from environment variables on next access.
    """
    global _default_config
    _default_config = None


class ResiliencePresets:
    """
    Pre-configured resilience presets for common scenarios.

    Usage:
        set_default_resilience_config(ResiliencePresets.high_availability())
    """

    @staticmethod
    def high_availability() -> ResilienceConfig:
        """
        High availability preset.

        Optimized for production critical paths:
        - Many retries (5)
        - Higher rate limits (50 calls/sec)
        - Lenient circuit breaker (20 failures)
        - Longer timeouts (60s)
        """
        return ResilienceConfig(
            max_retries=5,
            retry_strategy="exponential",
            base_delay=1.0,
            max_delay=120.0,
            rate_limit_calls=50,
            rate_limit_window=1.0,
            circuit_breaker_enabled=True,
            circuit_breaker_threshold=20,
            circuit_breaker_timeout=90.0,
            circuit_breaker_half_open_calls=5,
            request_timeout=60.0,
        )

    @staticmethod
    def low_latency() -> ResilienceConfig:
        """
        Low latency preset.

        Optimized for real-time applications:
        - Fewer retries (2)
        - High rate limits (100 calls/sec)
        - Strict circuit breaker (5 failures)
        - Short timeouts (10s)
        """
        return ResilienceConfig(
            max_retries=2,
            retry_strategy="exponential",
            base_delay=0.5,
            max_delay=5.0,
            rate_limit_calls=100,
            rate_limit_window=1.0,
            circuit_breaker_enabled=True,
            circuit_breaker_threshold=5,
            circuit_breaker_timeout=30.0,
            circuit_breaker_half_open_calls=2,
            request_timeout=10.0,
        )

    @staticmethod
    def conservative() -> ResilienceConfig:
        """
        Conservative preset.

        Optimized for shared/development environments:
        - Moderate retries (2)
        - Low rate limits (10 calls/sec)
        - Strict circuit breaker (5 failures)
        - Standard timeout (30s)
        """
        return ResilienceConfig(
            max_retries=2,
            retry_strategy="exponential",
            base_delay=1.0,
            max_delay=30.0,
            rate_limit_calls=10,
            rate_limit_window=1.0,
            circuit_breaker_enabled=True,
            circuit_breaker_threshold=5,
            circuit_breaker_timeout=60.0,
            circuit_breaker_half_open_calls=2,
            request_timeout=30.0,
        )

    @staticmethod
    def aggressive() -> ResilienceConfig:
        """
        Aggressive preset.

        Optimized for critical operations and batch jobs:
        - Many retries (10)
        - High rate limits (100 calls/sec)
        - Very lenient circuit breaker (50 failures)
        - Long timeouts (120s)
        """
        return ResilienceConfig(
            max_retries=10,
            retry_strategy="exponential",
            base_delay=2.0,
            max_delay=300.0,
            rate_limit_calls=100,
            rate_limit_window=1.0,
            circuit_breaker_enabled=True,
            circuit_breaker_threshold=50,
            circuit_breaker_timeout=120.0,
            circuit_breaker_half_open_calls=5,
            request_timeout=120.0,
        )

    @staticmethod
    def testing() -> ResilienceConfig:
        """
        Testing preset.

        Optimized for unit/integration tests:
        - Minimal retries (1)
        - Very high rate limits (effectively disabled)
        - Circuit breaker disabled
        - Short timeouts (5s) for fast failures
        """
        return ResilienceConfig(
            max_retries=1,
            retry_strategy="constant",
            base_delay=0.1,
            max_delay=1.0,
            rate_limit_calls=1000,
            rate_limit_window=1.0,
            circuit_breaker_enabled=False,
            circuit_breaker_threshold=100,
            circuit_breaker_timeout=1.0,
            circuit_breaker_half_open_calls=1,
            request_timeout=5.0,
        )

"""
Production-grade resilience patterns for Lakehouse-AppKit.

Provides retry logic, rate limiting, circuit breaker, and graceful degradation
for robust API communication with Databricks.
"""

import asyncio
import time
from enum import Enum
from typing import Optional, Callable, Any, TypeVar
from functools import wraps
import aiohttp
from lakehouse_appkit.resilience_config import get_default_resilience_config, ResilienceConfig


T = TypeVar('T')


class RetryStrategy(Enum):
    """Retry backoff strategies."""
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    CONSTANT = "constant"


class CircuitBreakerError(Exception):
    """Raised when circuit breaker is open."""
    pass


class CircuitBreakerState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Reject all requests
    HALF_OPEN = "half_open"  # Testing recovery


class CircuitBreaker:
    """
    Circuit breaker pattern implementation.
    
    Stops requests to a failing service to allow recovery.
    """
    
    def __init__(
        self,
        failure_threshold: int = 10,
        recovery_timeout: float = 60.0,
        half_open_max_calls: int = 3
    ):
        """
        Initialize circuit breaker.
        
        Args:
            failure_threshold: Consecutive failures before opening circuit
            recovery_timeout: Seconds to wait before attempting recovery
            half_open_max_calls: Test calls allowed in half-open state
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = CircuitBreakerState.CLOSED
        self.half_open_calls = 0
    
    async def __aenter__(self):
        """Context manager entry."""
        if self.state == CircuitBreakerState.OPEN:
            # Check if we should transition to half-open
            if self.last_failure_time and \
               time.time() - self.last_failure_time >= self.recovery_timeout:
                self.state = CircuitBreakerState.HALF_OPEN
                self.half_open_calls = 0
            else:
                raise CircuitBreakerError(
                    f"Circuit breaker is OPEN. "
                    f"Failed {self.failure_count} times. "
                    f"Recovery in {self.recovery_timeout - (time.time() - self.last_failure_time):.1f}s"
                )
        
        if self.state == CircuitBreakerState.HALF_OPEN:
            if self.half_open_calls >= self.half_open_max_calls:
                raise CircuitBreakerError(
                    "Circuit breaker is HALF_OPEN and test limit reached"
                )
            self.half_open_calls += 1
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if exc_type is None:
            # Success
            self.on_success()
        else:
            # Failure
            self.on_failure()
        return False
    
    def on_success(self):
        """Record successful call."""
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.half_open_max_calls:
                # Recovered!
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
                self.success_count = 0
                self.half_open_calls = 0
        else:
            self.failure_count = 0
    
    def on_failure(self):
        """Record failed call."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.state == CircuitBreakerState.HALF_OPEN:
            # Failed during recovery, go back to OPEN
            self.state = CircuitBreakerState.OPEN
            self.success_count = 0
            self.half_open_calls = 0
        elif self.failure_count >= self.failure_threshold:
            # Too many failures, open circuit
            self.state = CircuitBreakerState.OPEN


class RateLimiter:
    """
    Token bucket rate limiter.
    
    Limits the rate of API calls to prevent overwhelming the service.
    """
    
    def __init__(self, max_calls: int = 20, time_window: float = 1.0):
        """
        Initialize rate limiter.
        
        Args:
            max_calls: Maximum calls allowed in time window
            time_window: Time window in seconds
        """
        self.max_calls = max_calls
        self.time_window = time_window
        self.tokens = max_calls
        self.last_update = time.time()
        self._lock = asyncio.Lock()
    
    async def acquire(self):
        """
        Acquire permission to make a call.
        
        Blocks if rate limit is exceeded.
        """
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_update
            
            # Refill tokens based on elapsed time
            self.tokens = min(
                self.max_calls,
                self.tokens + (elapsed * self.max_calls / self.time_window)
            )
            self.last_update = now
            
            if self.tokens >= 1.0:
                self.tokens -= 1.0
            else:
                # Need to wait
                wait_time = (1.0 - self.tokens) * self.time_window / self.max_calls
                await asyncio.sleep(wait_time)
                self.tokens = 0.0
                self.last_update = time.time()


def retry(
    max_attempts: int = 3,
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exceptions: tuple = (Exception,)
):
    """
    Decorator for retry logic with configurable backoff.
    
    Args:
        max_attempts: Maximum number of attempts
        strategy: Retry strategy (exponential, linear, constant)
        base_delay: Base delay between retries (seconds)
        max_delay: Maximum delay between retries (seconds)
        exceptions: Tuple of exceptions to catch and retry
    
    Example:
        @retry(max_attempts=3, strategy=RetryStrategy.EXPONENTIAL)
        async def api_call():
            return await client.get_data()
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt < max_attempts - 1:
                        # Calculate delay
                        if strategy == RetryStrategy.EXPONENTIAL:
                            delay = min(base_delay * (2 ** attempt), max_delay)
                        elif strategy == RetryStrategy.LINEAR:
                            delay = min(base_delay * (attempt + 1), max_delay)
                        else:  # CONSTANT
                            delay = base_delay
                        
                        await asyncio.sleep(delay)
            
            # All attempts failed
            raise last_exception
        
        return wrapper
    return decorator


class ResilientClient:
    """
    Base class for resilient REST API clients.
    
    Provides retry, rate limiting, and circuit breaker patterns.
    All Databricks REST API clients inherit from this.
    """
    
    def __init__(self, config: Optional[ResilienceConfig] = None):
        """
        Initialize resilient client.
        
        Args:
            config: ResilienceConfig instance (uses default if not provided)
        """
        self.config = config or get_default_resilience_config()
        
        # Initialize rate limiter
        self.rate_limiter = RateLimiter(
            max_calls=self.config.rate_limit_calls,
            time_window=self.config.rate_limit_window
        )
        
        # Initialize circuit breaker
        if self.config.circuit_breaker_enabled:
            self.circuit_breaker = CircuitBreaker(
                failure_threshold=self.config.circuit_breaker_threshold,
                recovery_timeout=self.config.circuit_breaker_timeout,
                half_open_max_calls=self.config.circuit_breaker_half_open_calls
            )
        else:
            self.circuit_breaker = None
        
        # HTTP session
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.config.request_timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session
    
    async def close(self):
        """Close HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
    
    async def _resilient_request(self, request_func: Callable, *args, **kwargs) -> Any:
        """
        Execute request with resilience patterns applied.
        
        Args:
            request_func: Async function to execute
            *args: Arguments to pass to request_func
            **kwargs: Keyword arguments to pass to request_func
        
        Returns:
            Result from request_func
        
        Raises:
            Exception: If all retry attempts fail
        """
        last_exception = None
        
        for attempt in range(self.config.max_retries + 1):
            try:
                # Rate limiting
                await self.rate_limiter.acquire()
                
                # Circuit breaker
                if self.circuit_breaker:
                    async with self.circuit_breaker:
                        return await request_func(*args, **kwargs)
                else:
                    return await request_func(*args, **kwargs)
                    
            except CircuitBreakerError:
                # Circuit is open, don't retry
                raise
            except Exception as e:
                last_exception = e
                
                # Convert aiohttp exceptions to our custom exceptions
                if hasattr(e, '__module__') and 'aiohttp' in e.__module__:
                    from lakehouse_appkit.sdk.exceptions import ConnectionError as AppConnectionError
                    status = getattr(e, 'status', None)
                    message = str(e)
                    last_exception = AppConnectionError(f"{status}: {message}" if status else message)
                
                if attempt < self.config.max_retries:
                    # Calculate delay
                    if self.config.retry_strategy == "exponential":
                        delay = min(
                            self.config.base_delay * (2 ** attempt),
                            self.config.max_delay
                        )
                    elif self.config.retry_strategy == "linear":
                        delay = min(
                            self.config.base_delay * (attempt + 1),
                            self.config.max_delay
                        )
                    else:  # constant
                        delay = self.config.base_delay
                    
                    await asyncio.sleep(delay)
        
        # All attempts failed
        raise last_exception


async def with_fallback(
    primary_func: Callable[[], T],
    fallback_func: Optional[Callable[[], T]] = None,
    fallback_value: Optional[T] = None
) -> T:
    """
    Execute function with fallback on failure.
    
    Args:
        primary_func: Primary function to execute
        fallback_func: Fallback function if primary fails
        fallback_value: Fallback value if both fail
    
    Returns:
        Result from primary, fallback function, or fallback value
    
    Example:
        result = await with_fallback(
            primary_func=lambda: client.get_data(),
            fallback_func=lambda: cache.get_cached_data(),
            fallback_value=[]
        )
    """
    try:
        return await primary_func()
    except Exception:
        if fallback_func:
            try:
                return await fallback_func()
            except Exception:
                pass
        
        if fallback_value is not None:
            return fallback_value
        
        # No fallback available
        raise

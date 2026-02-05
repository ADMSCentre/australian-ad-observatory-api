"""Retry utilities for transient failure handling.

Provides decorators and context managers for retrying operations
with exponential backoff.
"""

import time
import logging
from typing import Callable, Any, TypeVar, Optional, Type, Tuple
from functools import wraps

# Type variable for decorator
F = TypeVar('F', bound=Callable[..., Any])

logger = logging.getLogger(__name__)


class RetryConfig:
    """Configuration for retry behavior."""
    
    def __init__(
        self,
        max_attempts: int = 3,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        backoff_multiplier: float = 2.0,
        retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,)
    ):
        """Initialize retry configuration.
        
        Args:
            max_attempts: Maximum number of attempts (default: 3)
            initial_delay: Initial delay in seconds (default: 1.0)
            max_delay: Maximum delay in seconds (default: 60.0)
            backoff_multiplier: Multiplier for exponential backoff (default: 2.0)
            retryable_exceptions: Tuple of exceptions to retry on
        """
        self.max_attempts = max_attempts
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.backoff_multiplier = backoff_multiplier
        self.retryable_exceptions = retryable_exceptions


def retry(config: Optional[RetryConfig] = None) -> Callable[[F], F]:
    """Decorator to retry a function with exponential backoff.
    
    Args:
        config: RetryConfig instance (uses defaults if not provided)
        
    Returns:
        Decorated function that retries on failure
        
    Example:
        @retry(RetryConfig(max_attempts=3, initial_delay=1.0))
        def fetch_data():
            # May fail temporarily
            return api.get_data()
    """
    if config is None:
        config = RetryConfig()
    
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            delay = config.initial_delay
            last_exception = None
            
            for attempt in range(1, config.max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except config.retryable_exceptions as e:
                    last_exception = e
                    
                    if attempt == config.max_attempts:
                        # Last attempt failed
                        logger.error(
                            f"{func.__name__} failed after {config.max_attempts} attempts: {e}"
                        )
                        raise
                    
                    # Log retry attempt
                    logger.warning(
                        f"{func.__name__} failed (attempt {attempt}/{config.max_attempts}), "
                        f"retrying in {delay:.1f}s: {e}"
                    )
                    
                    time.sleep(delay)
                    delay = min(delay * config.backoff_multiplier, config.max_delay)
            
            # Should never reach here, but just in case
            if last_exception:
                raise last_exception
        
        return wrapper  # type: ignore
    
    return decorator


def retry_with_exponential_backoff(
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    backoff_multiplier: float = 2.0,
    retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,)
) -> Callable[[F], F]:
    """Convenience decorator for retry with common settings.
    
    Args:
        max_attempts: Maximum number of attempts
        initial_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
        backoff_multiplier: Multiplier for exponential backoff
        retryable_exceptions: Tuple of exceptions to retry on
        
    Returns:
        Decorated function that retries on failure
        
    Example:
        @retry_with_exponential_backoff(max_attempts=5)
        def risky_operation():
            return do_something()
    """
    config = RetryConfig(
        max_attempts=max_attempts,
        initial_delay=initial_delay,
        max_delay=max_delay,
        backoff_multiplier=backoff_multiplier,
        retryable_exceptions=retryable_exceptions
    )
    return retry(config)


class RetryableOperation:
    """Context manager for retrying a block of code."""
    
    def __init__(
        self,
        max_attempts: int = 3,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        backoff_multiplier: float = 2.0,
        retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
        operation_name: str = "Operation"
    ):
        """Initialize retryable operation context manager.
        
        Args:
            max_attempts: Maximum number of attempts
            initial_delay: Initial delay in seconds
            max_delay: Maximum delay in seconds
            backoff_multiplier: Multiplier for exponential backoff
            retryable_exceptions: Tuple of exceptions to retry on
            operation_name: Name of operation (for logging)
        """
        self.config = RetryConfig(
            max_attempts=max_attempts,
            initial_delay=initial_delay,
            max_delay=max_delay,
            backoff_multiplier=backoff_multiplier,
            retryable_exceptions=retryable_exceptions
        )
        self.operation_name = operation_name
        self.attempt = 0
    
    def __enter__(self) -> 'RetryableOperation':
        self.attempt += 1
        return self
    
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        """Handle exceptions and retry if necessary.
        
        Returns:
            True if exception was handled (will retry), False otherwise
        """
        if exc_type is None:
            # No exception, success
            return False
        
        if not issubclass(exc_type, self.config.retryable_exceptions):
            # Exception is not retryable
            return False
        
        if self.attempt >= self.config.max_attempts:
            # Max attempts reached, don't suppress exception
            logger.error(
                f"{self.operation_name} failed after {self.config.max_attempts} attempts: {exc_val}"
            )
            return False
        
        # Calculate delay for next attempt
        delay = self.config.initial_delay * (self.config.backoff_multiplier ** (self.attempt - 1))
        delay = min(delay, self.config.max_delay)
        
        logger.warning(
            f"{self.operation_name} failed (attempt {self.attempt}/{self.config.max_attempts}), "
            f"retrying in {delay:.1f}s: {exc_val}"
        )
        
        time.sleep(delay)
        
        # Suppress exception to allow retry
        return True


def retry_loop(
    func: Callable,
    *args: Any,
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    backoff_multiplier: float = 2.0,
    retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
    **kwargs: Any
) -> Any:
    """Execute a function with retry logic.
    
    Args:
        func: Function to execute
        *args: Positional arguments to pass to func
        max_attempts: Maximum number of attempts
        initial_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
        backoff_multiplier: Multiplier for exponential backoff
        retryable_exceptions: Tuple of exceptions to retry on
        **kwargs: Keyword arguments to pass to func
        
    Returns:
        Return value from func
        
    Example:
        result = retry_loop(
            api.fetch_data,
            max_attempts=3,
            initial_delay=1.0,
            retryable_exceptions=(ConnectionError, TimeoutError)
        )
    """
    config = RetryConfig(
        max_attempts=max_attempts,
        initial_delay=initial_delay,
        max_delay=max_delay,
        backoff_multiplier=backoff_multiplier,
        retryable_exceptions=retryable_exceptions
    )
    
    delay = config.initial_delay
    last_exception = None
    
    for attempt in range(1, config.max_attempts + 1):
        try:
            return func(*args, **kwargs)
        except config.retryable_exceptions as e:
            last_exception = e
            
            if attempt == config.max_attempts:
                logger.error(
                    f"{func.__name__} failed after {config.max_attempts} attempts: {e}"
                )
                raise
            
            logger.warning(
                f"{func.__name__} failed (attempt {attempt}/{config.max_attempts}), "
                f"retrying in {delay:.1f}s: {e}"
            )
            
            time.sleep(delay)
            delay = min(delay * config.backoff_multiplier, config.max_delay)
    
    # Should never reach here
    if last_exception:
        raise last_exception

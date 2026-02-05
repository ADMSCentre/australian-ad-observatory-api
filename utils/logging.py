"""Centralized logging configuration for AAO Ingestion Service.

Provides structured logging with configurable levels and formats.
"""

import logging
import sys
from pathlib import Path
from typing import Optional


# Create logs directory if it doesn't exist
LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)


def setup_logger(
    name: str,
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    console: bool = True
) -> logging.Logger:
    """Set up a logger with both console and file handlers.
    
    Args:
        name: Logger name (typically __name__)
        level: Logging level (default: INFO)
        log_file: Optional log file path (relative to logs/ directory)
        console: Whether to also output to console (default: True)
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Only configure if not already configured
    if logger.handlers:
        return logger
    
    logger.setLevel(level)
    
    # Create formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        log_path = LOGS_DIR / log_file
        file_handler = logging.FileHandler(log_path)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance (assumes it was already set up).
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)


# Script-specific loggers
def get_backfill_logger(script_name: str) -> logging.Logger:
    """Get a logger for backfill scripts with dedicated log file.
    
    Args:
        script_name: Name of the backfill script (e.g., 'rdo', 'clip')
        
    Returns:
        Configured logger instance
    """
    log_file = f"backfill_{script_name}.log"
    return setup_logger(
        name=f"backfill.{script_name}",
        level=logging.DEBUG,
        log_file=log_file,
        console=True
    )

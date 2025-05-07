"""
Logging configuration for the Heart Disease ETL pipeline.
"""

import os
import logging
import sys
from logging.handlers import RotatingFileHandler

def setup_logging(log_file='logs/etl_pipeline.log', log_level='INFO'):
    """
    Set up logging for the application.
    
    This function configures logging to output to both a file and stdout
    with the specified log level.
    
    Args:
        log_file (str): Path to the log file.
        log_level (str): Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    
    Returns:
        logging.Logger: Configured logger instance.
    """
    # Create logs directory if it doesn't exist
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # Parse log level
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")
    
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(numeric_level)
    
    # Remove existing handlers to avoid duplicate logs
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create formatters
    verbose_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    simple_formatter = logging.Formatter(
        '%(levelname)s: %(message)s'
    )
    
    # Create and configure file handler
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5
    )
    file_handler.setLevel(numeric_level)
    file_handler.setFormatter(verbose_formatter)
    
    # Create and configure console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(simple_formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logger.info(f"Logging configured with level {log_level}")
    
    return logger
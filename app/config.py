"""
Configuration module for the Heart Disease ETL pipeline.
"""

import os
from typing import Optional

class Config:
    """
    Configuration class for the ETL pipeline.
    
    This class provides configuration parameters for the ETL pipeline,
    including database connection details and other settings.
    """
    
    def __init__(self):
        """
        Initialize the configuration.
        
        Configuration parameters are read from environment variables
        with sensible defaults.
        """
        # Database configuration
        self.db_host = os.environ.get('DB_HOST', 'db')
        self.db_port = os.environ.get('DB_PORT', '5432')
        self.db_name = os.environ.get('DB_NAME', 'heart_disease')
        self.db_user = os.environ.get('DB_USER', 'postgres')
        self.db_password = os.environ.get('DB_PASSWORD', 'postgres')
        
        # Data source configuration
        self.dataset_id = int(os.environ.get('DATASET_ID', '45'))  # UCI Heart Disease dataset ID
        
        # ETL configuration
        self.log_level = os.environ.get('LOG_LEVEL', 'INFO')
        self.log_file = os.environ.get('LOG_FILE', 'logs/etl_pipeline.log')
    
    def get_db_uri(self) -> str:
        """
        Get the SQLAlchemy database URI.
        
        Returns:
            str: SQLAlchemy database URI
        """
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
"""
Data loading module for the Heart Disease ETL pipeline.
"""

import logging
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from app.database.connection import DatabaseManager
from app.database.models import HeartDisease, Base

logger = logging.getLogger(__name__)

class HeartDiseaseLoader:
    """
    Heart Disease data loader.
    
    This class is responsible for loading transformed Heart Disease 
    data into a PostgreSQL database.
    """
    
    def __init__(self, db_uri: str):
        """
        Initialize the loader.
        
        Args:
            db_uri (str): SQLAlchemy database URI.
        """
        self.db_manager = DatabaseManager(db_uri)
        logger.info("Initialized loader")
    
    def load(self, transformed_data: pd.DataFrame) -> int:
        """
        Load transformed data into the PostgreSQL database.
        
        Args:
            transformed_data (pd.DataFrame): Transformed data to load.
            
        Returns:
            int: Number of records loaded.
        """
        logger.info(f"Loading {len(transformed_data)} records into the database")
        
        try:
            # Create tables if they don't exist
            self.db_manager.create_tables()
            
            # Load data in chunks to avoid memory issues with large datasets
            records_loaded = self._load_data_in_chunks(transformed_data)
            
            logger.info(f"Successfully loaded {records_loaded} records into the database")
            
            return records_loaded
            
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}", exc_info=True)
            raise
    
    def _load_data_in_chunks(self, df: pd.DataFrame, chunk_size: int = 1000) -> int:
        """
        Load data into the database in chunks.
        
        Args:
            df (pd.DataFrame): Data to load.
            chunk_size (int): Number of records per chunk.
            
        Returns:
            int: Total number of records loaded.
        """
        total_records = 0
        total_chunks = (len(df) + chunk_size - 1) // chunk_size
        
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            chunk_records = self._load_chunk(chunk)
            total_records += chunk_records
            chunk_num = i // chunk_size + 1
            logger.info(f"Loaded chunk {chunk_num}/{total_chunks}: {chunk_records} records")
        
        return total_records
    
    def _load_chunk(self, chunk: pd.DataFrame) -> int:
        """
        Load a chunk of data into the database.
        
        Args:
            chunk (pd.DataFrame): Chunk of data to load.
            
        Returns:
            int: Number of records loaded.
        """
        try:
            # Convert DataFrame to dictionary
            records = chunk.to_dict(orient='records')
            
            # Insert records into the database
            with self.db_manager.session_scope() as session:
                # Convert dictionary records to ORM objects
                orm_objects = [HeartDisease(**record) for record in records]
                
                # Add all objects to the session
                session.bulk_save_objects(orm_objects)
                
                # Commit is handled by the context manager
            
            return len(records)
            
        except SQLAlchemyError as e:
            logger.error(f"Database error loading chunk: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Error loading chunk: {str(e)}", exc_info=True)
            raise
"""
Database connection management for the Heart Disease ETL pipeline.
"""

import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager
from app.database.models import Base

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Database connection manager.
    
    This class manages database connections and provides methods
    for interacting with the database.
    """
    
    def __init__(self, db_uri: str):
        """
        Initialize the database manager.
        
        Args:
            db_uri (str): SQLAlchemy database URI.
        """
        self.db_uri = db_uri
        self.engine = None
        self._session_factory = None
        
        logger.info(f"Initializing database connection to {db_uri}")
        self._initialize_engine()
    
    def _initialize_engine(self):
        """
        Initialize the SQLAlchemy engine and session factory.
        """
        try:
            # Create engine
            self.engine = create_engine(
                self.db_uri,
                echo=False,  # Set to True for SQL debugging
                pool_pre_ping=True,  # Verify connections before use
                pool_recycle=3600,  # Recycle connections after 1 hour
            )
            
            # Create session factory
            self._session_factory = sessionmaker(bind=self.engine)
            
            logger.debug("Database engine and session factory initialized")
        
        except SQLAlchemyError as e:
            logger.error(f"Failed to initialize database engine: {str(e)}")
            raise
    
    def create_tables(self):
        """
        Create all tables defined in the models.
        """
        try:
            logger.info("Creating database tables if they don't exist")
            Base.metadata.create_all(self.engine)
            logger.info("Database tables created successfully")
        
        except SQLAlchemyError as e:
            logger.error(f"Failed to create database tables: {str(e)}")
            raise
    
    @contextmanager
    def session_scope(self):
        """
        Provide a transactional scope around a series of operations.
        
        This context manager ensures that the session is properly closed
        and that transactions are properly committed or rolled back.
        
        Yields:
            Session: SQLAlchemy session
        """
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            logger.error(f"Database session error: {str(e)}")
            session.rollback()
            raise
        finally:
            session.close()
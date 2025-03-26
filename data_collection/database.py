"""
Database Configuration Module
----------------------------
This module handles database configuration and connection management.
It includes functionality for:
- Setting up database connections
- Creating database schemas
- Managing connection pools
"""

import os
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("database.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DatabaseManager:
    """Class to handle database connections and configuration"""
    
    def __init__(self, db_uri: str = None, pool_size: int = 5, max_overflow: int = 10):
        """
        Initialize the database manager
        
        Args:
            db_uri: SQLAlchemy database URI. If None, uses environment variable or default
            pool_size: Connection pool size
            max_overflow: Maximum number of connections to overflow
        """
        if db_uri is None:
            db_uri = os.environ.get('DATABASE_URI', 'sqlite:///saudi_stocks.db')
        
        self.db_uri = db_uri
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.engine = None
        self.session_factory = None
        self.scoped_session = None
        
        # Initialize the engine and session factory
        self._initialize_engine()
    
    def _initialize_engine(self) -> None:
        """Initialize the SQLAlchemy engine and session factory"""
        try:
            # Create engine with connection pooling
            self.engine = create_engine(
                self.db_uri,
                poolclass=QueuePool,
                pool_size=self.pool_size,
                max_overflow=self.max_overflow,
                pool_timeout=30,
                pool_recycle=3600  # Recycle connections after 1 hour
            )
            
            # Create session factory
            self.session_factory = sessionmaker(bind=self.engine)
            
            # Create scoped session for thread safety
            self.scoped_session = scoped_session(self.session_factory)
            
            logger.info(f"Database engine initialized with URI: {self.db_uri}")
            
        except Exception as e:
            logger.error(f"Error initializing database engine: {str(e)}")
            raise
    
    def create_session(self):
        """
        Create a new database session
        
        Returns:
            SQLAlchemy session
        """
        if self.session_factory is None:
            self._initialize_engine()
        
        return self.session_factory()
    
    def get_scoped_session(self):
        """
        Get a thread-local scoped session
        
        Returns:
            SQLAlchemy scoped session
        """
        if self.scoped_session is None:
            self._initialize_engine()
        
        return self.scoped_session
    
    def create_all_tables(self, base) -> None:
        """
        Create all tables defined in the SQLAlchemy Base
        
        Args:
            base: SQLAlchemy declarative base
        """
        try:
            base.metadata.create_all(self.engine)
            logger.info("All database tables created")
        except Exception as e:
            logger.error(f"Error creating database tables: {str(e)}")
            raise
    
    def get_engine_status(self) -> Dict[str, Any]:
        """
        Get status information about the database engine
        
        Returns:
            Dictionary with engine status information
        """
        if self.engine is None:
            return {'status': 'Not initialized'}
        
        return {
            'status': 'Active',
            'pool_size': self.pool_size,
            'max_overflow': self.max_overflow,
            'db_uri': self.db_uri
        }
    
    def dispose_engine(self) -> None:
        """Dispose of the database engine and all connections"""
        if self.engine is not None:
            self.engine.dispose()
            logger.info("Database engine disposed")


# Example usage
if __name__ == "__main__":
    from sqlalchemy.ext.declarative import declarative_base
    
    # Create a base class for declarative models
    Base = declarative_base()
    
    # Define a simple model for testing
    from sqlalchemy import Column, Integer, String
    
    class TestModel(Base):
        __tablename__ = 'test_table'
        id = Column(Integer, primary_key=True)
        name = Column(String(50))
    
    # Initialize the database manager
    db_manager = DatabaseManager()
    
    # Create tables
    db_manager.create_all_tables(Base)
    
    # Create a session and add a test record
    session = db_manager.create_session()
    try:
        test_record = TestModel(name="Test Record")
        session.add(test_record)
        session.commit()
        print("Test record added successfully")
    except Exception as e:
        session.rollback()
        print(f"Error adding test record: {str(e)}")
    finally:
        session.close()
    
    # Get engine status
    status = db_manager.get_engine_status()
    print(f"Engine status: {status}")
    
    # Dispose of the engine
    db_manager.dispose_engine()

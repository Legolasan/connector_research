"""
Database Service
Handles PostgreSQL database connections and models for persistent storage.
"""

import os
import json
from typing import Optional, List, Dict, Any
from datetime import datetime

from sqlalchemy import create_engine, Column, String, Integer, Float, Text, DateTime, JSON, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool

# Database URL from environment
DATABASE_URL = os.getenv("DATABASE_URL")

# SQLAlchemy setup
Base = declarative_base()
engine = None
SessionLocal = None


class ConnectorModel(Base):
    """SQLAlchemy model for Connector storage."""
    __tablename__ = "connectors"
    
    id = Column(String(255), primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    connector_type = Column(String(50), nullable=False)
    status = Column(String(50), default="not_started")
    github_url = Column(String(500), nullable=True)
    description = Column(Text, default="")
    
    # Fivetran URLs stored as JSON
    fivetran_urls = Column(JSON, nullable=True)
    
    # Metadata
    objects_count = Column(Integer, default=0)
    vectors_count = Column(Integer, default=0)
    fivetran_parity = Column(Float, nullable=True)
    
    # Progress stored as JSON
    progress = Column(JSON, default=dict)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    
    # Sources
    sources = Column(JSON, default=list)
    
    # Pinecone index name
    pinecone_index = Column(String(255), default="")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'id': self.id,
            'name': self.name,
            'connector_type': self.connector_type,
            'status': self.status,
            'github_url': self.github_url,
            'description': self.description,
            'fivetran_urls': self.fivetran_urls,
            'objects_count': self.objects_count,
            'vectors_count': self.vectors_count,
            'fivetran_parity': self.fivetran_parity,
            'progress': self.progress or {},
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'sources': self.sources or [],
            'pinecone_index': self.pinecone_index
        }


class ResearchDocumentModel(Base):
    """SQLAlchemy model for storing research documents."""
    __tablename__ = "research_documents"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    connector_id = Column(String(255), index=True, nullable=False)
    content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


def init_database() -> bool:
    """Initialize database connection and create tables.
    
    Returns:
        True if database is available and initialized, False otherwise
    """
    global engine, SessionLocal, DATABASE_URL
    
    # Re-read DATABASE_URL in case it was set after module import
    DATABASE_URL = os.getenv("DATABASE_URL")
    
    if not DATABASE_URL:
        print("⚠ DATABASE_URL not set, using file-based storage")
        return False
    
    try:
        # Handle Railway's postgres:// vs postgresql:// URL format
        db_url = DATABASE_URL
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        
        # Create engine with connection pooling
        engine = create_engine(
            db_url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,  # Verify connections before using
            echo=False  # Set to True for SQL debugging
        )
        
        # Create session factory
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        
        # Create tables
        Base.metadata.create_all(bind=engine)
        
        print(f"✓ Database initialized successfully")
        return True
        
    except Exception as e:
        print(f"⚠ Database initialization failed: {e}")
        engine = None
        SessionLocal = None
        return False


def get_db_session() -> Optional[Session]:
    """Get a database session.
    
    Returns:
        Database session or None if database is not available
    """
    if SessionLocal is None:
        return None
    return SessionLocal()


def is_database_available() -> bool:
    """Check if database is available."""
    return engine is not None and SessionLocal is not None


class DatabaseConnectorStorage:
    """Database-backed storage for connectors."""
    
    def __init__(self):
        """Initialize storage."""
        self.db_available = is_database_available()
    
    def get_session(self) -> Optional[Session]:
        """Get a new database session."""
        return get_db_session()
    
    def create_connector(self, connector_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create a new connector in database."""
        session = self.get_session()
        if not session:
            return None
        
        try:
            connector = ConnectorModel(
                id=connector_data['id'],
                name=connector_data['name'],
                connector_type=connector_data['connector_type'],
                status=connector_data.get('status', 'not_started'),
                github_url=connector_data.get('github_url'),
                description=connector_data.get('description', ''),
                fivetran_urls=connector_data.get('fivetran_urls'),
                objects_count=connector_data.get('objects_count', 0),
                vectors_count=connector_data.get('vectors_count', 0),
                fivetran_parity=connector_data.get('fivetran_parity'),
                progress=connector_data.get('progress', {}),
                sources=connector_data.get('sources', []),
                pinecone_index=connector_data.get('pinecone_index', f"{connector_data['id']}-docs")
            )
            
            session.add(connector)
            session.commit()
            session.refresh(connector)
            
            return connector.to_dict()
        except Exception as e:
            session.rollback()
            print(f"Error creating connector in DB: {e}")
            raise
        finally:
            session.close()
    
    def get_connector(self, connector_id: str) -> Optional[Dict[str, Any]]:
        """Get a connector by ID."""
        session = self.get_session()
        if not session:
            return None
        
        try:
            connector = session.query(ConnectorModel).filter(
                ConnectorModel.id == connector_id
            ).first()
            
            if connector:
                return connector.to_dict()
            return None
        finally:
            session.close()
    
    def list_connectors(self) -> List[Dict[str, Any]]:
        """List all connectors."""
        session = self.get_session()
        if not session:
            return []
        
        try:
            connectors = session.query(ConnectorModel).all()
            return [c.to_dict() for c in connectors]
        finally:
            session.close()
    
    def update_connector(self, connector_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update a connector."""
        session = self.get_session()
        if not session:
            return None
        
        try:
            connector = session.query(ConnectorModel).filter(
                ConnectorModel.id == connector_id
            ).first()
            
            if not connector:
                return None
            
            # Update allowed fields
            for key, value in updates.items():
                if hasattr(connector, key):
                    setattr(connector, key, value)
            
            connector.updated_at = datetime.utcnow()
            session.commit()
            session.refresh(connector)
            
            return connector.to_dict()
        except Exception as e:
            session.rollback()
            print(f"Error updating connector in DB: {e}")
            return None
        finally:
            session.close()
    
    def delete_connector(self, connector_id: str) -> bool:
        """Delete a connector."""
        session = self.get_session()
        if not session:
            return False
        
        try:
            connector = session.query(ConnectorModel).filter(
                ConnectorModel.id == connector_id
            ).first()
            
            if not connector:
                return False
            
            # Also delete associated research document
            session.query(ResearchDocumentModel).filter(
                ResearchDocumentModel.connector_id == connector_id
            ).delete()
            
            session.delete(connector)
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            print(f"Error deleting connector from DB: {e}")
            return False
        finally:
            session.close()
    
    def save_research_document(self, connector_id: str, content: str) -> bool:
        """Save or update research document."""
        session = self.get_session()
        if not session:
            return False
        
        try:
            # Check if document exists
            doc = session.query(ResearchDocumentModel).filter(
                ResearchDocumentModel.connector_id == connector_id
            ).first()
            
            if doc:
                doc.content = content
                doc.updated_at = datetime.utcnow()
            else:
                doc = ResearchDocumentModel(
                    connector_id=connector_id,
                    content=content
                )
                session.add(doc)
            
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            print(f"Error saving research document: {e}")
            return False
        finally:
            session.close()
    
    def get_research_document(self, connector_id: str) -> Optional[str]:
        """Get research document content."""
        session = self.get_session()
        if not session:
            return None
        
        try:
            doc = session.query(ResearchDocumentModel).filter(
                ResearchDocumentModel.connector_id == connector_id
            ).first()
            
            if doc:
                return doc.content
            return None
        finally:
            session.close()


# Singleton instance
_db_storage: Optional[DatabaseConnectorStorage] = None


def get_database_storage() -> Optional[DatabaseConnectorStorage]:
    """Get the database storage instance."""
    global _db_storage
    if _db_storage is None and is_database_available():
        _db_storage = DatabaseConnectorStorage()
    return _db_storage

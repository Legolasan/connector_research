"""
Database Service
Handles PostgreSQL database connections and models for persistent storage.
Includes pgvector support for vector similarity search.
"""

import os
import json
from typing import Optional, List, Dict, Any
from datetime import datetime

from sqlalchemy import create_engine, Column, String, Integer, Float, Text, DateTime, JSON, Boolean, Index, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool

# Try to import pgvector
try:
    from pgvector.sqlalchemy import Vector
    PGVECTOR_AVAILABLE = True
except ImportError:
    PGVECTOR_AVAILABLE = False
    Vector = None

# Database URL from environment
DATABASE_URL = os.getenv("DATABASE_URL")

# Embedding dimension for text-embedding-3-small
EMBEDDING_DIMENSION = 1536

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
    hevo_github_url = Column(String(500), nullable=True)  # Optional Hevo connector GitHub URL
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
    
    # Citation validation columns
    citation_report_json = Column(JSON, nullable=True)
    citation_overrides_json = Column(JSON, nullable=True)
    validation_attempts = Column(Integer, default=0, nullable=True)
    assumptions_section = Column(Text, nullable=True)
    
    # Claim graph storage columns
    claims_json = Column(JSON, nullable=True)
    canonical_facts_json = Column(JSON, nullable=True)
    evidence_map_json = Column(JSON, nullable=True)


class DocumentChunkModel(Base):
    """SQLAlchemy model for storing document chunks with embeddings."""
    __tablename__ = "document_chunks"
    
    id = Column(String(255), primary_key=True)
    connector_id = Column(String(255), index=True, nullable=False)
    connector_name = Column(String(255), nullable=False)
    chunk_index = Column(Integer, nullable=False)
    text = Column(Text, nullable=False)
    section = Column(String(255), default="General")
    source_type = Column(String(50), default="research")
    # Embedding stored as JSON array (fallback if pgvector not available)
    embedding_json = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Add index for connector_id queries
    __table_args__ = (
        Index('idx_chunks_connector', 'connector_id'),
    )


# Track if pgvector extension is actually available in the database
# This is set to True only if the extension is successfully enabled
PGVECTOR_EXTENSION_AVAILABLE = False

# Note: VECTOR column will be added dynamically after checking if extension is available


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
        
        # Test the connection with a simple query first
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
            print("✓ Database connection test successful")
        except Exception as conn_error:
            print(f"⚠ Database connection test failed: {conn_error}")
            raise RuntimeError(f"Database connection failed: {conn_error}") from conn_error
        
        # Enable pgvector extension if available
        global PGVECTOR_EXTENSION_AVAILABLE
        PGVECTOR_EXTENSION_AVAILABLE = False
        
        if PGVECTOR_AVAILABLE:
            try:
                with engine.connect() as conn:
                    conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
                    conn.commit()
                # Verify extension is actually available
                with engine.connect() as conn:
                    result = conn.execute(text("SELECT 1 FROM pg_extension WHERE extname = 'vector'"))
                    if result.fetchone():
                        PGVECTOR_EXTENSION_AVAILABLE = True
                        print("✓ pgvector extension enabled and verified")
                    else:
                        print("⚠ pgvector extension not found after creation attempt")
            except Exception as e:
                print(f"⚠ Could not enable pgvector extension: {e}")
                print("  → Will use embedding_json (JSON) storage instead")
        
        # Only add VECTOR column if extension is actually available
        # Note: This is for runtime use. Schema changes should be done via Alembic migrations.
        if PGVECTOR_EXTENSION_AVAILABLE and not hasattr(DocumentChunkModel, 'embedding'):
            DocumentChunkModel.embedding = Column(Vector(EMBEDDING_DIMENSION), nullable=True)
        
        # Verify database tables exist (but don't create them - that's handled by Alembic migrations)
        try:
            with engine.connect() as conn:
                # Check if tables exist
                tables_exist = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name IN ('connectors', 'research_documents', 'document_chunks')
                    )
                """)).scalar()
                
                if not tables_exist:
                    print("⚠ Database tables not found. Run migrations first: python migrate.py upgrade")
                    print("  → For existing databases, run: alembic stamp head")
                else:
                    print("✓ Database tables verified")
                    
                    # Auto-add missing columns (for deployments where migrations haven't run)
                    try:
                        # Check and add hevo_github_url column to connectors
                        hevo_col_exists = conn.execute(text("""
                            SELECT EXISTS (
                                SELECT FROM information_schema.columns 
                                WHERE table_name = 'connectors' AND column_name = 'hevo_github_url'
                            )
                        """)).scalar()
                        
                        if not hevo_col_exists:
                            print("⚠ Adding missing column: connectors.hevo_github_url")
                            conn.execute(text("ALTER TABLE connectors ADD COLUMN hevo_github_url VARCHAR(500)"))
                            conn.commit()
                            print("✓ Added hevo_github_url column")
                        
                        # Check and add claim graph columns to research_documents
                        claim_graph_columns = [
                            ("citation_report_json", "JSONB"),
                            ("citation_overrides_json", "JSONB"),
                            ("validation_attempts", "INTEGER"),
                            ("assumptions_section", "TEXT"),
                            ("claims_json", "JSONB"),
                            ("canonical_facts_json", "JSONB"),
                            ("evidence_map_json", "JSONB"),
                        ]
                        
                        for col_name, col_type in claim_graph_columns:
                            col_exists = conn.execute(text(f"""
                                SELECT EXISTS (
                                    SELECT FROM information_schema.columns 
                                    WHERE table_name = 'research_documents' AND column_name = '{col_name}'
                                )
                            """)).scalar()
                            
                            if not col_exists:
                                print(f"⚠ Adding missing column: research_documents.{col_name}")
                                conn.execute(text(f"ALTER TABLE research_documents ADD COLUMN {col_name} {col_type}"))
                                conn.commit()
                                print(f"✓ Added {col_name} column")
                                
                    except Exception as col_error:
                        print(f"⚠ Could not add missing columns: {col_error}")
                    
                    # Check migration status (warn if pending, but don't block)
                    try:
                        # Check if alembic_version table exists
                        alembic_version_exists = conn.execute(text("""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables 
                                WHERE table_name = 'alembic_version'
                            )
                        """)).scalar()
                        
                        if not alembic_version_exists:
                            print("⚠ Alembic version tracking not found. Consider running: alembic stamp head")
                    except Exception:
                        pass  # Ignore errors in migration check
                        
        except Exception as table_error:
            # Don't fail on table check - migrations will handle schema
            print(f"⚠ Could not verify database tables: {table_error}")
            print("  → Tables will be created by migrations if needed")
        
        print(f"✓ Database connection initialized (migrations not applied - use 'python migrate.py upgrade')")
        return True
        
    except Exception as e:
        import traceback
        print(f"⚠ Database initialization failed: {e}")
        print(f"⚠ Traceback: {traceback.format_exc()}")
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
                hevo_github_url=connector_data.get('hevo_github_url'),
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
    
    def save_research_document(
        self,
        connector_id: str,
        content: str,
        claims_json: Optional[Dict[str, Any]] = None,
        canonical_facts_json: Optional[Dict[str, Any]] = None,
        evidence_map_json: Optional[Dict[str, Any]] = None,
        citation_report_json: Optional[Dict[str, Any]] = None,
        citation_overrides_json: Optional[Dict[str, Any]] = None,
        validation_attempts: Optional[int] = None,
        assumptions_section: Optional[str] = None
    ) -> bool:
        """Save or update research document with claim graph data."""
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
                if claims_json is not None:
                    doc.claims_json = claims_json
                if canonical_facts_json is not None:
                    doc.canonical_facts_json = canonical_facts_json
                if evidence_map_json is not None:
                    doc.evidence_map_json = evidence_map_json
                if citation_report_json is not None:
                    doc.citation_report_json = citation_report_json
                if citation_overrides_json is not None:
                    doc.citation_overrides_json = citation_overrides_json
                if validation_attempts is not None:
                    doc.validation_attempts = validation_attempts
                if assumptions_section is not None:
                    doc.assumptions_section = assumptions_section
            else:
                doc = ResearchDocumentModel(
                    connector_id=connector_id,
                    content=content,
                    claims_json=claims_json,
                    canonical_facts_json=canonical_facts_json,
                    evidence_map_json=evidence_map_json,
                    citation_report_json=citation_report_json,
                    citation_overrides_json=citation_overrides_json,
                    validation_attempts=validation_attempts or 0,
                    assumptions_section=assumptions_section
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

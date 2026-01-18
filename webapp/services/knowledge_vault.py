"""
📚 Knowledge Vault Service
Pre-indexed official documentation for enterprise-grade connector research.

"The Vault remembers what the web forgets."

Now with:
- PDF parsing support (500+ documents)
- Bulk upload with background processing
- Progress tracking for large batches
"""

import os
import re
import io
import hashlib
import asyncio
import traceback
from typing import List, Dict, Any, Optional, Tuple, Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
import tempfile
import json

from openai import OpenAI
from sqlalchemy import text
from dotenv import load_dotenv

from services.database import (
    get_db_session, is_database_available, init_database,
    DocumentChunkModel, PGVECTOR_AVAILABLE, PGVECTOR_EXTENSION_AVAILABLE, EMBEDDING_DIMENSION
)

load_dotenv()


# PDF parsing support
try:
    from pypdf import PdfReader
    PDF_SUPPORT = True
    print("📄 PDF parsing enabled (pypdf)")
except ImportError:
    PDF_SUPPORT = False
    print("⚠ PDF parsing not available (install pypdf)")


class KnowledgeSourceType(str, Enum):
    """Types of knowledge sources in the vault."""
    OFFICIAL_DOCS = "official_docs"      # Official API documentation
    SDK_REFERENCE = "sdk_reference"       # SDK/Library documentation
    ERD_SCHEMA = "erd_schema"            # Entity-Relationship diagrams, schemas
    CHANGELOG = "changelog"               # API changelogs, release notes
    FIVETRAN_DOCS = "fivetran_docs"      # Fivetran connector documentation
    AIRBYTE_DOCS = "airbyte_docs"        # Airbyte connector documentation
    GITHUB_IMPLEMENTATION = "github_implementation"  # GitHub code implementation
    CUSTOM = "custom"                     # User-provided documentation


@dataclass
class VaultDocument:
    """A document stored in the Knowledge Vault."""
    id: str
    connector_name: str
    title: str
    content: str
    source_type: KnowledgeSourceType
    source_url: Optional[str] = None
    version: Optional[str] = None
    indexed_at: datetime = field(default_factory=datetime.utcnow)
    chunk_count: int = 0


@dataclass
class VaultSearchResult:
    """A search result from the Knowledge Vault."""
    text: str
    score: float
    source_type: str
    title: str
    connector_name: str
    source_url: Optional[str] = None


@dataclass
class BulkUploadProgress:
    """Tracks progress of bulk document upload."""
    job_id: str
    connector_name: str
    total_files: int
    processed_files: int = 0
    successful_files: int = 0
    failed_files: int = 0
    total_chunks: int = 0
    status: str = "pending"  # pending, processing, completed, failed
    current_file: str = ""
    errors: List[str] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    
    @property
    def percentage(self) -> float:
        if self.total_files == 0:
            return 0.0
        return (self.processed_files / self.total_files) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "connector_name": self.connector_name,
            "total_files": self.total_files,
            "processed_files": self.processed_files,
            "successful_files": self.successful_files,
            "failed_files": self.failed_files,
            "total_chunks": self.total_chunks,
            "status": self.status,
            "current_file": self.current_file,
            "percentage": self.percentage,
            "errors": self.errors[-10:],  # Last 10 errors
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None
        }


# Global tracking for bulk upload jobs
_bulk_upload_jobs: Dict[str, BulkUploadProgress] = {}


class KnowledgeVault:
    """
    📚 Knowledge Vault - The sacred repository of connector wisdom!
    
    Pre-index official documentation so research can query authentic sources
    before falling back to web search.
    
    "Ask the Vault before you ask the web."
    """
    
    # Prefix for vault connector IDs (to separate from research docs)
    VAULT_PREFIX = "vault_"
    
    def __init__(self):
        """Initialize the Knowledge Vault."""
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.embedding_model = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
        self.dimension = EMBEDDING_DIMENSION
        
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY is required")
        
        self.openai = OpenAI(api_key=self.openai_api_key)
        
        # Ensure database is initialized
        db_url = os.getenv("DATABASE_URL")
        db_available = is_database_available()
        
        if db_url and not db_available:
            print("📚 Knowledge Vault: Database URL found but not initialized, initializing now...")
            print(f"  DATABASE_URL: {db_url[:50]}..." if len(db_url) > 50 else f"  DATABASE_URL: {db_url}")
            try:
                init_result = init_database()
                print(f"📚 Knowledge Vault: Database init result: {init_result}")
                db_available = is_database_available()
            except Exception as e:
                print(f"📚 Knowledge Vault: Database init exception: {e}")
                traceback.print_exc()
        
        # Check if pgvector extension is actually available (not just the Python package)
        # Re-read from module in case it was just enabled during init_database()
        import services.database as db_module
        self._pgvector_available = db_module.PGVECTOR_EXTENSION_AVAILABLE and db_available
        
        print("📚 Knowledge Vault initialized!")
        print(f"  DATABASE_URL present: {bool(db_url)}")
        print(f"  Database available: {db_available}")
        print(f"  pgvector available: {self._pgvector_available}")
        
        if db_url and not db_available:
            print("  ⚠ WARNING: DATABASE_URL is set but database is not available!")
            print("  ⚠ Check Railway logs for database connection errors")
        
        if self._pgvector_available:
            print("  ✓ Using pgvector for semantic search")
        else:
            print("  ⚠ Using JSON fallback for embeddings")
        
        if PDF_SUPPORT:
            print("  ✓ PDF parsing enabled")
        else:
            print("  ⚠ PDF parsing disabled")
    
    def parse_pdf(self, pdf_content: bytes, filename: str = "document.pdf") -> Tuple[str, Dict[str, Any]]:
        """
        Parse PDF content and extract text.
        
        Args:
            pdf_content: Raw PDF bytes
            filename: Original filename for metadata
            
        Returns:
            Tuple of (extracted_text, metadata)
        """
        if not PDF_SUPPORT:
            raise RuntimeError("PDF parsing not available. Install pypdf: pip install pypdf")
        
        try:
            # Create PDF reader from bytes
            pdf_file = io.BytesIO(pdf_content)
            reader = PdfReader(pdf_file)
            
            # Extract metadata
            metadata = {
                "filename": filename,
                "pages": len(reader.pages),
                "pdf_info": {}
            }
            
            if reader.metadata:
                metadata["pdf_info"] = {
                    "title": reader.metadata.get("/Title", ""),
                    "author": reader.metadata.get("/Author", ""),
                    "subject": reader.metadata.get("/Subject", ""),
                    "creator": reader.metadata.get("/Creator", "")
                }
            
            # Extract text from all pages
            text_parts = []
            for i, page in enumerate(reader.pages):
                try:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(f"--- Page {i+1} ---\n{page_text}")
                except Exception as e:
                    text_parts.append(f"--- Page {i+1} (extraction error: {str(e)}) ---")
            
            full_text = "\n\n".join(text_parts)
            
            # Clean up text
            full_text = self._clean_pdf_text(full_text)
            
            return full_text, metadata
            
        except Exception as e:
            raise RuntimeError(f"Failed to parse PDF '{filename}': {str(e)}")
    
    def _clean_pdf_text(self, text: str) -> str:
        """Clean extracted PDF text."""
        # Remove excessive whitespace
        text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
        text = re.sub(r' +', ' ', text)
        
        # Fix common PDF extraction issues
        text = re.sub(r'(\w)-\n(\w)', r'\1\2', text)  # Fix hyphenated words
        
        return text.strip()
    
    def _generate_embedding(self, text: str) -> List[float]:
        """Generate embedding vector for text."""
        response = self.openai.embeddings.create(
            model=self.embedding_model,
            input=text
        )
        return response.data[0].embedding
    
    def _chunk_text(
        self,
        text: str,
        chunk_size: int = 1500,  # Larger chunks for documentation
        overlap: int = 300
    ) -> List[str]:
        """Split text into overlapping chunks optimized for documentation."""
        if len(text) <= chunk_size:
            return [text]
        
        chunks = []
        start = 0
        
        while start < len(text):
            end = start + chunk_size
            
            if end < len(text):
                # Try to break at markdown headers
                header_break = text.rfind('\n## ', start, end)
                if header_break > start + chunk_size // 2:
                    end = header_break
                else:
                    # Try paragraph break
                    para_break = text.rfind('\n\n', start, end)
                    if para_break > start + chunk_size // 2:
                        end = para_break
                    else:
                        # Try sentence break
                        sent_break = text.rfind('. ', start, end)
                        if sent_break > start + chunk_size // 2:
                            end = sent_break + 1
            
            chunks.append(text[start:end].strip())
            start = end - overlap
        
        return [c for c in chunks if c]
    
    def _generate_chunk_id(
        self,
        connector_name: str,
        title: str,
        index: int
    ) -> str:
        """Generate unique ID for a vault chunk."""
        content_hash = hashlib.md5(f"{connector_name}:{title}:{index}".encode()).hexdigest()[:8]
        return f"{self.VAULT_PREFIX}{connector_name.lower()}-{index}-{content_hash}"
    
    def _get_vault_connector_id(self, connector_name: str) -> str:
        """Get the vault-specific connector ID."""
        return f"{self.VAULT_PREFIX}{connector_name.lower().replace(' ', '_')}"
    
    def _extract_section_title(self, chunk: str, default_title: str) -> str:
        """Extract section title from chunk content."""
        # Look for markdown headers
        header_match = re.search(r'^#+\s+(.+)$', chunk, re.MULTILINE)
        if header_match:
            return header_match.group(1)[:100]
        return default_title
    
    def index_document(
        self,
        connector_name: str,
        title: str,
        content: str,
        source_type: KnowledgeSourceType,
        source_url: Optional[str] = None,
        version: Optional[str] = None
    ) -> VaultDocument:
        """
        Index a document into the Knowledge Vault.
        
        Args:
            connector_name: Name of the connector (e.g., "Shopify", "Facebook")
            title: Document title
            content: Document content (markdown, text, etc.)
            source_type: Type of knowledge source
            source_url: Optional source URL
            version: Optional version/date of the document
            
        Returns:
            VaultDocument with indexing stats
        
        Raises:
            RuntimeError: If indexing fails
        """
        # Check database availability with detailed diagnostics
        db_available = is_database_available()
        if not db_available:
            db_url = os.getenv("DATABASE_URL")
            raise RuntimeError(
                f"Database not available for Knowledge Vault. "
                f"DATABASE_URL set: {bool(db_url)}, "
                f"is_database_available(): {db_available}"
            )
        
        session = get_db_session()
        if not session:
            raise RuntimeError("Database session could not be created")
        
        vault_connector_id = self._get_vault_connector_id(connector_name)
        
        try:
            # Split content into chunks
            chunks = self._chunk_text(content)
            print(f"  📄 Split into {len(chunks)} chunks")
            
            if not chunks:
                raise RuntimeError("No content to index after chunking")
            
            # Create and store vectors
            created_count = 0
            for i, chunk in enumerate(chunks):
                # Generate embedding
                try:
                    embedding = self._generate_embedding(chunk)
                except Exception as e:
                    raise RuntimeError(f"Failed to generate embedding for chunk {i}: {e}")
                
                # Create chunk record
                chunk_id = self._generate_chunk_id(connector_name, title, i)
                section_title = self._extract_section_title(chunk, title)
                
                # Check if chunk already exists
                existing = session.query(DocumentChunkModel).filter(
                    DocumentChunkModel.id == chunk_id
                ).first()
                
                if existing:
                    # Update existing chunk
                    existing.text = chunk[:5000]
                    existing.section = section_title
                    existing.embedding_json = embedding
                    if self._pgvector_available and hasattr(existing, 'embedding'):
                        existing.embedding = embedding
                else:
                    # Create new chunk
                    chunk_model = DocumentChunkModel(
                        id=chunk_id,
                        connector_id=vault_connector_id,
                        connector_name=connector_name,
                        chunk_index=i,
                        text=chunk[:5000],
                        section=section_title,
                        source_type=source_type.value,
                        embedding_json=embedding
                    )
                    
                    if self._pgvector_available and hasattr(chunk_model, 'embedding'):
                        chunk_model.embedding = embedding
                    
                    session.add(chunk_model)
                
                created_count += 1
                
                # Commit in batches
                if created_count % 25 == 0:
                    session.commit()
            
            session.commit()
            
            print(f"📚 Vault: Indexed {created_count} chunks for '{connector_name}' - {title}")
            
            return VaultDocument(
                id=vault_connector_id,
                connector_name=connector_name,
                title=title,
                content=content[:500] + "...",  # Preview only
                source_type=source_type,
                source_url=source_url,
                version=version,
                chunk_count=created_count
            )
            
        except Exception as e:
            session.rollback()
            raise RuntimeError(f"Failed to index document: {e}")
        finally:
            session.close()
    
    def index_from_url(
        self,
        connector_name: str,
        url: str,
        source_type: KnowledgeSourceType = KnowledgeSourceType.OFFICIAL_DOCS
    ) -> VaultDocument:
        """
        Index documentation from a URL.
        
        Args:
            connector_name: Name of the connector
            url: URL to fetch documentation from
            source_type: Type of knowledge source
            
        Returns:
            VaultDocument with indexing stats
        """
        import httpx
        
        try:
            # Fetch content
            response = httpx.get(url, timeout=30.0, follow_redirects=True)
            response.raise_for_status()
            content = response.text
            
            # Try to extract title from HTML
            title = url.split('/')[-1] or "Documentation"
            title_match = re.search(r'<title>([^<]+)</title>', content, re.IGNORECASE)
            if title_match:
                title = title_match.group(1).strip()
            
            # Convert HTML to text if needed
            if '<html' in content.lower() or '<body' in content.lower():
                content = self._html_to_text(content)
            
            return self.index_document(
                connector_name=connector_name,
                title=title,
                content=content,
                source_type=source_type,
                source_url=url
            )
            
        except Exception as e:
            raise RuntimeError(f"Failed to fetch URL: {e}")
    
    def _html_to_text(self, html: str) -> str:
        """Convert HTML to plain text, preserving structure."""
        # Remove script and style tags
        html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
        
        # Convert headers to markdown
        html = re.sub(r'<h1[^>]*>([^<]+)</h1>', r'\n# \1\n', html, flags=re.IGNORECASE)
        html = re.sub(r'<h2[^>]*>([^<]+)</h2>', r'\n## \1\n', html, flags=re.IGNORECASE)
        html = re.sub(r'<h3[^>]*>([^<]+)</h3>', r'\n### \1\n', html, flags=re.IGNORECASE)
        
        # Convert paragraphs and line breaks
        html = re.sub(r'<p[^>]*>', '\n', html, flags=re.IGNORECASE)
        html = re.sub(r'</p>', '\n', html, flags=re.IGNORECASE)
        html = re.sub(r'<br[^>]*>', '\n', html, flags=re.IGNORECASE)
        
        # Convert lists
        html = re.sub(r'<li[^>]*>', '\n- ', html, flags=re.IGNORECASE)
        
        # Remove remaining tags
        html = re.sub(r'<[^>]+>', '', html)
        
        # Clean up whitespace
        html = re.sub(r'\n\s*\n', '\n\n', html)
        html = re.sub(r' +', ' ', html)
        
        return html.strip()
    
    def search(
        self,
        connector_name: str,
        query: str,
        top_k: int = 5,
        source_types: Optional[List[KnowledgeSourceType]] = None
    ) -> List[VaultSearchResult]:
        """
        Search the Knowledge Vault for a connector.
        
        Args:
            connector_name: Name of the connector
            query: Search query
            top_k: Number of results
            source_types: Optional filter by source types
            
        Returns:
            List of search results
        """
        session = get_db_session()
        if not session:
            return []
        
        vault_connector_id = self._get_vault_connector_id(connector_name)
        
        try:
            # Generate query embedding
            query_embedding = self._generate_embedding(query)
            
            if self._pgvector_available:
                # Use pgvector's cosine distance
                results = session.execute(
                    text("""
                        SELECT id, connector_name, text, section, source_type,
                               1 - (embedding <=> :query_embedding::vector) as score
                        FROM document_chunks
                        WHERE connector_id = :connector_id
                        AND embedding IS NOT NULL
                        ORDER BY embedding <=> :query_embedding::vector
                        LIMIT :top_k
                    """),
                    {
                        "query_embedding": str(query_embedding),
                        "connector_id": vault_connector_id,
                        "top_k": top_k
                    }
                ).fetchall()
                
                return [
                    VaultSearchResult(
                        text=row[2],
                        score=float(row[5]) if row[5] else 0.0,
                        source_type=row[4],
                        title=row[3],
                        connector_name=row[1]
                    )
                    for row in results
                ]
            else:
                # Fallback: Load all and compute similarity
                chunks = session.query(DocumentChunkModel).filter(
                    DocumentChunkModel.connector_id == vault_connector_id
                ).all()
                
                results = []
                for chunk in chunks:
                    if chunk.embedding_json:
                        score = self._cosine_similarity(query_embedding, chunk.embedding_json)
                        results.append(VaultSearchResult(
                            text=chunk.text,
                            score=score,
                            source_type=chunk.source_type,
                            title=chunk.section,
                            connector_name=chunk.connector_name
                        ))
                
                results.sort(key=lambda x: x.score, reverse=True)
                return results[:top_k]
                
        except Exception as e:
            print(f"Vault search error: {e}")
            return []
        finally:
            session.close()
    
    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """Compute cosine similarity between two vectors."""
        import math
        
        dot_product = sum(a * b for a, b in zip(vec1, vec2))
        magnitude1 = math.sqrt(sum(a * a for a in vec1))
        magnitude2 = math.sqrt(sum(b * b for b in vec2))
        
        if magnitude1 == 0 or magnitude2 == 0:
            return 0.0
        
        return dot_product / (magnitude1 * magnitude2)
    
    def has_knowledge(self, connector_name: str) -> bool:
        """Check if the vault has knowledge for a connector."""
        session = get_db_session()
        if not session:
            return False
        
        vault_connector_id = self._get_vault_connector_id(connector_name)
        
        try:
            count = session.query(DocumentChunkModel).filter(
                DocumentChunkModel.connector_id == vault_connector_id
            ).count()
            return count > 0
        finally:
            session.close()
    
    def get_stats(self, connector_name: Optional[str] = None) -> Dict[str, Any]:
        """Get Knowledge Vault statistics."""
        session = get_db_session()
        if not session:
            return {"available": False, "connectors": [], "total_chunks": 0}
        
        try:
            if connector_name:
                vault_connector_id = self._get_vault_connector_id(connector_name)
                count = session.query(DocumentChunkModel).filter(
                    DocumentChunkModel.connector_id == vault_connector_id
                ).count()
                
                return {
                    "available": True,
                    "connector": connector_name,
                    "chunks": count,
                    "has_knowledge": count > 0
                }
            else:
                # Get all vault connectors
                results = session.execute(
                    text("""
                        SELECT connector_id, connector_name, COUNT(*) as chunks
                        FROM document_chunks
                        WHERE connector_id LIKE 'vault_%'
                        GROUP BY connector_id, connector_name
                    """)
                ).fetchall()
                
                connectors = [
                    {
                        "id": row[0],
                        "name": row[1],
                        "chunks": row[2]
                    }
                    for row in results
                ]
                
                total_chunks = sum(c["chunks"] for c in connectors)
                
                return {
                    "available": True,
                    "connectors": connectors,
                    "connector_count": len(connectors),
                    "total_chunks": total_chunks
                }
                
        finally:
            session.close()
    
    def delete_knowledge(self, connector_name: str) -> bool:
        """Delete all knowledge for a connector."""
        session = get_db_session()
        if not session:
            return False
        
        vault_connector_id = self._get_vault_connector_id(connector_name)
        
        try:
            deleted = session.query(DocumentChunkModel).filter(
                DocumentChunkModel.connector_id == vault_connector_id
            ).delete()
            session.commit()
            print(f"📚 Vault: Deleted {deleted} chunks for '{connector_name}'")
            return deleted > 0
        except Exception as e:
            session.rollback()
            print(f"Error deleting vault knowledge: {e}")
            return False
        finally:
            session.close()
    
    def list_connectors(self) -> List[str]:
        """List all connectors with knowledge in the vault."""
        stats = self.get_stats()
        return [c["name"] for c in stats.get("connectors", [])]
    
    # =========================================================================
    # Bulk Upload Methods (for 500+ documents)
    # =========================================================================
    
    def index_pdf(
        self,
        connector_name: str,
        pdf_content: bytes,
        filename: str,
        source_type: KnowledgeSourceType = KnowledgeSourceType.OFFICIAL_DOCS
    ) -> VaultDocument:
        """
        Index a single PDF document.
        
        Args:
            connector_name: Name of the connector
            pdf_content: Raw PDF bytes
            filename: Original filename
            source_type: Type of knowledge source
            
        Returns:
            VaultDocument with indexing stats
        """
        # Parse PDF
        text_content, metadata = self.parse_pdf(pdf_content, filename)
        
        # Use PDF title or filename
        title = metadata.get("pdf_info", {}).get("title") or filename
        
        # Index the extracted text
        return self.index_document(
            connector_name=connector_name,
            title=title,
            content=text_content,
            source_type=source_type,
            source_url=f"file://{filename}"
        )
    
    def start_bulk_upload(
        self,
        connector_name: str,
        total_files: int
    ) -> str:
        """
        Start a bulk upload job and return the job ID.
        
        Args:
            connector_name: Name of the connector
            total_files: Total number of files to process
            
        Returns:
            Job ID for tracking progress
        """
        job_id = hashlib.md5(f"{connector_name}:{datetime.utcnow().isoformat()}".encode()).hexdigest()[:12]
        
        progress = BulkUploadProgress(
            job_id=job_id,
            connector_name=connector_name,
            total_files=total_files,
            status="processing"
        )
        
        _bulk_upload_jobs[job_id] = progress
        
        print(f"📚 Vault: Started bulk upload job {job_id} for {connector_name} ({total_files} files)")
        
        return job_id
    
    def process_bulk_file(
        self,
        job_id: str,
        file_content: bytes,
        filename: str,
        source_type: KnowledgeSourceType = KnowledgeSourceType.OFFICIAL_DOCS
    ) -> bool:
        """
        Process a single file in a bulk upload job.
        
        Args:
            job_id: The bulk upload job ID
            file_content: File content bytes
            filename: Original filename
            source_type: Type of knowledge source
            
        Returns:
            True if successful, False otherwise
        """
        if job_id not in _bulk_upload_jobs:
            return False
        
        progress = _bulk_upload_jobs[job_id]
        progress.current_file = filename
        
        try:
            # Determine file type and process accordingly
            filename_lower = filename.lower()
            
            print(f"📚 Processing: {filename} ({len(file_content)} bytes)")
            
            if filename_lower.endswith('.pdf'):
                doc = self.index_pdf(
                    connector_name=progress.connector_name,
                    pdf_content=file_content,
                    filename=filename,
                    source_type=source_type
                )
            else:
                # Try to decode as text
                try:
                    text_content = file_content.decode('utf-8')
                except UnicodeDecodeError:
                    text_content = file_content.decode('latin-1')
                
                # Skip empty files
                if not text_content.strip():
                    raise ValueError("File is empty")
                
                print(f"  → Decoded {len(text_content)} chars, indexing...")
                
                doc = self.index_document(
                    connector_name=progress.connector_name,
                    title=filename,
                    content=text_content,
                    source_type=source_type,
                    source_url=f"file://{filename}"
                )
            
            progress.successful_files += 1
            progress.total_chunks += doc.chunk_count
            print(f"  ✓ Indexed {doc.chunk_count} chunks")
            return True
            
        except Exception as e:
            progress.failed_files += 1
            error_msg = f"{filename}: {str(e)}"
            progress.errors.append(error_msg)
            print(f"  ⚠ Failed to process {filename}: {e}")
            traceback.print_exc()
            return False
        finally:
            progress.processed_files += 1
    
    def complete_bulk_upload(self, job_id: str) -> BulkUploadProgress:
        """
        Mark a bulk upload job as complete.
        
        Args:
            job_id: The bulk upload job ID
            
        Returns:
            Final progress status
        """
        if job_id not in _bulk_upload_jobs:
            raise ValueError(f"Unknown job ID: {job_id}")
        
        progress = _bulk_upload_jobs[job_id]
        progress.status = "completed"
        progress.completed_at = datetime.utcnow()
        progress.current_file = ""
        
        print(f"📚 Vault: Completed bulk upload job {job_id}")
        print(f"  ✓ Processed: {progress.processed_files}/{progress.total_files}")
        print(f"  ✓ Successful: {progress.successful_files}")
        print(f"  ✗ Failed: {progress.failed_files}")
        print(f"  📄 Total chunks: {progress.total_chunks}")
        
        return progress
    
    def get_bulk_upload_progress(self, job_id: str) -> Optional[BulkUploadProgress]:
        """
        Get progress for a bulk upload job.
        
        Args:
            job_id: The bulk upload job ID
            
        Returns:
            Progress object or None if not found
        """
        return _bulk_upload_jobs.get(job_id)
    
    def list_bulk_upload_jobs(self) -> List[Dict[str, Any]]:
        """List all bulk upload jobs."""
        return [progress.to_dict() for progress in _bulk_upload_jobs.values()]
    
    async def process_bulk_upload_async(
        self,
        connector_name: str,
        files: List[Tuple[str, bytes]],
        source_type: KnowledgeSourceType = KnowledgeSourceType.OFFICIAL_DOCS,
        on_progress: Optional[Callable[[BulkUploadProgress], None]] = None
    ) -> BulkUploadProgress:
        """
        Process a bulk upload asynchronously.
        
        Args:
            connector_name: Name of the connector
            files: List of (filename, content_bytes) tuples
            source_type: Type of knowledge source
            on_progress: Optional callback for progress updates
            
        Returns:
            Final progress status
        """
        job_id = self.start_bulk_upload(connector_name, len(files))
        
        for filename, content in files:
            self.process_bulk_file(job_id, content, filename, source_type)
            
            if on_progress:
                on_progress(_bulk_upload_jobs[job_id])
            
            # Yield control to allow other tasks
            await asyncio.sleep(0.01)
        
        return self.complete_bulk_upload(job_id)
    
    def index_text(
        self,
        connector_name: str,
        title: str,
        content: str,
        source_type: str = "official_docs",
        source_url: Optional[str] = None
    ) -> VaultDocument:
        """
        Convenience method to index plain text content.
        
        Args:
            connector_name: Name of the connector
            title: Document title
            content: Text content to index
            source_type: Type of source as string
            source_url: Optional source URL
            
        Returns:
            VaultDocument with indexing stats
        """
        # Convert string source_type to enum
        try:
            source_enum = KnowledgeSourceType(source_type)
        except ValueError:
            source_enum = KnowledgeSourceType.CUSTOM
        
        return self.index_document(
            connector_name=connector_name,
            title=title,
            content=content,
            source_type=source_enum,
            source_url=source_url
        )
    
    def index_github_repo(
        self,
        connector_name: str,
        repo_path: str,
        source_type: str = "github_implementation"
    ) -> Dict[str, Any]:
        """
        Index relevant files from a cloned GitHub repository.
        
        Indexes:
        - Java files (.java) - Hevo uses Java
        - Python files (.py)
        - JavaScript/TypeScript files (.js, .ts)
        - README files
        - Documentation files (docs/, *.md)
        - Configuration files (pom.xml, package.json, etc.)
        
        Args:
            connector_name: Name of the connector
            repo_path: Path to the cloned repository
            source_type: Type of source
            
        Returns:
            Dictionary with indexing statistics
        """
        repo_dir = Path(repo_path)
        if not repo_dir.exists():
            print(f"⚠ GitHub repo path does not exist: {repo_path}")
            return {"error": f"Path not found: {repo_path}", "files_indexed": 0}
        
        # File patterns to index (prioritize Java for Hevo)
        patterns = [
            # Java files (highest priority for Hevo)
            ("**/*.java", "java_source"),
            # Python files
            ("**/*.py", "python_source"),
            # JavaScript/TypeScript
            ("**/*.js", "javascript_source"),
            ("**/*.ts", "typescript_source"),
            # Documentation
            ("**/README*", "readme"),
            ("**/README.md", "readme"),
            ("**/*.md", "documentation"),
            ("**/docs/**/*.md", "documentation"),
            # Configuration
            ("**/pom.xml", "maven_config"),
            ("**/build.gradle", "gradle_config"),
            ("**/package.json", "npm_config"),
        ]
        
        # Directories to skip
        skip_dirs = {
            'node_modules', '__pycache__', '.git', 'target', 'build', 
            'dist', '.gradle', '.mvn', 'venv', '.venv', '.idea', '.vscode'
        }
        
        stats = {
            "files_indexed": 0,
            "total_chunks": 0,
            "files_by_type": {},
            "errors": []
        }
        
        try:
            source_enum = KnowledgeSourceType(source_type)
        except ValueError:
            source_enum = KnowledgeSourceType.GITHUB_IMPLEMENTATION
        
        indexed_files = set()  # Track to avoid duplicates
        
        print(f"📦 Indexing GitHub repo: {repo_path}")
        
        for pattern, file_type in patterns:
            for file_path in repo_dir.glob(pattern):
                # Skip if already indexed
                if str(file_path) in indexed_files:
                    continue
                
                # Skip directories we don't want
                if any(skip_dir in file_path.parts for skip_dir in skip_dirs):
                    continue
                
                # Skip non-files
                if not file_path.is_file():
                    continue
                
                # Skip very large files (> 500KB)
                if file_path.stat().st_size > 500 * 1024:
                    continue
                
                try:
                    # Read file content
                    content = file_path.read_text(encoding='utf-8', errors='ignore')
                    
                    # Skip empty files
                    if not content.strip():
                        continue
                    
                    # Create relative path for title
                    relative_path = file_path.relative_to(repo_dir)
                    title = f"{connector_name} - {relative_path}"
                    
                    # Index the file
                    doc = self.index_document(
                        connector_name=connector_name,
                        title=title,
                        content=content,
                        source_type=source_enum,
                        source_url=f"github://{relative_path}"
                    )
                    
                    stats["files_indexed"] += 1
                    stats["total_chunks"] += doc.chunk_count
                    stats["files_by_type"][file_type] = stats["files_by_type"].get(file_type, 0) + 1
                    indexed_files.add(str(file_path))
                    
                    print(f"  ✓ Indexed: {relative_path} ({doc.chunk_count} chunks)")
                    
                except Exception as e:
                    error_msg = f"Failed to index {file_path}: {str(e)}"
                    stats["errors"].append(error_msg)
                    print(f"  ⚠ {error_msg}")
        
        print(f"📦 GitHub indexing complete: {stats['files_indexed']} files, {stats['total_chunks']} chunks")
        
        return stats


# Singleton instance
_vault: Optional[KnowledgeVault] = None


def get_knowledge_vault() -> KnowledgeVault:
    """Get the singleton KnowledgeVault instance."""
    global _vault
    if _vault is None:
        _vault = KnowledgeVault()
    return _vault


def get_bulk_upload_jobs() -> Dict[str, BulkUploadProgress]:
    """Get all bulk upload jobs."""
    return _bulk_upload_jobs

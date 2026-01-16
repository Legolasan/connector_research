"""
📚 Knowledge Vault Service
Pre-indexed official documentation for enterprise-grade connector research.

"The Vault remembers what the web forgets."
"""

import os
import re
import hashlib
import asyncio
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from openai import OpenAI
from sqlalchemy import text
from dotenv import load_dotenv

from services.database import (
    get_db_session, is_database_available,
    DocumentChunkModel, PGVECTOR_AVAILABLE, EMBEDDING_DIMENSION
)

load_dotenv()


class KnowledgeSourceType(str, Enum):
    """Types of knowledge sources in the vault."""
    OFFICIAL_DOCS = "official_docs"      # Official API documentation
    SDK_REFERENCE = "sdk_reference"       # SDK/Library documentation
    ERD_SCHEMA = "erd_schema"            # Entity-Relationship diagrams, schemas
    CHANGELOG = "changelog"               # API changelogs, release notes
    FIVETRAN_DOCS = "fivetran_docs"      # Fivetran connector documentation
    AIRBYTE_DOCS = "airbyte_docs"        # Airbyte connector documentation
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
        self._pgvector_available = PGVECTOR_AVAILABLE and is_database_available()
        
        print("📚 Knowledge Vault initialized!")
        if self._pgvector_available:
            print("  ✓ Using pgvector for semantic search")
        else:
            print("  ⚠ Using JSON fallback for embeddings")
    
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
        """
        session = get_db_session()
        if not session:
            raise RuntimeError("Database not available for Knowledge Vault")
        
        vault_connector_id = self._get_vault_connector_id(connector_name)
        
        try:
            # Split content into chunks
            chunks = self._chunk_text(content)
            
            # Create and store vectors
            created_count = 0
            for i, chunk in enumerate(chunks):
                # Generate embedding
                embedding = self._generate_embedding(chunk)
                
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


# Singleton instance
_vault: Optional[KnowledgeVault] = None


def get_knowledge_vault() -> KnowledgeVault:
    """Get the singleton KnowledgeVault instance."""
    global _vault
    if _vault is None:
        _vault = KnowledgeVault()
    return _vault

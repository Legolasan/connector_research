"""
Vector Manager Service
Manages document embeddings using PostgreSQL pgvector extension.
Replaces Pinecone with in-database vector storage.
"""

import os
import re
import hashlib
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime

from openai import OpenAI
from sqlalchemy import text
from dotenv import load_dotenv

from services.database import (
    get_db_session, is_database_available,
    DocumentChunkModel, PGVECTOR_AVAILABLE, PGVECTOR_EXTENSION_AVAILABLE, EMBEDDING_DIMENSION
)

load_dotenv()


@dataclass
class VectorDocument:
    """Represents a document chunk to be vectorized."""
    id: str
    text: str
    metadata: Dict[str, Any]


class VectorManager:
    """Manages document embeddings using PostgreSQL pgvector."""
    
    def __init__(self):
        """Initialize the vector manager."""
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.embedding_model = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
        self.dimension = EMBEDDING_DIMENSION
        
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY is required")
        
        self.openai = OpenAI(api_key=self.openai_api_key)
        # Check if pgvector extension is actually available (not just the Python package)
        self._pgvector_available = PGVECTOR_EXTENSION_AVAILABLE and is_database_available()
        
        if self._pgvector_available:
            print("✓ VectorManager using pgvector for embeddings")
        else:
            print("⚠ VectorManager using JSON fallback for embeddings")
    
    def _generate_embedding(self, text: str) -> List[float]:
        """Generate embedding vector for text.
        
        Args:
            text: Text to embed
            
        Returns:
            Embedding vector
        """
        response = self.openai.embeddings.create(
            model=self.embedding_model,
            input=text
        )
        return response.data[0].embedding
    
    def _chunk_text(
        self, 
        text: str, 
        chunk_size: int = 1000, 
        overlap: int = 200
    ) -> List[str]:
        """Split text into overlapping chunks.
        
        Args:
            text: Text to chunk
            chunk_size: Maximum chunk size in characters
            overlap: Overlap between chunks
            
        Returns:
            List of text chunks
        """
        if len(text) <= chunk_size:
            return [text]
        
        chunks = []
        start = 0
        
        while start < len(text):
            end = start + chunk_size
            
            # Try to break at paragraph or sentence
            if end < len(text):
                # Look for paragraph break
                para_break = text.rfind('\n\n', start, end)
                if para_break > start + chunk_size // 2:
                    end = para_break
                else:
                    # Look for sentence break
                    sent_break = text.rfind('. ', start, end)
                    if sent_break > start + chunk_size // 2:
                        end = sent_break + 1
            
            chunks.append(text[start:end].strip())
            start = end - overlap
        
        return [c for c in chunks if c]  # Remove empty chunks
    
    def _generate_chunk_id(self, connector_id: str, text: str, index: int) -> str:
        """Generate a unique ID for a chunk.
        
        Args:
            connector_id: Connector ID
            text: Chunk text
            index: Chunk index
            
        Returns:
            Unique chunk ID
        """
        content_hash = hashlib.md5(text.encode()).hexdigest()[:8]
        return f"{connector_id}-{index}-{content_hash}"
    
    def _extract_section(self, chunk: str) -> str:
        """Extract section name from chunk content.
        
        Args:
            chunk: Text chunk
            
        Returns:
            Section name
        """
        section = "General"
        if "## " in chunk:
            section_match = re.search(r'##\s+(\d+)\.\s+([^\n]+)', chunk)
            if section_match:
                section = f"{section_match.group(1)}. {section_match.group(2)}"
        return section
    
    def index_exists(self, connector_id: str) -> bool:
        """Check if vectors exist for a connector.
        
        Args:
            connector_id: Connector ID
            
        Returns:
            True if vectors exist
        """
        session = get_db_session()
        if not session:
            return False
        
        try:
            count = session.query(DocumentChunkModel).filter(
                DocumentChunkModel.connector_id == connector_id
            ).count()
            return count > 0
        finally:
            session.close()
    
    def get_index_stats(self, connector_id: str) -> Dict[str, Any]:
        """Get statistics for a connector's vectors.
        
        Args:
            connector_id: Connector ID
            
        Returns:
            Vector statistics
        """
        session = get_db_session()
        if not session:
            return {"exists": False, "vectors": 0}
        
        try:
            count = session.query(DocumentChunkModel).filter(
                DocumentChunkModel.connector_id == connector_id
            ).count()
            
            return {
                "exists": count > 0,
                "vectors": count,
                "dimension": self.dimension,
                "storage": "pgvector" if self._pgvector_available else "json"
            }
        finally:
            session.close()
    
    def vectorize_research(
        self, 
        connector_id: str, 
        connector_name: str,
        research_content: str,
        source_type: str = "research"
    ) -> int:
        """Vectorize a research document and store in database.
        
        Args:
            connector_id: Connector ID
            connector_name: Connector display name
            research_content: Research document content
            source_type: Type of source (research, code, web)
            
        Returns:
            Number of vectors created
        """
        session = get_db_session()
        if not session:
            print("⚠ Database not available for vectorization")
            return 0
        
        try:
            # Delete existing chunks for this connector
            session.query(DocumentChunkModel).filter(
                DocumentChunkModel.connector_id == connector_id
            ).delete()
            session.commit()
            
            # Split into chunks
            chunks = self._chunk_text(research_content)
            
            # Create and store vectors
            created_count = 0
            for i, chunk in enumerate(chunks):
                # Generate embedding
                embedding = self._generate_embedding(chunk)
                
                # Create chunk record
                chunk_id = self._generate_chunk_id(connector_id, chunk, i)
                section = self._extract_section(chunk)
                
                chunk_model = DocumentChunkModel(
                    id=chunk_id,
                    connector_id=connector_id,
                    connector_name=connector_name,
                    chunk_index=i,
                    text=chunk[:5000],  # Limit text size
                    section=section,
                    source_type=source_type,
                    embedding_json=embedding  # Store as JSON (always works)
                )
                
                # If pgvector is available, also set the vector column
                if self._pgvector_available and hasattr(chunk_model, 'embedding'):
                    chunk_model.embedding = embedding
                
                session.add(chunk_model)
                created_count += 1
                
                # Commit in batches
                if created_count % 50 == 0:
                    session.commit()
            
            session.commit()
            print(f"✓ Stored {created_count} vectors for {connector_name}")
            return created_count
            
        except Exception as e:
            session.rollback()
            print(f"Error vectorizing research: {e}")
            return 0
        finally:
            session.close()
    
    def search(
        self,
        connector_id: str,
        query: str,
        top_k: int = 5,
        filter: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Search vectors for a connector using similarity.
        
        Args:
            connector_id: Connector ID
            query: Search query
            top_k: Number of results
            filter: Optional metadata filter (not implemented)
            
        Returns:
            List of search results
        """
        session = get_db_session()
        if not session:
            return []
        
        try:
            # Generate query embedding
            query_embedding = self._generate_embedding(query)
            
            if self._pgvector_available:
                # Use pgvector's cosine distance operator
                results = session.execute(
                    text("""
                        SELECT id, connector_id, connector_name, text, section, source_type,
                               1 - (embedding <=> :query_embedding::vector) as score
                        FROM document_chunks
                        WHERE connector_id = :connector_id
                        AND embedding IS NOT NULL
                        ORDER BY embedding <=> :query_embedding::vector
                        LIMIT :top_k
                    """),
                    {
                        "query_embedding": str(query_embedding),
                        "connector_id": connector_id,
                        "top_k": top_k
                    }
                ).fetchall()
                
                return [
                    {
                        "id": row[0],
                        "score": float(row[6]) if row[6] else 0.0,
                        "text": row[3],
                        "section": row[4],
                        "source_type": row[5],
                        "connector_name": row[2]
                    }
                    for row in results
                ]
            else:
                # Fallback: Load all chunks and compute similarity in Python
                chunks = session.query(DocumentChunkModel).filter(
                    DocumentChunkModel.connector_id == connector_id
                ).all()
                
                # Compute cosine similarity
                results = []
                for chunk in chunks:
                    if chunk.embedding_json:
                        score = self._cosine_similarity(query_embedding, chunk.embedding_json)
                        results.append({
                            "id": chunk.id,
                            "score": score,
                            "text": chunk.text,
                            "section": chunk.section,
                            "source_type": chunk.source_type,
                            "connector_name": chunk.connector_name
                        })
                
                # Sort by score and take top_k
                results.sort(key=lambda x: x["score"], reverse=True)
                return results[:top_k]
                
        except Exception as e:
            print(f"Search error: {e}")
            return []
        finally:
            session.close()
    
    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """Compute cosine similarity between two vectors.
        
        Args:
            vec1: First vector
            vec2: Second vector
            
        Returns:
            Cosine similarity score (0-1)
        """
        import math
        
        dot_product = sum(a * b for a, b in zip(vec1, vec2))
        magnitude1 = math.sqrt(sum(a * a for a in vec1))
        magnitude2 = math.sqrt(sum(b * b for b in vec2))
        
        if magnitude1 == 0 or magnitude2 == 0:
            return 0.0
        
        return dot_product / (magnitude1 * magnitude2)
    
    def search_all_connectors(
        self,
        query: str,
        connector_ids: List[str],
        top_k: int = 5
    ) -> List[Dict[str, Any]]:
        """Search across multiple connectors.
        
        Args:
            query: Search query
            connector_ids: List of connector IDs to search
            top_k: Number of results per connector
            
        Returns:
            Combined search results sorted by score
        """
        all_results = []
        
        for connector_id in connector_ids:
            results = self.search(connector_id, query, top_k)
            all_results.extend(results)
        
        # Sort by score
        all_results.sort(key=lambda x: x["score"], reverse=True)
        
        return all_results[:top_k * 2]  # Return top results
    
    def delete_index(self, connector_id: str) -> bool:
        """Delete all vectors for a connector.
        
        Args:
            connector_id: Connector ID
            
        Returns:
            True if deleted
        """
        session = get_db_session()
        if not session:
            return False
        
        try:
            deleted = session.query(DocumentChunkModel).filter(
                DocumentChunkModel.connector_id == connector_id
            ).delete()
            session.commit()
            return deleted > 0
        except Exception as e:
            session.rollback()
            print(f"Error deleting vectors: {e}")
            return False
        finally:
            session.close()


# Singleton instance
_manager: Optional[VectorManager] = None


def get_vector_manager() -> VectorManager:
    """Get the singleton VectorManager instance."""
    global _manager
    if _manager is None:
        _manager = VectorManager()
    return _manager

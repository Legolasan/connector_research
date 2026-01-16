"""
Pinecone Manager Service
Manages per-connector Pinecone indices for vector storage.
"""

import os
import hashlib
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime

from openai import OpenAI
from pinecone import Pinecone, ServerlessSpec
from dotenv import load_dotenv

load_dotenv()


@dataclass
class VectorDocument:
    """Represents a document chunk to be vectorized."""
    id: str
    text: str
    metadata: Dict[str, Any]


class PineconeManager:
    """Manages per-connector Pinecone indices."""
    
    def __init__(self):
        """Initialize the Pinecone manager."""
        self.pinecone_api_key = os.getenv("PINECONE_API_KEY")
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.embedding_model = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
        self.dimension = 1536  # Dimension for text-embedding-3-small
        
        if not self.pinecone_api_key:
            raise ValueError("PINECONE_API_KEY is required")
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY is required")
        
        self.pinecone = Pinecone(api_key=self.pinecone_api_key)
        self.openai = OpenAI(api_key=self.openai_api_key)
        
        # Cache of index connections
        self._indices: Dict[str, Any] = {}
    
    def _get_index_name(self, connector_id: str) -> str:
        """Get the Pinecone index name for a connector."""
        return f"{connector_id}-docs"
    
    def _get_or_create_index(self, connector_id: str):
        """Get or create a Pinecone index for a connector.
        
        Args:
            connector_id: Connector ID
            
        Returns:
            Pinecone Index object
        """
        index_name = self._get_index_name(connector_id)
        
        # Check cache first
        if index_name in self._indices:
            return self._indices[index_name]
        
        # Check if index exists
        existing_indices = [idx.name for idx in self.pinecone.list_indexes()]
        
        if index_name not in existing_indices:
            # Create new index
            self.pinecone.create_index(
                name=index_name,
                dimension=self.dimension,
                metric="cosine",
                spec=ServerlessSpec(
                    cloud="aws",
                    region="us-east-1"
                )
            )
            # Wait for index to be ready
            import time
            while True:
                desc = self.pinecone.describe_index(index_name)
                if desc.status.ready:
                    break
                time.sleep(1)
        
        # Connect to index
        index = self.pinecone.Index(index_name)
        self._indices[index_name] = index
        
        return index
    
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
    
    def index_exists(self, connector_id: str) -> bool:
        """Check if an index exists for a connector.
        
        Args:
            connector_id: Connector ID
            
        Returns:
            True if index exists
        """
        index_name = self._get_index_name(connector_id)
        existing_indices = [idx.name for idx in self.pinecone.list_indexes()]
        return index_name in existing_indices
    
    def get_index_stats(self, connector_id: str) -> Dict[str, Any]:
        """Get statistics for a connector's index.
        
        Args:
            connector_id: Connector ID
            
        Returns:
            Index statistics
        """
        if not self.index_exists(connector_id):
            return {"exists": False, "vectors": 0}
        
        index = self._get_or_create_index(connector_id)
        stats = index.describe_index_stats()
        
        return {
            "exists": True,
            "vectors": stats.total_vector_count,
            "dimension": stats.dimension,
            "index_name": self._get_index_name(connector_id)
        }
    
    def vectorize_research(
        self, 
        connector_id: str, 
        connector_name: str,
        research_content: str,
        source_type: str = "research"
    ) -> int:
        """Vectorize a research document into a connector's index.
        
        Args:
            connector_id: Connector ID
            connector_name: Connector display name
            research_content: Research document content
            source_type: Type of source (research, code, web)
            
        Returns:
            Number of vectors created
        """
        index = self._get_or_create_index(connector_id)
        
        # Split into chunks
        chunks = self._chunk_text(research_content)
        
        # Create vectors
        vectors = []
        for i, chunk in enumerate(chunks):
            # Generate embedding
            embedding = self._generate_embedding(chunk)
            
            # Create vector
            vector_id = self._generate_chunk_id(connector_id, chunk, i)
            
            # Determine section from content
            section = "General"
            if "## " in chunk:
                # Extract section number/name
                import re
                section_match = re.search(r'##\s+(\d+)\.\s+([^\n]+)', chunk)
                if section_match:
                    section = f"{section_match.group(1)}. {section_match.group(2)}"
            
            metadata = {
                "connector_id": connector_id,
                "connector_name": connector_name,
                "source_type": source_type,
                "section": section,
                "chunk_index": i,
                "text": chunk[:2000],  # Truncate for metadata limit
                "created_at": datetime.utcnow().isoformat()
            }
            
            vectors.append({
                "id": vector_id,
                "values": embedding,
                "metadata": metadata
            })
        
        # Upsert in batches
        batch_size = 100
        for i in range(0, len(vectors), batch_size):
            batch = vectors[i:i + batch_size]
            index.upsert(vectors=batch)
        
        return len(vectors)
    
    def search(
        self,
        connector_id: str,
        query: str,
        top_k: int = 5,
        filter: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Search a connector's index.
        
        Args:
            connector_id: Connector ID
            query: Search query
            top_k: Number of results
            filter: Optional metadata filter
            
        Returns:
            List of search results
        """
        if not self.index_exists(connector_id):
            return []
        
        index = self._get_or_create_index(connector_id)
        
        # Generate query embedding
        query_embedding = self._generate_embedding(query)
        
        # Search
        results = index.query(
            vector=query_embedding,
            top_k=top_k,
            filter=filter,
            include_metadata=True
        )
        
        # Format results
        formatted = []
        for match in results.matches:
            formatted.append({
                "id": match.id,
                "score": match.score,
                "text": match.metadata.get("text", ""),
                "section": match.metadata.get("section", ""),
                "source_type": match.metadata.get("source_type", ""),
                "connector_name": match.metadata.get("connector_name", "")
            })
        
        return formatted
    
    def search_all_connectors(
        self,
        query: str,
        connector_ids: List[str],
        top_k: int = 5
    ) -> List[Dict[str, Any]]:
        """Search across multiple connector indices.
        
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
        """Delete a connector's index.
        
        Args:
            connector_id: Connector ID
            
        Returns:
            True if deleted
        """
        index_name = self._get_index_name(connector_id)
        
        if not self.index_exists(connector_id):
            return False
        
        try:
            self.pinecone.delete_index(index_name)
            # Remove from cache
            if index_name in self._indices:
                del self._indices[index_name]
            return True
        except Exception as e:
            print(f"Error deleting index: {e}")
            return False


# Singleton instance
_manager: Optional[PineconeManager] = None


def get_pinecone_manager() -> PineconeManager:
    """Get the singleton PineconeManager instance."""
    global _manager
    if _manager is None:
        _manager = PineconeManager()
    return _manager

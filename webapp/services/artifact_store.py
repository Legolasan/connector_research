"""
📦 Artifact Store Service
Redis-backed shared artifact and facts registry for task coordination.

Artifacts are pieces of information discovered during research:
- Search results
- Page content
- Extracted facts
- Draft sections

Facts are structured claims with provenance and confidence scores.
"""

import os
import json
import hashlib
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Dict, List, Optional, Set, Any
import redis

from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


@dataclass
class Artifact:
    """A discovered piece of information."""
    id: str
    artifact_type: str  # "search_result", "page_content", "fact", "draft_section"
    source_url: Optional[str]
    content: str
    confidence: float
    created_at: str  # ISO format string for JSON serialization
    created_by_task: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Artifact":
        return cls(**data)
    
    @staticmethod
    def generate_id(content: str, source_url: Optional[str] = None) -> str:
        """Generate deterministic ID from content hash."""
        hash_input = content[:1000]  # First 1000 chars for efficiency
        if source_url:
            hash_input = f"{source_url}:{hash_input}"
        return hashlib.sha256(hash_input.encode()).hexdigest()[:16]


@dataclass
class Fact:
    """An extracted fact with provenance."""
    id: str
    claim: str
    evidence: List[str]  # Artifact IDs
    confidence: float
    category: str  # "auth", "rate_limit", "endpoint", "object", "sdk", "webhook"
    created_at: str  # ISO format string
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Fact":
        return cls(**data)
    
    @staticmethod
    def generate_id(claim: str) -> str:
        """Generate deterministic ID from claim hash."""
        normalized = claim.lower().strip()
        return hashlib.sha256(normalized.encode()).hexdigest()[:16]


class ArtifactStore:
    """
    Redis-backed shared artifact store for task coordination.
    
    Enables multiple Celery workers to:
    - Share discovered artifacts (search results, page content)
    - Deduplicate work (don't fetch same URL twice)
    - Build a shared facts registry
    - Track progress and convergence
    """
    
    # Key prefixes
    ARTIFACTS_KEY = "artifacts"
    FACTS_KEY = "facts"
    SOURCES_KEY = "sources"
    STATS_KEY = "stats"
    
    def __init__(self, redis_url: str = REDIS_URL, connector_name: str = ""):
        """
        Initialize artifact store.
        
        Args:
            redis_url: Redis connection URL
            connector_name: Name of connector (used as key prefix)
        """
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.connector_name = connector_name
        self.prefix = f"research:{connector_name}" if connector_name else "research"
    
    def _key(self, *parts: str) -> str:
        """Generate Redis key with prefix."""
        return f"{self.prefix}:{':'.join(parts)}"
    
    # =========================================================================
    # Artifact Operations
    # =========================================================================
    
    def add_artifact(self, artifact: Artifact) -> str:
        """
        Add artifact to store. Deduplicates by ID.
        
        Args:
            artifact: Artifact to add
            
        Returns:
            Artifact ID
        """
        key = self._key(self.ARTIFACTS_KEY, artifact.artifact_type, artifact.id)
        
        # Check if already exists
        if self.redis.exists(key):
            return artifact.id
        
        # Store artifact
        self.redis.set(key, json.dumps(artifact.to_dict()))
        
        # Add to type index
        self.redis.sadd(self._key(self.ARTIFACTS_KEY, artifact.artifact_type, "_index"), artifact.id)
        
        # Track source URL if present
        if artifact.source_url:
            self.redis.sadd(self._key(self.SOURCES_KEY), artifact.source_url)
        
        # Update stats
        self.redis.hincrby(self._key(self.STATS_KEY), f"artifacts:{artifact.artifact_type}", 1)
        
        return artifact.id
    
    def get_artifact(self, artifact_id: str, artifact_type: str) -> Optional[Artifact]:
        """Get artifact by ID and type."""
        key = self._key(self.ARTIFACTS_KEY, artifact_type, artifact_id)
        data = self.redis.get(key)
        if data:
            return Artifact.from_dict(json.loads(data))
        return None
    
    def get_artifacts_by_type(self, artifact_type: str) -> List[Artifact]:
        """Get all artifacts of a specific type."""
        index_key = self._key(self.ARTIFACTS_KEY, artifact_type, "_index")
        artifact_ids = self.redis.smembers(index_key)
        
        artifacts = []
        for artifact_id in artifact_ids:
            artifact = self.get_artifact(artifact_id, artifact_type)
            if artifact:
                artifacts.append(artifact)
        
        return artifacts
    
    def artifact_exists(self, artifact_id: str, artifact_type: str) -> bool:
        """Check if artifact exists."""
        key = self._key(self.ARTIFACTS_KEY, artifact_type, artifact_id)
        return self.redis.exists(key) > 0
    
    # =========================================================================
    # Fact Operations (Facts Registry)
    # =========================================================================
    
    def add_fact(self, fact: Fact) -> bool:
        """
        Add fact to registry. Deduplicates by claim similarity.
        
        Args:
            fact: Fact to add
            
        Returns:
            True if new fact added, False if duplicate
        """
        key = self._key(self.FACTS_KEY, fact.category, fact.id)
        
        # Check if fact with same ID exists
        if self.redis.exists(key):
            # Merge evidence from new fact
            existing_data = json.loads(self.redis.get(key))
            existing_evidence = set(existing_data.get("evidence", []))
            new_evidence = set(fact.evidence)
            merged_evidence = list(existing_evidence | new_evidence)
            existing_data["evidence"] = merged_evidence
            
            # Update confidence (take max)
            existing_data["confidence"] = max(existing_data.get("confidence", 0), fact.confidence)
            
            self.redis.set(key, json.dumps(existing_data))
            return False  # Not a new fact
        
        # Store new fact
        self.redis.set(key, json.dumps(fact.to_dict()))
        
        # Add to category index
        self.redis.sadd(self._key(self.FACTS_KEY, fact.category, "_index"), fact.id)
        
        # Add to global index
        self.redis.sadd(self._key(self.FACTS_KEY, "_all"), f"{fact.category}:{fact.id}")
        
        # Update stats
        self.redis.hincrby(self._key(self.STATS_KEY), f"facts:{fact.category}", 1)
        self.redis.hincrby(self._key(self.STATS_KEY), "facts:total", 1)
        
        return True
    
    def get_fact(self, fact_id: str, category: str) -> Optional[Fact]:
        """Get fact by ID and category."""
        key = self._key(self.FACTS_KEY, category, fact_id)
        data = self.redis.get(key)
        if data:
            return Fact.from_dict(json.loads(data))
        return None
    
    def get_facts_by_category(self, category: str) -> List[Fact]:
        """Get all facts for a category."""
        index_key = self._key(self.FACTS_KEY, category, "_index")
        fact_ids = self.redis.smembers(index_key)
        
        facts = []
        for fact_id in fact_ids:
            fact = self.get_fact(fact_id, category)
            if fact:
                facts.append(fact)
        
        return facts
    
    def get_all_facts(self) -> Dict[str, List[Fact]]:
        """Get all facts grouped by category."""
        categories = ["auth", "rate_limit", "endpoint", "object", "sdk", "webhook"]
        return {cat: self.get_facts_by_category(cat) for cat in categories}
    
    def get_fact_count(self) -> int:
        """Get total unique facts discovered."""
        return int(self.redis.hget(self._key(self.STATS_KEY), "facts:total") or 0)
    
    def get_fact_count_by_category(self, category: str) -> int:
        """Get fact count for a specific category."""
        return int(self.redis.hget(self._key(self.STATS_KEY), f"facts:{category}") or 0)
    
    # =========================================================================
    # Source Tracking
    # =========================================================================
    
    def get_unique_sources(self) -> Set[str]:
        """Get set of unique source URLs processed."""
        return self.redis.smembers(self._key(self.SOURCES_KEY))
    
    def source_exists(self, url: str) -> bool:
        """Check if source URL has been processed."""
        return self.redis.sismember(self._key(self.SOURCES_KEY), url)
    
    def get_source_count(self) -> int:
        """Get count of unique sources."""
        return self.redis.scard(self._key(self.SOURCES_KEY))
    
    # =========================================================================
    # Statistics and Progress
    # =========================================================================
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current statistics."""
        stats = self.redis.hgetall(self._key(self.STATS_KEY))
        return {k: int(v) for k, v in stats.items()}
    
    def increment_stat(self, stat_name: str, amount: int = 1) -> int:
        """Increment a stat counter."""
        return self.redis.hincrby(self._key(self.STATS_KEY), stat_name, amount)
    
    # =========================================================================
    # Cleanup
    # =========================================================================
    
    def clear(self):
        """Clear all data for this connector."""
        # Get all keys with our prefix
        pattern = f"{self.prefix}:*"
        cursor = 0
        while True:
            cursor, keys = self.redis.scan(cursor, match=pattern, count=100)
            if keys:
                self.redis.delete(*keys)
            if cursor == 0:
                break
    
    def get_ttl_remaining(self, key: str) -> int:
        """Get TTL remaining for a key."""
        return self.redis.ttl(self._key(key))


# =========================================================================
# Progress Events
# =========================================================================

class ProgressEmitter:
    """Emit progress events to Redis for UI consumption."""
    
    PROGRESS_KEY = "progress"
    MAX_EVENTS = 100  # Keep last 100 events
    
    def __init__(self, redis_url: str = REDIS_URL, connector_name: str = ""):
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.connector_name = connector_name
        self.prefix = f"research:{connector_name}" if connector_name else "research"
    
    def _key(self, *parts: str) -> str:
        return f"{self.prefix}:{':'.join(parts)}"
    
    def emit(self, phase: str, message: str, metadata: Optional[Dict] = None):
        """
        Emit a progress event.
        
        Args:
            phase: Current phase (web_search, fetch, summarize, synthesis)
            message: Human-readable progress message
            metadata: Optional additional data
        """
        event = {
            "phase": phase,
            "message": message,
            "timestamp": datetime.utcnow().isoformat(),
            "metadata": metadata or {}
        }
        
        key = self._key(self.PROGRESS_KEY)
        
        # Push event to list
        self.redis.rpush(key, json.dumps(event))
        
        # Trim to keep only last N events
        self.redis.ltrim(key, -self.MAX_EVENTS, -1)
    
    def get_events(self, limit: int = 10) -> List[Dict]:
        """Get recent progress events."""
        key = self._key(self.PROGRESS_KEY)
        events = self.redis.lrange(key, -limit, -1)
        return [json.loads(e) for e in events]
    
    def get_phase_counts(self) -> Dict[str, int]:
        """Get count of events by phase."""
        events = self.get_events(limit=self.MAX_EVENTS)
        counts = {}
        for event in events:
            phase = event.get("phase", "unknown")
            counts[phase] = counts.get(phase, 0) + 1
        return counts
    
    def clear(self):
        """Clear all progress events."""
        self.redis.delete(self._key(self.PROGRESS_KEY))


# =========================================================================
# Factory Functions
# =========================================================================

def get_artifact_store(connector_name: str) -> ArtifactStore:
    """Get artifact store for a connector."""
    return ArtifactStore(REDIS_URL, connector_name)


def get_progress_emitter(connector_name: str) -> ProgressEmitter:
    """Get progress emitter for a connector."""
    return ProgressEmitter(REDIS_URL, connector_name)


def emit_progress(connector_name: str, phase: str, message: str, metadata: Optional[Dict] = None):
    """Convenience function to emit progress event."""
    emitter = get_progress_emitter(connector_name)
    emitter.emit(phase, message, metadata)

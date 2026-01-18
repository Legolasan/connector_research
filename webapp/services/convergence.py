"""
🎯 Convergence Checker
Determines when research has gathered enough information to stop.

Convergence Criteria:
1. Minimum facts per category met
2. No new unique sources in last N tasks (diminishing returns)
3. Confidence threshold met for core categories

This enables early-exit for:
- Short queries
- Highly documented topics (Shopify, Zendesk)
- Topics with clear answers
"""

import os
from typing import Tuple, Dict, List, Optional
from datetime import datetime, timedelta
import redis
import json

from dotenv import load_dotenv

# Deferred import to avoid circular dependency at module load time
# The actual import is done inside methods that need it

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


class ConvergenceChecker:
    """
    Check if research has converged (enough information gathered).
    
    Uses shared Redis counters to track:
    - Facts discovered per category
    - Sources processed
    - Recent task results
    """
    
    # Minimum facts required per category before convergence
    MIN_FACTS = {
        "auth": 2,
        "rate_limit": 1,
        "endpoint": 3,
        "object": 2,
        "sdk": 1,
        "webhook": 1
    }
    
    # Categories that must meet minimum threshold
    REQUIRED_CATEGORIES = ["auth", "endpoint"]
    
    # Confidence threshold for "high confidence" convergence
    HIGH_CONFIDENCE_THRESHOLD = 0.8
    
    # Number of tasks with no new sources before "diminishing returns" convergence
    NO_NEW_SOURCE_THRESHOLD = 5
    
    def __init__(self, redis_url: str = REDIS_URL, connector_name: str = ""):
        """
        Initialize convergence checker.
        
        Args:
            redis_url: Redis connection URL
            connector_name: Name of connector being researched
        """
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.connector_name = connector_name
        self.prefix = f"convergence:{connector_name}" if connector_name else "convergence"
    
    def _key(self, *parts: str) -> str:
        """Generate Redis key with prefix."""
        return f"{self.prefix}:{':'.join(parts)}"
    
    def check_convergence(self) -> Tuple[bool, str]:
        """
        Check if research has converged.
        
        Returns:
            Tuple of (converged: bool, reason: str)
        """
        # Import here to avoid circular dependency
        from services.artifact_store import get_artifact_store
        
        store = get_artifact_store(self.connector_name)
        
        # Check 1: Minimum facts per required category
        for category in self.REQUIRED_CATEGORIES:
            fact_count = store.get_fact_count_by_category(category)
            required = self.MIN_FACTS.get(category, 1)
            
            if fact_count < required:
                return False, f"Need {required - fact_count} more {category} facts"
        
        # Check 2: Diminishing returns (no new sources recently)
        recent_source_count = self._get_recent_source_count()
        if recent_source_count == 0:
            total_facts = store.get_fact_count()
            if total_facts >= 5:  # At least some facts discovered
                return True, f"Converged: No new sources, {total_facts} facts gathered"
        
        # Check 3: High confidence in core categories
        avg_confidence = self._get_avg_confidence(self.REQUIRED_CATEGORIES)
        if avg_confidence > self.HIGH_CONFIDENCE_THRESHOLD:
            return True, f"Converged: High confidence ({avg_confidence:.2f})"
        
        # Check 4: Total facts threshold (absolute ceiling)
        total_facts = store.get_fact_count()
        if total_facts >= 20:
            return True, f"Converged: Sufficient facts ({total_facts})"
        
        return False, "Still gathering information"
    
    def _get_recent_source_count(self) -> int:
        """
        Get count of new sources discovered in recent tasks.
        
        Returns:
            Number of new sources in last N tasks
        """
        recent_key = self._key("recent_sources")
        count = self.redis.get(recent_key)
        return int(count) if count else 0
    
    def _get_avg_confidence(self, categories: List[str]) -> float:
        """
        Get average confidence score across categories.
        
        Args:
            categories: List of categories to check
            
        Returns:
            Average confidence score (0.0 - 1.0)
        """
        from services.artifact_store import get_artifact_store
        
        store = get_artifact_store(self.connector_name)
        
        total_confidence = 0.0
        total_facts = 0
        
        for category in categories:
            facts = store.get_facts_by_category(category)
            for fact in facts:
                total_confidence += fact.confidence
                total_facts += 1
        
        if total_facts == 0:
            return 0.0
        
        return total_confidence / total_facts
    
    def record_task_result(self, new_sources: int, new_facts: int):
        """
        Record a task result for convergence tracking.
        
        Args:
            new_sources: Number of new sources discovered
            new_facts: Number of new facts extracted
        """
        # Track recent new sources (rolling window)
        recent_key = self._key("recent_sources")
        if new_sources > 0:
            self.redis.set(recent_key, new_sources)
            self.redis.expire(recent_key, 300)  # 5 minute window
        else:
            # Decrement if no new sources
            current = int(self.redis.get(recent_key) or 0)
            if current > 0:
                self.redis.decr(recent_key)
        
        # Track task count
        self.redis.incr(self._key("task_count"))
        
        # Track cumulative facts
        self.redis.incrby(self._key("total_facts"), new_facts)
    
    def get_convergence_stats(self) -> Dict:
        """
        Get current convergence statistics.
        
        Returns:
            Dict with convergence metrics
        """
        from services.artifact_store import get_artifact_store
        
        store = get_artifact_store(self.connector_name)
        
        stats = {
            "total_facts": store.get_fact_count(),
            "total_sources": store.get_source_count(),
            "recent_sources": self._get_recent_source_count(),
            "task_count": int(self.redis.get(self._key("task_count")) or 0),
            "facts_by_category": {},
            "confidence_by_category": {}
        }
        
        for category in self.MIN_FACTS.keys():
            facts = store.get_facts_by_category(category)
            stats["facts_by_category"][category] = len(facts)
            
            if facts:
                avg_conf = sum(f.confidence for f in facts) / len(facts)
                stats["confidence_by_category"][category] = round(avg_conf, 2)
            else:
                stats["confidence_by_category"][category] = 0.0
        
        # Check convergence
        converged, reason = self.check_convergence()
        stats["converged"] = converged
        stats["convergence_reason"] = reason
        
        return stats
    
    def reset(self):
        """Reset convergence tracking for a new research session."""
        pattern = f"{self.prefix}:*"
        cursor = 0
        while True:
            cursor, keys = self.redis.scan(cursor, match=pattern, count=100)
            if keys:
                self.redis.delete(*keys)
            if cursor == 0:
                break


def get_convergence_checker(connector_name: str) -> ConvergenceChecker:
    """Get convergence checker for a connector."""
    return ConvergenceChecker(REDIS_URL, connector_name)

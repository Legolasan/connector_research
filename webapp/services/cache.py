"""
🗄️ Research Cache Service
Redis-based cache with semantic + structural keys for web search and LLM responses.

Cache Key Design:
- Web Search: hash(normalized_query + domain_filters)
- LLM Response: hash(model + prompt_type + instruction_type + normalized_input)

This enables high cache hit rates by normalizing queries and including
structural metadata in cache keys.
"""

import os
import re
import json
import hashlib
from typing import Optional, Dict, Any, List
from datetime import datetime
import redis

from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


class ResearchCache:
    """
    Redis cache with semantic + structural keys.
    
    Optimized for research workloads:
    - Web search results (24h TTL)
    - LLM responses (6h TTL)
    - Crawled page content (12h TTL)
    """
    
    # Cache TTLs (in seconds)
    WEB_SEARCH_TTL = 86400      # 24 hours
    LLM_RESPONSE_TTL = 21600    # 6 hours
    PAGE_CONTENT_TTL = 43200    # 12 hours
    FACT_EXTRACTION_TTL = 3600  # 1 hour (facts may need refresh)
    
    # Key prefixes
    SEARCH_PREFIX = "cache:search"
    LLM_PREFIX = "cache:llm"
    PAGE_PREFIX = "cache:page"
    FACT_PREFIX = "cache:fact"
    
    # Stopwords to remove from search queries for normalization
    STOPWORDS = {
        'a', 'an', 'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
        'of', 'with', 'by', 'from', 'is', 'are', 'was', 'were', 'be', 'been',
        'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
        'could', 'should', 'may', 'might', 'must', 'shall', 'can', 'need',
        'how', 'what', 'which', 'who', 'when', 'where', 'why', 'this', 'that'
    }
    
    def __init__(self, redis_url: str = REDIS_URL):
        """Initialize cache with Redis connection."""
        self.redis = redis.from_url(redis_url, decode_responses=True)
    
    # =========================================================================
    # Key Generation (Semantic + Structural)
    # =========================================================================
    
    def _normalize_query(self, query: str) -> str:
        """
        Normalize search query for cache key generation.
        
        - Lowercase
        - Remove stopwords
        - Collapse whitespace
        - Sort words alphabetically (order-independent matching)
        """
        # Lowercase and split
        words = query.lower().split()
        
        # Remove stopwords
        words = [w for w in words if w not in self.STOPWORDS]
        
        # Remove special characters from each word
        words = [re.sub(r'[^\w]', '', w) for w in words]
        
        # Remove empty strings
        words = [w for w in words if w]
        
        # Sort for order independence
        words.sort()
        
        return ' '.join(words)
    
    def _web_search_key(
        self, 
        query: str, 
        domain_filter: Optional[str] = None,
        time_range: Optional[str] = None
    ) -> str:
        """
        Generate semantic cache key for web search.
        
        Components:
        - Normalized query (stopwords removed, sorted)
        - Domain filter (if specified)
        - Time range (if specified)
        """
        normalized = self._normalize_query(query)
        components = [normalized]
        
        if domain_filter:
            components.append(f"domain:{domain_filter}")
        if time_range:
            components.append(f"time:{time_range}")
        
        key_input = '|'.join(components)
        key_hash = hashlib.sha256(key_input.encode()).hexdigest()[:24]
        
        return f"{self.SEARCH_PREFIX}:{key_hash}"
    
    def _llm_key(
        self, 
        model: str, 
        prompt_type: str, 
        input_content: str,
        instruction_type: Optional[str] = None
    ) -> str:
        """
        Generate cache key for LLM response.
        
        Components:
        - Model name/version
        - Prompt type (extract_facts, summarize, synthesize)
        - Instruction type (if applicable)
        - Input content hash
        """
        # Hash the input content
        input_hash = hashlib.sha256(input_content.encode()).hexdigest()[:16]
        
        components = [model, prompt_type]
        if instruction_type:
            components.append(instruction_type)
        components.append(input_hash)
        
        key_input = ':'.join(components)
        return f"{self.LLM_PREFIX}:{key_input}"
    
    def _page_key(self, url: str) -> str:
        """Generate cache key for crawled page content."""
        # Normalize URL
        url = url.lower().rstrip('/')
        url_hash = hashlib.sha256(url.encode()).hexdigest()[:24]
        return f"{self.PAGE_PREFIX}:{url_hash}"
    
    # =========================================================================
    # Web Search Cache
    # =========================================================================
    
    def get_web_search(
        self, 
        query: str, 
        domain_filter: Optional[str] = None,
        time_range: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get cached web search result.
        
        Args:
            query: Search query
            domain_filter: Optional domain to filter results
            time_range: Optional time range filter
            
        Returns:
            Cached search results dict or None
        """
        key = self._web_search_key(query, domain_filter, time_range)
        cached = self.redis.get(key)
        
        if cached:
            data = json.loads(cached)
            data['_cached'] = True
            data['_cache_key'] = key
            return data
        
        return None
    
    def set_web_search(
        self, 
        query: str, 
        result: Dict[str, Any],
        domain_filter: Optional[str] = None,
        time_range: Optional[str] = None,
        ttl: Optional[int] = None
    ):
        """
        Cache web search result.
        
        Args:
            query: Search query
            result: Search results dict
            domain_filter: Optional domain filter
            time_range: Optional time range filter
            ttl: Optional custom TTL (defaults to WEB_SEARCH_TTL)
        """
        key = self._web_search_key(query, domain_filter, time_range)
        ttl = ttl or self.WEB_SEARCH_TTL
        
        # Add cache metadata
        result['_cached_at'] = datetime.utcnow().isoformat()
        result['_original_query'] = query
        
        self.redis.setex(key, ttl, json.dumps(result))
    
    # =========================================================================
    # LLM Response Cache
    # =========================================================================
    
    def get_llm_response(
        self, 
        model: str, 
        prompt_type: str, 
        input_content: str,
        instruction_type: Optional[str] = None
    ) -> Optional[str]:
        """
        Get cached LLM response.
        
        Args:
            model: Model name (e.g., "gpt-4o-mini")
            prompt_type: Type of prompt (e.g., "extract_facts")
            input_content: The input content being processed
            instruction_type: Optional instruction variant
            
        Returns:
            Cached response string or None
        """
        key = self._llm_key(model, prompt_type, input_content, instruction_type)
        return self.redis.get(key)
    
    def set_llm_response(
        self, 
        model: str, 
        prompt_type: str, 
        input_content: str,
        response: str,
        instruction_type: Optional[str] = None,
        ttl: Optional[int] = None
    ):
        """
        Cache LLM response.
        
        Args:
            model: Model name
            prompt_type: Type of prompt
            input_content: The input content being processed
            response: The LLM response to cache
            instruction_type: Optional instruction variant
            ttl: Optional custom TTL
        """
        key = self._llm_key(model, prompt_type, input_content, instruction_type)
        ttl = ttl or self.LLM_RESPONSE_TTL
        self.redis.setex(key, ttl, response)
    
    # =========================================================================
    # Page Content Cache
    # =========================================================================
    
    def get_page_content(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Get cached page content.
        
        Args:
            url: Page URL
            
        Returns:
            Cached page data dict or None
        """
        key = self._page_key(url)
        cached = self.redis.get(key)
        
        if cached:
            data = json.loads(cached)
            data['_cached'] = True
            return data
        
        return None
    
    def set_page_content(
        self, 
        url: str, 
        content: str, 
        title: Optional[str] = None,
        metadata: Optional[Dict] = None,
        ttl: Optional[int] = None
    ):
        """
        Cache page content.
        
        Args:
            url: Page URL
            content: Page text content
            title: Optional page title
            metadata: Optional additional metadata
            ttl: Optional custom TTL
        """
        key = self._page_key(url)
        ttl = ttl or self.PAGE_CONTENT_TTL
        
        data = {
            'url': url,
            'content': content,
            'title': title,
            'metadata': metadata or {},
            '_cached_at': datetime.utcnow().isoformat()
        }
        
        self.redis.setex(key, ttl, json.dumps(data))
    
    # =========================================================================
    # Statistics and Management
    # =========================================================================
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        stats = {
            'search_keys': 0,
            'llm_keys': 0,
            'page_keys': 0,
            'total_keys': 0
        }
        
        # Count keys by prefix (using SCAN for efficiency)
        for prefix, stat_key in [
            (self.SEARCH_PREFIX, 'search_keys'),
            (self.LLM_PREFIX, 'llm_keys'),
            (self.PAGE_PREFIX, 'page_keys')
        ]:
            cursor = 0
            count = 0
            while True:
                cursor, keys = self.redis.scan(cursor, match=f"{prefix}:*", count=100)
                count += len(keys)
                if cursor == 0:
                    break
            stats[stat_key] = count
        
        stats['total_keys'] = stats['search_keys'] + stats['llm_keys'] + stats['page_keys']
        
        return stats
    
    def clear_search_cache(self):
        """Clear all search cache entries."""
        self._clear_by_prefix(self.SEARCH_PREFIX)
    
    def clear_llm_cache(self):
        """Clear all LLM response cache entries."""
        self._clear_by_prefix(self.LLM_PREFIX)
    
    def clear_page_cache(self):
        """Clear all page content cache entries."""
        self._clear_by_prefix(self.PAGE_PREFIX)
    
    def clear_all(self):
        """Clear entire cache."""
        self._clear_by_prefix("cache:")
    
    def _clear_by_prefix(self, prefix: str):
        """Clear all keys with given prefix."""
        cursor = 0
        while True:
            cursor, keys = self.redis.scan(cursor, match=f"{prefix}*", count=100)
            if keys:
                self.redis.delete(*keys)
            if cursor == 0:
                break


# =========================================================================
# Factory Functions
# =========================================================================

_cache_instance: Optional[ResearchCache] = None


def get_research_cache() -> ResearchCache:
    """Get singleton cache instance."""
    global _cache_instance
    if _cache_instance is None:
        _cache_instance = ResearchCache(REDIS_URL)
    return _cache_instance


def invalidate_cache():
    """Invalidate singleton cache instance (for testing)."""
    global _cache_instance
    _cache_instance = None

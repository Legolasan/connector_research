"""
🕷️ LLM Crawler Service
Wrapper around the llm-crawler package for programmatic use in the research agent.

This service:
- Crawls documentation URLs using llm-crawler's smart extraction
- Returns chunked content optimized for LLM consumption
- Integrates with Knowledge Vault for vector storage
"""

import asyncio
import json
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class CrawlChunk:
    """A single chunk from the crawler output."""
    chunk_id: str
    content: str
    char_count: int
    estimated_tokens: int
    position: int
    heading_context: str
    url: str
    title: str
    crawled_at: str


@dataclass
class LLMCrawlResult:
    """Result of an LLM crawler operation."""
    connector_name: str
    chunks: List[CrawlChunk] = field(default_factory=list)
    total_pages: int = 0
    total_chunks: int = 0
    total_words: int = 0
    crawl_duration_seconds: float = 0.0
    urls_crawled: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "connector_name": self.connector_name,
            "total_pages": self.total_pages,
            "total_chunks": self.total_chunks,
            "total_words": self.total_words,
            "crawl_duration_seconds": self.crawl_duration_seconds,
            "urls_crawled": self.urls_crawled,
            "errors": self.errors,
            "chunks": [
                {
                    "chunk_id": c.chunk_id,
                    "content": c.content,
                    "char_count": c.char_count,
                    "estimated_tokens": c.estimated_tokens,
                    "position": c.position,
                    "heading_context": c.heading_context,
                    "url": c.url,
                    "title": c.title,
                }
                for c in self.chunks
            ]
        }
    
    @property
    def total_content(self) -> str:
        """Combine all chunks into a single string for backward compatibility."""
        return "\n\n---\n\n".join([
            f"## {c.heading_context or c.title}\nSource: {c.url}\n\n{c.content}"
            for c in self.chunks
        ])


class LLMCrawlerService:
    """
    Service wrapper for llm-crawler package.
    
    Provides async interface for crawling documentation URLs
    and returns content optimized for LLM consumption and vector storage.
    """
    
    def __init__(self):
        """Initialize the LLM Crawler service."""
        self._crawler = None
        self._initialized = False
        
        try:
            from crawler.crawler import WebCrawler
            self._crawler_class = WebCrawler
            self._initialized = True
            print("  🕷️ LLM Crawler service initialized!")
        except ImportError as e:
            print(f"  ⚠ LLM Crawler not available: {e}")
            print("    Install with: pip install llm-crawler")
            self._crawler_class = None
    
    @property
    def is_available(self) -> bool:
        """Check if the crawler is available."""
        return self._initialized and self._crawler_class is not None
    
    async def crawl_urls(
        self,
        urls: List[str],
        connector_name: str = "unknown",
        depth: int = 2,
        chunk_size: int = 4000,
        rate_limit: float = 1.0,
        max_pages: int = 50
    ) -> LLMCrawlResult:
        """
        Crawl multiple URLs and return chunked content.
        
        Args:
            urls: List of URLs to crawl
            connector_name: Name of the connector (for logging)
            depth: Maximum crawl depth (default: 2)
            chunk_size: Target chunk size in characters (default: 4000)
            rate_limit: Delay between requests in seconds (default: 1.0)
            max_pages: Maximum pages to crawl (default: 50)
            
        Returns:
            LLMCrawlResult with chunked content and metadata
        """
        result = LLMCrawlResult(connector_name=connector_name)
        start_time = datetime.utcnow()
        
        if not self.is_available:
            result.errors.append("LLM Crawler not available")
            return result
        
        if not urls:
            result.errors.append("No URLs provided")
            return result
        
        print(f"  🕷️ Starting LLM crawler for {connector_name}")
        print(f"     URLs: {len(urls)}, Depth: {depth}, Max pages: {max_pages}")
        
        try:
            # Run crawler in thread pool since it's synchronous
            crawl_output = await asyncio.get_event_loop().run_in_executor(
                None,
                self._run_crawler_sync,
                urls,
                depth,
                chunk_size,
                rate_limit,
                max_pages
            )
            
            if crawl_output:
                result = self._parse_crawler_output(crawl_output, connector_name)
                result.crawl_duration_seconds = (datetime.utcnow() - start_time).total_seconds()
                
                print(f"  ✓ Crawled {result.total_pages} pages, {result.total_chunks} chunks")
            else:
                result.errors.append("Crawler returned no output")
                
        except Exception as e:
            result.errors.append(f"Crawler error: {str(e)}")
            print(f"  ⚠ Crawler error: {e}")
        
        result.crawl_duration_seconds = (datetime.utcnow() - start_time).total_seconds()
        return result
    
    def _run_crawler_sync(
        self,
        urls: List[str],
        depth: int,
        chunk_size: int,
        rate_limit: float,
        max_pages: int
    ) -> Optional[Dict[str, Any]]:
        """
        Run the crawler synchronously (to be called from executor).
        
        The llm-crawler is a CLI tool, so we need to use its internal
        components programmatically.
        """
        try:
            from crawler.crawler import WebCrawler
            from crawler.chunker import TextChunker
            from crawler.url_manager import URLManager
            
            all_chunks = []
            all_urls_crawled = []
            total_pages = 0
            
            for start_url in urls:
                try:
                    # Create crawler instance for each URL
                    crawler = WebCrawler(
                        start_url=start_url,
                        max_depth=depth,
                        chunk_size=chunk_size,
                        rate_limit=rate_limit,
                        max_pages=max_pages,
                        same_domain=True,
                        respect_robots=True
                    )
                    
                    # Run the crawl
                    output = crawler.crawl()
                    
                    if output and "chunks" in output:
                        all_chunks.extend(output["chunks"])
                        total_pages += output.get("crawl_metadata", {}).get("total_pages_crawled", 0)
                        all_urls_crawled.append(start_url)
                        
                except Exception as e:
                    print(f"    ⚠ Failed to crawl {start_url}: {e}")
                    continue
            
            return {
                "crawl_metadata": {
                    "total_pages_crawled": total_pages,
                    "total_chunks": len(all_chunks)
                },
                "chunks": all_chunks,
                "urls_crawled": all_urls_crawled
            }
            
        except Exception as e:
            print(f"  ⚠ Crawler sync error: {e}")
            return None
    
    def _parse_crawler_output(
        self,
        output: Dict[str, Any],
        connector_name: str
    ) -> LLMCrawlResult:
        """Parse llm-crawler output into our result format."""
        result = LLMCrawlResult(connector_name=connector_name)
        
        metadata = output.get("crawl_metadata", {})
        result.total_pages = metadata.get("total_pages_crawled", 0)
        result.total_chunks = metadata.get("total_chunks", 0)
        result.urls_crawled = output.get("urls_crawled", [])
        
        chunks = output.get("chunks", [])
        total_words = 0
        
        for chunk_data in chunks:
            page_meta = chunk_data.get("page_metadata", {})
            
            chunk = CrawlChunk(
                chunk_id=chunk_data.get("chunk_id", ""),
                content=chunk_data.get("content", ""),
                char_count=chunk_data.get("char_count", 0),
                estimated_tokens=chunk_data.get("estimated_tokens", 0),
                position=chunk_data.get("position", 0),
                heading_context=chunk_data.get("heading_context", ""),
                url=page_meta.get("url", ""),
                title=page_meta.get("title", ""),
                crawled_at=page_meta.get("crawled_at", "")
            )
            result.chunks.append(chunk)
            total_words += len(chunk.content.split())
        
        result.total_words = total_words
        result.total_chunks = len(result.chunks)
        
        return result
    
    async def crawl_single_url(
        self,
        url: str,
        connector_name: str = "unknown",
        depth: int = 1,
        chunk_size: int = 4000
    ) -> LLMCrawlResult:
        """
        Crawl a single URL with minimal depth.
        
        Convenience method for crawling individual pages during
        the fallback search flow.
        """
        return await self.crawl_urls(
            urls=[url],
            connector_name=connector_name,
            depth=depth,
            chunk_size=chunk_size,
            max_pages=10
        )
    
    def chunks_to_vault_format(
        self,
        result: LLMCrawlResult
    ) -> List[Dict[str, Any]]:
        """
        Convert LLMCrawlResult chunks to Knowledge Vault indexing format.
        
        Returns list of dicts ready for knowledge_vault.index_chunk()
        """
        vault_chunks = []
        
        for chunk in result.chunks:
            vault_chunks.append({
                "content": chunk.content,
                "title": chunk.heading_context or chunk.title,
                "url": chunk.url,
                "source_type": "official_docs",
                "metadata": {
                    "chunk_id": chunk.chunk_id,
                    "position": chunk.position,
                    "estimated_tokens": chunk.estimated_tokens,
                    "heading_context": chunk.heading_context,
                    "crawled_at": chunk.crawled_at
                }
            })
        
        return vault_chunks


# Singleton instance
_llm_crawler_service: Optional[LLMCrawlerService] = None


def get_llm_crawler_service() -> LLMCrawlerService:
    """Get or create the LLM Crawler service singleton."""
    global _llm_crawler_service
    if _llm_crawler_service is None:
        _llm_crawler_service = LLMCrawlerService()
    return _llm_crawler_service

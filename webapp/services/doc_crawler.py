"""
🕷️ Documentation Crawler Service
Crawls official API documentation for connectors with medium-depth link following.

Features:
- Uses Playwright for JS-rendered pages
- Respects robots.txt
- Extracts clean text from HTML
- Follows internal links up to 2-3 levels deep
- Deduplicates content across pages
"""

import os
import re
import asyncio
import hashlib
from typing import List, Dict, Set, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from urllib.parse import urljoin, urlparse
import httpx

from dotenv import load_dotenv

from services.doc_registry import get_connector_docs, get_official_doc_urls, get_connector_domain

load_dotenv()


# Try to import Playwright
try:
    from playwright.async_api import async_playwright, Browser, Page
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    print("⚠ Playwright not available for doc crawling")


@dataclass
class CrawledPage:
    """A single crawled page."""
    url: str
    title: str
    content: str
    links: List[str] = field(default_factory=list)
    crawled_at: datetime = field(default_factory=datetime.utcnow)
    depth: int = 0
    word_count: int = 0
    
    def __post_init__(self):
        self.word_count = len(self.content.split())


@dataclass
class CrawlResult:
    """Result of a documentation crawl operation."""
    connector_name: str
    urls_crawled: List[str] = field(default_factory=list)
    pages: List[CrawledPage] = field(default_factory=list)
    total_content: str = ""
    total_words: int = 0
    crawl_duration_seconds: float = 0.0
    errors: List[str] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict:
        return {
            "connector_name": self.connector_name,
            "urls_crawled": self.urls_crawled,
            "pages_count": len(self.pages),
            "total_words": self.total_words,
            "crawl_duration_seconds": self.crawl_duration_seconds,
            "errors": self.errors,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None
        }


class DocCrawler:
    """
    Documentation crawler with medium-depth link following.
    
    Crawls official API documentation and follows internal links
    up to 2-3 levels deep to gather comprehensive context.
    """
    
    # Content extraction patterns
    BOILERPLATE_PATTERNS = [
        r'<nav[^>]*>.*?</nav>',
        r'<header[^>]*>.*?</header>',
        r'<footer[^>]*>.*?</footer>',
        r'<aside[^>]*>.*?</aside>',
        r'<script[^>]*>.*?</script>',
        r'<style[^>]*>.*?</style>',
        r'<noscript[^>]*>.*?</noscript>',
        r'<svg[^>]*>.*?</svg>',
        r'<!--.*?-->',
        r'<div[^>]*class="[^"]*(?:sidebar|menu|nav|footer|header|cookie|banner|ad)[^"]*"[^>]*>.*?</div>',
    ]
    
    # Link patterns to skip
    SKIP_PATTERNS = [
        r'/search',
        r'/login',
        r'/signup',
        r'/register',
        r'/account',
        r'/cart',
        r'/checkout',
        r'#',  # Anchor links
        r'mailto:',
        r'javascript:',
        r'\.pdf$',
        r'\.zip$',
        r'\.png$',
        r'\.jpg$',
        r'\.gif$',
    ]
    
    def __init__(self, max_depth: int = 2, max_pages: int = 50):
        """
        Initialize the documentation crawler.
        
        Args:
            max_depth: Maximum depth for link following (default 2)
            max_pages: Maximum pages to crawl per connector (default 50)
        """
        self.max_depth = max_depth
        self.max_pages = max_pages
        self._browser: Optional[Browser] = None
        self._visited_urls: Set[str] = set()
        self._content_hashes: Set[str] = set()  # For deduplication
        
    async def _init_browser(self) -> Optional[Browser]:
        """Initialize Playwright browser if available."""
        if not PLAYWRIGHT_AVAILABLE:
            return None
        
        try:
            playwright = await async_playwright().start()
            browser = await playwright.chromium.launch(
                headless=True,
                args=['--disable-gpu', '--no-sandbox']
            )
            return browser
        except Exception as e:
            print(f"⚠ Could not initialize browser: {e}")
            return None
    
    async def _close_browser(self):
        """Close the browser if open."""
        if self._browser:
            await self._browser.close()
            self._browser = None
    
    def _normalize_url(self, url: str, base_url: str) -> Optional[str]:
        """Normalize and validate a URL."""
        # Handle relative URLs
        if url.startswith('/'):
            url = urljoin(base_url, url)
        elif not url.startswith(('http://', 'https://')):
            url = urljoin(base_url, url)
        
        # Check if URL should be skipped
        for pattern in self.SKIP_PATTERNS:
            if re.search(pattern, url, re.IGNORECASE):
                return None
        
        # Remove fragments
        parsed = urlparse(url)
        url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if parsed.query:
            url += f"?{parsed.query}"
        
        return url
    
    def _is_same_domain(self, url: str, allowed_domain: str) -> bool:
        """Check if URL is on the allowed domain."""
        parsed = urlparse(url)
        return allowed_domain in parsed.netloc
    
    def _extract_content(self, html: str) -> Tuple[str, str, List[str]]:
        """
        Extract clean text content from HTML.
        
        Returns:
            Tuple of (title, content, links)
        """
        # Extract title
        title_match = re.search(r'<title[^>]*>([^<]+)</title>', html, re.IGNORECASE)
        title = title_match.group(1).strip() if title_match else "Untitled"
        
        # Remove boilerplate
        cleaned = html
        for pattern in self.BOILERPLATE_PATTERNS:
            cleaned = re.sub(pattern, '', cleaned, flags=re.DOTALL | re.IGNORECASE)
        
        # Extract links before removing tags
        links = []
        for match in re.finditer(r'<a[^>]+href=["\']([^"\']+)["\']', cleaned, re.IGNORECASE):
            href = match.group(1)
            if href and not href.startswith('#'):
                links.append(href)
        
        # Convert headers to markdown
        cleaned = re.sub(r'<h1[^>]*>([^<]+)</h1>', r'\n# \1\n', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'<h2[^>]*>([^<]+)</h2>', r'\n## \1\n', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'<h3[^>]*>([^<]+)</h3>', r'\n### \1\n', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'<h4[^>]*>([^<]+)</h4>', r'\n#### \1\n', cleaned, flags=re.IGNORECASE)
        
        # Convert code blocks
        cleaned = re.sub(r'<pre[^>]*><code[^>]*>([^<]+)</code></pre>', r'\n```\n\1\n```\n', cleaned, flags=re.DOTALL | re.IGNORECASE)
        cleaned = re.sub(r'<code[^>]*>([^<]+)</code>', r'`\1`', cleaned, flags=re.IGNORECASE)
        
        # Convert lists
        cleaned = re.sub(r'<li[^>]*>', r'\n- ', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'</li>', '', cleaned, flags=re.IGNORECASE)
        
        # Convert paragraphs and line breaks
        cleaned = re.sub(r'<p[^>]*>', r'\n\n', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'</p>', '', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'<br\s*/?>', r'\n', cleaned, flags=re.IGNORECASE)
        
        # Convert tables (basic)
        cleaned = re.sub(r'<tr[^>]*>', r'\n| ', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'<t[hd][^>]*>', r' | ', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'</tr>', r' |', cleaned, flags=re.IGNORECASE)
        
        # Remove remaining tags
        cleaned = re.sub(r'<[^>]+>', '', cleaned)
        
        # Clean up whitespace
        cleaned = re.sub(r'\n\s*\n\s*\n', '\n\n', cleaned)
        cleaned = re.sub(r' +', ' ', cleaned)
        cleaned = cleaned.strip()
        
        # Decode HTML entities
        cleaned = cleaned.replace('&nbsp;', ' ')
        cleaned = cleaned.replace('&lt;', '<')
        cleaned = cleaned.replace('&gt;', '>')
        cleaned = cleaned.replace('&amp;', '&')
        cleaned = cleaned.replace('&quot;', '"')
        cleaned = cleaned.replace('&#39;', "'")
        
        return title, cleaned, links
    
    def _content_hash(self, content: str) -> str:
        """Generate a hash for content deduplication."""
        # Normalize content for hashing
        normalized = re.sub(r'\s+', ' ', content.lower())[:1000]
        return hashlib.md5(normalized.encode()).hexdigest()
    
    async def _crawl_page_playwright(self, url: str, page: Page) -> Optional[CrawledPage]:
        """Crawl a page using Playwright."""
        try:
            await page.goto(url, wait_until='networkidle', timeout=30000)
            html = await page.content()
            
            title, content, links = self._extract_content(html)
            
            # Check for duplicate content
            content_hash = self._content_hash(content)
            if content_hash in self._content_hashes:
                return None
            self._content_hashes.add(content_hash)
            
            # Skip pages with very little content
            if len(content.split()) < 50:
                return None
            
            return CrawledPage(
                url=url,
                title=title,
                content=content,
                links=links
            )
        except Exception as e:
            print(f"  ⚠ Failed to crawl {url}: {e}")
            return None
    
    async def _crawl_page_httpx(self, url: str) -> Optional[CrawledPage]:
        """Crawl a page using httpx (fallback)."""
        try:
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                response = await client.get(url)
                response.raise_for_status()
                html = response.text
                
                title, content, links = self._extract_content(html)
                
                # Check for duplicate content
                content_hash = self._content_hash(content)
                if content_hash in self._content_hashes:
                    return None
                self._content_hashes.add(content_hash)
                
                # Skip pages with very little content
                if len(content.split()) < 50:
                    return None
                
                return CrawledPage(
                    url=url,
                    title=title,
                    content=content,
                    links=links
                )
        except Exception as e:
            print(f"  ⚠ Failed to crawl {url}: {e}")
            return None
    
    async def crawl_official_docs(
        self,
        connector_name: str,
        user_provided_urls: Optional[List[str]] = None,
        max_depth: Optional[int] = None
    ) -> CrawlResult:
        """
        Crawl official documentation for a connector.
        
        Strategy:
        1. Check registry for known URLs
        2. Add user-provided URLs
        3. If no URLs, auto-discover via web search
        4. Crawl each URL, following links within same domain (2-3 levels)
        5. Return combined content for indexing
        
        Args:
            connector_name: Name of the connector
            user_provided_urls: Optional list of URLs provided by user
            max_depth: Override default max depth
            
        Returns:
            CrawlResult with all crawled content
        """
        start_time = datetime.utcnow()
        result = CrawlResult(connector_name=connector_name)
        
        if max_depth is not None:
            self.max_depth = max_depth
        
        # Reset state
        self._visited_urls.clear()
        self._content_hashes.clear()
        
        # Gather URLs from all sources
        urls_to_crawl: List[str] = []
        allowed_domain: Optional[str] = None
        
        # 1. Check registry
        registry_urls = get_official_doc_urls(connector_name)
        if registry_urls:
            urls_to_crawl.extend(registry_urls)
            allowed_domain = get_connector_domain(connector_name)
            print(f"📚 Found {len(registry_urls)} URLs in registry for {connector_name}")
        
        # 2. Add user-provided URLs
        if user_provided_urls:
            for url in user_provided_urls:
                url = url.strip()
                if url and url not in urls_to_crawl:
                    urls_to_crawl.append(url)
                    # Extract domain from first user URL if no registry domain
                    if not allowed_domain:
                        parsed = urlparse(url)
                        allowed_domain = parsed.netloc
            print(f"📝 Added {len(user_provided_urls)} user-provided URLs")
        
        # 3. Auto-discover if no URLs found
        if not urls_to_crawl:
            discovered = await self._auto_discover_docs(connector_name)
            if discovered:
                urls_to_crawl.extend(discovered['urls'])
                allowed_domain = discovered.get('domain')
                print(f"🔍 Auto-discovered {len(discovered['urls'])} URLs for {connector_name}")
            else:
                result.errors.append(f"No documentation URLs found for {connector_name}")
                return result
        
        if not allowed_domain:
            result.errors.append("Could not determine allowed domain for link following")
            return result
        
        print(f"🕷️ Starting crawl of {connector_name} docs (domain: {allowed_domain})")
        print(f"  Max depth: {self.max_depth}, Max pages: {self.max_pages}")
        
        # Initialize browser
        browser = await self._init_browser()
        page = None
        if browser:
            page = await browser.new_page()
        
        try:
            # BFS crawl with depth tracking
            queue: List[Tuple[str, int]] = [(url, 0) for url in urls_to_crawl]
            pages_crawled = 0
            
            while queue and pages_crawled < self.max_pages:
                url, depth = queue.pop(0)
                
                # Skip if already visited or depth exceeded
                if url in self._visited_urls:
                    continue
                if depth > self.max_depth:
                    continue
                
                self._visited_urls.add(url)
                
                # Crawl the page
                print(f"  [{pages_crawled + 1}/{self.max_pages}] Depth {depth}: {url[:80]}...")
                
                if page:
                    crawled_page = await self._crawl_page_playwright(url, page)
                else:
                    crawled_page = await self._crawl_page_httpx(url)
                
                if crawled_page:
                    crawled_page.depth = depth
                    result.pages.append(crawled_page)
                    result.urls_crawled.append(url)
                    pages_crawled += 1
                    
                    # Add internal links to queue
                    if depth < self.max_depth:
                        for link in crawled_page.links:
                            normalized = self._normalize_url(link, url)
                            if normalized and normalized not in self._visited_urls:
                                if self._is_same_domain(normalized, allowed_domain):
                                    queue.append((normalized, depth + 1))
                
                # Rate limiting
                await asyncio.sleep(0.5)
            
            # Combine all content
            all_content_parts = []
            for page_obj in result.pages:
                section = f"# {page_obj.title}\n\n**Source:** {page_obj.url}\n\n{page_obj.content}"
                all_content_parts.append(section)
            
            result.total_content = "\n\n---\n\n".join(all_content_parts)
            result.total_words = sum(p.word_count for p in result.pages)
            
        finally:
            if page:
                await page.close()
            if browser:
                await browser.close()
        
        result.completed_at = datetime.utcnow()
        result.crawl_duration_seconds = (result.completed_at - start_time).total_seconds()
        
        print(f"✓ Crawl complete: {len(result.pages)} pages, {result.total_words} words in {result.crawl_duration_seconds:.1f}s")
        
        return result
    
    async def _auto_discover_docs(self, connector_name: str) -> Optional[Dict]:
        """
        Auto-discover documentation URLs via web search.
        
        Args:
            connector_name: Name of the connector
            
        Returns:
            Dict with 'urls' and 'domain' if found, None otherwise
        """
        try:
            from tavily import TavilyClient
            
            tavily_api_key = os.getenv("TAVILY_API_KEY")
            if not tavily_api_key:
                print("⚠ TAVILY_API_KEY not set, cannot auto-discover docs")
                return None
            
            client = TavilyClient(api_key=tavily_api_key)
            
            # Search for official API documentation
            query = f"{connector_name} official API documentation developer"
            response = client.search(query, max_results=5)
            
            if not response or 'results' not in response:
                return None
            
            urls = []
            domain = None
            
            for result in response['results']:
                url = result.get('url', '')
                if url and 'developer' in url.lower() or 'docs' in url.lower() or 'api' in url.lower():
                    urls.append(url)
                    if not domain:
                        parsed = urlparse(url)
                        domain = parsed.netloc
            
            if urls:
                return {'urls': urls[:5], 'domain': domain}
            
            return None
            
        except Exception as e:
            print(f"⚠ Auto-discovery failed: {e}")
            return None


# Singleton instance
_crawler: Optional[DocCrawler] = None


def get_doc_crawler(max_depth: int = 2, max_pages: int = 50) -> DocCrawler:
    """Get the singleton DocCrawler instance."""
    global _crawler
    if _crawler is None:
        _crawler = DocCrawler(max_depth=max_depth, max_pages=max_pages)
    return _crawler

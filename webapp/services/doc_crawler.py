"""
🕷️ Documentation Crawler Service
Crawls official API documentation for connectors with medium-depth link following.

Features:
- Prioritizes llms.txt (LLM-optimized content per llmstxt.org)
- Respects robots.txt with proper User-Agent
- Uses Playwright for JS-rendered pages
- Extracts clean text from HTML
- Follows internal links up to 2-3 levels deep
- Deduplicates content across pages
- Discovers URLs via sitemap.xml
"""

import os
import re
import asyncio
import hashlib
from typing import List, Dict, Set, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
import xml.etree.ElementTree as ET
import httpx

from dotenv import load_dotenv

from services.doc_registry import get_connector_docs, get_official_doc_urls, get_connector_domain

load_dotenv()


# Bot-style User-Agent with contact info (follows best practices)
USER_AGENT = "ConnectorResearchBot/1.0 (+https://github.com/Legolasan/connector_research)"

# HTTP headers for requests
DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,text/plain,application/xml",
    "Accept-Language": "en-US,en;q=0.9"
}


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
        self._robot_parsers: Dict[str, RobotFileParser] = {}  # Cache robots.txt parsers
        self._llms_txt_cache: Dict[str, Optional[str]] = {}  # Cache llms.txt content
        self._sitemap_urls: Dict[str, List[str]] = {}  # Cache sitemap URLs
        
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
    
    # =========================================================================
    # llms.txt Support (LLM-optimized content per llmstxt.org)
    # =========================================================================
    
    async def _fetch_llms_txt(self, domain: str) -> Optional[str]:
        """
        Fetch llms.txt if available (LLM-optimized content).
        
        Per https://llmstxt.org/, llms.txt provides content specifically
        optimized for LLM consumption. This is the preferred source.
        
        Args:
            domain: The domain to check (e.g., "shopify.dev")
            
        Returns:
            Content of llms.txt if exists, None otherwise
        """
        if domain in self._llms_txt_cache:
            return self._llms_txt_cache[domain]
        
        llms_url = f"https://{domain}/llms.txt"
        
        try:
            async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
                response = await client.get(llms_url, headers=DEFAULT_HEADERS)
                
                if response.status_code == 200:
                    content = response.text
                    # Verify it looks like llms.txt content (not an error page)
                    if len(content) > 100 and not content.strip().startswith('<!DOCTYPE'):
                        self._llms_txt_cache[domain] = content
                        print(f"  📄 Found llms.txt for {domain} ({len(content)} chars)")
                        return content
        except Exception as e:
            print(f"  ⚠ Could not fetch llms.txt for {domain}: {e}")
        
        self._llms_txt_cache[domain] = None
        return None
    
    async def _fetch_txt_variant(self, url: str) -> Optional[str]:
        """
        Try to fetch .txt variant of a URL (Shopify feature).
        
        Some sites (like Shopify) support appending .txt to any URL
        to get a plain text version optimized for LLM consumption.
        
        Args:
            url: The original URL
            
        Returns:
            Plain text content if .txt variant exists, None otherwise
        """
        # Don't try if URL already ends in common extensions
        if url.endswith(('.txt', '.json', '.xml', '.pdf', '.zip')):
            return None
        
        txt_url = url.rstrip('/') + '.txt'
        
        try:
            async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
                response = await client.get(txt_url, headers=DEFAULT_HEADERS)
                
                if response.status_code == 200:
                    content = response.text
                    # Verify it's actual text content, not HTML
                    if not content.strip().startswith('<!DOCTYPE') and not content.strip().startswith('<html'):
                        return content
        except Exception:
            pass
        
        return None
    
    # =========================================================================
    # robots.txt Compliance
    # =========================================================================
    
    async def _load_robots_txt(self, domain: str) -> RobotFileParser:
        """
        Load and parse robots.txt for a domain.
        
        Args:
            domain: The domain to load robots.txt for
            
        Returns:
            Configured RobotFileParser instance
        """
        if domain in self._robot_parsers:
            return self._robot_parsers[domain]
        
        rp = RobotFileParser()
        robots_url = f"https://{domain}/robots.txt"
        
        try:
            async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
                response = await client.get(robots_url, headers=DEFAULT_HEADERS)
                
                if response.status_code == 200:
                    # Parse the robots.txt content
                    rp.parse(response.text.splitlines())
                    
                    # Extract sitemap URL if present
                    for line in response.text.splitlines():
                        if line.lower().startswith('sitemap:'):
                            sitemap_url = line.split(':', 1)[1].strip()
                            if domain not in self._sitemap_urls:
                                self._sitemap_urls[domain] = []
                            self._sitemap_urls[domain].append(sitemap_url)
                    
                    print(f"  🤖 Loaded robots.txt for {domain}")
                else:
                    # No robots.txt = allow all
                    rp.allow_all = True
        except Exception as e:
            print(f"  ⚠ Could not load robots.txt for {domain}: {e}")
            rp.allow_all = True
        
        self._robot_parsers[domain] = rp
        return rp
    
    async def _can_fetch(self, url: str) -> bool:
        """
        Check if URL is allowed by robots.txt for our User-Agent.
        
        Args:
            url: The URL to check
            
        Returns:
            True if allowed to crawl, False otherwise
        """
        parsed = urlparse(url)
        domain = parsed.netloc
        
        rp = await self._load_robots_txt(domain)
        
        # Check if we can fetch this URL
        try:
            return rp.can_fetch(USER_AGENT, url)
        except Exception:
            # If there's any error, default to allowing
            return True
    
    # =========================================================================
    # Sitemap Discovery
    # =========================================================================
    
    async def _parse_sitemap(self, sitemap_url: str, domain: str, max_urls: int = 100) -> List[str]:
        """
        Extract URLs from sitemap.xml, filtering by robots.txt rules.
        
        Args:
            sitemap_url: URL of the sitemap
            domain: Domain for robots.txt checking
            max_urls: Maximum URLs to extract
            
        Returns:
            List of allowed URLs from the sitemap
        """
        urls = []
        
        try:
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                response = await client.get(sitemap_url, headers=DEFAULT_HEADERS)
                
                if response.status_code != 200:
                    return urls
                
                content = response.text
                
                # Parse XML
                try:
                    root = ET.fromstring(content)
                except ET.ParseError:
                    return urls
                
                # Handle namespace
                ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                
                # Check if this is a sitemap index (contains other sitemaps)
                sitemap_refs = root.findall('.//sm:sitemap/sm:loc', ns)
                if sitemap_refs:
                    # It's a sitemap index - recursively parse referenced sitemaps
                    for sitemap_ref in sitemap_refs[:5]:  # Limit to 5 sub-sitemaps
                        sub_urls = await self._parse_sitemap(sitemap_ref.text, domain, max_urls - len(urls))
                        urls.extend(sub_urls)
                        if len(urls) >= max_urls:
                            break
                else:
                    # It's a regular sitemap - extract URLs
                    url_elements = root.findall('.//sm:url/sm:loc', ns)
                    
                    # Also try without namespace (some sitemaps don't use it)
                    if not url_elements:
                        url_elements = root.findall('.//url/loc')
                    
                    for url_elem in url_elements:
                        if len(urls) >= max_urls:
                            break
                        
                        url = url_elem.text
                        if url and await self._can_fetch(url):
                            # Filter to documentation-related URLs
                            if any(kw in url.lower() for kw in ['doc', 'api', 'reference', 'guide', 'tutorial']):
                                urls.append(url)
                
                print(f"  📋 Sitemap provided {len(urls)} doc URLs for {domain}")
                
        except Exception as e:
            print(f"  ⚠ Could not parse sitemap {sitemap_url}: {e}")
        
        return urls
    
    async def _discover_urls_from_sitemap(self, domain: str) -> List[str]:
        """
        Discover documentation URLs from sitemap.xml.
        
        Args:
            domain: Domain to check
            
        Returns:
            List of discovered documentation URLs
        """
        # First ensure robots.txt is loaded (which extracts sitemap URLs)
        await self._load_robots_txt(domain)
        
        # Check for cached sitemap URLs
        if domain in self._sitemap_urls:
            all_urls = []
            for sitemap_url in self._sitemap_urls[domain]:
                urls = await self._parse_sitemap(sitemap_url, domain)
                all_urls.extend(urls)
            return all_urls[:self.max_pages]
        
        # Try default sitemap location
        default_sitemap = f"https://{domain}/sitemap.xml"
        return await self._parse_sitemap(default_sitemap, domain)
    
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
        """Crawl a page using httpx (fallback) with proper User-Agent."""
        try:
            # First try .txt variant (LLM-optimized content)
            txt_content = await self._fetch_txt_variant(url)
            if txt_content:
                return CrawledPage(
                    url=url + '.txt',
                    title=f"LLM-optimized: {url}",
                    content=txt_content,
                    links=[]  # .txt files don't have links to follow
                )
            
            # Fall back to HTML version
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                response = await client.get(url, headers=DEFAULT_HEADERS)
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
        Crawl official documentation for a connector with ethical compliance.
        
        Strategy:
        1. Check for llms.txt (LLM-optimized content) - PRIORITY
        2. Check registry for known URLs
        3. Add user-provided URLs
        4. Discover URLs from sitemap.xml
        5. Auto-discover via web search if needed
        6. Crawl each URL with robots.txt compliance
        7. Return combined content for indexing
        
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
        
        # 1. Check registry for domain
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
        
        print(f"🕷️ Starting ethical crawl of {connector_name} docs")
        print(f"  📍 Domain: {allowed_domain}")
        print(f"  🤖 User-Agent: {USER_AGENT}")
        print(f"  🔢 Max depth: {self.max_depth}, Max pages: {self.max_pages}")
        
        # =================================================================
        # PRIORITY: Check for llms.txt (LLM-optimized content)
        # =================================================================
        llms_content = await self._fetch_llms_txt(allowed_domain)
        if llms_content:
            print(f"  ✅ Using llms.txt content (optimized for LLM consumption)")
            result.pages.append(CrawledPage(
                url=f"https://{allowed_domain}/llms.txt",
                title=f"{connector_name} LLM-Optimized Documentation",
                content=llms_content,
                links=[],
                depth=0
            ))
            result.urls_crawled.append(f"https://{allowed_domain}/llms.txt")
            
            # llms.txt often contains everything we need - check word count
            if len(llms_content.split()) > 1000:
                # Substantial content, can potentially skip further crawling
                result.total_content = f"# {connector_name} Official Documentation (llms.txt)\n\n{llms_content}"
                result.total_words = len(llms_content.split())
                result.completed_at = datetime.utcnow()
                result.crawl_duration_seconds = (result.completed_at - start_time).total_seconds()
                print(f"  ✓ llms.txt provided sufficient content ({result.total_words} words)")
                return result
        
        # =================================================================
        # Load robots.txt and discover sitemap URLs
        # =================================================================
        await self._load_robots_txt(allowed_domain)
        
        # Try to get additional URLs from sitemap
        sitemap_urls = await self._discover_urls_from_sitemap(allowed_domain)
        if sitemap_urls:
            for url in sitemap_urls:
                if url not in urls_to_crawl:
                    urls_to_crawl.append(url)
            print(f"  📋 Added {len(sitemap_urls)} URLs from sitemap")
        
        # Initialize browser
        browser = await self._init_browser()
        page = None
        if browser:
            page = await browser.new_page()
            # Set User-Agent in Playwright
            await page.set_extra_http_headers(DEFAULT_HEADERS)
        
        try:
            # BFS crawl with depth tracking
            queue: List[Tuple[str, int]] = [(url, 0) for url in urls_to_crawl]
            pages_crawled = len(result.pages)  # Account for llms.txt if we got partial content
            
            while queue and pages_crawled < self.max_pages:
                url, depth = queue.pop(0)
                
                # Skip if already visited or depth exceeded
                if url in self._visited_urls:
                    continue
                if depth > self.max_depth:
                    continue
                
                # =================================================================
                # robots.txt compliance check
                # =================================================================
                if not await self._can_fetch(url):
                    print(f"  🚫 Blocked by robots.txt: {url[:60]}...")
                    continue
                
                self._visited_urls.add(url)
                
                # Crawl the page
                print(f"  [{pages_crawled + 1}/{self.max_pages}] Depth {depth}: {url[:70]}...")
                
                if page:
                    crawled_page = await self._crawl_page_playwright(url, page)
                else:
                    crawled_page = await self._crawl_page_httpx(url)
                
                if crawled_page:
                    crawled_page.depth = depth
                    result.pages.append(crawled_page)
                    result.urls_crawled.append(url)
                    pages_crawled += 1
                    
                    # Add internal links to queue (only if robots.txt allows)
                    if depth < self.max_depth:
                        for link in crawled_page.links:
                            normalized = self._normalize_url(link, url)
                            if normalized and normalized not in self._visited_urls:
                                if self._is_same_domain(normalized, allowed_domain):
                                    queue.append((normalized, depth + 1))
                
                # Rate limiting (be polite to servers)
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

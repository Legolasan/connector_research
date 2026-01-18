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
import fnmatch
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
    Documentation crawler with two-gate URL filtering system.
    
    Gate 1 (Hard): Pattern-based allow/deny - URLs must pass ALL checks
    Gate 2 (Soft): Keyword scoring for ranking passed URLs
    
    Crawls official API documentation and follows internal links
    up to 2-3 levels deep to gather comprehensive context.
    """
    
    # Gate 2: Content type keywords for scoring (soft ranking, not filtering)
    CONTENT_TYPE_KEYWORDS = {
        "api_reference": {
            "path_keywords": ["/api/", "/reference/", "/rest/", "/graphql/", "/endpoint"],
            "title_keywords": ["api", "reference", "endpoint", "resource", "schema"],
            "weight": 1.0
        },
        "authentication": {
            "path_keywords": ["/auth", "/oauth", "/token", "/scope", "/credential"],
            "title_keywords": ["authentication", "authorization", "oauth", "token"],
            "weight": 0.9
        },
        "rate_limits": {
            "path_keywords": ["/rate-limit", "/limit", "/throttle", "/quota"],
            "title_keywords": ["rate limit", "throttle", "quota"],
            "weight": 0.85
        },
        "webhooks": {
            "path_keywords": ["/webhook", "/event", "/callback", "/notification"],
            "title_keywords": ["webhook", "event", "notification"],
            "weight": 0.8
        },
        "sdk": {
            "path_keywords": ["/sdk", "/library", "/client", "/java", "/python", "/node"],
            "title_keywords": ["sdk", "client library", "java", "python"],
            "weight": 0.7
        },
        "changelog": {
            "path_keywords": ["/changelog", "/release", "/version", "/migration", "/whats-new"],
            "title_keywords": ["changelog", "release notes", "what's new"],
            "weight": 0.6
        }
    }
    
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
        self._sitemap_cache: Dict[str, List[Tuple[str, Optional[float]]]] = {}  # Cache parsed sitemaps
    
    # =========================================================================
    # URL Normalization (Before Any Checks)
    # =========================================================================
    
    def _normalize_url_strict(self, url: str) -> str:
        """
        Normalize URL before pattern matching and deduplication.
        
        1. Parse URL
        2. Lowercase host
        3. Strip fragment (#...)
        4. Collapse // in path
        5. Normalize trailing slash (remove for docs)
        6. Strip query params (docs sites don't need them)
        7. Rebuild URL
        """
        parsed = urlparse(url)
        
        # Lowercase host
        host = parsed.netloc.lower()
        
        # Clean path: collapse //, remove trailing slash
        path = re.sub(r'/+', '/', parsed.path).rstrip('/')
        if not path:
            path = '/'
        
        # Rebuild without fragment and query
        return f"{parsed.scheme}://{host}{path}"
    
    # =========================================================================
    # Gate 1: Hard Pattern Matching (Primary Filter)
    # =========================================================================
    
    def _passes_gate1(self, url: str, config) -> Tuple[bool, str]:
        """
        Gate 1: Hard allow/deny checks.
        Returns (passed, reason) tuple.
        
        Critical Rule: If url_patterns exists, reject anything outside it.
        """
        parsed = urlparse(url)
        path = parsed.path
        
        # 1. Domain must match
        if config.domain and parsed.netloc.lower() != config.domain.lower():
            return False, f"Domain mismatch: {parsed.netloc} != {config.domain}"
        
        # 2. If include_patterns exist, URL MUST match at least one
        if hasattr(config, 'url_patterns') and config.url_patterns:
            if not self._matches_any_pattern(path, config.url_patterns):
                return False, f"Path '{path}' doesn't match any include pattern"
        
        # 3. URL must NOT match any exclude pattern
        if hasattr(config, 'exclude_patterns') and config.exclude_patterns:
            if self._matches_any_pattern(path, config.exclude_patterns):
                return False, f"Path '{path}' matches exclude pattern"
        
        # 4. Must be HTML-like (reject PDFs, images, etc.)
        if not self._is_html_url(path):
            return False, f"Non-HTML content: {path}"
        
        return True, "Passed all checks"
    
    def _matches_any_pattern(self, path: str, patterns: List[str]) -> bool:
        """Check if path matches any glob-style pattern."""
        for pattern in patterns:
            if fnmatch.fnmatch(path, pattern):
                return True
        return False
    
    def _is_html_url(self, path: str) -> bool:
        """Check if URL likely points to HTML content."""
        # Reject known non-HTML extensions
        non_html = {'.pdf', '.zip', '.png', '.jpg', '.jpeg', '.gif', '.svg', 
                    '.json', '.xml', '.css', '.js', '.ico', '.woff', '.woff2', '.ttf'}
        ext = os.path.splitext(path)[1].lower()
        return ext not in non_html
    
    # =========================================================================
    # Gate 2: Soft Keyword Scoring (For Ranking Only)
    # =========================================================================
    
    def _score_url(self, url: str, sitemap_priority: Optional[float] = None) -> float:
        """
        Gate 2: Score URL relevance for ranking.
        Only called AFTER Gate 1 passes.
        Keywords only affect order, never admission.
        """
        path = urlparse(url).path.lower()
        score = 0.0
        
        # Score by content type keywords
        for content_type, config in self.CONTENT_TYPE_KEYWORDS.items():
            for kw in config["path_keywords"]:
                if kw in path:
                    score += config["weight"]
                    break  # Only count once per type
        
        # Boost from sitemap priority if available
        if sitemap_priority is not None:
            score += sitemap_priority * 0.5
        
        # Penalize very deep paths (likely less important)
        depth = path.count('/') - 1
        if depth > 5:
            score -= 0.2 * (depth - 5)
        
        return score
        
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
    
    async def _parse_sitemap_with_priority(
        self, 
        sitemap_url: str, 
        max_sitemaps: int = 10,
        _depth: int = 0
    ) -> List[Tuple[str, Optional[float]]]:
        """
        Parse sitemap, handling both index and urlset formats.
        Returns normalized URLs with optional priority.
        
        NO FILTERING HERE - Gate 1 handles filtering.
        
        Args:
            sitemap_url: URL of the sitemap
            max_sitemaps: Maximum sitemaps to parse (prevents infinite loops)
            _depth: Current recursion depth
            
        Returns:
            List of (url, priority) tuples
        """
        # Depth limit to prevent infinite recursion
        if _depth >= max_sitemaps:
            print(f"  ⚠ Max sitemap depth ({max_sitemaps}) reached at {sitemap_url}")
            return []
        
        # Check cache
        cache_key = sitemap_url
        if cache_key in self._sitemap_cache:
            return self._sitemap_cache[cache_key]
        
        urls: List[Tuple[str, Optional[float]]] = []
        
        try:
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                response = await client.get(sitemap_url, headers=DEFAULT_HEADERS)
                
                if response.status_code != 200:
                    print(f"  ⚠ Sitemap returned {response.status_code}: {sitemap_url}")
                    return urls
                
                content = response.text
                
                # Parse XML
                try:
                    root = ET.fromstring(content)
                except ET.ParseError as e:
                    print(f"  ⚠ Sitemap XML parse error: {e}")
                    return urls
                
                # Handle namespace
                ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                
                # Check if this is a SITEMAP INDEX (<sitemapindex>)
                sitemap_refs = root.findall('.//sm:sitemap', ns)
                if not sitemap_refs:
                    sitemap_refs = root.findall('.//sitemap')  # Try without namespace
                
                if sitemap_refs:
                    # It's an INDEX - recursively parse referenced sitemaps
                    print(f"  📂 Found sitemap index with {len(sitemap_refs)} sitemaps")
                    for ref in sitemap_refs[:max_sitemaps]:
                        loc = ref.find('sm:loc', ns)
                        if loc is None:
                            loc = ref.find('loc')
                        if loc is not None and loc.text:
                            sub_urls = await self._parse_sitemap_with_priority(
                                loc.text, max_sitemaps, _depth + 1
                            )
                            urls.extend(sub_urls)
                else:
                    # It's a URLSET - extract URLs with priority
                    url_elements = root.findall('.//sm:url', ns)
                    if not url_elements:
                        url_elements = root.findall('.//url')  # Try without namespace
                    
                    for url_elem in url_elements:
                        loc = url_elem.find('sm:loc', ns)
                        if loc is None:
                            loc = url_elem.find('loc')
                        
                        priority_elem = url_elem.find('sm:priority', ns)
                        if priority_elem is None:
                            priority_elem = url_elem.find('priority')
                        
                        if loc is not None and loc.text:
                            normalized = self._normalize_url_strict(loc.text)
                            priority = None
                            if priority_elem is not None and priority_elem.text:
                                try:
                                    priority = float(priority_elem.text)
                                except ValueError:
                                    pass
                            urls.append((normalized, priority))
            
            # Cache results
            self._sitemap_cache[cache_key] = urls
            
            if urls:
                print(f"  📋 Sitemap parsed: {len(urls)} URLs from {sitemap_url}")
                
        except Exception as e:
            print(f"  ⚠ Sitemap error for {sitemap_url}: {e}")
        
        return urls
    
    async def _get_sitemap_urls(self, domain: str) -> List[Tuple[str, Optional[float]]]:
        """
        Get all URLs from sitemap(s) for a domain.
        
        Args:
            domain: Domain to check
            
        Returns:
            List of (url, priority) tuples from sitemap
        """
        # First ensure robots.txt is loaded (which extracts sitemap URLs)
        await self._load_robots_txt(domain)
        
        all_urls: List[Tuple[str, Optional[float]]] = []
        
        # Check for sitemap URLs from robots.txt
        if domain in self._sitemap_urls:
            for sitemap_url in self._sitemap_urls[domain]:
                urls = await self._parse_sitemap_with_priority(sitemap_url)
                all_urls.extend(urls)
        
        # Also try default sitemap location if not found via robots.txt
        if not all_urls:
            default_sitemap = f"https://{domain}/sitemap.xml"
            all_urls = await self._parse_sitemap_with_priority(default_sitemap)
        
        # Deduplicate while preserving order and keeping highest priority
        seen: Dict[str, Optional[float]] = {}
        for url, priority in all_urls:
            if url not in seen or (priority is not None and (seen[url] is None or priority > seen[url])):
                seen[url] = priority
        
        return [(url, priority) for url, priority in seen.items()]
    
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
        Crawl official documentation using the two-gate URL filtering system.
        
        Strategy:
        1. Check for llms.txt (LLM-optimized content) - PRIORITY
        2. Get connector config from registry (patterns, domain)
        3. Gather URLs from sitemap + registry + user-provided
        4. Gate 1: Hard filter (domain, patterns, robots.txt, HTML check)
        5. Gate 2: Score and rank URLs by keyword relevance
        6. Crawl top N URLs
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
        
        # Reset state for new crawl
        self._visited_urls.clear()
        self._content_hashes.clear()
        self._sitemap_cache.clear()
        
        # Get connector config from registry
        config = get_connector_docs(connector_name)
        allowed_domain: Optional[str] = None
        
        if config:
            allowed_domain = config.domain
            has_patterns = bool(config.url_patterns)
            print(f"📚 Loaded config for {connector_name}")
            print(f"  📍 Domain: {allowed_domain}")
            if has_patterns:
                print(f"  🎯 URL patterns: {len(config.url_patterns)} include, {len(config.exclude_patterns)} exclude")
        else:
            # Create a minimal config for user-provided URLs
            if user_provided_urls:
                parsed = urlparse(user_provided_urls[0])
                allowed_domain = parsed.netloc
            print(f"⚠ No registry config for {connector_name}, using fallback")
        
        if not allowed_domain and not user_provided_urls:
            result.errors.append(f"No documentation URLs found for {connector_name}")
            return result
        
        if not allowed_domain:
            parsed = urlparse(user_provided_urls[0])
            allowed_domain = parsed.netloc
        
        print(f"🕷️ Starting two-gate crawl of {connector_name} docs")
        print(f"  🤖 User-Agent: {USER_AGENT}")
        print(f"  🔢 Max depth: {self.max_depth}, Max pages: {self.max_pages}")
        
        # =================================================================
        # PRIORITY: Check for llms.txt (LLM-optimized content)
        # =================================================================
        llms_content = await self._fetch_llms_txt(allowed_domain)
        if llms_content:
            print(f"  ✅ Found llms.txt (optimized for LLM consumption)")
            result.pages.append(CrawledPage(
                url=f"https://{allowed_domain}/llms.txt",
                title=f"{connector_name} LLM-Optimized Documentation",
                content=llms_content,
                links=[],
                depth=0
            ))
            result.urls_crawled.append(f"https://{allowed_domain}/llms.txt")
            
            # llms.txt often contains everything we need
            if len(llms_content.split()) > 1000:
                result.total_content = f"# {connector_name} Official Documentation (llms.txt)\n\n{llms_content}"
                result.total_words = len(llms_content.split())
                result.completed_at = datetime.utcnow()
                result.crawl_duration_seconds = (result.completed_at - start_time).total_seconds()
                print(f"  ✓ llms.txt provided sufficient content ({result.total_words} words)")
                return result
        
        # =================================================================
        # Gather candidate URLs from all sources
        # =================================================================
        candidate_urls: List[Tuple[str, Optional[float]]] = []
        
        # 1. Get URLs from sitemap (with priorities)
        sitemap_urls = await self._get_sitemap_urls(allowed_domain)
        candidate_urls.extend(sitemap_urls)
        print(f"  📋 Sitemap provided {len(sitemap_urls)} candidate URLs")
        
        # 2. Add registry URLs (high priority)
        if config and config.official_docs:
            for url in config.official_docs:
                normalized = self._normalize_url_strict(url)
                if not any(u == normalized for u, _ in candidate_urls):
                    candidate_urls.append((normalized, 1.0))  # High priority
        
        # 3. Add user-provided URLs (highest priority)
        if user_provided_urls:
            for url in user_provided_urls:
                normalized = self._normalize_url_strict(url.strip())
                if normalized and not any(u == normalized for u, _ in candidate_urls):
                    candidate_urls.append((normalized, 1.0))  # High priority
            print(f"  📝 Added {len(user_provided_urls)} user-provided URLs")
        
        print(f"  📊 Total candidates before filtering: {len(candidate_urls)}")
        
        # =================================================================
        # Gate 1: Hard Filter (domain, patterns, robots.txt, HTML)
        # =================================================================
        passed_gate1: List[Tuple[str, Optional[float]]] = []
        rejected_gate1 = 0
        rejection_reasons: Dict[str, int] = {}
        
        for url, priority in candidate_urls:
            # robots.txt check
            if not await self._can_fetch(url):
                rejected_gate1 += 1
                rejection_reasons["robots.txt blocked"] = rejection_reasons.get("robots.txt blocked", 0) + 1
                continue
            
            # Pattern-based checks (if config exists)
            if config:
                passed, reason = self._passes_gate1(url, config)
                if not passed:
                    rejected_gate1 += 1
                    rejection_reasons[reason.split(':')[0]] = rejection_reasons.get(reason.split(':')[0], 0) + 1
                    continue
            
            passed_gate1.append((url, priority))
        
        print(f"  🚪 Gate 1: {len(passed_gate1)}/{len(candidate_urls)} URLs passed")
        if rejection_reasons:
            for reason, count in sorted(rejection_reasons.items(), key=lambda x: -x[1])[:3]:
                print(f"     ❌ {count} rejected: {reason}")
        
        if not passed_gate1:
            result.errors.append("No URLs passed Gate 1 filtering")
            return result
        
        # =================================================================
        # Gate 2: Score and Rank URLs
        # =================================================================
        scored_urls: List[Tuple[str, float]] = []
        for url, priority in passed_gate1:
            score = self._score_url(url, priority)
            scored_urls.append((url, score))
        
        # Sort by score descending
        scored_urls.sort(key=lambda x: x[1], reverse=True)
        
        # Take top N URLs
        urls_to_crawl = [url for url, _ in scored_urls[:self.max_pages * 2]]  # Get more for BFS
        
        print(f"  🏆 Gate 2: Top {min(len(urls_to_crawl), 5)} URLs by score:")
        for url, score in scored_urls[:5]:
            print(f"     [{score:.2f}] {url[:60]}...")
        
        # =================================================================
        # Crawl URLs with depth tracking
        # =================================================================
        browser = await self._init_browser()
        page = None
        if browser:
            page = await browser.new_page()
            await page.set_extra_http_headers(DEFAULT_HEADERS)
        
        try:
            queue: List[Tuple[str, int]] = [(url, 0) for url in urls_to_crawl]
            pages_crawled = len(result.pages)  # Account for llms.txt
            
            while queue and pages_crawled < self.max_pages:
                url, depth = queue.pop(0)
                
                # Skip if already visited or depth exceeded
                if url in self._visited_urls:
                    continue
                if depth > self.max_depth:
                    continue
                
                self._visited_urls.add(url)
                
                # Crawl the page
                print(f"  ✓ [{pages_crawled + 1}/{self.max_pages}] {url[:70]}...")
                
                if page:
                    crawled_page = await self._crawl_page_playwright(url, page)
                else:
                    crawled_page = await self._crawl_page_httpx(url)
                
                if crawled_page:
                    crawled_page.depth = depth
                    result.pages.append(crawled_page)
                    result.urls_crawled.append(url)
                    pages_crawled += 1
                    
                    # Add internal links to queue (must pass Gate 1)
                    if depth < self.max_depth:
                        for link in crawled_page.links:
                            normalized = self._normalize_url(link, url)
                            if normalized and normalized not in self._visited_urls:
                                # Quick Gate 1 check for discovered links
                                if config:
                                    passed, _ = self._passes_gate1(normalized, config)
                                    if passed:
                                        queue.append((normalized, depth + 1))
                                elif self._is_same_domain(normalized, allowed_domain):
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

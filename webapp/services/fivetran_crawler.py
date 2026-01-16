"""
Fivetran Crawler Service
Crawls and parses Fivetran documentation pages for parity comparison.
Supports headless browser for JS-rendered pages and manual CSV/PDF input.
"""

import re
import os
import csv
import io
import httpx
import asyncio
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

# Try to import optional dependencies
try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    print("⚠ Playwright not available. Install with: pip install playwright && playwright install chromium")

try:
    from pypdf import PdfReader
    PYPDF_AVAILABLE = True
except ImportError:
    PYPDF_AVAILABLE = False
    print("⚠ PyPDF not available. Install with: pip install pypdf")


@dataclass
class FivetranSetupContext:
    """Extracted from Setup Guide page."""
    prerequisites: List[str] = field(default_factory=list)
    auth_methods: List[str] = field(default_factory=list)
    auth_instructions: str = ""
    raw_content: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'prerequisites': self.prerequisites[:20],
            'auth_methods': self.auth_methods[:10],
            'auth_instructions': self.auth_instructions[:3000],
            'raw_content': self.raw_content[:5000]
        }


@dataclass
class FivetranOverviewContext:
    """Extracted from Connector Overview page."""
    supported_features: Dict[str, bool] = field(default_factory=dict)  # capture_deletes, history_mode, etc.
    sync_overview: str = ""
    sync_limitations: List[str] = field(default_factory=list)
    historical_sync_timeframe: str = ""
    incremental_sync_details: str = ""
    raw_content: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'supported_features': self.supported_features,
            'sync_overview': self.sync_overview[:2000],
            'sync_limitations': self.sync_limitations[:20],
            'historical_sync_timeframe': self.historical_sync_timeframe[:500],
            'incremental_sync_details': self.incremental_sync_details[:1500],
            'raw_content': self.raw_content[:5000]
        }


@dataclass
class FivetranSchemaObject:
    """Represents a single object/table from schema info."""
    name: str
    sync_mode: str = ""  # incremental, full_load
    parent: Optional[str] = None
    permissions: List[str] = field(default_factory=list)
    description: str = ""
    cursor_field: str = ""  # Field used for incremental sync (e.g., updated_at)
    primary_key: str = ""  # Primary key field
    is_supported: bool = True  # Whether Fivetran supports this object
    delete_method: str = ""  # How deletes are captured: Soft Delete, Deleted Endpoint, Webhook, Audit Log, None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'sync_mode': self.sync_mode,
            'parent': self.parent,
            'permissions': self.permissions,
            'description': self.description[:500],
            'cursor_field': self.cursor_field,
            'primary_key': self.primary_key,
            'is_supported': self.is_supported,
            'delete_method': self.delete_method
        }


@dataclass
class FivetranSchemaContext:
    """Extracted from Schema Information page."""
    objects: List[FivetranSchemaObject] = field(default_factory=list)
    parent_child_relationships: List[Tuple[str, str]] = field(default_factory=list)
    supported_objects: List[str] = field(default_factory=list)
    unsupported_objects: List[str] = field(default_factory=list)
    object_limitations: Dict[str, str] = field(default_factory=dict)
    permissions_required: Dict[str, List[str]] = field(default_factory=dict)
    raw_content: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'objects': [obj.to_dict() for obj in self.objects[:100]],
            'parent_child_relationships': self.parent_child_relationships[:50],
            'supported_objects': self.supported_objects[:100],
            'unsupported_objects': self.unsupported_objects[:50],
            'object_limitations': dict(list(self.object_limitations.items())[:30]),
            'permissions_required': dict(list(self.permissions_required.items())[:30]),
            'raw_content': self.raw_content[:5000]
        }


@dataclass
class FivetranContext:
    """Complete Fivetran context for parity comparison."""
    setup: Optional[FivetranSetupContext] = None
    overview: Optional[FivetranOverviewContext] = None
    schema: Optional[FivetranSchemaContext] = None
    crawled_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    
    def to_dict(self) -> Dict[str, Any]:
        result = {
            'crawled_at': self.crawled_at,
            'has_setup': self.setup is not None,
            'has_overview': self.overview is not None,
            'has_schema': self.schema is not None
        }
        
        if self.setup:
            result['setup'] = self.setup.to_dict()
        if self.overview:
            result['overview'] = self.overview.to_dict()
        if self.schema:
            result['schema'] = self.schema.to_dict()
        
        return result
    
    def get_summary(self) -> str:
        """Get a text summary of Fivetran context for prompts."""
        parts = []
        
        if self.setup:
            parts.append("**Fivetran Setup Guide:**")
            if self.setup.auth_methods:
                parts.append(f"- Auth Methods: {', '.join(self.setup.auth_methods)}")
            if self.setup.prerequisites:
                parts.append(f"- Prerequisites: {'; '.join(self.setup.prerequisites[:5])}")
            if self.setup.auth_instructions:
                parts.append(f"- Auth Instructions:\n{self.setup.auth_instructions[:1500]}")
        
        if self.overview:
            parts.append("\n**Fivetran Connector Overview:**")
            if self.overview.supported_features:
                features = [f"{k}: {v}" for k, v in self.overview.supported_features.items()]
                parts.append(f"- Features: {', '.join(features)}")
            if self.overview.sync_overview:
                parts.append(f"- Sync Overview: {self.overview.sync_overview[:800]}")
            if self.overview.sync_limitations:
                parts.append(f"- Limitations: {'; '.join(self.overview.sync_limitations[:5])}")
        
        if self.schema:
            parts.append("\n**Fivetran Schema Information:**")
            if self.schema.supported_objects:
                parts.append(f"- Supported Objects ({len(self.schema.supported_objects)}): {', '.join(self.schema.supported_objects[:30])}")
            if self.schema.parent_child_relationships:
                rels = [f"{p}->{c}" for p, c in self.schema.parent_child_relationships[:10]]
                parts.append(f"- Parent-Child Relationships: {', '.join(rels)}")
            if self.schema.unsupported_objects:
                parts.append(f"- Unsupported Objects: {', '.join(self.schema.unsupported_objects[:10])}")
        
        return "\n".join(parts)


class FivetranCrawler:
    """Crawls and parses Fivetran documentation pages.
    
    Supports multiple crawling methods:
    - Simple HTTP (fast, but doesn't work for JS-rendered pages)
    - Headless browser via Playwright (slower, but handles JS-rendered content)
    - Manual input via CSV or PDF files
    """
    
    # Feature keywords to look for in overview
    FEATURE_KEYWORDS = [
        'capture deletes', 'history mode', 'custom data', 're-sync',
        'column hashing', 'api configurable', 'priority-first sync',
        'fivetran data models'
    ]
    
    # Auth method keywords
    AUTH_KEYWORDS = [
        'oauth', 'api key', 'api token', 'username', 'password',
        'service account', 'client id', 'client secret', 'bearer token',
        'basic auth', 'certificate', 'ssh', 'jwt'
    ]
    
    def __init__(self, use_headless: bool = True):
        """Initialize the crawler.
        
        Args:
            use_headless: Whether to use headless browser for JS-rendered pages
        """
        self.timeout = 30.0
        self.use_headless = use_headless and PLAYWRIGHT_AVAILABLE
        self._browser = None
        self._playwright = None
    
    async def _init_browser(self):
        """Initialize the headless browser if not already initialized."""
        if not PLAYWRIGHT_AVAILABLE:
            return False
        
        if self._playwright is None:
            try:
                self._playwright = await async_playwright().start()
                self._browser = await self._playwright.chromium.launch(headless=True)
                print("✓ Headless browser initialized")
                return True
            except Exception as e:
                print(f"⚠ Could not initialize headless browser: {e}")
                self._playwright = None
                self._browser = None
                return False
        return True
    
    async def _close_browser(self):
        """Close the headless browser."""
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None
    
    async def crawl_page_headless(self, url: str, wait_selector: str = None) -> str:
        """Crawl a page using headless browser for JS-rendered content.
        
        Args:
            url: URL to crawl
            wait_selector: Optional CSS selector to wait for before extracting content
            
        Returns:
            Extracted text content from the page
        """
        if not url:
            return ""
        
        if not await self._init_browser():
            print("  - Falling back to simple HTTP crawling")
            return await self.crawl_page_simple(url)
        
        try:
            page = await self._browser.new_page()
            await page.goto(url, wait_until='networkidle', timeout=60000)
            
            # Wait for content to load
            if wait_selector:
                try:
                    await page.wait_for_selector(wait_selector, timeout=10000)
                except:
                    pass  # Continue even if selector not found
            
            # Wait a bit more for any lazy-loaded content
            await asyncio.sleep(2)
            
            # Get the full page content
            html_content = await page.content()
            await page.close()
            
            return self._html_to_text(html_content)
            
        except Exception as e:
            print(f"Error crawling {url} with headless browser: {e}")
            # Fallback to simple HTTP
            return await self.crawl_page_simple(url)
    
    async def crawl_page_simple(self, url: str) -> str:
        """Crawl a page using simple HTTP request (no JS execution).
        
        Args:
            url: URL to crawl
            
        Returns:
            Extracted text content from the page
        """
        if not url:
            return ""
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
                response = await client.get(url, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                })
                response.raise_for_status()
                
                html_content = response.text
                return self._html_to_text(html_content)
                
        except Exception as e:
            print(f"Error crawling {url}: {e}")
            return ""
    
    async def crawl_page(self, url: str) -> str:
        """Crawl a single page and return its text content.
        
        Automatically chooses between headless browser and simple HTTP.
        Uses headless browser for Fivetran docs (which are JS-rendered).
        
        Args:
            url: URL to crawl
            
        Returns:
            Extracted text content from the page
        """
        if not url:
            return ""
        
        # Use headless browser for Fivetran docs (they're JS-rendered)
        if self.use_headless and 'fivetran.com' in url:
            print(f"  - Using headless browser for JS-rendered page")
            return await self.crawl_page_headless(url, wait_selector='article, .content, main')
        else:
            return await self.crawl_page_simple(url)
    
    def parse_csv_objects(self, csv_content: str) -> FivetranSchemaContext:
        """Parse objects from CSV content.
        
        Expected CSV format:
        object_name,sync_mode,parent,primary_key,cursor_field,permissions,delete_method
        
        Args:
            csv_content: CSV content as string
            
        Returns:
            FivetranSchemaContext with parsed objects
        """
        context = FivetranSchemaContext()
        
        try:
            reader = csv.DictReader(io.StringIO(csv_content))
            
            for row in reader:
                name = row.get('object_name', row.get('name', row.get('table', ''))).strip()
                if not name:
                    continue
                
                obj = FivetranSchemaObject(
                    name=name.lower(),
                    sync_mode=row.get('sync_mode', 'incremental').lower(),
                    parent=row.get('parent', '').strip() or None,
                    primary_key=row.get('primary_key', 'id').strip(),
                    cursor_field=row.get('cursor_field', 'updated_at').strip(),
                    delete_method=row.get('delete_method', 'None').strip(),
                    is_supported=True
                )
                
                # Parse permissions (comma-separated)
                perms = row.get('permissions', '').strip()
                if perms:
                    obj.permissions = [p.strip() for p in perms.split(',')]
                
                context.objects.append(obj)
                context.supported_objects.append(obj.name)
                
                # Track parent-child relationships
                if obj.parent:
                    context.parent_child_relationships.append((obj.parent.lower(), obj.name))
            
            print(f"✓ Parsed {len(context.objects)} objects from CSV")
            
        except Exception as e:
            print(f"Error parsing CSV: {e}")
        
        return context
    
    def parse_pdf_objects(self, pdf_path: str) -> FivetranSchemaContext:
        """Parse objects from a PDF file.
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            FivetranSchemaContext with parsed objects
        """
        context = FivetranSchemaContext()
        
        if not PYPDF_AVAILABLE:
            print("⚠ PyPDF not available. Cannot parse PDF files.")
            return context
        
        try:
            reader = PdfReader(pdf_path)
            
            # Extract text from all pages
            full_text = ""
            for page in reader.pages:
                full_text += page.extract_text() + "\n"
            
            # Use the existing schema parser
            context = self.parse_schema_info(full_text)
            context.raw_content = full_text[:5000]
            
            print(f"✓ Parsed {len(context.objects)} objects from PDF")
            
        except Exception as e:
            print(f"Error parsing PDF: {e}")
        
        return context
    
    def parse_pdf_bytes(self, pdf_bytes: bytes) -> FivetranSchemaContext:
        """Parse objects from PDF bytes (for file uploads).
        
        Args:
            pdf_bytes: PDF file content as bytes
            
        Returns:
            FivetranSchemaContext with parsed objects
        """
        context = FivetranSchemaContext()
        
        if not PYPDF_AVAILABLE:
            print("⚠ PyPDF not available. Cannot parse PDF files.")
            return context
        
        try:
            reader = PdfReader(io.BytesIO(pdf_bytes))
            
            # Extract text from all pages
            full_text = ""
            for page in reader.pages:
                full_text += page.extract_text() + "\n"
            
            # Use the existing schema parser
            context = self.parse_schema_info(full_text)
            context.raw_content = full_text[:5000]
            
            print(f"✓ Parsed {len(context.objects)} objects from PDF")
            
        except Exception as e:
            print(f"Error parsing PDF: {e}")
        
        return context
    
    def parse_text_objects(self, text_content: str) -> FivetranSchemaContext:
        """Parse objects from plain text (one object per line or comma-separated).
        
        Args:
            text_content: Plain text with object names
            
        Returns:
            FivetranSchemaContext with parsed objects
        """
        context = FivetranSchemaContext()
        
        # Split by newlines or commas
        lines = text_content.replace(',', '\n').split('\n')
        
        for line in lines:
            name = line.strip().lower()
            if not name or len(name) < 2:
                continue
            
            # Skip common non-object words
            if name in {'name', 'table', 'object', 'entity', 'column', 'field'}:
                continue
            
            obj = FivetranSchemaObject(
                name=name,
                sync_mode='incremental',
                primary_key='id',
                cursor_field='updated_at',
                delete_method='None',
                is_supported=True
            )
            
            context.objects.append(obj)
            context.supported_objects.append(name)
        
        print(f"✓ Parsed {len(context.objects)} objects from text")
        
        return context
    
    def _html_to_text(self, html_content: str) -> str:
        """Convert HTML to plain text.
        
        Args:
            html_content: Raw HTML content
            
        Returns:
            Extracted text content
        """
        # Remove script and style elements
        html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        html_content = re.sub(r'<nav[^>]*>.*?</nav>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        html_content = re.sub(r'<footer[^>]*>.*?</footer>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        html_content = re.sub(r'<header[^>]*>.*?</header>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        
        # Convert common HTML elements
        html_content = re.sub(r'<br\s*/?>', '\n', html_content, flags=re.IGNORECASE)
        html_content = re.sub(r'<p[^>]*>', '\n\n', html_content, flags=re.IGNORECASE)
        html_content = re.sub(r'</p>', '', html_content, flags=re.IGNORECASE)
        html_content = re.sub(r'<li[^>]*>', '\n- ', html_content, flags=re.IGNORECASE)
        html_content = re.sub(r'<h[1-6][^>]*>', '\n## ', html_content, flags=re.IGNORECASE)
        html_content = re.sub(r'</h[1-6]>', '\n', html_content, flags=re.IGNORECASE)
        html_content = re.sub(r'<tr[^>]*>', '\n', html_content, flags=re.IGNORECASE)
        html_content = re.sub(r'<td[^>]*>', ' | ', html_content, flags=re.IGNORECASE)
        html_content = re.sub(r'<th[^>]*>', ' | ', html_content, flags=re.IGNORECASE)
        
        # Remove remaining HTML tags
        text = re.sub(r'<[^>]+>', ' ', html_content)
        
        # Decode HTML entities
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'&lt;', '<', text)
        text = re.sub(r'&gt;', '>', text)
        text = re.sub(r'&amp;', '&', text)
        text = re.sub(r'&quot;', '"', text)
        text = re.sub(r'&#39;', "'", text)
        text = re.sub(r'&[a-zA-Z]+;', ' ', text)  # Remove other entities
        
        # Clean up whitespace
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n\s*\n\s*\n', '\n\n', text)
        
        return text.strip()
    
    def parse_setup_guide(self, content: str) -> FivetranSetupContext:
        """Parse Setup Guide page content.
        
        Args:
            content: Text content from Setup Guide page
            
        Returns:
            FivetranSetupContext with extracted data
        """
        context = FivetranSetupContext(raw_content=content)
        
        content_lower = content.lower()
        
        # Extract prerequisites
        prereq_section = self._extract_section(content, ['prerequisite', 'before you begin', 'requirements', 'what you need'])
        if prereq_section:
            # Extract bullet points
            prereqs = re.findall(r'[-•]\s*([^\n]+)', prereq_section)
            context.prerequisites = [p.strip() for p in prereqs if len(p.strip()) > 5]
        
        # Extract auth methods
        for keyword in self.AUTH_KEYWORDS:
            if keyword in content_lower:
                context.auth_methods.append(keyword.title())
        
        # Extract auth instructions section
        auth_section = self._extract_section(content, ['authentication', 'authorization', 'connect', 'configure'])
        if auth_section:
            context.auth_instructions = auth_section[:3000]
        
        return context
    
    def parse_connector_overview(self, content: str) -> FivetranOverviewContext:
        """Parse Connector Overview page content.
        
        Args:
            content: Text content from Connector Overview page
            
        Returns:
            FivetranOverviewContext with extracted data
        """
        context = FivetranOverviewContext(raw_content=content)
        
        content_lower = content.lower()
        
        # Extract supported features
        for feature in self.FEATURE_KEYWORDS:
            if feature in content_lower:
                # Look for yes/no/supported/not supported near the feature
                pattern = rf'{feature}[:\s]*([^\n]{{0,50}})'
                match = re.search(pattern, content_lower)
                if match:
                    value_text = match.group(1).lower()
                    is_supported = 'yes' in value_text or 'supported' in value_text or 'available' in value_text
                    context.supported_features[feature.replace(' ', '_')] = is_supported
        
        # Extract sync overview
        sync_section = self._extract_section(content, ['sync overview', 'how we sync', 'synchronization', 'data sync'])
        if sync_section:
            context.sync_overview = sync_section[:2000]
        
        # Extract limitations
        limit_section = self._extract_section(content, ['limitation', 'restriction', 'not supported', 'known issue'])
        if limit_section:
            limitations = re.findall(r'[-•]\s*([^\n]+)', limit_section)
            context.sync_limitations = [l.strip() for l in limitations if len(l.strip()) > 10]
        
        # Extract historical sync info
        hist_match = re.search(r'historical[^\n]*sync[^\n]*(\d+\s*(?:day|month|year)s?)', content_lower)
        if hist_match:
            context.historical_sync_timeframe = hist_match.group(1)
        
        # Extract incremental sync details
        incr_section = self._extract_section(content, ['incremental', 'delta', 'change data'])
        if incr_section:
            context.incremental_sync_details = incr_section[:1500]
        
        return context
    
    def parse_schema_info(self, content: str) -> FivetranSchemaContext:
        """Parse Schema Information page content.
        
        Args:
            content: Text content from Schema Information page
            
        Returns:
            FivetranSchemaContext with extracted data
        """
        context = FivetranSchemaContext(raw_content=content)
        
        # Common cursor field names
        CURSOR_FIELDS = ['updated_at', 'modified_at', 'last_modified', 'modified_date', 
                         'updated_date', 'timestamp', 'created_at', 'sync_time', 'last_updated']
        
        # Common primary key patterns
        PK_PATTERNS = ['_id', 'id', '_pk', 'key', 'uuid']
        
        # Extract table/object names from the content
        # Look for patterns like "table_name" or "TableName" in table rows or lists
        table_pattern = r'\|\s*([a-z_]+[a-z0-9_]*)\s*\|'
        tables = re.findall(table_pattern, content.lower())
        
        # Also look for headings that might be table names
        heading_tables = re.findall(r'##\s*([a-z_]+[a-z0-9_]*)\s*\n', content.lower())
        
        # Look for table names in bold or links
        bold_tables = re.findall(r'\*\*([a-z_]+[a-z0-9_]*)\*\*', content.lower())
        
        all_tables = list(set(tables + heading_tables + bold_tables))
        
        # Filter out common non-table words
        excluded_words = {'table', 'name', 'type', 'description', 'column', 'field', 
                          'value', 'data', 'sync', 'mode', 'status', 'boolean', 'string',
                          'integer', 'timestamp', 'date', 'time', 'primary', 'foreign'}
        
        for table_name in all_tables:
            if len(table_name) < 3 or table_name in excluded_words:
                continue
            
            obj = FivetranSchemaObject(name=table_name)
            
            # Get the context around this table name (for extracting details)
            table_context = self._get_table_context(content, table_name)
            table_context_lower = table_context.lower()
            
            # Try to find sync mode
            if 'incremental' in table_context_lower:
                obj.sync_mode = 'incremental'
            elif 'full' in table_context_lower or 'full_load' in table_context_lower:
                obj.sync_mode = 'full_load'
            else:
                # Default assumption based on common patterns
                obj.sync_mode = 'incremental'  # Most objects support incremental
            
            # Try to find cursor field
            for cursor in CURSOR_FIELDS:
                if cursor in table_context_lower:
                    obj.cursor_field = cursor
                    break
            
            # If incremental but no cursor found, try to infer
            if obj.sync_mode == 'incremental' and not obj.cursor_field:
                obj.cursor_field = 'updated_at'  # Common default
            
            # Try to find primary key
            pk_match = re.search(rf'{table_name}[^\n]*(?:primary\s*key|pk|id)[:\s]*([a-z_]+)', table_context_lower)
            if pk_match:
                obj.primary_key = pk_match.group(1)
            else:
                # Default to common patterns
                if f'{table_name}_id' in table_context_lower:
                    obj.primary_key = f'{table_name}_id'
                else:
                    obj.primary_key = 'id'  # Common default
            
            context.objects.append(obj)
            context.supported_objects.append(table_name)
        
        # Extract parent-child relationships
        # Look for patterns like "parent_table → child_table" or "parent: X, child: Y"
        rel_patterns = [
            r'(\w+)\s*(?:→|->|is parent of|has many|contains)\s*(\w+)',
            r'parent[:\s]+(\w+)[,\s]+child[:\s]+(\w+)',
            r'(\w+)\s*table\s*(?:has|contains)\s*(\w+)',
        ]
        
        for pattern in rel_patterns:
            for match in re.finditer(pattern, content, re.IGNORECASE):
                parent = match.group(1).lower()
                child = match.group(2).lower()
                if parent != child and len(parent) > 2 and len(child) > 2:
                    if parent not in excluded_words and child not in excluded_words:
                        context.parent_child_relationships.append((parent, child))
        
        # Update objects with parent info from relationships
        parent_map = {child: parent for parent, child in context.parent_child_relationships}
        for obj in context.objects:
            if obj.name in parent_map:
                obj.parent = parent_map[obj.name]
        
        # Extract unsupported objects
        unsupported_section = self._extract_section(content, ['not supported', 'unsupported', 'excluded', 'not available'])
        if unsupported_section:
            unsupported = re.findall(r'[-•]\s*([a-z_]+[a-z0-9_]*)', unsupported_section.lower())
            context.unsupported_objects = [u.strip() for u in unsupported 
                                           if len(u.strip()) > 2 and u.strip() not in excluded_words]
            
            # Mark objects as unsupported
            for obj in context.objects:
                if obj.name in context.unsupported_objects:
                    obj.is_supported = False
        
        # Extract permissions
        perm_section = self._extract_section(content, ['permission', 'scope', 'role', 'access', 'require'])
        if perm_section:
            # Look for permission patterns like "read:accounts" or "accounts.read"
            perms = re.findall(r'([a-z_]+[:.][a-z_]+)', perm_section.lower())
            for perm in perms:
                perm = perm.strip()
                if perm and len(perm) > 4:
                    # Try to associate with an object
                    for obj in context.objects:
                        if obj.name in perm:
                            obj.permissions.append(perm)
                            if obj.name not in context.permissions_required:
                                context.permissions_required[obj.name] = []
                            context.permissions_required[obj.name].append(perm)
                            break
                    else:
                        # General permission
                        if 'general' not in context.permissions_required:
                            context.permissions_required['general'] = []
                        context.permissions_required['general'].append(perm)
            
            # Also look for more general permission patterns
            general_perms = re.findall(r'(?:permission|scope|role)[:\s]+([^\n,]+)', perm_section, re.IGNORECASE)
            for perm in general_perms:
                perm = perm.strip()
                if perm and len(perm) > 2:
                    if 'general' not in context.permissions_required:
                        context.permissions_required['general'] = []
                    context.permissions_required['general'].append(perm)
        
        # Detect delete methods for objects
        content_lower = content.lower()
        
        # Soft delete field patterns
        SOFT_DELETE_FIELDS = ['is_deleted', 'deleted', 'deleted_at', 'is_active', 'active', 
                              'status', 'archived', 'is_archived', 'removed', 'is_removed']
        
        # Check for global delete patterns
        has_deleted_endpoint = bool(re.search(r'(?:get|fetch)\s*/[^\s]*deleted', content_lower) or
                                    'deleted endpoint' in content_lower or
                                    'deleted records' in content_lower)
        has_webhook_deletes = bool(re.search(r'webhook[^\n]*delete', content_lower) or
                                   re.search(r'delete[^\n]*webhook', content_lower) or
                                   re.search(r'\.deleted|deleted\.', content_lower))
        has_audit_log = bool('audit' in content_lower and ('log' in content_lower or 'trail' in content_lower))
        has_soft_delete = any(field in content_lower for field in SOFT_DELETE_FIELDS)
        
        # Assign delete methods to objects
        for obj in context.objects:
            table_context = self._get_table_context(content, obj.name)
            table_context_lower = table_context.lower()
            
            # Check for object-specific delete patterns
            if any(field in table_context_lower for field in SOFT_DELETE_FIELDS):
                # Find which field
                for field in SOFT_DELETE_FIELDS:
                    if field in table_context_lower:
                        obj.delete_method = f"Soft Delete ({field})"
                        break
            elif re.search(rf'{obj.name}[^\n]*deleted\s*endpoint', table_context_lower):
                obj.delete_method = "Deleted Endpoint"
            elif re.search(rf'webhook[^\n]*{obj.name}[^\n]*delete|{obj.name}[^\n]*\.deleted', table_context_lower):
                obj.delete_method = "Webhook"
            elif 'audit' in table_context_lower:
                obj.delete_method = "Audit Log"
            # Fall back to global patterns
            elif has_deleted_endpoint:
                obj.delete_method = "Deleted Endpoint"
            elif has_webhook_deletes:
                obj.delete_method = "Webhook"
            elif has_soft_delete:
                obj.delete_method = "Soft Delete"
            elif has_audit_log:
                obj.delete_method = "Audit Log"
            else:
                obj.delete_method = "None"
        
        return context
    
    def _get_table_context(self, content: str, table_name: str, context_size: int = 500) -> str:
        """Get the context around a table name in the content.
        
        Args:
            content: Full content
            table_name: Table name to find
            context_size: Number of characters of context to extract
            
        Returns:
            Context string around the table name
        """
        content_lower = content.lower()
        idx = content_lower.find(table_name)
        
        if idx == -1:
            return ""
        
        start = max(0, idx - 100)
        end = min(len(content), idx + context_size)
        
        return content[start:end]
    
    def _extract_section(self, content: str, keywords: List[str]) -> str:
        """Extract a section of content based on keywords.
        
        Args:
            content: Full text content
            keywords: Keywords that might start the section
            
        Returns:
            Extracted section text
        """
        content_lower = content.lower()
        
        for keyword in keywords:
            # Find the keyword
            idx = content_lower.find(keyword)
            if idx != -1:
                # Extract from keyword to next major section or 2000 chars
                section_start = idx
                section_end = min(idx + 2000, len(content))
                
                # Try to find next section marker
                next_markers = ['##', '\n\n\n', '---']
                for marker in next_markers:
                    next_idx = content.find(marker, idx + len(keyword) + 50)
                    if next_idx != -1 and next_idx < section_end:
                        section_end = next_idx
                
                return content[section_start:section_end].strip()
        
        return ""
    
    async def crawl_all(
        self, 
        setup_url: str = None, 
        overview_url: str = None, 
        schema_url: str = None,
        manual_csv: str = None,
        manual_pdf_bytes: bytes = None,
        manual_text: str = None
    ) -> FivetranContext:
        """Crawl all provided Fivetran documentation pages or use manual input.
        
        Args:
            setup_url: Setup Guide URL
            overview_url: Connector Overview URL
            schema_url: Schema Information URL
            manual_csv: CSV content with object definitions
            manual_pdf_bytes: PDF file content as bytes
            manual_text: Plain text with object names
            
        Returns:
            FivetranContext with all extracted data
        """
        context = FivetranContext()
        
        try:
            # Crawl Setup Guide
            if setup_url:
                print(f"  - Crawling Fivetran Setup Guide: {setup_url}")
                setup_content = await self.crawl_page(setup_url)
                if setup_content:
                    context.setup = self.parse_setup_guide(setup_content)
            
            # Crawl Connector Overview
            if overview_url:
                print(f"  - Crawling Fivetran Connector Overview: {overview_url}")
                overview_content = await self.crawl_page(overview_url)
                if overview_content:
                    context.overview = self.parse_connector_overview(overview_content)
            
            # Crawl Schema Information
            if schema_url:
                print(f"  - Crawling Fivetran Schema Information: {schema_url}")
                schema_content = await self.crawl_page(schema_url)
                if schema_content:
                    context.schema = self.parse_schema_info(schema_content)
            
            # Process manual input (takes priority over crawled schema if provided)
            if manual_csv:
                print("  - Parsing manual CSV input")
                manual_schema = self.parse_csv_objects(manual_csv)
                if manual_schema.objects:
                    # Merge with existing schema or replace
                    if context.schema:
                        # Merge objects from manual input
                        existing_names = {obj.name for obj in context.schema.objects}
                        for obj in manual_schema.objects:
                            if obj.name not in existing_names:
                                context.schema.objects.append(obj)
                                context.schema.supported_objects.append(obj.name)
                        context.schema.parent_child_relationships.extend(manual_schema.parent_child_relationships)
                    else:
                        context.schema = manual_schema
            
            if manual_pdf_bytes:
                print("  - Parsing manual PDF input")
                manual_schema = self.parse_pdf_bytes(manual_pdf_bytes)
                if manual_schema.objects:
                    if context.schema:
                        existing_names = {obj.name for obj in context.schema.objects}
                        for obj in manual_schema.objects:
                            if obj.name not in existing_names:
                                context.schema.objects.append(obj)
                                context.schema.supported_objects.append(obj.name)
                    else:
                        context.schema = manual_schema
            
            if manual_text:
                print("  - Parsing manual text input")
                manual_schema = self.parse_text_objects(manual_text)
                if manual_schema.objects:
                    if context.schema:
                        existing_names = {obj.name for obj in context.schema.objects}
                        for obj in manual_schema.objects:
                            if obj.name not in existing_names:
                                context.schema.objects.append(obj)
                                context.schema.supported_objects.append(obj.name)
                    else:
                        context.schema = manual_schema
            
        finally:
            # Close the browser if we used it
            await self._close_browser()
        
        # Log summary
        obj_count = len(context.schema.objects) if context.schema else 0
        print(f"  ✓ Fivetran crawl complete: {obj_count} objects found")
        
        return context


# Singleton instance
_crawler: Optional[FivetranCrawler] = None


def get_fivetran_crawler(use_headless: bool = True) -> FivetranCrawler:
    """Get the singleton FivetranCrawler instance.
    
    Args:
        use_headless: Whether to use headless browser for JS-rendered pages
        
    Returns:
        FivetranCrawler instance
    """
    global _crawler
    if _crawler is None:
        _crawler = FivetranCrawler(use_headless=use_headless)
    return _crawler


async def cleanup_crawler():
    """Cleanup the crawler and close browser."""
    global _crawler
    if _crawler:
        await _crawler._close_browser()
        _crawler = None

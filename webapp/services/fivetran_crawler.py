"""
Fivetran Crawler Service
Crawls and parses Fivetran documentation pages for parity comparison.
"""

import re
import httpx
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime


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
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'sync_mode': self.sync_mode,
            'parent': self.parent,
            'permissions': self.permissions,
            'description': self.description[:500]
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
    """Crawls and parses Fivetran documentation pages."""
    
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
    
    def __init__(self):
        """Initialize the crawler."""
        self.timeout = 30.0
    
    async def crawl_page(self, url: str) -> str:
        """Crawl a single page and return its text content.
        
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
                    'User-Agent': 'Mozilla/5.0 (compatible; ConnectorResearchBot/1.0)'
                })
                response.raise_for_status()
                
                html_content = response.text
                return self._html_to_text(html_content)
                
        except Exception as e:
            print(f"Error crawling {url}: {e}")
            return ""
    
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
        
        # Extract table/object names from the content
        # Look for patterns like "table_name" or "TableName" in table rows or lists
        table_pattern = r'\|\s*([a-z_]+[a-z0-9_]*)\s*\|'
        tables = re.findall(table_pattern, content.lower())
        
        # Also look for headings that might be table names
        heading_tables = re.findall(r'##\s*([a-z_]+[a-z0-9_]*)\s*\n', content.lower())
        
        all_tables = list(set(tables + heading_tables))
        
        for table_name in all_tables:
            if len(table_name) < 3 or table_name in ['table', 'name', 'type', 'description', 'column']:
                continue
            
            obj = FivetranSchemaObject(name=table_name)
            
            # Try to find sync mode
            sync_pattern = rf'{table_name}[^\n]*(?:incremental|full)'
            sync_match = re.search(sync_pattern, content.lower())
            if sync_match:
                if 'incremental' in sync_match.group(0):
                    obj.sync_mode = 'incremental'
                elif 'full' in sync_match.group(0):
                    obj.sync_mode = 'full_load'
            
            context.objects.append(obj)
            context.supported_objects.append(table_name)
        
        # Extract parent-child relationships
        # Look for patterns like "parent_table → child_table" or "parent: X, child: Y"
        rel_patterns = [
            r'(\w+)\s*(?:→|->|is parent of|has many)\s*(\w+)',
            r'parent[:\s]+(\w+)[,\s]+child[:\s]+(\w+)',
        ]
        
        for pattern in rel_patterns:
            for match in re.finditer(pattern, content, re.IGNORECASE):
                parent = match.group(1).lower()
                child = match.group(2).lower()
                if parent != child and len(parent) > 2 and len(child) > 2:
                    context.parent_child_relationships.append((parent, child))
        
        # Extract unsupported objects
        unsupported_section = self._extract_section(content, ['not supported', 'unsupported', 'excluded'])
        if unsupported_section:
            unsupported = re.findall(r'[-•]\s*([a-z_]+[a-z0-9_]*)', unsupported_section.lower())
            context.unsupported_objects = [u.strip() for u in unsupported if len(u.strip()) > 2]
        
        # Extract permissions
        perm_section = self._extract_section(content, ['permission', 'scope', 'role', 'access'])
        if perm_section:
            perms = re.findall(r'(?:permission|scope|role)[:\s]+([^\n,]+)', perm_section, re.IGNORECASE)
            for perm in perms:
                perm = perm.strip()
                if perm and len(perm) > 2:
                    context.permissions_required['general'] = context.permissions_required.get('general', [])
                    context.permissions_required['general'].append(perm)
        
        return context
    
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
    
    async def crawl_all(self, setup_url: str = None, overview_url: str = None, schema_url: str = None) -> FivetranContext:
        """Crawl all provided Fivetran documentation pages.
        
        Args:
            setup_url: Setup Guide URL
            overview_url: Connector Overview URL
            schema_url: Schema Information URL
            
        Returns:
            FivetranContext with all extracted data
        """
        context = FivetranContext()
        
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
        
        return context


# Singleton instance
_crawler: Optional[FivetranCrawler] = None


def get_fivetran_crawler() -> FivetranCrawler:
    """Get the singleton FivetranCrawler instance."""
    global _crawler
    if _crawler is None:
        _crawler = FivetranCrawler()
    return _crawler

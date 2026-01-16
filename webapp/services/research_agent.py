"""
Research Agent Service
Auto-generates dynamic connector research documents with auto-discovered extraction methods.
"""

import os
import re
import asyncio
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime
from openai import AsyncOpenAI
from dotenv import load_dotenv

load_dotenv()


@dataclass
class ResearchSection:
    """Defines a research section."""
    number: int
    name: str
    phase: int
    phase_name: str
    prompts: List[str]
    requires_fivetran: bool = False
    requires_code_analysis: bool = False
    is_method_section: bool = False  # True if this is a per-method deep dive
    method_name: str = ""  # The method this section covers (e.g., "REST API")


# Extraction methods to discover
EXTRACTION_METHODS = [
    "REST API",
    "GraphQL API", 
    "SOAP/XML API",
    "Webhooks",
    "Bulk/Batch API",
    "Official SDK",
    "JDBC/ODBC",
    "File Export"
]


# Method section template - generated dynamically for each discovered method
def create_method_section(method_name: str, section_number: int) -> ResearchSection:
    """Create a deep-dive section for a specific extraction method."""
    return ResearchSection(
        number=section_number,
        name=f"{method_name} Deep Dive",
        phase=2,
        phase_name="Extraction Methods",
        is_method_section=True,
        method_name=method_name,
        prompts=[
            f"Describe the {method_name} for {{connector}} in detail.",
            f"**Authentication**: What authentication methods are supported for {method_name}? (OAuth, API Key, Basic Auth, etc.) Provide exact steps and code examples.",
            f"**Base URL/Endpoint Structure**: What is the base URL? What is the endpoint naming convention?",
            f"**Available Operations**: List all key endpoints/operations available via {method_name}. Create a table with: Operation Name, HTTP Method (if applicable), URL/Query, Description.",
            f"**Objects Accessible**: Which data objects/entities can be accessed via {method_name}? Are there any objects NOT accessible via this method?",
            f"**Pagination**: How does pagination work? (cursor-based, offset-based, page-based) What are the max records per request? Provide code example.",
            f"**Rate Limits**: What are the specific rate limits for {method_name}? (requests per minute/hour/day) Are limits per user, app, or account?",
            f"**Sync Capabilities**: What sync modes are supported via {method_name}? (Full Load, Incremental, CDC/Real-time) What cursor fields are available for incremental sync?",
            f"**Delete Detection**: How can deletions be detected via {method_name}? (Soft delete fields, deleted endpoint, events, etc.)",
            f"**Code Example**: Provide a complete Python code example showing how to authenticate and extract data from 2-3 key objects using {method_name}, including pagination handling.",
            f"**Pros & Cons**: What are the advantages and disadvantages of using {method_name} compared to other available methods? When should you use this method vs others?"
        ]
    )


# Base sections (always included)
BASE_SECTIONS = [
    # Phase 1: Discovery (Sections 1-3)
    ResearchSection(1, "Platform Overview", 1, "Platform Discovery", [
        "What does {connector} do? Describe its purpose, target users, and main functionality.",
        "What are the key modules and features?",
        "What types of data entities does it store?",
        "Does it have reporting/analytics modules?",
        "What are the limitations of its data model?",
        "Who are the typical users (enterprise, SMB, developers)?"
    ]),
    
    ResearchSection(2, "Extraction Methods Discovery", 1, "Platform Discovery", [
        "**IMPORTANT**: Discover ALL available data extraction methods for {connector}. For each method that EXISTS, provide details. If a method does NOT exist, explicitly state 'Not Available'.",
        "",
        "Check for the following methods and report availability:",
        "",
        "1. **REST API**: Does {connector} have a REST API? If yes: What is the base URL? Is it documented? What version?",
        "2. **GraphQL API**: Does {connector} have a GraphQL API? If yes: What is the endpoint? What schemas are available?",
        "3. **SOAP/XML API**: Does {connector} have a SOAP or XML-based API? If yes: Where is the WSDL?",
        "4. **Webhooks**: Does {connector} support webhooks for real-time events? If yes: What events are available?",
        "5. **Bulk/Batch API**: Does {connector} have bulk data export or async batch APIs? If yes: How do they work?",
        "6. **Official SDK**: Does {connector} provide official SDKs? If yes: What languages (Python, Java, Node.js, etc.)?",
        "7. **JDBC/ODBC**: Does {connector} support direct database connections via JDBC/ODBC? If yes: What drivers?",
        "8. **File Export**: Does {connector} support data export to files (CSV, JSON, etc.)? If yes: Manual or API-triggered?",
        "",
        "Create a summary table at the end:",
        "| Method | Available | Base URL/Endpoint | Documentation Link | Best Use Case |",
        "|--------|-----------|-------------------|-------------------|---------------|"
    ]),
    
    ResearchSection(3, "Developer Environment", 1, "Platform Discovery", [
        "Does {connector} provide sandbox or developer environments?",
        "How do you request access (self-service, sales, partner program)?",
        "What are the limitations of sandbox vs production?",
        "How do you register a developer app/integration?",
        "What credentials are needed (API keys, OAuth app, service account)?",
        "Are there IP whitelists or redirect URI requirements?",
        "Provide a minimal health check code example to verify API access."
    ]),
]


# Cross-cutting sections (always included, after method sections)
CROSS_CUTTING_SECTIONS = [
    ResearchSection(100, "Authentication Comparison", 3, "Cross-Cutting Concerns", [
        "Compare authentication methods across ALL available extraction methods for {connector}.",
        "Create a comparison table: Method | Auth Type | Token Lifetime | Refresh Strategy | Scopes Required",
        "Which authentication method is recommended for production ETL pipelines?",
        "What are the security best practices for credential management?",
        "Provide unified authentication code that works across multiple methods."
    ]),
    
    ResearchSection(101, "Rate Limiting Strategy", 3, "Cross-Cutting Concerns", [
        "Compare rate limits across ALL available extraction methods for {connector}.",
        "Create a comparison table: Method | Requests/Min | Requests/Hour | Requests/Day | Concurrency Limit",
        "Which method has the most generous rate limits for bulk extraction?",
        "What retry strategies should be used when rate limited?",
        "Provide a rate limiter implementation in Python that respects these limits."
    ]),
    
    ResearchSection(102, "Error Handling & Retries", 3, "Cross-Cutting Concerns", [
        "What error codes and responses are returned by {connector} APIs?",
        "Create a table: Error Code | Meaning | Retryable? | Resolution",
        "What errors require re-authentication vs simple retry?",
        "What is the recommended exponential backoff strategy?",
        "Provide error handling code with proper retry logic."
    ]),
    
    ResearchSection(103, "Data Model & Relationships", 3, "Cross-Cutting Concerns", [
        "Document the complete data model for {connector}.",
        "What are all the main objects/entities?",
        "What parent-child relationships exist? Create a relationship diagram or table.",
        "What is the correct load order for related objects?",
        "Are there any circular dependencies to handle?",
        "What foreign keys link objects together?"
    ], requires_fivetran=True, requires_code_analysis=True),
    
    ResearchSection(104, "Delete Detection Strategies", 3, "Cross-Cutting Concerns", [
        "Compare delete detection methods across ALL extraction methods for {connector}.",
        "Create a table: Method | Delete Detection | Field/Endpoint | Reliability",
        "Which method is most reliable for detecting deletions?",
        "How should soft deletes vs hard deletes be handled?",
        "Provide code for delete detection using the recommended method."
    ]),
]


# Final sections (always included, after cross-cutting)
FINAL_SECTIONS = [
    ResearchSection(200, "Recommended Extraction Strategy", 4, "Implementation Guide", [
        "Based on all discovered methods, what is the RECOMMENDED extraction strategy for {connector}?",
        "Consider: reliability, performance, completeness, delete detection, real-time needs.",
        "Create a decision matrix: Use Case | Recommended Method | Reason",
        "What combination of methods provides the best coverage?",
        "Provide a high-level architecture diagram for a production ETL pipeline.",
        "What are the trade-offs between different approaches?"
    ]),
    
    ResearchSection(201, "Object Catalog & Replication Guide", 4, "Implementation Guide", [
        "List ALL available objects/entities that can be extracted from {connector}.",
        "Create a comprehensive catalog table with columns:",
        "| Object | Extraction Method | Primary Key | Cursor Field | Sync Mode | Delete Method | Fivetran Support |",
        "For each object, specify:",
        "- Which extraction method(s) can access it",
        "- Primary key field",
        "- Best cursor field for incremental sync",
        "- Supported sync modes (Full/Incremental/CDC)",
        "- Delete detection method",
        "- Whether Fivetran supports this object (if known)",
        "Provide sample extraction code for the top 5 most important objects."
    ], requires_fivetran=True, requires_code_analysis=True),
    
    ResearchSection(202, "Production Checklist", 4, "Implementation Guide", [
        "Create a production readiness checklist for {connector} data extraction.",
        "**Authentication**: [ ] OAuth app registered, [ ] Credentials secured, [ ] Token refresh implemented",
        "**Rate Limiting**: [ ] Rate limiter configured, [ ] Backoff strategy implemented",
        "**Error Handling**: [ ] All error codes handled, [ ] Alerts configured",
        "**Monitoring**: [ ] Sync metrics tracked, [ ] Data quality checks in place",
        "**Testing**: [ ] Sandbox testing complete, [ ] Load testing done",
        "What are the top 10 things that can go wrong in production?",
        "What monitoring and alerting should be in place?"
    ]),
]


@dataclass
class ResearchProgress:
    """Tracks research generation progress."""
    connector_id: str
    connector_name: str
    current_section: int = 0
    total_sections: int = 0  # Dynamic - calculated based on discovered methods
    status: str = "idle"  # idle, running, completed, failed, cancelled
    sections_completed: List[int] = field(default_factory=list)
    current_content: str = ""
    error_message: str = ""
    discovered_methods: List[str] = field(default_factory=list)  # Methods found during discovery


@dataclass
class ResearchMetrics:
    """Tracks metrics for Quick Summary dashboard."""
    
    # API Capabilities (from web search/documentation)
    total_objects: int = 0
    objects_by_method: Dict[str, int] = field(default_factory=dict)  # REST, GraphQL, SOAP, etc.
    full_load_count: int = 0
    incremental_count: int = 0
    cdc_count: int = 0
    auth_types: List[str] = field(default_factory=list)
    sdk_info: List[str] = field(default_factory=list)
    delete_methods: List[str] = field(default_factory=list)
    
    # Current Implementation (from GitHub)
    impl_objects: int = 0
    impl_by_method: Dict[str, int] = field(default_factory=dict)
    impl_full_load: int = 0
    impl_incremental: int = 0
    impl_auth: List[str] = field(default_factory=list)
    impl_sdk: str = ""
    impl_object_names: List[str] = field(default_factory=list)
    
    # Fivetran Parity
    fivetran_objects: int = 0
    fivetran_supported: List[str] = field(default_factory=list)
    fivetran_unsupported: List[str] = field(default_factory=list)
    fivetran_features: Dict[str, bool] = field(default_factory=dict)
    fivetran_auth: List[str] = field(default_factory=list)
    fivetran_full_load: int = 0
    fivetran_incremental: int = 0


class ResearchAgent:
    """Agent that auto-generates connector research documents."""
    
    def __init__(self):
        """Initialize the research agent."""
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.tavily_api_key = os.getenv("TAVILY_API_KEY")
        self.model = os.getenv("RESEARCH_MODEL", "gpt-4o")
        
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY is required")
        
        self.client = AsyncOpenAI(api_key=self.openai_api_key)
        self._cancel_requested = False
        self._current_progress: Optional[ResearchProgress] = None
    
    def get_progress(self) -> Optional[ResearchProgress]:
        """Get current research progress."""
        return self._current_progress
    
    def cancel(self):
        """Request cancellation of current research."""
        self._cancel_requested = True
    
    def _extract_github_metrics(self, github_context: Dict[str, Any]) -> ResearchMetrics:
        """Extract metrics from GitHub context for Quick Summary.
        
        Args:
            github_context: Context extracted from GitHub repository
            
        Returns:
            ResearchMetrics with implementation stats populated
        """
        metrics = ResearchMetrics()
        
        if not github_context:
            return metrics
        
        # Check if structured format
        is_structured = github_context.get('structure_type') == 'structured'
        
        if is_structured:
            impl = github_context.get('implementation', {})
            sdk = github_context.get('sdk', {})
            
            # Count models/objects
            models = impl.get('models', [])
            metrics.impl_objects = len(models)
            metrics.impl_object_names = models[:50]
            
            # Detect API methods from api_calls
            api_calls = impl.get('api_calls', [])
            rest_count = sum(1 for c in api_calls if 'get' in c.lower() or 'post' in c.lower() or 'http' in c.lower())
            graphql_count = sum(1 for c in api_calls if 'graphql' in c.lower() or 'query' in c.lower())
            metrics.impl_by_method = {'REST': rest_count, 'GraphQL': graphql_count}
            
            # Auth detection
            auth_impl = impl.get('auth_implementation', '')
            if 'oauth' in auth_impl.lower():
                metrics.impl_auth.append('OAuth 2.0')
            if 'api_key' in auth_impl.lower() or 'apikey' in auth_impl.lower():
                metrics.impl_auth.append('API Key')
            if 'bearer' in auth_impl.lower():
                metrics.impl_auth.append('Bearer Token')
            if 'basic' in auth_impl.lower():
                metrics.impl_auth.append('Basic Auth')
            
            # SDK info
            sdk_name = sdk.get('sdk_name', '')
            if sdk_name:
                metrics.impl_sdk = sdk_name
            
            # Sync patterns
            sync_patterns = impl.get('sync_patterns', [])
            metrics.impl_incremental = sum(1 for p in sync_patterns if 'incremental' in p.lower() or 'cursor' in p.lower())
            metrics.impl_full_load = max(0, metrics.impl_objects - metrics.impl_incremental)
            
        else:
            # Legacy flat format
            object_types = github_context.get('object_types', [])
            metrics.impl_objects = len(object_types)
            metrics.impl_object_names = object_types[:50]
            
            auth_patterns = github_context.get('auth_patterns', [])
            for pattern in auth_patterns:
                if 'oauth' in pattern.lower():
                    metrics.impl_auth.append('OAuth 2.0')
                elif 'api' in pattern.lower() and 'key' in pattern.lower():
                    metrics.impl_auth.append('API Key')
        
        return metrics
    
    def _extract_fivetran_metrics(self, fivetran_context: Dict[str, Any]) -> ResearchMetrics:
        """Extract metrics from Fivetran context for Quick Summary.
        
        Args:
            fivetran_context: Context from Fivetran crawler
            
        Returns:
            ResearchMetrics with Fivetran stats populated
        """
        metrics = ResearchMetrics()
        
        if not fivetran_context:
            return metrics
        
        setup = fivetran_context.get('setup', {})
        overview = fivetran_context.get('overview', {})
        schema = fivetran_context.get('schema', {})
        
        # Supported objects
        supported = schema.get('supported_objects', [])
        unsupported = schema.get('unsupported_objects', [])
        metrics.fivetran_objects = len(supported)
        metrics.fivetran_supported = supported
        metrics.fivetran_unsupported = unsupported
        
        # Auth methods
        auth_methods = setup.get('auth_methods', [])
        metrics.fivetran_auth = auth_methods
        
        # Features
        features = overview.get('supported_features', {})
        metrics.fivetran_features = features
        
        # Sync modes from objects
        objects = schema.get('objects', [])
        for obj in objects:
            sync_mode = obj.get('sync_mode', '')
            if sync_mode == 'incremental':
                metrics.fivetran_incremental += 1
            elif sync_mode == 'full_load':
                metrics.fivetran_full_load += 1
        
        # If no specific sync modes, estimate from total
        if metrics.fivetran_incremental == 0 and metrics.fivetran_full_load == 0:
            metrics.fivetran_incremental = int(metrics.fivetran_objects * 0.7)  # Estimate
            metrics.fivetran_full_load = metrics.fivetran_objects - metrics.fivetran_incremental
        
        return metrics
    
    def _calculate_parity(self, impl_objects: List[str], fivetran_objects: List[str]) -> Dict[str, Any]:
        """Calculate parity between implementation and Fivetran.
        
        Args:
            impl_objects: List of implemented object names
            fivetran_objects: List of Fivetran supported object names
            
        Returns:
            Dict with parity percentage and gap analysis
        """
        impl_set = set(obj.lower() for obj in impl_objects)
        fivetran_set = set(obj.lower() for obj in fivetran_objects)
        
        # Objects we have that Fivetran doesn't
        extra_objects = impl_set - fivetran_set
        
        # Objects Fivetran has that we're missing
        missing_objects = fivetran_set - impl_set
        
        # Common objects
        common = impl_set & fivetran_set
        
        # Parity percentage (what % of Fivetran objects do we support)
        if fivetran_set:
            parity_pct = (len(common) / len(fivetran_set)) * 100
        else:
            parity_pct = 0
        
        return {
            'parity_percentage': round(parity_pct, 1),
            'common_count': len(common),
            'fivetran_total': len(fivetran_set),
            'extra_objects': list(extra_objects)[:20],
            'missing_objects': list(missing_objects)[:20],
            'extra_count': len(extra_objects),
            'missing_count': len(missing_objects)
        }
    
    def _generate_quick_summary(
        self,
        connector_name: str,
        connector_type: str,
        github_context: Optional[Dict[str, Any]] = None,
        fivetran_context: Optional[Dict[str, Any]] = None
    ) -> str:
        """Generate Quick Summary Dashboard with stacked cards.
        
        Args:
            connector_name: Name of the connector
            connector_type: Type of connector (REST, GraphQL, etc.)
            github_context: Optional GitHub context for implementation stats
            fivetran_context: Optional Fivetran context for parity analysis
            
        Returns:
            Markdown string for Quick Summary section
        """
        summary_parts = []
        
        # Header
        summary_parts.append("""
# 📋 Quick Summary Dashboard

> At-a-glance metrics and comparison for rapid assessment

---
""")
        
        # Card 1: API Capabilities (always shown)
        summary_parts.append("""
## 📊 API Capabilities (from Documentation)

| Metric | Value |
|--------|-------|
| **Connector Type** | """ + connector_type.upper() + """ |
| **Primary API** | """ + self._get_primary_api(connector_type) + """ |
| **Auth Types** | _See Section 5_ |
| **Official SDKs** | _See Section 15_ |
| **Rate Limits** | _See Section 12_ |

> 💡 Detailed object catalog available in **Section 19**

---
""")
        
        # Card 2: Current Implementation (if GitHub provided)
        if github_context:
            github_metrics = self._extract_github_metrics(github_context)
            
            impl_auth_str = ', '.join(github_metrics.impl_auth) if github_metrics.impl_auth else '_Not detected_'
            sdk_str = github_metrics.impl_sdk if github_metrics.impl_sdk else '_Not detected_'
            
            # Build method breakdown
            method_parts = []
            for method, count in github_metrics.impl_by_method.items():
                if count > 0:
                    method_parts.append(f"{method}: {count}")
            method_str = ', '.join(method_parts) if method_parts else '_Mixed_'
            
            summary_parts.append(f"""
## 🔧 Current Implementation (from GitHub)

| Metric | Value |
|--------|-------|
| **Objects Implemented** | {github_metrics.impl_objects} |
| **By Extraction Method** | {method_str} |
| **Full Load Objects** | {github_metrics.impl_full_load} |
| **Incremental Objects** | {github_metrics.impl_incremental} |
| **Auth Implemented** | {impl_auth_str} |
| **SDK Used** | {sdk_str} |

> 📁 Repository: `{github_context.get('repo_url', 'N/A')}`

---
""")
        
        # Card 3: Fivetran Parity (if Fivetran URLs provided)
        if fivetran_context:
            fivetran_metrics = self._extract_fivetran_metrics(fivetran_context)
            
            # Calculate parity if we have implementation data
            parity_info = None
            if github_context:
                github_metrics = self._extract_github_metrics(github_context)
                parity_info = self._calculate_parity(
                    github_metrics.impl_object_names,
                    fivetran_metrics.fivetran_supported
                )
            
            fivetran_auth_str = ', '.join(fivetran_metrics.fivetran_auth) if fivetran_metrics.fivetran_auth else '_Not specified_'
            
            # Feature checkmarks
            features = fivetran_metrics.fivetran_features
            capture_deletes = '✓ Supported' if features.get('capture_deletes') else '✗ Not supported'
            history_mode = '✓ Supported' if features.get('history_mode') else '✗ Not supported'
            
            summary_parts.append(f"""
## 🎯 Fivetran Parity Analysis

| Metric | Value |
|--------|-------|
| **Fivetran Objects** | {fivetran_metrics.fivetran_objects} |
| **Full Load Objects** | {fivetran_metrics.fivetran_full_load} |
| **Incremental Objects** | {fivetran_metrics.fivetran_incremental} |
| **Auth Methods** | {fivetran_auth_str} |
| **Capture Deletes** | {capture_deletes} |
| **History Mode** | {history_mode} |
""")
            
            # Add parity score and gap analysis if we have implementation data
            if parity_info:
                summary_parts.append(f"""
### Parity Score

| | |
|---|---|
| **Score** | **{parity_info['parity_percentage']}%** ({parity_info['common_count']}/{parity_info['fivetran_total']} objects) |

### Gap Analysis

**Objects we have that Fivetran doesn't ({parity_info['extra_count']}):**
""")
                if parity_info['extra_objects']:
                    summary_parts.append('- ' + ', '.join(parity_info['extra_objects'][:10]))
                    if parity_info['extra_count'] > 10:
                        summary_parts.append(f'- _...and {parity_info["extra_count"] - 10} more_')
                else:
                    summary_parts.append('- _None_')
                
                summary_parts.append(f"""
**Objects Fivetran has that we're missing ({parity_info['missing_count']}):**
""")
                if parity_info['missing_objects']:
                    summary_parts.append('- ' + ', '.join(parity_info['missing_objects'][:10]))
                    if parity_info['missing_count'] > 10:
                        summary_parts.append(f'- _...and {parity_info["missing_count"] - 10} more_')
                else:
                    summary_parts.append('- _None - Full parity achieved!_')
            
            summary_parts.append("""
---
""")
        
        # Navigation hint
        summary_parts.append("""
## 📑 Document Navigation

| Phase | Sections | Focus |
|-------|----------|-------|
| **Phase 1** | 1-3 | Platform Understanding |
| **Phase 2** | 4-7 | Data Access & Auth |
| **Phase 3** | 8-11 | Sync & Extraction |
| **Phase 4** | 12-14 | Reliability & Performance |
| **Phase 5** | 15-17 | Advanced Topics |
| **Phase 6** | 18 | Troubleshooting |
| **Phase 7** | 19 | Object Catalog |

---

""")
        
        return '\n'.join(summary_parts)
    
    def _get_primary_api(self, connector_type: str) -> str:
        """Get primary API description from connector type."""
        type_map = {
            'rest_api': 'REST API',
            'graphql': 'GraphQL API',
            'soap': 'SOAP/XML Web Services',
            'jdbc': 'JDBC Database Connection',
            'sdk': 'Official SDK',
            'webhook': 'Webhooks/Event-driven'
        }
        return type_map.get(connector_type.lower(), connector_type.upper())
    
    async def _web_search(self, query: str) -> str:
        """Perform web search using Tavily.
        
        Args:
            query: Search query
            
        Returns:
            Search results as formatted text
        """
        if not self.tavily_api_key:
            return "Web search not available (no TAVILY_API_KEY)"
        
        try:
            from tavily import TavilyClient
            tavily = TavilyClient(api_key=self.tavily_api_key)
            
            response = tavily.search(
                query=query,
                search_depth="advanced",
                max_results=5
            )
            
            results = []
            for i, result in enumerate(response.get('results', []), 1):
                results.append(f"[web:{i}] {result.get('title', 'No title')}")
                results.append(f"URL: {result.get('url', '')}")
                results.append(f"Content: {result.get('content', '')[:500]}...")
                results.append("")
            
            return "\n".join(results) if results else "No results found"
            
        except Exception as e:
            return f"Web search error: {str(e)}"
    
    def _build_section_context(self, section_number: int, structured_context: Dict[str, Any]) -> str:
        """Build section-specific context from structured repository data.
        
        Args:
            section_number: The section number (1-18)
            structured_context: Dict with 'implementation', 'sdk', 'documentation' keys
            
        Returns:
            Formatted context string relevant to the section
        """
        parts = []
        impl = structured_context.get('implementation', {})
        sdk = structured_context.get('sdk', {})
        docs = structured_context.get('documentation', {})
        
        # Section 4: Data Access Mechanisms
        if section_number == 4:
            if sdk.get('available_methods'):
                parts.append(f"**SDK Available Methods:**\n{', '.join(sdk['available_methods'][:30])}")
            if sdk.get('client_classes'):
                parts.append(f"**SDK Client Classes:**\n{', '.join(sdk['client_classes'][:20])}")
            if impl.get('api_calls'):
                parts.append(f"**Implementation API Calls (from Connector_Code):**")
                for call in impl['api_calls'][:10]:
                    parts.append(f"  - {call[:200]}")
            if docs.get('api_reference'):
                parts.append(f"**From Public Documentation - API Reference:**\n{docs['api_reference'][:1500]}")
        
        # Section 5: Authentication Mechanics
        elif section_number == 5:
            if impl.get('auth_implementation'):
                parts.append(f"**Current Auth Implementation (from Connector_Code):**\n```\n{impl['auth_implementation'][:2000]}\n```")
            if sdk.get('auth_methods'):
                parts.append(f"**SDK Auth Methods:**\n{', '.join(sdk['auth_methods'][:20])}")
            if docs.get('auth_guide'):
                parts.append(f"**From Public Documentation - Auth Guide:**\n{docs['auth_guide'][:1500]}")
            if docs.get('permissions'):
                parts.append(f"**Documented Permissions/Scopes:**\n{', '.join(docs['permissions'][:30])}")
        
        # Section 6: App Registration
        elif section_number == 6:
            if docs.get('auth_guide'):
                parts.append(f"**From Public Documentation - Auth/Registration:**\n{docs['auth_guide'][:1500]}")
        
        # Section 7: Metadata Discovery & Schema
        elif section_number == 7:
            if sdk.get('data_types'):
                parts.append(f"**SDK Data Types/Models:**\n{', '.join(sdk['data_types'][:50])}")
            if impl.get('models'):
                parts.append(f"**Implementation Models (from Connector_Code):**\n{', '.join(impl['models'][:30])}")
            if docs.get('objects_schema'):
                parts.append(f"**From Public Documentation - Objects/Schema:**\n{docs['objects_schema'][:2000]}")
            if docs.get('endpoints_list'):
                parts.append(f"**Documented Endpoints:**")
                for ep in docs['endpoints_list'][:20]:
                    parts.append(f"  - {ep}")
        
        # Section 8: Sync Strategies
        elif section_number == 8:
            if impl.get('sync_patterns'):
                parts.append(f"**Sync Patterns Found in Implementation:**")
                for pattern in impl['sync_patterns'][:15]:
                    parts.append(f"  - {pattern[:150]}")
        
        # Section 9: Bulk Extraction & Pagination
        elif section_number == 9:
            if impl.get('sync_patterns'):
                parts.append(f"**Pagination Patterns Found:**")
                for pattern in impl['sync_patterns'][:10]:
                    parts.append(f"  - {pattern[:150]}")
            if impl.get('api_calls'):
                bulk_calls = [c for c in impl['api_calls'] if 'bulk' in c.lower() or 'batch' in c.lower() or 'export' in c.lower()]
                if bulk_calls:
                    parts.append(f"**Bulk API Calls Found:**")
                    for call in bulk_calls[:5]:
                        parts.append(f"  - {call[:200]}")
        
        # Section 12: Rate Limits
        elif section_number == 12:
            if docs.get('rate_limits'):
                parts.append(f"**From Public Documentation - Rate Limits:**\n{docs['rate_limits'][:1500]}")
        
        # Section 13: API Failure Types & Retry
        elif section_number == 13:
            if impl.get('error_handling'):
                parts.append(f"**Error Handling Patterns in Implementation:**")
                for err in impl['error_handling'][:10]:
                    parts.append(f"  - {err[:150]}")
        
        # Section 15: Dependencies, Drivers & SDK
        elif section_number == 15:
            if sdk.get('sdk_name'):
                parts.append(f"**SDK Name:** {sdk['sdk_name']}")
            if sdk.get('client_classes'):
                parts.append(f"**SDK Client Classes:**\n{', '.join(sdk['client_classes'][:20])}")
            if sdk.get('constants'):
                parts.append(f"**SDK Constants/Enums:**\n{', '.join(sdk['constants'][:30])}")
        
        # Section 17: Relationships
        elif section_number == 17:
            if impl.get('models'):
                parts.append(f"**Models Found (potential relationships):**\n{', '.join(impl['models'][:30])}")
            if sdk.get('data_types'):
                parts.append(f"**SDK Data Types:**\n{', '.join(sdk['data_types'][:30])}")
        
        # Section 18: Troubleshooting
        elif section_number == 18:
            if impl.get('error_handling'):
                parts.append(f"**Error Handling Found in Implementation:**")
                for err in impl['error_handling'][:10]:
                    parts.append(f"  - {err[:150]}")
            if impl.get('config_patterns'):
                parts.append(f"**Configuration Patterns:**")
                for cfg in impl['config_patterns'][:10]:
                    parts.append(f"  - {cfg}")
        
        # Section 19: Available Objects & Replication Guide
        elif section_number == 19:
            if sdk.get('data_types'):
                parts.append(f"**SDK Data Types/Objects ({len(sdk['data_types'])} found):**\n{', '.join(sdk['data_types'][:100])}")
            if impl.get('models'):
                parts.append(f"**Implementation Models ({len(impl['models'])} found):**\n{', '.join(impl['models'][:100])}")
            if docs.get('objects_schema'):
                parts.append(f"**From Public Documentation - Objects/Schema:**\n{docs['objects_schema'][:3000]}")
            if docs.get('endpoints_list'):
                parts.append(f"**Documented Endpoints ({len(docs['endpoints_list'])} found):**")
                for ep in docs['endpoints_list'][:40]:
                    parts.append(f"  - {ep}")
            if impl.get('api_calls'):
                parts.append(f"**API Calls Found in Implementation:**")
                for call in impl['api_calls'][:20]:
                    parts.append(f"  - {call[:200]}")
        
        # For other sections, provide general context if available
        else:
            if docs.get('raw_content') and section_number in [1, 2, 3]:
                # Platform understanding sections can use general docs
                parts.append(f"**From Public Documentation:**\n{docs['raw_content'][:1500]}")
        
        return "\n\n".join(parts) if parts else ""
    
    def _build_fivetran_section_context(self, section_number: int, fivetran_context: Dict[str, Any]) -> str:
        """Build section-specific context from Fivetran documentation.
        
        Args:
            section_number: The section number (1-18)
            fivetran_context: Dict with 'setup', 'overview', 'schema' keys from FivetranCrawler
            
        Returns:
            Formatted context string relevant to the section
        """
        parts = []
        setup = fivetran_context.get('setup', {})
        overview = fivetran_context.get('overview', {})
        schema = fivetran_context.get('schema', {})
        
        # Section 1: Product Overview - Use overview features
        if section_number == 1:
            if overview.get('supported_features'):
                features = [f"{k.replace('_', ' ').title()}: {'Yes' if v else 'No'}" 
                           for k, v in overview['supported_features'].items()]
                parts.append(f"**Fivetran Supported Features:**\n{', '.join(features)}")
            if overview.get('sync_overview'):
                parts.append(f"**Fivetran Sync Overview:**\n{overview['sync_overview'][:1500]}")
        
        # Section 3: Pre-Call Config - Use setup prerequisites
        elif section_number == 3:
            if setup.get('prerequisites'):
                parts.append(f"**Fivetran Prerequisites:**")
                for prereq in setup['prerequisites'][:10]:
                    parts.append(f"  - {prereq}")
        
        # Section 5: Authentication - Use setup auth methods and instructions
        elif section_number == 5:
            if setup.get('auth_methods'):
                parts.append(f"**Fivetran Auth Methods:**\n{', '.join(setup['auth_methods'])}")
            if setup.get('auth_instructions'):
                parts.append(f"**Fivetran Auth Instructions:**\n{setup['auth_instructions'][:2000]}")
        
        # Section 6: App Registration - Use setup auth instructions
        elif section_number == 6:
            if setup.get('auth_instructions'):
                parts.append(f"**Fivetran Setup Instructions:**\n{setup['auth_instructions'][:1500]}")
        
        # Section 7: Metadata Discovery - Use schema objects
        elif section_number == 7:
            if schema.get('supported_objects'):
                parts.append(f"**Fivetran Supported Objects ({len(schema['supported_objects'])}):**")
                parts.append(f"{', '.join(schema['supported_objects'][:50])}")
            if schema.get('unsupported_objects'):
                parts.append(f"**Fivetran Unsupported Objects:**\n{', '.join(schema['unsupported_objects'][:20])}")
            if schema.get('permissions_required'):
                parts.append(f"**Fivetran Permissions Required:**")
                for obj, perms in list(schema['permissions_required'].items())[:10]:
                    parts.append(f"  - {obj}: {', '.join(perms)}")
        
        # Section 8: Sync Strategies - Use overview sync details and schema object sync modes
        elif section_number == 8:
            if overview.get('incremental_sync_details'):
                parts.append(f"**Fivetran Incremental Sync:**\n{overview['incremental_sync_details'][:1500]}")
            if overview.get('historical_sync_timeframe'):
                parts.append(f"**Fivetran Historical Sync Timeframe:** {overview['historical_sync_timeframe']}")
            if schema.get('objects'):
                incremental_objs = [o['name'] for o in schema['objects'] if o.get('sync_mode') == 'incremental']
                full_objs = [o['name'] for o in schema['objects'] if o.get('sync_mode') == 'full_load']
                if incremental_objs:
                    parts.append(f"**Fivetran Incremental Objects:** {', '.join(incremental_objs[:20])}")
                if full_objs:
                    parts.append(f"**Fivetran Full Load Objects:** {', '.join(full_objs[:20])}")
        
        # Section 11: Deletion Handling - Use overview capture_deletes feature
        elif section_number == 11:
            if overview.get('supported_features'):
                capture_deletes = overview['supported_features'].get('capture_deletes')
                if capture_deletes is not None:
                    parts.append(f"**Fivetran Capture Deletes:** {'Supported' if capture_deletes else 'Not Supported'}")
        
        # Section 17: Relationships - Use schema parent-child relationships
        elif section_number == 17:
            if schema.get('parent_child_relationships'):
                parts.append(f"**Fivetran Parent-Child Relationships:**")
                for parent, child in schema['parent_child_relationships'][:20]:
                    parts.append(f"  - {parent} → {child}")
        
        # Section 18: Troubleshooting - Use overview limitations
        elif section_number == 18:
            if overview.get('sync_limitations'):
                parts.append(f"**Fivetran Known Limitations:**")
                for lim in overview['sync_limitations'][:10]:
                    parts.append(f"  - {lim}")
        
        # Section 19: Available Objects & Replication Guide - Comprehensive object catalog
        elif section_number == 19:
            # Provide detailed Fivetran object information for the catalog table
            if schema.get('supported_objects'):
                parts.append(f"**Fivetran Supported Objects ({len(schema['supported_objects'])} total):**")
                parts.append(f"Objects: {', '.join(schema['supported_objects'])}")
            
            if schema.get('unsupported_objects'):
                parts.append(f"\n**Fivetran Unsupported Objects ({len(schema['unsupported_objects'])} total):**")
                parts.append(f"Objects: {', '.join(schema['unsupported_objects'])}")
            
            if schema.get('objects'):
                # Build detailed object info for the table
                parts.append(f"\n**Fivetran Object Details (for table columns):**")
                for obj in schema['objects'][:50]:
                    obj_name = obj.get('name', 'Unknown')
                    sync_mode = obj.get('sync_mode', 'Unknown')
                    parent = obj.get('parent', '-')
                    cursor = obj.get('cursor_field', '-')
                    delete_method = obj.get('delete_method', 'Unknown')
                    parts.append(f"  - {obj_name}: sync_mode={sync_mode}, parent={parent}, cursor={cursor}, delete_method={delete_method}")
            
            # Include capture_deletes feature from overview
            if overview.get('supported_features'):
                capture_deletes = overview['supported_features'].get('capture_deletes')
                if capture_deletes is not None:
                    parts.append(f"\n**Fivetran Capture Deletes Feature:** {'Supported' if capture_deletes else 'Not Supported'}")
            
            if schema.get('parent_child_relationships'):
                parts.append(f"\n**Fivetran Parent-Child Relationships:**")
                for parent, child in schema['parent_child_relationships'][:30]:
                    parts.append(f"  - {parent} → {child}")
            
            if schema.get('permissions_required'):
                parts.append(f"\n**Fivetran Permissions by Object:**")
                for obj, perms in list(schema['permissions_required'].items())[:20]:
                    parts.append(f"  - {obj}: {', '.join(perms)}")
            
            # Include overview sync info
            if overview.get('incremental_sync_details'):
                parts.append(f"\n**Fivetran Incremental Sync Details:**")
                parts.append(overview['incremental_sync_details'][:1500])
            
            if overview.get('supported_features'):
                features = [f"{k.replace('_', ' ').title()}: {'Yes' if v else 'No'}" 
                           for k, v in overview['supported_features'].items()]
                parts.append(f"\n**Fivetran Supported Features:**\n{', '.join(features)}")
        
        return "\n\n".join(parts) if parts else ""
    
    async def _generate_section(
        self,
        section: ResearchSection,
        connector_name: str,
        connector_type: str,
        github_context: str = "",
        fivetran_context: str = "",
        structured_context: Optional[Dict[str, Any]] = None
    ) -> str:
        """Generate content for a single section.
        
        Args:
            section: Section definition
            connector_name: Name of connector
            connector_type: Type of connector
            github_context: Context from GitHub code analysis (legacy flat format)
            fivetran_context: Context from Fivetran comparison
            structured_context: Structured context with implementation, sdk, and documentation
            
        Returns:
            Generated markdown content
        """
        # Build search query
        search_query = f"{connector_name} API {section.name} documentation 2024 2025"
        web_results = await self._web_search(search_query)
        
        # Build the prompt
        prompts_text = "\n".join(f"- {p.format(connector=connector_name)}" for p in section.prompts)
        
        # Special system prompt for Section 19 (Object Catalog)
        if section.number == 19:
            system_prompt = """You are an expert technical writer specializing in data integration and ETL connector development.
Your task is to create a comprehensive Object Catalog for connector research.

CRITICAL OUTPUT FORMAT REQUIREMENTS:
1. Start with a markdown table listing ALL available objects with these exact columns:
   | Object | Extraction Method | Primary Key | Cursor Field | Parent | Permissions | Delete Method | Fivetran Support |
   
2. The table should include:
   - Object: Name of the entity/object (e.g., accounts, contacts, orders)
   - Extraction Method: Exact API endpoint or method (e.g., "GET /v1/accounts", "GraphQL query accounts", "SOAP GetAccounts")
   - Primary Key: The unique identifier field (e.g., id, account_id)
   - Cursor Field: Field for incremental sync (e.g., updated_at, modified_date) or "-" if full load only
   - Parent: Parent object name if this is a child entity, or "-" if top-level
   - Permissions: Required scopes/permissions (e.g., read:accounts, accounts.read)
   - Delete Method: How to detect deleted records. Use one of:
     * "Soft Delete (field_name)" - e.g., "Soft Delete (is_deleted)", "Soft Delete (deleted_at)"
     * "Deleted Endpoint" - API provides GET /deleted_records or similar
     * "Webhook (event_name)" - e.g., "Webhook (record.deleted)"
     * "Audit Log" - Deletions tracked in audit/activity endpoint
     * "None" - Hard deletes only, no detection available
   - Fivetran Support: "✓" if supported by Fivetran, "✗" if not, or "?" if unknown

3. After the table, include:
   - Replication Strategy Notes: List objects by category (Full Load Only, Incremental, CDC-capable)
   - Delete Detection Summary: Group objects by delete method
   - Sample Extraction Code: Python code example for 2-3 key objects with pagination
   - Volume Considerations: Rate limits or pagination specific to high-volume objects

4. Include inline citations like [web:1], [web:2] referencing web search results
5. If Fivetran context is provided, prioritize that for the Fivetran Support column
6. List at least 15-30 objects if available, or all objects if fewer exist
"""
        else:
            system_prompt = """You are an expert technical writer specializing in data integration and ETL connector development.
Your task is to write detailed, production-grade documentation for connector research.

Requirements:
- Write 8-10 detailed sentences per subsection
- Include exact values from documentation (OAuth scopes, permissions, rate limits)
- Use markdown tables where appropriate
- Include inline citations like [web:1], [web:2] referencing web search results
- When structured context is provided (from Connector_Code, Connector_SDK, Public_Documentation), prioritize that information
- Focus on data extraction (read operations), not write operations
- If information is not available, explicitly state "N/A - not documented" or "N/A - not supported"
"""

        # Build section-specific context from structured data
        section_context = ""
        if structured_context:
            section_context = self._build_section_context(section.number, structured_context)

        # Special user prompt for Section 19 (Object Catalog)
        if section.number == 19:
            user_prompt = f"""Generate Section {section.number}: {section.name} for the {connector_name} connector research document.

Connector Type: {connector_type}
Phase: {section.phase_name}

IMPORTANT: This section MUST start with a comprehensive markdown table of ALL available objects.

Questions to answer:
{prompts_text}

Web Search Results:
{web_results}

{f"GitHub Code Analysis Context:{chr(10)}{github_context}" if github_context else ""}
{f"Fivetran Comparison Context (use for Fivetran Support column):{chr(10)}{fivetran_context}" if fivetran_context else ""}
{f"Structured Repository Context:{chr(10)}{section_context}" if section_context else ""}

OUTPUT FORMAT REQUIRED:

### 19.1 Object Catalog Table

| Object | Extraction Method | Primary Key | Cursor Field | Parent | Permissions | Delete Method | Fivetran Support |
|--------|-------------------|-------------|--------------|--------|-------------|---------------|------------------|
| (list all objects here - include Delete Method for each: Soft Delete (field), Deleted Endpoint, Webhook (event), Audit Log, or None) |

### 19.2 Replication Strategy Notes

**Full Load Objects:** (list objects with no cursor field)
**Incremental Objects:** (list objects with cursor fields)
**CDC-Capable Objects:** (list objects with real-time change tracking if any)

### 19.3 Delete Detection Summary

**Soft Delete:** (list objects with soft delete flag - specify field name)
**Deleted Endpoint:** (list objects with dedicated deleted records endpoint)
**Webhook:** (list objects with delete webhook events)
**Audit Log:** (list objects tracked via audit log)
**No Delete Detection:** (list objects with hard deletes only)

### 19.4 Sample Extraction Code

```python
# Python code example for extracting 2-3 key objects with pagination
# Include example for detecting deleted records if applicable
```

### 19.5 Volume Considerations

(Rate limits, pagination limits, high-volume object notes)
"""
        else:
            user_prompt = f"""Generate Section {section.number}: {section.name} for the {connector_name} connector research document.

Connector Type: {connector_type}
Phase: {section.phase_name}

Questions to answer:
{prompts_text}

Web Search Results:
{web_results}

{f"GitHub Code Analysis Context:{chr(10)}{github_context}" if github_context else ""}
{f"Fivetran Comparison Context:{chr(10)}{fivetran_context}" if fivetran_context else ""}
{f"Structured Repository Context:{chr(10)}{section_context}" if section_context else ""}

Generate comprehensive markdown content for this section. Include:
1. Clear subsection headers (e.g., {section.number}.1, {section.number}.2)
2. Detailed explanations with citations
3. Tables where appropriate (objects, limits, permissions)
4. Code examples if relevant
5. Exact values from documentation (no placeholders)
"""

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.3,
                max_tokens=3000
            )
            
            content = response.choices[0].message.content
            
            # Phase emoji mapping
            phase_emojis = {
                1: "🔍",  # Understand the Platform
                2: "🔐",  # Data Access Mechanisms
                3: "🔄",  # Sync Design & Extraction
                4: "⚡",  # Reliability & Performance
                5: "🔧",  # Advanced Considerations
                6: "🛠️",  # Troubleshooting
                7: "📋",  # Object Catalog
            }
            phase_emoji = phase_emojis.get(section.phase, "📄")
            
            # Format as markdown section with improved layout
            formatted = f"""

---

# {phase_emoji} Phase {section.phase}: {section.phase_name}

## {section.number}. {section.name}

{content}

<details>
<summary>📌 Section Metadata</summary>

- Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}
- Source: Web search + AI synthesis

</details>

[↑ Back to Summary](#-quick-summary-dashboard)

"""
            return formatted
            
        except Exception as e:
            return f"""

---

## {section.number}. {section.name}

**Error generating section:** {str(e)}

---
"""
    
    def _parse_discovered_methods(self, discovery_content: str) -> List[str]:
        """Parse the discovery section content to extract available methods.
        
        Args:
            discovery_content: The generated content for section 2 (Extraction Methods Discovery)
            
        Returns:
            List of method names that were found to be available
        """
        discovered = []
        content_lower = discovery_content.lower()
        
        # Check for each method
        method_indicators = {
            "REST API": ["rest api", "rest endpoint", "restful", "rest-based", "/api/v"],
            "GraphQL API": ["graphql", "graph ql", "graphql endpoint", "graphql api"],
            "SOAP/XML API": ["soap", "wsdl", "xml api", "soap endpoint"],
            "Webhooks": ["webhook", "web hook", "event subscription", "push notification"],
            "Bulk/Batch API": ["bulk api", "batch api", "async export", "bulk export", "batch request"],
            "Official SDK": ["official sdk", "sdk available", "python sdk", "java sdk", "node sdk", "client library"],
            "JDBC/ODBC": ["jdbc", "odbc", "database driver", "direct database"],
            "File Export": ["file export", "csv export", "data export", "export to file", "downloadable report"]
        }
        
        for method, indicators in method_indicators.items():
            # Check if the method is mentioned positively (not "not available")
            for indicator in indicators:
                if indicator in content_lower:
                    # Check it's not marked as unavailable
                    idx = content_lower.find(indicator)
                    surrounding = content_lower[max(0, idx-50):idx+100]
                    if "not available" not in surrounding and "unavailable" not in surrounding and "does not" not in surrounding and "no " + indicator not in surrounding:
                        if method not in discovered:
                            discovered.append(method)
                        break
        
        # If nothing found, default to REST API (most common)
        if not discovered:
            discovered = ["REST API"]
        
        return discovered
    
    async def generate_research(
        self,
        connector_id: str,
        connector_name: str,
        connector_type: str = "auto",
        github_context: Optional[Dict[str, Any]] = None,
        fivetran_context: Optional[Dict[str, Any]] = None,
        on_progress: Optional[Callable[[ResearchProgress], None]] = None
    ) -> str:
        """Generate complete research document for a connector with dynamic method discovery.
        
        Args:
            connector_id: Connector ID
            connector_name: Connector display name
            connector_type: Type of connector (default "auto" for discovery)
            github_context: Optional extracted code patterns from GitHub
            fivetran_context: Optional Fivetran documentation context for parity comparison
            on_progress: Optional callback for progress updates
            
        Returns:
            Complete research document as markdown
        """
        self._cancel_requested = False
        self._current_progress = ResearchProgress(
            connector_id=connector_id,
            connector_name=connector_name,
            status="running"
        )
        
        # Detect if we have structured context
        is_structured = github_context and github_context.get('structure_type') == 'structured'
        structured_context = None
        
        if is_structured:
            structured_context = {
                'implementation': github_context.get('implementation', {}),
                'sdk': github_context.get('sdk', {}),
                'documentation': github_context.get('documentation', {})
            }
        
        # Build research method description
        research_method_parts = ["Automated generation using web search"]
        if github_context:
            if is_structured:
                research_method_parts.append("structured repository analysis")
            else:
                research_method_parts.append("GitHub code analysis")
        if fivetran_context:
            research_method_parts.append("Fivetran documentation parity analysis")
        
        # Prepare GitHub context string
        github_context_str = self._build_github_context_string(github_context, is_structured, structured_context)
        
        # Initialize document with header (section count will be updated later)
        document_parts = []
        discovered_methods = []
        
        # ========================================
        # PHASE 1: Discovery Sections (1-3)
        # ========================================
        print(f"  Phase 1: Platform Discovery")
        
        discovery_content = ""
        for section in BASE_SECTIONS:
            if self._cancel_requested:
                self._current_progress.status = "cancelled"
                break
            
            self._current_progress.current_section = section.number
            self._current_progress.current_content = f"Generating Section {section.number}: {section.name}..."
            
            if on_progress:
                on_progress(self._current_progress)
            
            # Generate section
            section_content = await self._generate_section(
                section=section,
                connector_name=connector_name,
                connector_type=connector_type,
                github_context=github_context_str if section.requires_code_analysis else "",
                fivetran_context="",
                structured_context=structured_context
            )
            
            # Save discovery section content for parsing
            if section.number == 2:
                discovery_content = section_content
            
            document_parts.append(section_content)
            self._current_progress.sections_completed.append(section.number)
            await asyncio.sleep(1)
        
        # Parse discovered methods from Section 2
        discovered_methods = self._parse_discovered_methods(discovery_content)
        self._current_progress.discovered_methods = discovered_methods
        print(f"  Discovered extraction methods: {', '.join(discovered_methods)}")
        
        # Calculate total sections
        total_sections = len(BASE_SECTIONS) + len(discovered_methods) + len(CROSS_CUTTING_SECTIONS) + len(FINAL_SECTIONS)
        self._current_progress.total_sections = total_sections
        
        # ========================================
        # PHASE 2: Per-Method Deep Dives (Dynamic)
        # ========================================
        print(f"  Phase 2: Extraction Methods ({len(discovered_methods)} methods)")
        
        method_section_number = 4  # Start after base sections
        for method in discovered_methods:
            if self._cancel_requested:
                self._current_progress.status = "cancelled"
                break
            
            method_section = create_method_section(method, method_section_number)
            
            self._current_progress.current_section = method_section_number
            self._current_progress.current_content = f"Generating Section {method_section_number}: {method} Deep Dive..."
            
            if on_progress:
                on_progress(self._current_progress)
            
            section_content = await self._generate_section(
                section=method_section,
                connector_name=connector_name,
                connector_type=connector_type,
                github_context=github_context_str,
                fivetran_context="",
                structured_context=structured_context
            )
            
            document_parts.append(section_content)
            self._current_progress.sections_completed.append(method_section_number)
            method_section_number += 1
            await asyncio.sleep(1)
        
        # ========================================
        # PHASE 3: Cross-Cutting Concerns
        # ========================================
        print(f"  Phase 3: Cross-Cutting Concerns")
        
        # Prepare methods list for cross-cutting context
        methods_context = f"Available extraction methods for {connector_name}: {', '.join(discovered_methods)}"
        
        for i, section in enumerate(CROSS_CUTTING_SECTIONS):
            if self._cancel_requested:
                self._current_progress.status = "cancelled"
                break
            
            actual_section_number = method_section_number + i
            section_copy = ResearchSection(
                number=actual_section_number,
                name=section.name,
                phase=section.phase,
                phase_name=section.phase_name,
                prompts=section.prompts,
                requires_fivetran=section.requires_fivetran,
                requires_code_analysis=section.requires_code_analysis
            )
            
            self._current_progress.current_section = actual_section_number
            self._current_progress.current_content = f"Generating Section {actual_section_number}: {section.name}..."
            
            if on_progress:
                on_progress(self._current_progress)
            
            # Build Fivetran context
            section_fivetran_context = ""
            if fivetran_context and section.requires_fivetran:
                section_fivetran_context = self._build_fivetran_section_context(section.number, fivetran_context)
            
            section_content = await self._generate_section(
                section=section_copy,
                connector_name=connector_name,
                connector_type=connector_type,
                github_context=github_context_str + "\n\n" + methods_context if section.requires_code_analysis else methods_context,
                fivetran_context=section_fivetran_context,
                structured_context=structured_context
            )
            
            document_parts.append(section_content)
            self._current_progress.sections_completed.append(actual_section_number)
            await asyncio.sleep(1)
        
        # ========================================
        # PHASE 4: Implementation Guide
        # ========================================
        print(f"  Phase 4: Implementation Guide")
        
        final_section_start = method_section_number + len(CROSS_CUTTING_SECTIONS)
        for i, section in enumerate(FINAL_SECTIONS):
            if self._cancel_requested:
                self._current_progress.status = "cancelled"
                break
            
            actual_section_number = final_section_start + i
            section_copy = ResearchSection(
                number=actual_section_number,
                name=section.name,
                phase=section.phase,
                phase_name=section.phase_name,
                prompts=section.prompts,
                requires_fivetran=section.requires_fivetran,
                requires_code_analysis=section.requires_code_analysis
            )
            
            self._current_progress.current_section = actual_section_number
            self._current_progress.current_content = f"Generating Section {actual_section_number}: {section.name}..."
            
            if on_progress:
                on_progress(self._current_progress)
            
            # Build Fivetran context - map final sections to original section logic
            section_fivetran_context = ""
            if fivetran_context and section.requires_fivetran:
                # Map final section names to original section numbers for Fivetran context
                fivetran_section_map = {
                    200: 8,   # Recommended Extraction Strategy -> Sync Strategies
                    201: 19,  # Object Catalog & Replication Guide -> Object Catalog (Section 19)
                    202: 16,  # Production Checklist -> Operational Test Data
                }
                mapped_section = fivetran_section_map.get(section.number, section.number)
                section_fivetran_context = self._build_fivetran_section_context(mapped_section, fivetran_context)
            
            section_content = await self._generate_section(
                section=section_copy,
                connector_name=connector_name,
                connector_type=connector_type,
                github_context=github_context_str + "\n\n" + methods_context if section.requires_code_analysis else methods_context,
                fivetran_context=section_fivetran_context,
                structured_context=structured_context
            )
            
            document_parts.append(section_content)
            self._current_progress.sections_completed.append(actual_section_number)
            await asyncio.sleep(1)
        
        # ========================================
        # Build Final Document
        # ========================================
        
        # Create document header with accurate section count
        header = f"""# 📚 Connector Research: {connector_name}

**Subject:** {connector_name} Connector - Full Production Research  
**Status:** Complete  
**Generated:** {datetime.utcnow().strftime('%Y-%m-%d')}  
**Total Sections:** {total_sections}  
**Discovered Methods:** {', '.join(discovered_methods)}

---

## 📝 Research Overview

**Goal:** Produce exhaustive, production-grade research on building a data connector for {connector_name}.

**Extraction Methods Discovered:** {len(discovered_methods)} ({', '.join(discovered_methods)})

**Research Method:** {' and '.join(research_method_parts)}

{f"**Repository Structure:** Structured (Connector_Code, Connector_SDK, Public_Documentation)" if is_structured else ""}

---

## 📑 Document Structure

| Phase | Sections | Content |
|-------|----------|---------|
| 1. Platform Discovery | 1-3 | Overview, Methods Discovery, Dev Environment |
| 2. Extraction Methods | 4-{3 + len(discovered_methods)} | Deep dive for each discovered method |
| 3. Cross-Cutting | {4 + len(discovered_methods)}-{3 + len(discovered_methods) + len(CROSS_CUTTING_SECTIONS)} | Auth, Rate Limits, Errors, Data Model, Deletes |
| 4. Implementation | {4 + len(discovered_methods) + len(CROSS_CUTTING_SECTIONS)}-{total_sections} | Strategy, Object Catalog, Checklist |

---
"""
        
        # Generate Quick Summary Dashboard
        quick_summary = self._generate_quick_summary(
            connector_name=connector_name,
            connector_type=', '.join(discovered_methods) if discovered_methods else "auto",
            github_context=github_context,
            fivetran_context=fivetran_context
        )
        
        # Combine all parts
        full_document = header + quick_summary + '\n'.join(document_parts)
        
        # Add final deliverables section
        final_section = f"""

---

# ✅ Final Deliverables

## 🎯 Production Recommendations

| Priority | Recommendation |
|----------|----------------|
| **Critical** | Implement exponential backoff for rate limit handling |
| **Critical** | Implement proper OAuth token refresh before expiration |
| **Critical** | Handle pagination consistently across all objects |
| **High** | Use incremental sync with cursor where available |
| **High** | Implement delete detection mechanism |
| **High** | Set appropriate timeouts for long-running operations |
| **Medium** | Use bulk APIs for historical loads when available |
| **Medium** | Implement proper error categorization |
| **Medium** | Monitor API usage against quotas |
| **Low** | Document all custom field mappings |

---

## ☑️ Implementation Checklist

| Task | Status | Notes |
|------|--------|-------|
| Authentication configured and tested | ⬜ | |
| Rate limiting implemented with backoff | ⬜ | |
| Error handling with retry logic | ⬜ | |
| Incremental sync with cursor fields | ⬜ | |
| Delete detection mechanism | ⬜ | |
| Custom fields discovery | ⬜ | |
| Parent-child load ordering | ⬜ | |
| Monitoring and alerting | ⬜ | |
| Documentation complete | ⬜ | |

---

## 📚 Sources and Methodology

This research document was generated using:

| Source | Description |
|--------|-------------|
| **Tavily API** | Web search for official documentation |
| **OpenAI GPT-4** | AI synthesis and analysis |
"""
        
        if github_context:
            repo_url = github_context.get('repo_url', 'N/A')
            if is_structured:
                final_section += f"""| **GitHub Repository** | `{repo_url}` |
| | Connector_Code: Implementation patterns |
| | Connector_SDK: SDK methods, data types |
| | Public_Documentation: API reference |
"""
            else:
                final_section += f"| **GitHub Repository** | `{repo_url}` |\n"
        
        if fivetran_context:
            fivetran_sources = []
            if fivetran_context.get('has_setup'):
                fivetran_sources.append("Setup Guide")
            if fivetran_context.get('has_overview'):
                fivetran_sources.append("Connector Overview")
            if fivetran_context.get('has_schema'):
                fivetran_sources.append("Schema Information")
            final_section += f"| **Fivetran Docs** | {', '.join(fivetran_sources)} |\n"
        
        final_section += f"""

---

## 🏷️ Document Info

| | |
|---|---|
| **Generated By** | Connector Research Agent |
| **Generated On** | {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} |
| **Total Sections** | {total_sections} |
| **Discovered Methods** | {', '.join(discovered_methods)} |
| **Version** | 2.0 (Dynamic Discovery) |

---

*End of Document*
"""
        
        full_document += final_section
        
        # Update final status
        if not self._cancel_requested:
            self._current_progress.status = "completed"
            self._current_progress.current_content = full_document
        
        if on_progress:
            on_progress(self._current_progress)
        
        return full_document
    
    def _build_github_context_string(
        self,
        github_context: Optional[Dict[str, Any]],
        is_structured: bool,
        structured_context: Optional[Dict[str, Any]]
    ) -> str:
        """Build GitHub context string for prompts.
        
        Args:
            github_context: GitHub context dict
            is_structured: Whether repo has structured format
            structured_context: Structured context dict
            
        Returns:
            Formatted context string
        """
        if not github_context:
            return ""
        
        if not is_structured:
            return f"""
Repository: {github_context.get('repo_url', 'N/A')}
Languages: {', '.join(github_context.get('languages_detected', []))}
Objects Found: {', '.join(github_context.get('object_types', [])[:20])}
API Endpoints: {', '.join(github_context.get('api_endpoints', [])[:10])}
Auth Patterns: {', '.join(github_context.get('auth_patterns', []))}
"""
        else:
            impl = structured_context.get('implementation', {}) if structured_context else {}
            sdk = structured_context.get('sdk', {}) if structured_context else {}
            docs = structured_context.get('documentation', {}) if structured_context else {}
            return f"""
Repository: {github_context.get('repo_url', 'N/A')}
Structure: Structured Repository Format
SDK Name: {sdk.get('sdk_name', 'N/A')}
Implementation Models: {len(impl.get('models', []))} found
SDK Methods: {len(sdk.get('available_methods', []))} found
SDK Data Types: {len(sdk.get('data_types', []))} found
Documentation Endpoints: {len(docs.get('endpoints_list', []))} documented
"""


# Singleton instance
_agent: Optional[ResearchAgent] = None


def get_research_agent() -> ResearchAgent:
    """Get the singleton ResearchAgent instance."""
    global _agent
    if _agent is None:
        _agent = ResearchAgent()
    return _agent
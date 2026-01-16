"""
Research Agent Service
Auto-generates 19-section connector research documents.
"""

import os
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


# Define all 19 sections
RESEARCH_SECTIONS = [
    # Phase 1: Understand the Platform
    ResearchSection(1, "Product Overview", 1, "Understand the Platform", [
        "What does {connector} do? Describe its purpose, target users, and main functionality.",
        "What are the key modules and features?",
        "What types of data entities does it store?",
        "Does it have reporting/analytics modules?",
        "What are the limitations of its data model?"
    ]),
    ResearchSection(2, "Sandbox/Dev Environments", 1, "Understand the Platform", [
        "Does {connector} provide sandbox or developer environments?",
        "How do you request sandbox access (self-service, sales, partner)?",
        "Is sandbox permanent or temporary? What are refresh rules?",
        "What are alternatives if sandbox is paid or limited?"
    ]),
    ResearchSection(3, "Pre-Call Configurations", 1, "Understand the Platform", [
        "What prerequisites must be configured before API access works?",
        "What feature toggles need to be enabled?",
        "What integration/app registrations are required?",
        "Are there IP whitelists or redirect URI requirements?",
        "Provide a minimal health check code example."
    ]),
    
    # Phase 2: Data Access Mechanisms
    ResearchSection(4, "Data Access Mechanisms", 2, "Data Access Mechanisms", [
        "What data access methods are available (REST, GraphQL, SOAP, JDBC, SDK, Webhooks)?",
        "For each method, what are the rate limits and auth types?",
        "Which method is best for historical extraction?",
        "Which method is best for incremental sync?",
        "Which method is best for high-volume analytics?"
    ]),
    ResearchSection(5, "Authentication Mechanics", 2, "Data Access Mechanisms", [
        "What authentication methods are supported (OAuth 2.0, API Key, etc.)?",
        "What are the exact OAuth scopes required for data extraction?",
        "What roles/permissions are required? List exact permission names.",
        "Provide Java/Python code examples for authentication."
    ]),
    ResearchSection(6, "App Registration & User Consent", 2, "Data Access Mechanisms", [
        "What are the step-by-step instructions to register an app/integration?",
        "How do you configure callback URLs and secrets?",
        "How does multi-tenant consent work?",
        "Can one app be used across multiple customer accounts?"
    ]),
    ResearchSection(7, "Metadata Discovery & Schema Introspection", 2, "Data Access Mechanisms", [
        "What objects/entities are available? Create a catalog table.",
        "Are there OpenAPI/WSDL schema definitions available?",
        "How do you discover custom fields?",
        "How do you use REST metadata endpoints or JDBC DatabaseMetaData?"
    ], requires_fivetran=True, requires_code_analysis=True),
    
    # Phase 3: Sync Design & Extraction
    ResearchSection(8, "Sync Strategies", 3, "Sync Design & Extraction", [
        "For each object, what cursor field should be used for incremental sync?",
        "What window strategies work best (time-based, ID-based)?",
        "What load modes are supported (full load, incremental, CDC)?",
        "Is reverse-historical sync recommended?"
    ]),
    ResearchSection(9, "Bulk Extraction & Billions of Rows", 3, "Sync Design & Extraction", [
        "What bulk/async APIs or export mechanisms are available?",
        "What are pagination rules and cursor fields?",
        "What are the max records per request?",
        "For JDBC, what streaming properties should be set (fetchSize, etc.)?"
    ]),
    ResearchSection(10, "Async Capabilities, Job Queues & Webhooks", 3, "Sync Design & Extraction", [
        "What async job mechanisms exist (bulk jobs, export tasks, reports)?",
        "How do you poll for job status?",
        "What webhook events are available?",
        "Can webhooks be used for incremental sync and delete detection?"
    ]),
    ResearchSection(11, "Deletion Handling", 3, "Sync Design & Extraction", [
        "How are deletions represented (hard delete, soft delete, archive)?",
        "Is there a deleted items endpoint?",
        "Can deletions be detected via webhooks?",
        "Are audit logs or tombstone tables available?"
    ]),
    
    # Phase 4: Reliability & Performance
    ResearchSection(12, "Rate Limits, Quotas & Concurrency", 4, "Reliability & Performance", [
        "What are the exact rate limits (per minute, hour, day)?",
        "Are limits per user, per account, or per app?",
        "What are concurrency limits for API calls?",
        "What is the recommended concurrency for bulk extraction?"
    ]),
    ResearchSection(13, "API Failure Types & Retry Strategy", 4, "Reliability & Performance", [
        "What error codes indicate retryable errors?",
        "What error codes indicate non-retryable errors?",
        "What errors require re-authentication?",
        "What retry strategy is recommended?"
    ]),
    ResearchSection(14, "Timeouts", 4, "Reliability & Performance", [
        "What are the default timeout settings?",
        "What are API-specific execution limits?",
        "What are empirical limits observed by the community?",
        "What JDBC driver timeouts should be configured?"
    ]),
    
    # Phase 5: Advanced Considerations
    ResearchSection(15, "Dependencies, Drivers & SDK Versions", 5, "Advanced Considerations", [
        "What official SDKs are available (Java, Python, Node)?",
        "What JDBC/ODBC drivers are available?",
        "What are the version compatibility requirements?",
        "Provide Maven/pip install instructions."
    ]),
    ResearchSection(16, "Operational Test Data & Runbooks", 5, "Advanced Considerations", [
        "How do you generate test data for historical loads?",
        "How do you insert, update, and delete test records?",
        "How do you test custom fields/objects?",
        "Which objects cannot have realistic test data generated?"
    ]),
    ResearchSection(17, "Relationships, Refresher Tasks & Multi-Account", 5, "Advanced Considerations", [
        "What parent-child relationships exist between objects?",
        "What is the correct load order for related objects?",
        "Is a refresher task required for attribution windows?",
        "How does multi-account setup work?"
    ]),
    
    # Phase 6: Troubleshooting
    ResearchSection(18, "Common Issues & Troubleshooting", 6, "Troubleshooting", [
        "What are the top 10 common issues encountered?",
        "What are typical auth failures and their resolutions?",
        "What pagination issues commonly occur?",
        "What timeout and rate limit issues occur?"
    ]),
    
    # Phase 7: Object Catalog
    ResearchSection(19, "Available Objects & Replication Guide", 7, "Object Catalog", [
        "List ALL available objects/entities that can be extracted from {connector}. Create a comprehensive catalog table.",
        "For each object, identify: Primary Key field, Cursor Field for incremental sync (e.g., updated_at, modified_date), Parent object if this is a child entity.",
        "For each object, specify the exact extraction method: REST endpoint (e.g., GET /v1/accounts), GraphQL query, SOAP operation, SDK method, or other mechanism.",
        "For each object, list required permissions/scopes needed to access that object.",
        "For each object, identify the delete detection method: Soft Delete (specify field like is_deleted, deleted_at), Deleted Endpoint (specify URL like GET /deleted_records), Webhook (specify event like record.deleted), Audit Log, or None if hard deletes only.",
        "Indicate if each object is supported by Fivetran (if Fivetran context is available). Mark with checkmark or 'Yes'/'No'.",
        "Categorize objects into: Full Load Only (no reliable cursor), Incremental (has cursor field), CDC-capable (real-time change tracking).",
        "Provide a sample Python code example showing how to extract records from 2-3 key objects with pagination.",
        "Note any rate limits, pagination limits, or volume considerations specific to high-volume objects."
    ], requires_fivetran=True, requires_code_analysis=True),
]


@dataclass
class ResearchProgress:
    """Tracks research generation progress."""
    connector_id: str
    connector_name: str
    current_section: int = 0
    total_sections: int = 19
    status: str = "idle"  # idle, running, completed, failed, cancelled
    sections_completed: List[int] = field(default_factory=list)
    current_content: str = ""
    error_message: str = ""


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
    
    async def generate_research(
        self,
        connector_id: str,
        connector_name: str,
        connector_type: str,
        github_context: Optional[Dict[str, Any]] = None,
        fivetran_context: Optional[Dict[str, Any]] = None,
        on_progress: Optional[Callable[[ResearchProgress], None]] = None
    ) -> str:
        """Generate complete research document for a connector.
        
        Args:
            connector_id: Connector ID
            connector_name: Connector display name
            connector_type: Type of connector
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
                research_method_parts.append("structured repository analysis (Connector_Code, Connector_SDK, Public_Documentation)")
            else:
                research_method_parts.append("GitHub code analysis")
        if fivetran_context:
            research_method_parts.append("Fivetran documentation parity analysis")
        
        # Initialize document with header
        document_parts = [f"""# 📚 Connector Research: {connector_name}

**Subject:** {connector_name} Connector - Full Production Research  
**Status:** Complete  
**Generated:** {datetime.utcnow().strftime('%Y-%m-%d')}  
**Sections:** 19 comprehensive research sections

---

## 📝 Research Overview

**Goal:** Produce exhaustive, production-grade research on building a data connector for {connector_name}.

**Connector Type:** {connector_type}

**Research Method:** {' and '.join(research_method_parts)}

{f"**Repository Structure:** Structured (Connector_Code, Connector_SDK, Public_Documentation)" if is_structured else ""}

---
"""]
        
        # Generate Quick Summary Dashboard
        quick_summary = self._generate_quick_summary(
            connector_name=connector_name,
            connector_type=connector_type,
            github_context=github_context,
            fivetran_context=fivetran_context
        )
        document_parts.append(quick_summary)
        
        # Prepare GitHub context string (for legacy flat format)
        github_context_str = ""
        if github_context and not is_structured:
            github_context_str = f"""
Repository: {github_context.get('repo_url', 'N/A')}
Languages: {', '.join(github_context.get('languages_detected', []))}
Objects Found: {', '.join(github_context.get('object_types', [])[:20])}
API Endpoints: {', '.join(github_context.get('api_endpoints', [])[:10])}
Auth Patterns: {', '.join(github_context.get('auth_patterns', []))}
"""
        elif github_context and is_structured:
            # Provide summary for structured repos
            impl = structured_context.get('implementation', {})
            sdk = structured_context.get('sdk', {})
            docs = structured_context.get('documentation', {})
            github_context_str = f"""
Repository: {github_context.get('repo_url', 'N/A')}
Structure: Structured Repository Format
SDK Name: {sdk.get('sdk_name', 'N/A')}
Implementation Models: {len(impl.get('models', []))} found
SDK Methods: {len(sdk.get('available_methods', []))} found
SDK Data Types: {len(sdk.get('data_types', []))} found
Documentation Endpoints: {len(docs.get('endpoints_list', []))} documented
"""
        
        # Generate each section
        for section in RESEARCH_SECTIONS:
            if self._cancel_requested:
                self._current_progress.status = "cancelled"
                break
            
            # Update progress
            self._current_progress.current_section = section.number
            self._current_progress.current_content = f"Generating Section {section.number}: {section.name}..."
            
            if on_progress:
                on_progress(self._current_progress)
            
            # Build Fivetran context for this section
            section_fivetran_context = ""
            if fivetran_context:
                # Use provided Fivetran documentation context
                section_fivetran_context = self._build_fivetran_section_context(section.number, fivetran_context)
            elif section.requires_fivetran:
                # Fallback to web search if no Fivetran URLs were provided
                fivetran_search = await self._web_search(
                    f"Fivetran {connector_name} connector ERD objects supported"
                )
                section_fivetran_context = fivetran_search
            
            # Generate section
            section_content = await self._generate_section(
                section=section,
                connector_name=connector_name,
                connector_type=connector_type,
                github_context=github_context_str if section.requires_code_analysis else "",
                fivetran_context=section_fivetran_context,
                structured_context=structured_context
            )
            
            document_parts.append(section_content)
            self._current_progress.sections_completed.append(section.number)
            
            # Small delay to avoid rate limits
            await asyncio.sleep(1)
        
        # Add final sections with improved formatting
        document_parts.append("""

---

# ✅ Final Deliverables

## 🎯 Production Recommendations

| Priority | Recommendation |
|----------|----------------|
| **Critical** | Implement exponential backoff for rate limit handling |
| **Critical** | Implement proper OAuth token refresh before expiration |
| **Critical** | Handle pagination consistently across all objects |
| **High** | Use incremental sync with lastModifiedDate cursor where available |
| **High** | Implement delete detection via soft delete flags or audit logs |
| **High** | Set appropriate timeouts for long-running operations |
| **Medium** | Use bulk APIs for historical loads when available |
| **Medium** | Implement proper error categorization (retryable vs non-retryable) |
| **Medium** | Monitor API usage against quotas |
| **Medium** | Test thoroughly with sandbox environment before production |
| **Low** | Document all custom field mappings |
| **Low** | Implement proper parent-child load ordering |

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
""")
        
        if github_context:
            repo_url = github_context.get('repo_url', 'N/A')
            if is_structured:
                document_parts.append(f"""| **GitHub Repository** | `{repo_url}` |
| | Connector_Code: Implementation patterns |
| | Connector_SDK: SDK methods, data types |
| | Public_Documentation: API reference |""")
            else:
                document_parts.append(f"| **GitHub Repository** | `{repo_url}` |")
        
        if fivetran_context:
            fivetran_sources = []
            if fivetran_context.get('has_setup'):
                fivetran_sources.append("Setup Guide")
            if fivetran_context.get('has_overview'):
                fivetran_sources.append("Connector Overview")
            if fivetran_context.get('has_schema'):
                fivetran_sources.append("Schema Information")
            document_parts.append(f"| **Fivetran Docs** | {', '.join(fivetran_sources)} |")
        
        document_parts.append(f"""

---

## 🏷️ Document Info

| | |
|---|---|
| **Generated By** | Connector Research Agent |
| **Generated On** | {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} |
| **Total Sections** | 19 |
| **Version** | 1.0 |

---

*End of Document*
""")
        
        # Combine all parts
        full_document = "\n".join(document_parts)
        
        # Update final status
        if not self._cancel_requested:
            self._current_progress.status = "completed"
            self._current_progress.current_content = full_document
        
        if on_progress:
            on_progress(self._current_progress)
        
        return full_document


# Singleton instance
_agent: Optional[ResearchAgent] = None


def get_research_agent() -> ResearchAgent:
    """Get the singleton ResearchAgent instance."""
    global _agent
    if _agent is None:
        _agent = ResearchAgent()
    return _agent

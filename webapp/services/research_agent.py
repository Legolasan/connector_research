"""
Research Agent Service
Auto-generates 18-section connector research documents.
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


# Define all 18 sections
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
]


@dataclass
class ResearchProgress:
    """Tracks research generation progress."""
    connector_id: str
    connector_name: str
    current_section: int = 0
    total_sections: int = 18
    status: str = "idle"  # idle, running, completed, failed, cancelled
    sections_completed: List[int] = field(default_factory=list)
    current_content: str = ""
    error_message: str = ""


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
    
    async def _generate_section(
        self,
        section: ResearchSection,
        connector_name: str,
        connector_type: str,
        github_context: str = "",
        fivetran_context: str = ""
    ) -> str:
        """Generate content for a single section.
        
        Args:
            section: Section definition
            connector_name: Name of connector
            connector_type: Type of connector
            github_context: Context from GitHub code analysis
            fivetran_context: Context from Fivetran comparison
            
        Returns:
            Generated markdown content
        """
        # Build search query
        search_query = f"{connector_name} API {section.name} documentation 2024 2025"
        web_results = await self._web_search(search_query)
        
        # Build the prompt
        prompts_text = "\n".join(f"- {p.format(connector=connector_name)}" for p in section.prompts)
        
        system_prompt = """You are an expert technical writer specializing in data integration and ETL connector development.
Your task is to write detailed, production-grade documentation for connector research.

Requirements:
- Write 8-10 detailed sentences per subsection
- Include exact values from documentation (OAuth scopes, permissions, rate limits)
- Use markdown tables where appropriate
- Include inline citations like [web:1], [web:2] referencing web search results
- Focus on data extraction (read operations), not write operations
- If information is not available, explicitly state "N/A - not documented" or "N/A - not supported"
"""

        user_prompt = f"""Generate Section {section.number}: {section.name} for the {connector_name} connector research document.

Connector Type: {connector_type}
Phase: {section.phase_name}

Questions to answer:
{prompts_text}

Web Search Results:
{web_results}

{f"GitHub Code Analysis Context:{chr(10)}{github_context}" if github_context else ""}
{f"Fivetran Comparison Context:{chr(10)}{fivetran_context}" if fivetran_context else ""}

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
            
            # Format as markdown section
            formatted = f"""
# Phase {section.phase} - {section.phase_name}

## {section.number}. {section.name}

{content}

---
*Section generated on {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} using web search*

"""
            return formatted
            
        except Exception as e:
            return f"""
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
        on_progress: Optional[Callable[[ResearchProgress], None]] = None
    ) -> str:
        """Generate complete research document for a connector.
        
        Args:
            connector_id: Connector ID
            connector_name: Connector display name
            connector_type: Type of connector
            github_context: Optional extracted code patterns from GitHub
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
        
        # Initialize document
        document_parts = [f"""# Connector Research: {connector_name}

**Subject:** {connector_name} Connector - Full Production Research  
**Status:** Complete  
**Generated:** {datetime.utcnow().strftime('%Y-%m-%d')}  

---

## Research Overview

**Goal:** Produce exhaustive, production-grade research on building a data connector for {connector_name}.

**Connector Type:** {connector_type}

**Research Method:** Automated generation using web search and {"GitHub code analysis" if github_context else "documentation only"}

---
"""]
        
        # Prepare GitHub context string
        github_context_str = ""
        if github_context:
            github_context_str = f"""
Repository: {github_context.get('repo_url', 'N/A')}
Languages: {', '.join(github_context.get('languages_detected', []))}
Objects Found: {', '.join(github_context.get('object_types', [])[:20])}
API Endpoints: {', '.join(github_context.get('api_endpoints', [])[:10])}
Auth Patterns: {', '.join(github_context.get('auth_patterns', []))}
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
            
            # Get Fivetran context if needed
            fivetran_context = ""
            if section.requires_fivetran:
                fivetran_search = await self._web_search(
                    f"Fivetran {connector_name} connector ERD objects supported"
                )
                fivetran_context = fivetran_search
            
            # Generate section
            section_content = await self._generate_section(
                section=section,
                connector_name=connector_name,
                connector_type=connector_type,
                github_context=github_context_str if section.requires_code_analysis else "",
                fivetran_context=fivetran_context
            )
            
            document_parts.append(section_content)
            self._current_progress.sections_completed.append(section.number)
            
            # Small delay to avoid rate limits
            await asyncio.sleep(1)
        
        # Add final sections
        document_parts.append("""
# Final Deliverables

## Production Recommendations

1. Implement exponential backoff for rate limit handling
2. Use incremental sync with lastModifiedDate cursor where available
3. Implement proper OAuth token refresh before expiration
4. Handle pagination consistently across all objects
5. Implement delete detection via soft delete flags or audit logs
6. Set appropriate timeouts for long-running operations
7. Use bulk APIs for historical loads when available
8. Implement proper error categorization (retryable vs non-retryable)
9. Monitor API usage against quotas
10. Test thoroughly with sandbox environment before production
11. Document all custom field mappings
12. Implement proper parent-child load ordering

## Implementation Checklist

- [ ] Authentication configured and tested
- [ ] Rate limiting implemented with backoff
- [ ] Error handling with retry logic
- [ ] Incremental sync with cursor fields
- [ ] Delete detection mechanism
- [ ] Custom fields discovery
- [ ] Parent-child load ordering
- [ ] Monitoring and alerting
- [ ] Documentation complete

---

## Sources and Methodology

This research document was generated using:
- Web search via Tavily API
- Official documentation analysis
""")
        
        if github_context:
            document_parts.append(f"- GitHub repository analysis: {github_context.get('repo_url', 'N/A')}")
        
        document_parts.append(f"""

---

*Document generated by Connector Research Agent on {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}*
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

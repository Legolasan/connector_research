"""
⚡ Celery Tasks for Research Generation
Capability-based tasks split by function, not section.

Task Types:
- WebSearchTask: Search for information (fan-out parallel)
- SourceFetchTask: Fetch/crawl URLs (fan-out parallel)
- SummarizeTask: Extract facts from sources (fan-out parallel)
- SynthesisTask: Combine facts into sections (limited parallel)
- EditorTask: Final synthesis supervisor (serial)
"""

import os
import json
import hashlib
import asyncio
from datetime import datetime
from typing import Dict, List, Any, Optional
from celery import group, chain, chord

from dotenv import load_dotenv

load_dotenv()

# Import Celery app
from .celery_app import celery_app

# Import services
from .artifact_store import (
    ArtifactStore, Artifact, Fact, 
    get_artifact_store, emit_progress
)
from .cache import ResearchCache, get_research_cache

# Environment
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "")


# ============================================================================
# Helper Functions
# ============================================================================

def run_async(coro):
    """Run async coroutine in sync context."""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


def tavily_search(query: str, max_results: int = 5) -> Dict[str, Any]:
    """Execute Tavily web search."""
    try:
        from tavily import TavilyClient
        client = TavilyClient(api_key=TAVILY_API_KEY)
        response = client.search(query, max_results=max_results)
        return response
    except Exception as e:
        return {"error": str(e), "results": []}


def extract_facts_llm(content: str, category: str) -> List[Dict[str, Any]]:
    """Extract structured facts from content using LLM."""
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        
        system_prompt = f"""You are an expert at extracting structured facts about APIs and data connectors.
        
Extract facts related to '{category}' from the given content.

For each fact, provide:
- claim: A single, specific factual statement
- confidence: A score from 0.0 to 1.0 based on how well the content supports the claim
- evidence_quote: A brief quote from the content that supports the claim

Return a JSON array of facts. If no relevant facts found, return an empty array.

Categories and what to look for:
- auth: Authentication methods, OAuth scopes, API keys, tokens
- rate_limit: Rate limits, throttling, quotas, request limits
- endpoint: API endpoints, URL patterns, HTTP methods
- object: Data objects, schemas, entities, fields
- sdk: SDKs, client libraries, packages
- webhook: Webhooks, events, callbacks, notifications
"""
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Extract {category} facts from:\n\n{content[:4000]}"}
            ],
            response_format={"type": "json_object"},
            temperature=0.1
        )
        
        result = json.loads(response.choices[0].message.content)
        facts = result.get("facts", result.get("results", []))
        
        # Ensure facts is a list
        if isinstance(facts, dict):
            facts = [facts]
        
        return facts
        
    except Exception as e:
        print(f"Error extracting facts: {e}")
        return []


def crawl_page_sync(url: str) -> str:
    """Crawl a page synchronously."""
    try:
        import httpx
        headers = {
            "User-Agent": "ConnectorResearchBot/1.0 (+https://github.com/Legolasan/connector_research)",
            "Accept": "text/html,application/xhtml+xml,text/plain"
        }
        response = httpx.get(url, headers=headers, timeout=30.0, follow_redirects=True)
        response.raise_for_status()
        
        # Basic HTML to text extraction
        content = response.text
        
        # Remove script and style tags
        import re
        content = re.sub(r'<script[^>]*>.*?</script>', '', content, flags=re.DOTALL | re.IGNORECASE)
        content = re.sub(r'<style[^>]*>.*?</style>', '', content, flags=re.DOTALL | re.IGNORECASE)
        
        # Remove HTML tags
        content = re.sub(r'<[^>]+>', ' ', content)
        
        # Normalize whitespace
        content = re.sub(r'\s+', ' ', content).strip()
        
        return content[:10000]  # Limit content size
        
    except Exception as e:
        return f"Error fetching {url}: {str(e)}"


# ============================================================================
# Phase 1: Web Search Tasks
# ============================================================================

@celery_app.task(bind=True, name="webapp.services.tasks.web_search_task")
def web_search_task(self, connector_name: str, query: str, category: str) -> Dict[str, Any]:
    """
    Execute a single web search and store results as artifacts.
    
    Args:
        connector_name: Name of the connector being researched
        query: Search query
        category: Category of information (auth, rate_limit, etc.)
        
    Returns:
        Dict with status and result count
    """
    store = get_artifact_store(connector_name)
    cache = get_research_cache()
    
    # Check cache first
    cached = cache.get_web_search(query)
    if cached:
        emit_progress(connector_name, "web_search", f"Cache hit: {query[:40]}...")
        
        # Still store artifacts from cached results
        for result in cached.get("results", []):
            artifact = Artifact(
                id=Artifact.generate_id(result.get("content", ""), result.get("url")),
                artifact_type="search_result",
                source_url=result.get("url"),
                content=result.get("content", ""),
                confidence=result.get("score", 0.5),
                created_at=datetime.utcnow().isoformat(),
                created_by_task=self.request.id or "unknown",
                metadata={"category": category, "cached": True}
            )
            store.add_artifact(artifact)
        
        return {
            "status": "cached",
            "query": query,
            "category": category,
            "result_count": len(cached.get("results", []))
        }
    
    # Execute search
    emit_progress(connector_name, "web_search", f"Searching: {query[:40]}...")
    results = tavily_search(query)
    
    if "error" in results:
        return {
            "status": "error",
            "error": results["error"],
            "query": query,
            "category": category
        }
    
    # Cache results
    cache.set_web_search(query, results)
    
    # Store as artifacts
    stored_count = 0
    for result in results.get("results", []):
        artifact = Artifact(
            id=Artifact.generate_id(result.get("content", ""), result.get("url")),
            artifact_type="search_result",
            source_url=result.get("url"),
            content=result.get("content", ""),
            confidence=result.get("score", 0.5),
            created_at=datetime.utcnow().isoformat(),
            created_by_task=self.request.id or "unknown",
            metadata={"category": category, "title": result.get("title", "")}
        )
        store.add_artifact(artifact)
        stored_count += 1
    
    emit_progress(
        connector_name, 
        "web_search", 
        f"Found {stored_count} results for {category}",
        {"query": query, "count": stored_count}
    )
    
    return {
        "status": "completed",
        "query": query,
        "category": category,
        "result_count": stored_count
    }


# ============================================================================
# Phase 2: Source Fetch Tasks
# ============================================================================

@celery_app.task(bind=True, name="webapp.services.tasks.fetch_source_task")
def fetch_source_task(self, connector_name: str, url: str, category: str) -> Dict[str, Any]:
    """
    Fetch and extract content from a URL.
    
    Args:
        connector_name: Name of the connector
        url: URL to fetch
        category: Category of content
        
    Returns:
        Dict with status and content info
    """
    store = get_artifact_store(connector_name)
    cache = get_research_cache()
    
    # Check if already fetched
    if store.source_exists(url):
        return {
            "status": "already_fetched",
            "url": url,
            "category": category
        }
    
    # Check page cache
    cached = cache.get_page_content(url)
    if cached:
        content = cached.get("content", "")
        emit_progress(connector_name, "fetch", f"Cache hit: {url[:40]}...")
    else:
        # Fetch content
        emit_progress(connector_name, "fetch", f"Fetching: {url[:40]}...")
        content = crawl_page_sync(url)
        
        # Cache the content
        cache.set_page_content(url, content)
    
    # Store as artifact
    artifact = Artifact(
        id=Artifact.generate_id(content, url),
        artifact_type="page_content",
        source_url=url,
        content=content,
        confidence=0.9,  # Direct source = high confidence
        created_at=datetime.utcnow().isoformat(),
        created_by_task=self.request.id or "unknown",
        metadata={"category": category}
    )
    store.add_artifact(artifact)
    
    emit_progress(
        connector_name, 
        "fetch", 
        f"Fetched {len(content)} chars from {url[:30]}...",
        {"url": url, "length": len(content)}
    )
    
    return {
        "status": "completed",
        "url": url,
        "category": category,
        "content_length": len(content)
    }


# ============================================================================
# Phase 3: Summarize Tasks (Fact Extraction)
# ============================================================================

@celery_app.task(bind=True, name="webapp.services.tasks.summarize_task")
def summarize_task(self, connector_name: str, artifact_id: str, artifact_type: str, category: str) -> Dict[str, Any]:
    """
    Extract facts from an artifact.
    
    Args:
        connector_name: Name of the connector
        artifact_id: ID of artifact to process
        artifact_type: Type of artifact
        category: Category of facts to extract
        
    Returns:
        Dict with status and fact count
    """
    store = get_artifact_store(connector_name)
    cache = get_research_cache()
    
    # Get artifact
    artifact = store.get_artifact(artifact_id, artifact_type)
    if not artifact:
        return {
            "status": "artifact_not_found",
            "artifact_id": artifact_id,
            "category": category
        }
    
    # Check LLM cache
    cached_response = cache.get_llm_response(
        model="gpt-4o-mini",
        prompt_type="extract_facts",
        input_content=artifact.content,
        instruction_type=category
    )
    
    if cached_response:
        try:
            facts_data = json.loads(cached_response)
            emit_progress(connector_name, "summarize", f"Cache hit for {category} facts")
        except json.JSONDecodeError:
            facts_data = []
    else:
        # Extract facts via LLM
        emit_progress(connector_name, "summarize", f"Extracting {category} facts...")
        facts_data = extract_facts_llm(artifact.content, category)
        
        # Cache the response
        cache.set_llm_response(
            model="gpt-4o-mini",
            prompt_type="extract_facts",
            input_content=artifact.content,
            response=json.dumps(facts_data),
            instruction_type=category
        )
    
    # Store facts
    new_facts = 0
    for fact_data in facts_data:
        claim = fact_data.get("claim", "")
        if not claim:
            continue
            
        fact = Fact(
            id=Fact.generate_id(claim),
            claim=claim,
            evidence=[artifact_id],
            confidence=fact_data.get("confidence", 0.7),
            category=category,
            created_at=datetime.utcnow().isoformat(),
            metadata={
                "evidence_quote": fact_data.get("evidence_quote", ""),
                "source_url": artifact.source_url
            }
        )
        
        if store.add_fact(fact):  # Returns True if new
            new_facts += 1
    
    emit_progress(
        connector_name, 
        "summarize", 
        f"Extracted {new_facts} new {category} facts",
        {"category": category, "new_facts": new_facts, "total_facts": len(facts_data)}
    )
    
    return {
        "status": "completed",
        "artifact_id": artifact_id,
        "category": category,
        "new_facts": new_facts,
        "total_facts": len(facts_data)
    }


# ============================================================================
# Phase 4: Synthesis Supervisor (Editor-in-Chief)
# ============================================================================

@celery_app.task(bind=True, name="webapp.services.tasks.synthesis_supervisor_task")
def synthesis_supervisor_task(self, connector_name: str) -> Dict[str, Any]:
    """
    Final synthesis: deduplicate, resolve conflicts, generate document.
    
    This is the "Editor-in-Chief" task that:
    1. Reads all section outputs
    2. Deduplicates claims
    3. Resolves conflicts
    4. Rewrites into a coherent structure
    
    Args:
        connector_name: Name of the connector
        
    Returns:
        Dict with status and document info
    """
    store = get_artifact_store(connector_name)
    
    emit_progress(connector_name, "synthesis", "Starting final synthesis...")
    
    # 1. Gather all facts by category
    all_facts = store.get_all_facts()
    
    total_facts = sum(len(facts) for facts in all_facts.values())
    emit_progress(
        connector_name, 
        "synthesis", 
        f"Synthesizing {total_facts} facts across {len(all_facts)} categories"
    )
    
    # 2. Generate document sections
    document_parts = []
    
    # Header
    document_parts.append(f"# {connector_name} Connector Research Document\n")
    document_parts.append(f"*Generated: {datetime.utcnow().isoformat()}*\n")
    document_parts.append(f"*Total Facts: {total_facts}*\n\n")
    
    # Section order
    section_order = [
        ("auth", "Authentication & Authorization"),
        ("rate_limit", "Rate Limits & Quotas"),
        ("endpoint", "API Endpoints"),
        ("object", "Data Objects & Schemas"),
        ("webhook", "Webhooks & Events"),
        ("sdk", "SDKs & Client Libraries"),
    ]
    
    conflicts = []
    
    for category, section_title in section_order:
        facts = all_facts.get(category, [])
        
        if not facts:
            document_parts.append(f"## {section_title}\n")
            document_parts.append("*No information discovered.*\n\n")
            continue
        
        document_parts.append(f"## {section_title}\n\n")
        
        # Sort facts by confidence
        facts.sort(key=lambda f: f.confidence, reverse=True)
        
        for fact in facts:
            confidence_indicator = "✓" if fact.confidence >= 0.8 else "~" if fact.confidence >= 0.5 else "?"
            document_parts.append(f"- [{confidence_indicator}] {fact.claim}\n")
            
            # Add evidence reference
            if fact.metadata.get("source_url"):
                document_parts.append(f"  - Source: {fact.metadata['source_url']}\n")
            if fact.metadata.get("evidence_quote"):
                quote = fact.metadata['evidence_quote'][:100]
                document_parts.append(f"  - Evidence: \"{quote}...\"\n")
        
        document_parts.append("\n")
    
    # Combine document
    document = "".join(document_parts)
    
    # Store document as artifact
    doc_artifact = Artifact(
        id=Artifact.generate_id(document),
        artifact_type="final_document",
        source_url=None,
        content=document,
        confidence=1.0,
        created_at=datetime.utcnow().isoformat(),
        created_by_task=self.request.id or "unknown",
        metadata={"fact_count": total_facts, "conflict_count": len(conflicts)}
    )
    store.add_artifact(doc_artifact)
    
    emit_progress(
        connector_name, 
        "synthesis", 
        f"Document generated ({len(document)} chars)",
        {"document_length": len(document), "fact_count": total_facts}
    )
    
    return {
        "status": "completed",
        "fact_count": total_facts,
        "conflict_count": len(conflicts),
        "document_length": len(document),
        "document_id": doc_artifact.id
    }


# ============================================================================
# Aggregate Tasks (for chaining)
# ============================================================================

@celery_app.task(name="webapp.services.tasks.fetch_discovered_urls")
def fetch_discovered_urls(search_results: List[Dict], connector_name: str) -> List[Dict]:
    """
    Aggregate task: Fetch all URLs discovered from searches.
    
    Args:
        search_results: Results from web search tasks
        connector_name: Name of connector
        
    Returns:
        List of fetch task results
    """
    store = get_artifact_store(connector_name)
    
    # Collect unique URLs from search results
    urls_to_fetch = []
    artifacts = store.get_artifacts_by_type("search_result")
    
    for artifact in artifacts:
        if artifact.source_url and not store.source_exists(artifact.source_url):
            category = artifact.metadata.get("category", "general")
            urls_to_fetch.append((artifact.source_url, category))
    
    # Deduplicate
    urls_to_fetch = list(set(urls_to_fetch))[:20]  # Limit to 20 URLs
    
    emit_progress(
        connector_name, 
        "fetch", 
        f"Starting fetch of {len(urls_to_fetch)} URLs"
    )
    
    # Create fetch tasks
    if urls_to_fetch:
        fetch_tasks = group([
            fetch_source_task.s(connector_name, url, category)
            for url, category in urls_to_fetch
        ])
        result = fetch_tasks.apply_async()
        return result.get(timeout=120)  # Wait up to 2 minutes
    
    return []


@celery_app.task(name="webapp.services.tasks.summarize_all_artifacts")
def summarize_all_artifacts(fetch_results: List[Dict], connector_name: str) -> List[Dict]:
    """
    Aggregate task: Summarize all fetched artifacts.
    
    Args:
        fetch_results: Results from fetch tasks
        connector_name: Name of connector
        
    Returns:
        List of summarize task results
    """
    store = get_artifact_store(connector_name)
    
    # Get all page content artifacts
    artifacts = store.get_artifacts_by_type("page_content")
    
    emit_progress(
        connector_name, 
        "summarize", 
        f"Starting summarization of {len(artifacts)} sources"
    )
    
    # Create summarize tasks for each artifact
    summarize_tasks_list = []
    categories = ["auth", "rate_limit", "endpoint", "object", "sdk", "webhook"]
    
    for artifact in artifacts:
        category = artifact.metadata.get("category", "general")
        if category not in categories:
            category = "endpoint"  # Default category
        
        summarize_tasks_list.append(
            summarize_task.s(connector_name, artifact.id, "page_content", category)
        )
    
    if summarize_tasks_list:
        summarize_group = group(summarize_tasks_list)
        result = summarize_group.apply_async()
        return result.get(timeout=180)  # Wait up to 3 minutes
    
    return []


@celery_app.task(name="webapp.services.tasks.check_and_synthesize")
def check_and_synthesize(summarize_results: List[Dict], connector_name: str) -> Dict[str, Any]:
    """
    Check convergence and run synthesis if ready.
    
    Args:
        summarize_results: Results from summarize tasks
        connector_name: Name of connector
        
    Returns:
        Synthesis result or status
    """
    from services.convergence import ConvergenceChecker
    
    checker = ConvergenceChecker(REDIS_URL, connector_name)
    converged, reason = checker.check_convergence()
    
    emit_progress(
        connector_name, 
        "synthesis", 
        f"Convergence check: {converged} ({reason})"
    )
    
    # Always synthesize (even if not fully converged)
    return synthesis_supervisor_task(connector_name)

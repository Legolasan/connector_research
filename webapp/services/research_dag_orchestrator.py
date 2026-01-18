"""
🎭 Research DAG Orchestrator
Orchestrates research generation as a DAG of capability-based tasks.

DAG Structure:
  Phase 1: Web Search Tasks (fan-out parallel)
      ↓
  Phase 2: Source Fetch Tasks (fan-out parallel)
      ↓
  Phase 3: Summarize Tasks (fan-out parallel)
      ↓
  Convergence Check
      ↓ (if not converged, loop back to Phase 1 with new queries)
  Phase 4: Synthesis Supervisor (serial)
"""

import os
import json
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from celery import group, chain, chord
from celery.result import AsyncResult, GroupResult
import redis

from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


class ResearchDAGOrchestrator:
    """
    Orchestrate research generation as a DAG of capability tasks.
    
    Responsibilities:
    - Generate targeted search queries by category
    - Build and execute task DAG
    - Track progress across phases
    - Handle convergence and looping
    """
    
    # Search query templates by category
    QUERY_TEMPLATES = {
        "auth": [
            "{connector} API authentication method",
            "{connector} OAuth setup guide",
            "{connector} API key authentication",
            "{connector} access token scopes permissions",
        ],
        "rate_limit": [
            "{connector} API rate limits",
            "{connector} rate limiting throttling",
            "{connector} API quotas limits",
        ],
        "endpoint": [
            "{connector} REST API endpoints reference",
            "{connector} API documentation endpoints",
            "{connector} GraphQL API schema",
        ],
        "object": [
            "{connector} API data objects schema",
            "{connector} API entities resources",
            "{connector} data model structure",
        ],
        "webhook": [
            "{connector} webhooks events",
            "{connector} webhook callbacks notifications",
            "{connector} event subscriptions",
        ],
        "sdk": [
            "{connector} SDK client library",
            "{connector} Java SDK Maven",
            "{connector} Python SDK pip",
            "{connector} Node.js SDK npm",
        ],
    }
    
    def __init__(self, connector_name: str, context: Optional[Dict] = None):
        """
        Initialize orchestrator.
        
        Args:
            connector_name: Name of the connector to research
            context: Optional additional context (GitHub URL, Fivetran docs, etc.)
        """
        self.connector_name = connector_name
        self.context = context or {}
        self.redis = redis.from_url(REDIS_URL, decode_responses=True)
        self.prefix = f"dag:{connector_name}"
        
    def _key(self, *parts: str) -> str:
        """Generate Redis key with prefix."""
        return f"{self.prefix}:{':'.join(parts)}"
    
    def _generate_search_queries(self, iteration: int = 0) -> List[Dict[str, str]]:
        """
        Generate targeted search queries by category.
        
        Args:
            iteration: Current iteration (for expanding queries on subsequent passes)
            
        Returns:
            List of query dicts with 'query' and 'category' keys
        """
        queries = []
        
        for category, templates in self.QUERY_TEMPLATES.items():
            # Use more templates on later iterations
            max_templates = min(1 + iteration, len(templates))
            
            for template in templates[:max_templates]:
                query = template.format(connector=self.connector_name)
                queries.append({
                    "query": query,
                    "category": category
                })
        
        return queries
    
    def build_and_execute(self) -> str:
        """
        Build and execute the research DAG.
        
        Returns:
            Task group ID for tracking
        """
        from services.tasks import (
            web_search_task, 
            fetch_discovered_urls,
            summarize_all_artifacts,
            check_and_synthesize
        )
        from services.artifact_store import get_artifact_store, emit_progress
        
        # Clear previous state
        self._clear_state()
        
        # Initialize artifact store
        store = get_artifact_store(self.connector_name)
        store.clear()  # Fresh start
        
        emit_progress(self.connector_name, "init", f"Starting research for {self.connector_name}")
        
        # Generate initial search queries
        queries = self._generate_search_queries(iteration=0)
        
        # Phase 1: Web searches (parallel)
        search_tasks = group([
            web_search_task.s(self.connector_name, q["query"], q["category"])
            for q in queries
        ])
        
        # Build workflow: search → fetch → summarize → synthesize
        workflow = chain(
            search_tasks,
            fetch_discovered_urls.s(self.connector_name),
            summarize_all_artifacts.s(self.connector_name),
            check_and_synthesize.s(self.connector_name)
        )
        
        # Execute workflow
        result = workflow.apply_async()
        
        # Store workflow ID
        self._store_workflow_id(result.id)
        
        emit_progress(
            self.connector_name, 
            "init", 
            f"DAG started with {len(queries)} search queries",
            {"workflow_id": result.id, "query_count": len(queries)}
        )
        
        return result.id
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get current status of the research DAG.
        
        Returns:
            Dict with status information
        """
        from services.artifact_store import get_artifact_store, ProgressEmitter
        from services.convergence import get_convergence_checker
        
        workflow_id = self._get_workflow_id()
        
        if not workflow_id:
            return {
                "status": "not_started",
                "progress": 0,
                "phases": {},
                "events": []
            }
        
        # Get task result
        result = AsyncResult(workflow_id)
        
        # Get progress events
        emitter = ProgressEmitter(REDIS_URL, self.connector_name)
        events = emitter.get_events(limit=20)
        phase_counts = emitter.get_phase_counts()
        
        # Get artifact stats
        store = get_artifact_store(self.connector_name)
        stats = store.get_stats()
        
        # Get convergence stats
        checker = get_convergence_checker(self.connector_name)
        convergence_stats = checker.get_convergence_stats()
        
        # Calculate phase progress
        phases = {
            "web_search": {
                "completed": phase_counts.get("web_search", 0),
                "total": 6,  # Initial queries
                "status": "completed" if phase_counts.get("web_search", 0) >= 6 else "in_progress"
            },
            "fetch": {
                "completed": phase_counts.get("fetch", 0),
                "total": stats.get("artifacts:page_content", 0) or phase_counts.get("fetch", 0),
                "status": "completed" if phase_counts.get("summarize", 0) > 0 else "in_progress"
            },
            "summarize": {
                "completed": phase_counts.get("summarize", 0),
                "total": stats.get("artifacts:page_content", 0) or 1,
                "status": "completed" if phase_counts.get("synthesis", 0) > 0 else "pending"
            },
            "synthesis": {
                "completed": 1 if phase_counts.get("synthesis", 0) > 0 else 0,
                "total": 1,
                "status": "completed" if result.state == "SUCCESS" else "pending"
            }
        }
        
        # Overall progress
        total_phases = 4
        completed_phases = sum(1 for p in phases.values() if p["status"] == "completed")
        progress = int((completed_phases / total_phases) * 100)
        
        # Determine overall status
        if result.state == "SUCCESS":
            status = "complete"
            progress = 100
        elif result.state == "FAILURE":
            status = "failed"
        elif result.state == "PENDING":
            status = "pending"
        else:
            status = "generating"
        
        return {
            "status": status,
            "progress": progress,
            "workflow_id": workflow_id,
            "task_state": result.state,
            "phases": phases,
            "stats": {
                "total_facts": convergence_stats.get("total_facts", 0),
                "total_sources": convergence_stats.get("total_sources", 0),
                "facts_by_category": convergence_stats.get("facts_by_category", {}),
            },
            "convergence": {
                "converged": convergence_stats.get("converged", False),
                "reason": convergence_stats.get("convergence_reason", "")
            },
            "last_event": events[-1] if events else None,
            "events": events[-10:]  # Last 10 events
        }
    
    def get_result(self) -> Optional[Dict[str, Any]]:
        """
        Get the final result if complete.
        
        Returns:
            Result dict or None if not complete
        """
        workflow_id = self._get_workflow_id()
        
        if not workflow_id:
            return None
        
        result = AsyncResult(workflow_id)
        
        if result.state == "SUCCESS":
            return result.result
        
        return None
    
    def get_document(self) -> Optional[str]:
        """
        Get the generated research document.
        
        Returns:
            Document content or None if not ready
        """
        from services.artifact_store import get_artifact_store
        
        store = get_artifact_store(self.connector_name)
        
        # Get final document artifact
        docs = store.get_artifacts_by_type("final_document")
        
        if docs:
            # Return most recent document
            docs.sort(key=lambda d: d.created_at, reverse=True)
            return docs[0].content
        
        return None
    
    def cancel(self):
        """Cancel the running workflow."""
        from celery.app.control import Control
        from services.celery_app import celery_app
        
        workflow_id = self._get_workflow_id()
        
        if workflow_id:
            control = Control(celery_app)
            control.revoke(workflow_id, terminate=True)
            self._clear_state()
    
    def _store_workflow_id(self, workflow_id: str):
        """Store workflow ID in Redis."""
        self.redis.set(self._key("workflow_id"), workflow_id)
        self.redis.expire(self._key("workflow_id"), 3600)  # 1 hour expiry
    
    def _get_workflow_id(self) -> Optional[str]:
        """Get stored workflow ID."""
        return self.redis.get(self._key("workflow_id"))
    
    def _clear_state(self):
        """Clear DAG state."""
        pattern = f"{self.prefix}:*"
        cursor = 0
        while True:
            cursor, keys = self.redis.scan(cursor, match=pattern, count=100)
            if keys:
                self.redis.delete(*keys)
            if cursor == 0:
                break


# =========================================================================
# Factory Functions
# =========================================================================

def get_dag_orchestrator(connector_name: str, context: Optional[Dict] = None) -> ResearchDAGOrchestrator:
    """Get DAG orchestrator for a connector."""
    return ResearchDAGOrchestrator(connector_name, context)


def start_research_dag(connector_name: str, context: Optional[Dict] = None) -> str:
    """
    Start a new research DAG.
    
    Args:
        connector_name: Name of the connector
        context: Optional additional context
        
    Returns:
        Workflow ID for tracking
    """
    orchestrator = get_dag_orchestrator(connector_name, context)
    return orchestrator.build_and_execute()


def get_research_progress(connector_name: str) -> Dict[str, Any]:
    """
    Get progress of research DAG.
    
    Args:
        connector_name: Name of the connector
        
    Returns:
        Progress dict
    """
    orchestrator = get_dag_orchestrator(connector_name)
    return orchestrator.get_status()

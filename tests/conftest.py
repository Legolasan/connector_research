"""
Pytest configuration and fixtures for connector research tests.
"""

import pytest
import asyncio
from typing import Dict, Any, List, Optional
from unittest.mock import Mock, AsyncMock, MagicMock
from datetime import datetime

# Mock connector data
@pytest.fixture
def mock_connector_data():
    """Mock connector data for testing."""
    return {
        "id": "test_connector",
        "name": "Test Connector",
        "connector_type": "REST API",
        "status": "not_started",
        "github_url": "https://github.com/test/connector",
        "description": "Test connector description"
    }


@pytest.fixture
def mock_source_contexts():
    """Mock source contexts (vault, docwhisperer, web, fivetran, github)."""
    return {
        "vault": "Vault context: Official documentation from pre-indexed sources.",
        "docwhisperer": "DocWhisperer context: Official library documentation.",
        "web": "Web search results: Additional context from web search.",
        "fivetran": "Fivetran context: Fivetran implementation details.",
        "github": "GitHub context: Code patterns from repository."
    }


@pytest.fixture
def mock_llm_response():
    """Mock LLM response."""
    return {
        "content": "Generated research content with citations [web:1] [vault:1].",
        "model": "gpt-4o",
        "usage": {"total_tokens": 1000}
    }


@pytest.fixture
def mock_evidence_map():
    """Mock evidence map with stable IDs."""
    return {
        "web:1": {
            "evidence_id": "a3f5b2c1d4e6f7g8h9i0j1k2l3m4n5o6p7q8r9s0t1u2v3w4x5y6z7",
            "citation_tag": "web:1",
            "snippet": "Rate limit is 1000 requests per hour according to official documentation.",
            "url": "https://api.example.com/docs/rate-limits",
            "source_type": "web",
            "timestamp": datetime.utcnow().isoformat(),
            "confidence": 0.8
        },
        "vault:1": {
            "evidence_id": "b4g6c2d5e7f8g9h0i1j2k3l4m5n6o7p8q9r0s1t2u3v4w5x6y7z8a9",
            "citation_tag": "vault:1",
            "snippet": "OAuth 2.0 authentication is required for all API requests.",
            "url": "vault://test_connector/auth",
            "source_type": "vault",
            "timestamp": datetime.utcnow().isoformat(),
            "confidence": 0.9
        },
        "doc:1": {
            "evidence_id": "c5h7d3e6f9g0h1i2j3k4l5m6n7o8p9q0r1s2t3u4v5w6x7y8z9a0b1",
            "citation_tag": "doc:1",
            "snippet": "The API supports REST endpoints for data extraction.",
            "url": "https://docs.example.com/api",
            "source_type": "doc",
            "timestamp": datetime.utcnow().isoformat(),
            "confidence": 0.85
        }
    }


@pytest.fixture
def mock_database_session():
    """Mock database session."""
    session = MagicMock()
    session.query = MagicMock()
    session.commit = MagicMock()
    session.rollback = MagicMock()
    session.close = MagicMock()
    return session


@pytest.fixture
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_research_progress():
    """Mock ResearchProgress object."""
    from services.research_agent import ResearchProgress
    
    return ResearchProgress(
        connector_id="test_connector",
        connector_name="Test Connector",
        current_section=0,
        total_sections=20,
        status="running",
        sections_completed=[],
        current_content="",
        error_message="",
        discovered_methods=[],
        section_reviews={},
        stop_the_line_events=[],
        contradictions=[],
        engineering_costs={},
        overall_confidence=0.0,
        claims_json=[],
        canonical_facts_json={},
        evidence_map_json={}
    )

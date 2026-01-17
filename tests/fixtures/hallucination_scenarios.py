"""
Test fixtures for hallucination regression scenarios.

These fixtures simulate common failure modes to ensure the system
correctly handles hallucinations, contradictions, and missing citations.
"""

import pytest
from typing import Dict, Any, List


@pytest.fixture
def scenario_1_missing_rate_limit():
    """
    Scenario 1: Docs missing rate limit
    
    Mock content: "Rate limit is 1000 requests per hour" (no citation)
    Expected: Validator rejects, regeneration requested with failure report
    """
    return {
        "name": "Docs missing rate limit",
        "content": """
        The API has rate limiting in place to prevent abuse.
        Rate limit is 1000 requests per hour.
        This limit applies to all endpoints.
        """,
        "evidence_map": {},
        "expected_validation_result": {
            "is_valid": False,
            "uncited_claims_count": 1,
            "uncited_table_rows_count": 0,
            "claim_types": ["RATE_LIMIT"]
        },
        "expected_behavior": "reject_and_regenerate"
    }


@pytest.fixture
def scenario_2_contradicting_scopes():
    """
    Scenario 2: Docs contradict scopes
    
    Mock vault: "Scopes: read:accounts, write:contacts"
    Mock fivetran: "Scopes: accounts.read, contacts.write"
    Expected: Contradiction detected, stop-the-line or flag
    """
    return {
        "name": "Docs contradict scopes",
        "vault_content": "Required OAuth scopes: read:accounts, write:contacts [vault:1]",
        "fivetran_content": "Fivetran uses scopes: accounts.read, contacts.write",
        "evidence_map": {
            "vault:1": {
                "evidence_id": "vault_scope_evidence",
                "citation_tag": "vault:1",
                "snippet": "Required OAuth scopes: read:accounts, write:contacts",
                "url": "vault://test/auth",
                "source_type": "vault",
                "confidence": 0.9
            }
        },
        "expected_contradiction": {
            "detected": True,
            "category": "SCOPE",
            "severity": "WARNING"
        },
        "expected_behavior": "flag_or_stop"
    }


@pytest.fixture
def scenario_3_fivetran_object_not_in_docs():
    """
    Scenario 3: Fivetran mentions object not in official docs
    
    Mock vault: Objects: accounts, contacts
    Mock fivetran: Objects: accounts, contacts, **custom_fields** (not in vault)
    Expected: custom_fields marked with "?" in Fivetran Support column, flagged as uncertain
    """
    return {
        "name": "Fivetran mentions object not in official docs",
        "vault_objects": ["accounts", "contacts"],
        "fivetran_objects": ["accounts", "contacts", "custom_fields"],
        "content": """
        | Object | Extraction Method | Primary Key | Cursor Field | Parent | Permissions | Delete Method | Fivetran Support |
        |--------|-------------------|-------------|--------------|--------|-------------|---------------|------------------|
        | accounts | GET /v1/accounts | id | updated_at | - | read:accounts | Soft Delete (deleted_at) | ✓ |
        | contacts | GET /v1/contacts | id | updated_at | - | read:contacts | Soft Delete (deleted_at) | ✓ |
        | custom_fields | GET /v1/custom_fields | id | updated_at | - | read:custom | Soft Delete (deleted_at) | ✓ |
        """,
        "expected_uncertainty": {
            "object": "custom_fields",
            "flag": True,
            "reason": "Not found in official documentation"
        },
        "expected_behavior": "flag_as_uncertain"
    }


@pytest.fixture
def scenario_4_github_endpoint_mismatch():
    """
    Scenario 4: GitHub uses endpoint that docs don't mention
    
    Mock vault: Endpoint: `/v1/accounts`
    Mock github: Code uses `/v2/api/accounts` (different version)
    Expected: Both documented, version conflict flagged
    """
    return {
        "name": "GitHub uses endpoint that docs don't mention",
        "vault_endpoint": "/v1/accounts",
        "github_endpoint": "/v2/api/accounts",
        "content": """
        The accounts endpoint is available at /v1/accounts [vault:1].
        However, the GitHub implementation uses /v2/api/accounts [github:1].
        """,
        "evidence_map": {
            "vault:1": {
                "evidence_id": "vault_endpoint_evidence",
                "citation_tag": "vault:1",
                "snippet": "Endpoint: /v1/accounts",
                "url": "vault://test/endpoints",
                "source_type": "vault",
                "confidence": 0.9
            },
            "github:1": {
                "evidence_id": "github_endpoint_evidence",
                "citation_tag": "github:1",
                "snippet": "Code uses /v2/api/accounts",
                "url": "https://github.com/test/connector",
                "source_type": "github",
                "confidence": 0.7
            }
        },
        "expected_conflict": {
            "detected": True,
            "type": "VERSION_MISMATCH",
            "severity": "WARNING"
        },
        "expected_behavior": "flag_version_conflict"
    }


@pytest.fixture
def scenario_5_table_row_without_citation():
    """
    Scenario 5: Table row without evidence pointer
    
    Mock content: Object Catalog table with row missing citation
    Expected: Validator rejects row, requests citation
    """
    return {
        "name": "Table row without evidence pointer",
        "content": """
        ## Object Catalog
        
        | Object | Extraction Method | Primary Key | Cursor Field | Parent | Permissions | Delete Method | Fivetran Support |
        |--------|-------------------|-------------|--------------|--------|-------------|---------------|------------------|
        | orders | GET /v1/orders | id | updated_at | - | read:orders | Soft Delete (deleted_at) | ✓ |
        | products | GET /v1/products | id | updated_at | - | read:products | Soft Delete (deleted_at) | ✓ |
        """,
        "evidence_map": {},
        "expected_validation_result": {
            "is_valid": False,
            "uncited_claims_count": 0,
            "uncited_table_rows_count": 2,  # Both rows missing citations
            "table_name": "Object Catalog"
        },
        "expected_behavior": "reject_table_rows"
    }


@pytest.fixture
def scenario_6_citation_spam():
    """
    Scenario 6: Citation spam
    
    Model adds [web:1] everywhere but evidence_map doesn't contain it, or snippet doesn't support claim
    Expected: Citation validator passes (sees citations), but evidence integrity validator fails
    """
    return {
        "name": "Citation spam",
        "content": """
        The API supports OAuth 2.0 authentication [web:1].
        Rate limit is 1000 requests per hour [web:1].
        The endpoint /v1/accounts returns user data [web:1].
        """,
        "evidence_map": {
            # Missing web:1 - citation spam!
        },
        "expected_citation_validation": {
            "is_valid": True,  # Citations are present
            "uncited_claims_count": 0
        },
        "expected_integrity_validation": {
            "is_valid": False,
            "issues_count": 3,  # All three citations are missing
            "issue_types": ["MISSING"]
        },
        "expected_behavior": "integrity_validator_catches"
    }


@pytest.fixture
def scenario_7_cross_section_inconsistency():
    """
    Scenario 7: Inconsistent claim across sections
    
    Mock: Section 100 says "OAuth required"
    Mock: Method section says "API key supported"
    Expected: Contradiction detector flags cross-section inconsistency, canonical registry marks "conflicting"
    """
    return {
        "name": "Inconsistent claim across sections",
        "section_100_content": """
        Authentication is required for all API requests.
        OAuth 2.0 is the only supported authentication method [vault:1].
        """,
        "method_section_content": """
        The REST API method supports multiple authentication options.
        API key authentication is supported [web:1].
        OAuth 2.0 is also available [vault:1].
        """,
        "evidence_map": {
            "vault:1": {
                "evidence_id": "vault_auth_evidence",
                "citation_tag": "vault:1",
                "snippet": "OAuth 2.0 is the only supported authentication method",
                "url": "vault://test/auth",
                "source_type": "vault",
                "confidence": 0.9
            },
            "web:1": {
                "evidence_id": "web_auth_evidence",
                "citation_tag": "web:1",
                "snippet": "API key authentication is supported",
                "url": "https://api.example.com/docs/auth",
                "source_type": "web",
                "confidence": 0.7
            }
        },
        "expected_contradiction": {
            "detected": True,
            "category": "AUTH",
            "severity": "CRITICAL",
            "cross_section": True
        },
        "expected_canonical_facts": {
            "auth": {
                "conflicting": True,
                "resolution": "requires_human_review"
            }
        },
        "expected_behavior": "stop_the_line_or_flag"
    }


@pytest.fixture
def all_hallucination_scenarios(
    scenario_1_missing_rate_limit,
    scenario_2_contradicting_scopes,
    scenario_3_fivetran_object_not_in_docs,
    scenario_4_github_endpoint_mismatch,
    scenario_5_table_row_without_citation,
    scenario_6_citation_spam,
    scenario_7_cross_section_inconsistency
):
    """All hallucination scenarios combined."""
    return [
        scenario_1_missing_rate_limit,
        scenario_2_contradicting_scopes,
        scenario_3_fivetran_object_not_in_docs,
        scenario_4_github_endpoint_mismatch,
        scenario_5_table_row_without_citation,
        scenario_6_citation_spam,
        scenario_7_cross_section_inconsistency
    ]

"""
Unit tests for Evidence Integrity Validator.

Tests that citation tags exist in evidence_map and snippets support claims.
"""

import pytest
from services.evidence_integrity_validator import EvidenceIntegrityValidator, IntegrityResult


class TestEvidenceIntegrityValidator:
    """Test suite for EvidenceIntegrityValidator."""
    
    def test_validates_citation_tags_exist_in_evidence_map(self):
        """Test that citation tags must exist in evidence_map."""
        validator = EvidenceIntegrityValidator()
        content = "Rate limit is 1000 requests per hour [web:1]."
        evidence_map = {
            "web:1": {
                "evidence_id": "test_id",
                "citation_tag": "web:1",
                "snippet": "Rate limit is 1000 requests per hour",
                "url": "https://api.example.com/docs",
                "source_type": "web",
                "timestamp": "2024-01-01T00:00:00Z",
                "confidence": 0.8
            }
        }
        
        result = validator.validate_evidence_integrity(content, evidence_map)
        
        # Should pass because web:1 exists in evidence_map
        assert result.is_valid
        assert result.valid_citations == 1
    
    def test_validates_evidence_has_url_snippet_timestamp(self):
        """Test that evidence entries must have required fields."""
        validator = EvidenceIntegrityValidator()
        content = "Rate limit is 1000 requests per hour [web:1]."
        evidence_map = {
            "web:1": {
                "evidence_id": "test_id",
                "citation_tag": "web:1",
                "snippet": "Rate limit is 1000 requests per hour",
                "url": "https://api.example.com/docs",
                "source_type": "web",
                "timestamp": "2024-01-01T00:00:00Z",
                "confidence": 0.8
            }
        }
        
        result = validator.validate_evidence_integrity(content, evidence_map)
        
        # Should pass because all required fields are present
        assert result.is_valid
    
    def test_validates_snippet_keyword_matching(self):
        """Test that snippet keyword matching works."""
        validator = EvidenceIntegrityValidator(enable_snippet_matching=True)
        content = "Rate limit is 1000 requests per hour [web:1]."
        evidence_map = {
            "web:1": {
                "evidence_id": "test_id",
                "citation_tag": "web:1",
                "snippet": "Rate limit is 1000 requests per hour according to official documentation",
                "url": "https://api.example.com/docs",
                "source_type": "web",
                "timestamp": "2024-01-01T00:00:00Z",
                "confidence": 0.8
            }
        }
        
        result = validator.validate_evidence_integrity(content, evidence_map)
        
        # Should pass because snippet contains keywords from claim
        assert result.is_valid
    
    def test_detects_missing_citations(self):
        """Test detection of citations not in evidence_map."""
        validator = EvidenceIntegrityValidator()
        content = "Rate limit is 1000 requests per hour [web:1]."
        evidence_map = {}  # Empty evidence_map
        
        result = validator.validate_evidence_integrity(content, evidence_map)
        
        # Should fail because web:1 is missing
        assert not result.is_valid
        assert len(result.issues) > 0
        assert any(issue.issue_type == "MISSING" for issue in result.issues)
    
    def test_detects_incomplete_evidence_entries(self):
        """Test detection of incomplete evidence entries."""
        validator = EvidenceIntegrityValidator()
        content = "Rate limit is 1000 requests per hour [web:1]."
        evidence_map = {
            "web:1": {
                "citation_tag": "web:1",
                # Missing url, snippet, source_type
            }
        }
        
        result = validator.validate_evidence_integrity(content, evidence_map)
        
        # Should fail because required fields are missing
        assert not result.is_valid
        assert len(result.issues) > 0
        assert any(issue.issue_type == "INCOMPLETE" for issue in result.issues)
    
    def test_detects_snippet_mismatch(self):
        """Test detection of snippet-keyword mismatches."""
        validator = EvidenceIntegrityValidator(enable_snippet_matching=True)
        content = "Rate limit is 1000 requests per hour [web:1]."
        evidence_map = {
            "web:1": {
                "evidence_id": "test_id",
                "citation_tag": "web:1",
                "snippet": "The API supports OAuth 2.0 authentication",  # Mismatch - no rate limit keywords
                "url": "https://api.example.com/docs",
                "source_type": "web",
                "timestamp": "2024-01-01T00:00:00Z",
                "confidence": 0.8
            }
        }
        
        result = validator.validate_evidence_integrity(content, evidence_map)
        
        # Should fail because snippet doesn't match claim keywords
        assert not result.is_valid
        assert len(result.issues) > 0
        assert any(issue.issue_type == "SNIPPET_MISMATCH" for issue in result.issues)
    
    def test_handles_multiple_citations(self):
        """Test validation with multiple citations."""
        validator = EvidenceIntegrityValidator()
        content = "Rate limit is 1000 requests per hour [web:1]. OAuth 2.0 is required [vault:1]."
        evidence_map = {
            "web:1": {
                "evidence_id": "web_id",
                "citation_tag": "web:1",
                "snippet": "Rate limit is 1000 requests per hour",
                "url": "https://api.example.com/docs",
                "source_type": "web",
                "timestamp": "2024-01-01T00:00:00Z",
                "confidence": 0.8
            },
            "vault:1": {
                "evidence_id": "vault_id",
                "citation_tag": "vault:1",
                "snippet": "OAuth 2.0 is required",
                "url": "vault://test/auth",
                "source_type": "vault",
                "timestamp": "2024-01-01T00:00:00Z",
                "confidence": 0.9
            }
        }
        
        result = validator.validate_evidence_integrity(content, evidence_map)
        
        # Should pass because both citations exist and are valid
        assert result.is_valid
        assert result.valid_citations == 2
        assert result.total_citations == 2
    
    def test_handles_mixed_valid_invalid_citations(self):
        """Test validation with mix of valid and invalid citations."""
        validator = EvidenceIntegrityValidator()
        content = "Rate limit is 1000 [web:1]. OAuth required [vault:1]. Unknown endpoint [web:999]."
        evidence_map = {
            "web:1": {
                "evidence_id": "web_id",
                "citation_tag": "web:1",
                "snippet": "Rate limit is 1000",
                "url": "https://api.example.com/docs",
                "source_type": "web",
                "timestamp": "2024-01-01T00:00:00Z",
                "confidence": 0.8
            },
            "vault:1": {
                "evidence_id": "vault_id",
                "citation_tag": "vault:1",
                "snippet": "OAuth required",
                "url": "vault://test/auth",
                "source_type": "vault",
                "timestamp": "2024-01-01T00:00:00Z",
                "confidence": 0.9
            }
            # web:999 is missing
        }
        
        result = validator.validate_evidence_integrity(content, evidence_map)
        
        # Should fail because web:999 is missing
        assert not result.is_valid
        assert result.valid_citations == 2
        assert result.total_citations == 3
        assert len(result.issues) == 1
        assert result.issues[0].citation_tag == "web:999"

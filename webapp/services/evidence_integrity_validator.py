"""
Evidence Integrity Validator Service
Validates that citation tags in content actually exist in evidence_map
and that snippets support the claims they reference.
"""

import re
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field


@dataclass
class IntegrityIssue:
    """An integrity issue with a citation."""
    citation_tag: str  # e.g., "web:1"
    issue_type: str  # "MISSING", "INCOMPLETE", "SNIPPET_MISMATCH"
    claim_text: str  # The claim that references this citation
    evidence_entry: Optional[Dict[str, Any]] = None  # If partial
    details: str = ""  # Additional details about the issue


@dataclass
class IntegrityResult:
    """Result of evidence integrity validation."""
    is_valid: bool
    issues: List[IntegrityIssue] = field(default_factory=list)
    total_citations: int = 0
    valid_citations: int = 0


class EvidenceIntegrityValidator:
    """Validates that citations in content have corresponding evidence entries."""
    
    # Citation tag pattern
    CITATION_PATTERN = re.compile(r'\[(web|vault|doc|github):(\d+)\]')
    
    def __init__(self, enable_snippet_matching: bool = True):
        """
        Initialize the evidence integrity validator.
        
        Args:
            enable_snippet_matching: If True, check that snippet contains keywords from claim
        """
        self.enable_snippet_matching = enable_snippet_matching
    
    def validate_evidence_integrity(
        self,
        content: str,
        evidence_map: Dict[str, Dict[str, Any]]
    ) -> IntegrityResult:
        """
        Validate that all citation tags in content exist in evidence_map.
        
        Args:
            content: Content with citation tags
            evidence_map: Dictionary mapping citation tags to evidence entries
            
        Returns:
            IntegrityResult with validation status and issues
        """
        # Extract all citation tags from content
        citation_tags = self._extract_citation_tags(content)
        
        total_citations = len(citation_tags)
        valid_citations = 0
        issues = []
        
        # For each citation, find the claim it references
        citation_to_claim = self._map_citations_to_claims(content, citation_tags)
        
        for citation_tag in citation_tags:
            # Check if citation exists in evidence_map
            if citation_tag not in evidence_map:
                issues.append(IntegrityIssue(
                    citation_tag=citation_tag,
                    issue_type="MISSING",
                    claim_text=citation_to_claim.get(citation_tag, "Unknown claim"),
                    details=f"Citation tag {citation_tag} not found in evidence_map"
                ))
                continue
            
            evidence_entry = evidence_map[citation_tag]
            
            # Check if evidence entry has required fields
            required_fields = ['url', 'snippet', 'source_type']
            missing_fields = [field for field in required_fields if field not in evidence_entry]
            
            if missing_fields:
                issues.append(IntegrityIssue(
                    citation_tag=citation_tag,
                    issue_type="INCOMPLETE",
                    claim_text=citation_to_claim.get(citation_tag, "Unknown claim"),
                    evidence_entry=evidence_entry,
                    details=f"Evidence entry missing fields: {', '.join(missing_fields)}"
                ))
                continue
            
            # Check timestamp (optional but recommended)
            if 'timestamp' not in evidence_entry:
                # Not a critical issue, but log it
                pass
            
            # Optional: Check snippet-keyword matching
            if self.enable_snippet_matching:
                claim_text = citation_to_claim.get(citation_tag, "")
                if claim_text and not self._check_snippet_keyword_match(claim_text, evidence_entry.get('snippet', '')):
                    issues.append(IntegrityIssue(
                        citation_tag=citation_tag,
                        issue_type="SNIPPET_MISMATCH",
                        claim_text=claim_text,
                        evidence_entry=evidence_entry,
                        details="Snippet does not appear to support the claim (keyword mismatch)"
                    ))
                    continue
            
            valid_citations += 1
        
        is_valid = len(issues) == 0
        
        return IntegrityResult(
            is_valid=is_valid,
            issues=issues,
            total_citations=total_citations,
            valid_citations=valid_citations
        )
    
    def _extract_citation_tags(self, content: str) -> List[str]:
        """Extract all citation tags from content."""
        tags = []
        for match in self.CITATION_PATTERN.finditer(content):
            source_type = match.group(1)
            number = match.group(2)
            tag = f"{source_type}:{number}"
            if tag not in tags:
                tags.append(tag)
        return tags
    
    def _map_citations_to_claims(self, content: str, citation_tags: List[str]) -> Dict[str, str]:
        """
        Map citation tags to the claims they reference.
        
        Finds the text near each citation tag (within 250 chars before).
        """
        citation_to_claim = {}
        
        for tag in citation_tags:
            # Find all occurrences of this citation tag
            pattern = re.compile(rf'\[{re.escape(tag)}\]')
            for match in pattern.finditer(content):
                # Extract text before citation (up to 250 chars)
                start_pos = max(0, match.start() - 250)
                context = content[start_pos:match.start()]
                
                # Find the sentence or phrase containing the claim
                # Look for sentence boundaries
                sentences = re.split(r'[.!?]+\s+', context)
                if sentences:
                    claim_text = sentences[-1].strip()
                    # Remove any existing citations from claim text
                    claim_text = re.sub(r'\[[^\]]+\]', '', claim_text).strip()
                    if claim_text:
                        citation_to_claim[tag] = claim_text[:200]  # Limit length
        
        return citation_to_claim
    
    def _check_snippet_keyword_match(self, claim_text: str, snippet: str) -> bool:
        """
        Lightweight check: does snippet contain keywords from claim?
        
        This is a simple heuristic - not a full semantic match.
        """
        if not claim_text or not snippet:
            return True  # Can't verify, so assume valid
        
        # Extract significant words from claim (exclude common words)
        stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'should', 'could', 'may', 'might', 'must', 'can', 'this', 'that', 'these', 'those'}
        
        claim_words = set(
            word.lower() 
            for word in re.findall(r'\b\w+\b', claim_text)
            if word.lower() not in stop_words and len(word) > 3
        )
        
        snippet_lower = snippet.lower()
        
        # Check if at least 2 significant words from claim appear in snippet
        matches = sum(1 for word in claim_words if word in snippet_lower)
        
        # Require at least 2 matches or 50% of significant words (whichever is lower threshold)
        if len(claim_words) == 0:
            return True  # No significant words to match
        
        match_ratio = matches / len(claim_words)
        return matches >= 2 or match_ratio >= 0.3  # At least 2 matches or 30% match ratio

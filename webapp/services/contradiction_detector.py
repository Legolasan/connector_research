"""
Contradiction Detector Service
Identifies contradictions between sources, especially for critical claims.
"""

import re
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from services.uncertainty_model import SourceType, SourceClaim


@dataclass
class Contradiction:
    """A contradiction between sources."""
    claim: str
    category: str  # "AUTH", "RATE_LIMIT", "OBJECT_SUPPORT", "FIELD_NAME", etc.
    severity: str  # "CRITICAL", "WARNING", "INFO"
    source_1: str
    source_2: str
    confidence_1: float
    confidence_2: float
    confidence_delta: float
    resolution_strategy: str  # "CONFIDENCE_WEIGHTED", "SOURCE_PRIORITY", "HUMAN_REVIEW"


class ContradictionDetector:
    """Detects contradictions between sources."""
    
    # Critical categories that require stop-the-line
    CRITICAL_CATEGORIES = ["AUTH", "RATE_LIMIT", "OBJECT_SUPPORT"]
    
    # Warning categories
    WARNING_CATEGORIES = ["FIELD_NAME", "DATA_TYPE", "OPTIONAL_FEATURE"]
    
    def __init__(self):
        """Initialize the contradiction detector."""
        pass
    
    def detect_contradictions(
        self,
        claims: List[Any],  # List[SourceClaim]
        category: str = "GENERAL"
    ) -> List[Contradiction]:
        """
        Detect contradictions in a list of claims.
        
        Args:
            claims: List of source claims
            category: Claim category
            
        Returns:
            List of detected contradictions
        """
        contradictions = []
        
        if len(claims) < 2:
            return contradictions
        
        # Compare each pair of claims
        for i, claim1_dict in enumerate(claims):
            for claim2_dict in claims[i+1:]:
                if self._are_contradictory(claim1_dict, claim2_dict):
                    contradiction = self._create_contradiction(
                        claim1_dict=claim1_dict,
                        claim2_dict=claim2_dict,
                        category=category
                    )
                    contradictions.append(contradiction)
        
        return contradictions
    
    def _are_contradictory(self, claim1_dict: Dict[str, Any], claim2_dict: Dict[str, Any]) -> bool:
        """
        Check if two claims are contradictory.
        
        Simple heuristic: claims are contradictory if they differ significantly.
        """
        # Normalize claims for comparison
        c1_normalized = claim1_dict.get('claim', '').lower().strip()
        c2_normalized = claim2_dict.get('claim', '').lower().strip()
        
        # Exact match - not contradictory
        if c1_normalized == c2_normalized:
            return False
        
        # Check for explicit contradictions
        contradiction_indicators = [
            ("not", "yes"),
            ("no", "yes"),
            ("does not", "does"),
            ("unsupported", "supported"),
            ("not available", "available"),
            ("none", "some"),
            ("0", ">0")
        ]
        
        for neg, pos in contradiction_indicators:
            if (neg in c1_normalized and pos in c2_normalized) or \
               (pos in c1_normalized and neg in c2_normalized):
                return True
        
        # Check for significant differences in numbers/values
        # (This is a simplified check - could be enhanced with NLP)
        if self._has_conflicting_values(c1_normalized, c2_normalized):
            return True
        
        return False
    
    def _has_conflicting_values(self, text1: str, text2: str) -> bool:
        """Check if texts have conflicting numeric or categorical values."""
        # Extract numbers
        numbers1 = set(re.findall(r'\d+', text1))
        numbers2 = set(re.findall(r'\d+', text2))
        
        # If both have numbers and they're different, might be contradictory
        if numbers1 and numbers2 and numbers1 != numbers2:
            # Check if they're in similar context (e.g., both rate limits)
            if any(keyword in text1 and keyword in text2 for keyword in 
                   ["rate", "limit", "per", "second", "minute", "hour", "day"]):
                return True
        
        return False
    
    def _create_contradiction(
        self,
        claim1_dict: Dict[str, Any],
        claim2_dict: Dict[str, Any],
        category: str
    ) -> Contradiction:
        """Create a Contradiction object from two conflicting claims."""
        
        # Determine severity
        if category in self.CRITICAL_CATEGORIES:
            severity = "CRITICAL"
        elif category in self.WARNING_CATEGORIES:
            severity = "WARNING"
        else:
            severity = "INFO"
        
        # Determine resolution strategy
        if severity == "CRITICAL":
            resolution_strategy = "HUMAN_REVIEW"
        else:
            resolution_strategy = "CONFIDENCE_WEIGHTED"
        
        confidence_1 = claim1_dict.get('confidence', 0.0)
        confidence_2 = claim2_dict.get('confidence', 0.0)
        confidence_delta = abs(confidence_1 - confidence_2)
        
        return Contradiction(
            claim=f"{claim1_dict.get('claim', '')} vs {claim2_dict.get('claim', '')}",
            category=category,
            severity=severity,
            source_1=f"{claim1_dict.get('source_type', 'UNKNOWN')}: {claim1_dict.get('source_name', '')}",
            source_2=f"{claim2_dict.get('source_type', 'UNKNOWN')}: {claim2_dict.get('source_name', '')}",
            confidence_1=confidence_1,
            confidence_2=confidence_2,
            confidence_delta=confidence_delta,
            resolution_strategy=resolution_strategy
        )
    
    def extract_claims_from_content(
        self,
        content: str,
        category: str = "GENERAL"
    ) -> List[str]:
        """
        Extract factual claims from section content.
        
        This is a simplified extraction - could be enhanced with NLP.
        """
        claims = []
        
        # Look for statements with specific patterns
        # Pattern 1: "X supports Y"
        pattern1 = r'([A-Z][^.]*supports?[^.]*\.)'
        matches1 = re.findall(pattern1, content, re.IGNORECASE)
        claims.extend(matches1)
        
        # Pattern 2: "X is Y"
        pattern2 = r'([A-Z][^.]*is [^.]*\.)'
        matches2 = re.findall(pattern2, content, re.IGNORECASE)
        claims.extend(matches2)
        
        # Pattern 3: "X requires Y"
        pattern3 = r'([A-Z][^.]*requires?[^.]*\.)'
        matches3 = re.findall(pattern3, content, re.IGNORECASE)
        claims.extend(matches3)
        
        # Pattern 4: Rate limit patterns
        if category == "RATE_LIMIT":
            pattern4 = r'(\d+\s*(?:requests?|calls?|queries?)\s*(?:per|/)\s*(?:second|minute|hour|day))'
            matches4 = re.findall(pattern4, content, re.IGNORECASE)
            claims.extend(matches4)
        
        # Deduplicate and clean
        claims = list(set(claims))
        claims = [c.strip() for c in claims if len(c.strip()) > 10]
        
        return claims

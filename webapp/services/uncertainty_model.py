"""
Uncertainty Model Service
Provides confidence scoring and uncertainty modeling for research claims.
"""

from typing import Dict, Optional
from dataclasses import dataclass
from enum import Enum


class SourceType(str, Enum):
    """Source type enumeration."""
    KNOWLEDGE_VAULT = "KNOWLEDGE_VAULT"
    DOCWHISPERER = "DOCWHISPERER"
    OFFICIAL_DOCS = "OFFICIAL_DOCS"
    FIVETRAN = "FIVETRAN"
    GITHUB_CODE = "GITHUB_CODE"
    COMMUNITY = "COMMUNITY"
    BLOG = "BLOG"
    OTHER = "OTHER"


@dataclass
class SourceClaim:
    """A claim from a specific source."""
    claim: str
    source_type: SourceType
    source_name: str
    confidence: float  # 0.0-1.0
    timestamp: Optional[str] = None  # When source was last updated


@dataclass
class UncertaintyFlag:
    """An uncertainty flag for a claim."""
    claim: str
    confidence: float
    source_count: int
    conflicting_sources: bool
    documentation_age: Optional[int] = None  # Days since last update
    category: str = "GENERAL"  # "AUTH", "RATE_LIMIT", "OBJECT_SUPPORT", etc.
    recommendation: str = "VERIFY"  # "VERIFY", "FLAG_IN_DOC", "ASSUME_DEFAULT"


class UncertaintyModel:
    """Model for uncertainty and confidence scoring."""
    
    # Source reliability weights (0.0-1.0)
    SOURCE_WEIGHTS = {
        SourceType.KNOWLEDGE_VAULT: 0.95,  # Pre-indexed official docs - highest
        SourceType.DOCWHISPERER: 0.85,      # Official library docs - high
        SourceType.OFFICIAL_DOCS: 0.75,     # Official docs from web - high
        SourceType.FIVETRAN: 0.65,          # Fivetran docs - signal, not ground truth
        SourceType.GITHUB_CODE: 0.60,       # GitHub code - medium-high
        SourceType.COMMUNITY: 0.40,         # Community sources - medium
        SourceType.BLOG: 0.30,              # Blog posts - low-medium
        SourceType.OTHER: 0.20              # Other sources - low
    }
    
    def __init__(self):
        """Initialize the uncertainty model."""
        pass
    
    def get_source_weight(self, source_type: SourceType) -> float:
        """Get reliability weight for a source type."""
        return self.SOURCE_WEIGHTS.get(source_type, 0.20)
    
    def calculate_confidence(
        self,
        claims: list[SourceClaim],
        require_agreement: bool = False
    ) -> float:
        """
        Calculate overall confidence for a claim based on multiple sources.
        
        Args:
            claims: List of source claims
            require_agreement: If True, confidence is reduced if sources disagree
            
        Returns:
            Confidence score (0.0-1.0)
        """
        if not claims:
            return 0.0
        
        if len(claims) == 1:
            claim = claims[0]
            weight = self.get_source_weight(claim.source_type)
            return claim.confidence * weight
        
        # Calculate weighted average
        weighted_sum = 0.0
        total_weight = 0.0
        
        for claim in claims:
            weight = self.get_source_weight(claim.source_type)
            weighted_sum += claim.confidence * weight
            total_weight += weight
        
        base_confidence = weighted_sum / total_weight if total_weight > 0 else 0.0
        
        # If require_agreement and sources conflict, reduce confidence
        if require_agreement:
            # Check if claims are similar (simple heuristic)
            claim_texts = [c.claim.lower() for c in claims]
            if len(set(claim_texts)) > 1:  # Different claims
                # Reduce confidence by disagreement penalty
                base_confidence *= 0.7
        
        return min(1.0, base_confidence)
    
    def create_uncertainty_flag(
        self,
        claim: str,
        claims: list[SourceClaim],
        category: str = "GENERAL"
    ) -> UncertaintyFlag:
        """
        Create an uncertainty flag for a claim.
        
        Args:
            claim: The claim text
            claims: List of source claims
            category: Claim category (AUTH, RATE_LIMIT, etc.)
            
        Returns:
            UncertaintyFlag
        """
        confidence = self.calculate_confidence(claims, require_agreement=True)
        
        # Check for conflicting sources
        conflicting = len(set(c.claim.lower() for c in claims)) > 1
        
        # Determine recommendation
        if confidence < 0.3:
            recommendation = "VERIFY"
        elif confidence < 0.5:
            recommendation = "FLAG_IN_DOC"
        else:
            recommendation = "ASSUME_DEFAULT"
        
        return UncertaintyFlag(
            claim=claim,
            confidence=confidence,
            source_count=len(claims),
            conflicting_sources=conflicting,
            category=category,
            recommendation=recommendation
        )
    
    def should_flag_uncertainty(
        self,
        confidence: float,
        category: str
    ) -> bool:
        """
        Determine if uncertainty should be flagged.
        
        Args:
            confidence: Confidence score
            category: Claim category
            
        Returns:
            True if should flag
        """
        # Critical categories have lower threshold
        critical_categories = ["AUTH", "RATE_LIMIT", "OBJECT_SUPPORT"]
        
        if category in critical_categories:
            return confidence < 0.6
        else:
            return confidence < 0.5

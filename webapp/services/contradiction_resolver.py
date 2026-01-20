"""
Contradiction Resolver Service
Resolves contradictions using confidence-weighted approach.
"""

from typing import Optional
from dataclasses import dataclass
from .contradiction_detector import Contradiction
from .uncertainty_model import UncertaintyModel, SourceType
from typing import Any


@dataclass
class Resolution:
    """Resolution of a contradiction."""
    selected_claim: Optional[str]  # None if both should be documented
    confidence: float
    uncertainty_note: str
    both_claims: Optional[list] = None
    alternative_claim: Optional[str] = None


class ContradictionResolver:
    """Resolves contradictions using confidence-weighted approach."""
    
    def __init__(self):
        """Initialize the contradiction resolver."""
        self.uncertainty_model = UncertaintyModel()
    
    async def resolve_contradiction(
        self,
        contradiction: Contradiction
    ) -> Resolution:
        """
        Resolve contradiction using confidence-weighted approach.
        
        Args:
            contradiction: The contradiction to resolve
            
        Returns:
            Resolution with selected claim and uncertainty note
        """
        # Extract source types from source strings
        source_1_type = self._extract_source_type(contradiction.source_1)
        source_2_type = self._extract_source_type(contradiction.source_2)
        
        # Get source weights
        source_1_weight = self.uncertainty_model.get_source_weight(source_1_type)
        source_2_weight = self.uncertainty_model.get_source_weight(source_2_type)
        
        # Calculate weighted confidence
        weighted_1 = contradiction.confidence_1 * source_1_weight
        weighted_2 = contradiction.confidence_2 * source_2_weight
        
        # If too close to call (within 0.1), document both
        if abs(weighted_1 - weighted_2) < 0.1:
            return Resolution(
                selected_claim=None,  # Document both
                confidence=min(weighted_1, weighted_2),
                uncertainty_note=(
                    f"Conflicting claims from {contradiction.source_1} (confidence: {contradiction.confidence_1:.2f}) "
                    f"and {contradiction.source_2} (confidence: {contradiction.confidence_2:.2f}). "
                    f"Both documented with confidence scores. Weighted confidence: {min(weighted_1, weighted_2):.2f}."
                ),
                both_claims=[
                    {
                        "claim": contradiction.claim.split(" vs ")[0],
                        "source": contradiction.source_1,
                        "confidence": contradiction.confidence_1,
                        "weighted_confidence": weighted_1
                    },
                    {
                        "claim": contradiction.claim.split(" vs ")[1] if " vs " in contradiction.claim else contradiction.claim,
                        "source": contradiction.source_2,
                        "confidence": contradiction.confidence_2,
                        "weighted_confidence": weighted_2
                    }
                ]
            )
        else:
            # Pick winner based on weighted confidence
            if weighted_1 > weighted_2:
                winner_claim = contradiction.claim.split(" vs ")[0] if " vs " in contradiction.claim else contradiction.claim
                winner_source = contradiction.source_1
                winner_confidence = weighted_1
                loser_source = contradiction.source_2
                loser_claim = contradiction.claim.split(" vs ")[1] if " vs " in contradiction.claim else contradiction.claim
            else:
                winner_claim = contradiction.claim.split(" vs ")[1] if " vs " in contradiction.claim else contradiction.claim
                winner_source = contradiction.source_2
                winner_confidence = weighted_2
                loser_source = contradiction.source_1
                loser_claim = contradiction.claim.split(" vs ")[0] if " vs " in contradiction.claim else contradiction.claim
            
            return Resolution(
                selected_claim=winner_claim,
                confidence=winner_confidence,
                uncertainty_note=(
                    f"Selected based on confidence-weighted analysis (weighted confidence: {winner_confidence:.2f}). "
                    f"Alternative claim from {loser_source} documented below with lower confidence."
                ),
                alternative_claim=f"{loser_claim} (Source: {loser_source}, Confidence: {min(weighted_1, weighted_2):.2f})"
            )
    
    def _extract_source_type(self, source_string: str) -> SourceType:
        """Extract SourceType from source string."""
        source_lower = source_string.lower()
        
        if "vault" in source_lower or "knowledge" in source_lower:
            return SourceType.KNOWLEDGE_VAULT
        elif "docwhisperer" in source_lower or "doc whisperer" in source_lower:
            return SourceType.DOCWHISPERER
        elif "fivetran" in source_lower:
            return SourceType.FIVETRAN
        elif "github" in source_lower:
            return SourceType.GITHUB_CODE
        elif "official" in source_lower or "docs" in source_lower:
            return SourceType.OFFICIAL_DOCS
        elif "blog" in source_lower:
            return SourceType.BLOG
        elif "community" in source_lower:
            return SourceType.COMMUNITY
        else:
            return SourceType.OTHER

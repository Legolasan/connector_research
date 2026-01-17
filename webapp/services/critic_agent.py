"""
Critic Agent Service
Reviews generated research sections for quality, contradictions, and uncertainty.
"""

import os
import json
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from openai import AsyncOpenAI
from dotenv import load_dotenv

load_dotenv()


@dataclass
class ReviewIssue:
    """An issue found during section review."""
    severity: str  # "CRITICAL", "WARNING", "INFO"
    category: str  # "CONTRADICTION", "UNCERTAINTY", "MISSING_INFO", "ENGINEERING_COST"
    description: str
    source_1: str
    source_2: Optional[str] = None  # If contradiction
    confidence_1: float = 0.0
    confidence_2: Optional[float] = None
    recommendation: str = ""


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


@dataclass
class SectionReview:
    """Review result for a section."""
    section_number: int
    section_name: str
    approval_status: str  # "APPROVED", "NEEDS_REVISION", "STOP_THE_LINE"
    confidence_score: float  # 0.0-1.0
    issues: List[ReviewIssue] = field(default_factory=list)
    contradictions: List[Contradiction] = field(default_factory=list)
    uncertainty_flags: List[UncertaintyFlag] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)


class CriticAgent:
    """Agent that reviews generated research sections for quality and accuracy."""
    
    def __init__(self):
        """Initialize the Critic Agent."""
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.model = os.getenv("RESEARCH_MODEL", "gpt-4o")
        
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY is required")
        
        self.client = AsyncOpenAI(api_key=self.openai_api_key)
    
    async def review_section(
        self,
        section_number: int,
        section_name: str,
        content: str,
        sources: Dict[str, Any],
        previous_sections: Optional[List[str]] = None
    ) -> SectionReview:
        """
        Review a generated section for quality, contradictions, and uncertainty.
        
        Args:
            section_number: Section number
            section_name: Section name
            content: Generated section content
            sources: Dictionary of source contexts (vault, docwhisperer, web, fivetran, github)
            previous_sections: List of previous section contents for consistency checking
            
        Returns:
            SectionReview with approval status and issues
        """
        
        # Build review prompt
        review_prompt = self._build_review_prompt(
            section_number=section_number,
            section_name=section_name,
            content=content,
            sources=sources,
            previous_sections=previous_sections or []
        )
        
        # Call LLM for review
        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[
                {
                    "role": "system",
                    "content": """You are a critical reviewer of technical documentation. Your task is to:
1. Identify factual contradictions between sources
2. Flag claims with low confidence or uncertainty
3. Check for missing critical information
4. Assess engineering feasibility concerns
5. Provide specific recommendations for improvement

Be thorough but fair. Focus on:
- Critical contradictions (auth methods, rate limits, object support)
- Low confidence claims (< 0.5 confidence)
- Missing essential information
- Engineering complexity concerns

Output your review as structured JSON."""
                },
                {
                    "role": "user",
                    "content": review_prompt
                }
            ],
            response_format={"type": "json_object"},
            temperature=0.3
        )
        
        # Parse review response
        review_data = json.loads(response.choices[0].message.content)
        
        # Build SectionReview from response
        review = self._parse_review_response(
            section_number=section_number,
            section_name=section_name,
            review_data=review_data
        )
        
        return review
    
    def _build_review_prompt(
        self,
        section_number: int,
        section_name: str,
        content: str,
        sources: Dict[str, Any],
        previous_sections: List[str]
    ) -> str:
        """Build the review prompt."""
        
        sources_text = ""
        if sources.get("vault"):
            sources_text += f"\n**Knowledge Vault Context:**\n{sources['vault'][:2000]}...\n"
        if sources.get("docwhisperer"):
            sources_text += f"\n**DocWhisperer Context:**\n{sources['docwhisperer'][:2000]}...\n"
        if sources.get("web"):
            sources_text += f"\n**Web Search Context:**\n{sources['web'][:2000]}...\n"
        if sources.get("fivetran"):
            sources_text += f"\n**Fivetran Context (Reference Only):**\n{sources['fivetran'][:2000]}...\n"
        if sources.get("github"):
            sources_text += f"\n**GitHub Code Context:**\n{sources['github'][:2000]}...\n"
        
        previous_context = ""
        if previous_sections:
            previous_context = "\n\n**Previous Sections (for consistency):**\n"
            for i, prev_content in enumerate(previous_sections[-3:], 1):  # Last 3 sections
                previous_context += f"\n--- Previous Section {i} ---\n{prev_content[:1000]}...\n"
        
        prompt = f"""Review Section {section_number}: {section_name}

**Generated Content:**
{content}

**Source Contexts:**
{sources_text}

{previous_context}

**Review Tasks:**
1. Extract all factual claims from the content
2. Compare claims against source contexts
3. Identify contradictions (especially for: auth methods, rate limits, object support)
4. Flag low confidence claims (confidence < 0.5)
5. Check for missing critical information
6. Assess engineering feasibility concerns

**Output JSON Format:**
{{
    "approval_status": "APPROVED" | "NEEDS_REVISION" | "STOP_THE_LINE",
    "confidence_score": 0.0-1.0,
    "contradictions": [
        {{
            "claim": "exact claim text",
            "category": "AUTH" | "RATE_LIMIT" | "OBJECT_SUPPORT" | "FIELD_NAME" | "OTHER",
            "severity": "CRITICAL" | "WARNING" | "INFO",
            "source_1": "source name",
            "source_2": "source name",
            "confidence_1": 0.0-1.0,
            "confidence_2": 0.0-1.0,
            "resolution_strategy": "CONFIDENCE_WEIGHTED" | "SOURCE_PRIORITY" | "HUMAN_REVIEW"
        }}
    ],
    "uncertainty_flags": [
        {{
            "claim": "exact claim text",
            "confidence": 0.0-1.0,
            "source_count": number,
            "conflicting_sources": true/false,
            "category": "AUTH" | "RATE_LIMIT" | "OBJECT_SUPPORT" | "GENERAL",
            "recommendation": "VERIFY" | "FLAG_IN_DOC" | "ASSUME_DEFAULT"
        }}
    ],
    "issues": [
        {{
            "severity": "CRITICAL" | "WARNING" | "INFO",
            "category": "CONTRADICTION" | "UNCERTAINTY" | "MISSING_INFO" | "ENGINEERING_COST",
            "description": "detailed description",
            "source_1": "source name",
            "source_2": "source name or null",
            "confidence_1": 0.0-1.0,
            "confidence_2": 0.0-1.0 or null,
            "recommendation": "specific recommendation"
        }}
    ],
    "recommendations": [
        "list of specific recommendations"
    ]
}}"""
        
        return prompt
    
    def _parse_review_response(
        self,
        section_number: int,
        section_name: str,
        review_data: Dict[str, Any]
    ) -> SectionReview:
        """Parse LLM review response into SectionReview."""
        
        # Parse contradictions
        contradictions = []
        for c in review_data.get("contradictions", []):
            contradictions.append(Contradiction(
                claim=c.get("claim", ""),
                category=c.get("category", "OTHER"),
                severity=c.get("severity", "INFO"),
                source_1=c.get("source_1", ""),
                source_2=c.get("source_2", ""),
                confidence_1=c.get("confidence_1", 0.0),
                confidence_2=c.get("confidence_2", 0.0),
                confidence_delta=abs(c.get("confidence_1", 0.0) - c.get("confidence_2", 0.0)),
                resolution_strategy=c.get("resolution_strategy", "CONFIDENCE_WEIGHTED")
            ))
        
        # Parse uncertainty flags
        uncertainty_flags = []
        for u in review_data.get("uncertainty_flags", []):
            uncertainty_flags.append(UncertaintyFlag(
                claim=u.get("claim", ""),
                confidence=u.get("confidence", 0.0),
                source_count=u.get("source_count", 0),
                conflicting_sources=u.get("conflicting_sources", False),
                category=u.get("category", "GENERAL"),
                recommendation=u.get("recommendation", "VERIFY")
            ))
        
        # Parse issues
        issues = []
        for i in review_data.get("issues", []):
            issues.append(ReviewIssue(
                severity=i.get("severity", "INFO"),
                category=i.get("category", "GENERAL"),
                description=i.get("description", ""),
                source_1=i.get("source_1", ""),
                source_2=i.get("source_2"),
                confidence_1=i.get("confidence_1", 0.0),
                confidence_2=i.get("confidence_2"),
                recommendation=i.get("recommendation", "")
            ))
        
        return SectionReview(
            section_number=section_number,
            section_name=section_name,
            approval_status=review_data.get("approval_status", "APPROVED"),
            confidence_score=review_data.get("confidence_score", 1.0),
            issues=issues,
            contradictions=contradictions,
            uncertainty_flags=uncertainty_flags,
            recommendations=review_data.get("recommendations", [])
        )

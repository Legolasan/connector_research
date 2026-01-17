"""
Engineering Cost Analyzer Service
Assesses implementation complexity and maintenance burden.
"""

import os
import json
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from openai import AsyncOpenAI
from dotenv import load_dotenv

load_dotenv()


@dataclass
class EngineeringCost:
    """Engineering cost assessment for a method or feature."""
    method_name: str
    complexity_score: float  # 0.0-1.0
    implementation_effort: str  # "LOW", "MEDIUM", "HIGH", "VERY_HIGH"
    maintenance_burden: str  # "LOW", "MEDIUM", "HIGH"
    risk_factors: List[str] = field(default_factory=list)
    recommendation: str = "RECOMMENDED"  # "RECOMMENDED", "CONDITIONAL", "AVOID"


class EngineeringCostAnalyzer:
    """Analyzes engineering cost and complexity."""
    
    def __init__(self):
        """Initialize the engineering cost analyzer."""
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.model = os.getenv("RESEARCH_MODEL", "gpt-4o")
        
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY is required")
        
        self.client = AsyncOpenAI(api_key=self.openai_api_key)
    
    async def analyze_method(
        self,
        method_name: str,
        method_content: str,
        documentation_quality: str = "UNKNOWN"
    ) -> EngineeringCost:
        """
        Analyze engineering cost for an extraction method.
        
        Args:
            method_name: Name of the extraction method
            method_content: Generated content for the method section
            documentation_quality: Quality of documentation ("EXCELLENT", "GOOD", "POOR", "UNKNOWN")
            
        Returns:
            EngineeringCost assessment
        """
        
        # Build analysis prompt
        analysis_prompt = self._build_analysis_prompt(
            method_name=method_name,
            method_content=method_content,
            documentation_quality=documentation_quality
        )
        
        # Call LLM for analysis
        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[
                {
                    "role": "system",
                    "content": """You are an engineering cost analyst for ETL connector development. Your task is to:
1. Assess implementation complexity (0.0-1.0 scale)
2. Estimate implementation effort (LOW, MEDIUM, HIGH, VERY_HIGH)
3. Assess maintenance burden (LOW, MEDIUM, HIGH)
4. Identify risk factors (undocumented APIs, breaking changes, etc.)
5. Provide recommendation (RECOMMENDED, CONDITIONAL, AVOID)

Consider:
- Documentation quality and completeness
- Authentication complexity
- Rate limiting complexity
- Error handling requirements
- Pagination complexity
- Data volume challenges
- API stability

Output as structured JSON."""
                },
                {
                    "role": "user",
                    "content": analysis_prompt
                }
            ],
            response_format={"type": "json_object"},
            temperature=0.3
        )
        
        # Parse analysis response
        analysis_data = json.loads(response.choices[0].message.content)
        
        # Build EngineeringCost from response
        return self._parse_analysis_response(
            method_name=method_name,
            analysis_data=analysis_data
        )
    
    def _build_analysis_prompt(
        self,
        method_name: str,
        method_content: str,
        documentation_quality: str
    ) -> str:
        """Build the analysis prompt."""
        
        prompt = f"""Analyze the engineering cost for implementing {method_name} extraction method.

**Method Content:**
{method_content[:4000]}...

**Documentation Quality:** {documentation_quality}

**Analysis Tasks:**
1. Assess implementation complexity (0.0 = trivial, 1.0 = extremely complex)
2. Estimate implementation effort based on:
   - Authentication setup complexity
   - API endpoint complexity
   - Rate limiting requirements
   - Error handling needs
   - Pagination complexity
   - Data transformation needs
3. Assess maintenance burden based on:
   - Documentation completeness
   - API stability
   - Breaking change frequency
   - Error rate
4. Identify risk factors:
   - Undocumented APIs
   - Frequent breaking changes
   - Complex rate limiting
   - Unpredictable errors
   - High maintenance requirements
5. Provide recommendation:
   - RECOMMENDED: Low complexity, high reliability, good docs
   - CONDITIONAL: Use only if customer requires specific features
   - AVOID: High complexity, low value, poor docs

**Output JSON Format:**
{{
    "complexity_score": 0.0-1.0,
    "implementation_effort": "LOW" | "MEDIUM" | "HIGH" | "VERY_HIGH",
    "maintenance_burden": "LOW" | "MEDIUM" | "HIGH",
    "risk_factors": [
        "list of risk factors"
    ],
    "recommendation": "RECOMMENDED" | "CONDITIONAL" | "AVOID"
}}"""
        
        return prompt
    
    def _parse_analysis_response(
        self,
        method_name: str,
        analysis_data: Dict[str, Any]
    ) -> EngineeringCost:
        """Parse LLM analysis response into EngineeringCost."""
        
        return EngineeringCost(
            method_name=method_name,
            complexity_score=analysis_data.get("complexity_score", 0.5),
            implementation_effort=analysis_data.get("implementation_effort", "MEDIUM"),
            maintenance_burden=analysis_data.get("maintenance_burden", "MEDIUM"),
            risk_factors=analysis_data.get("risk_factors", []),
            recommendation=analysis_data.get("recommendation", "CONDITIONAL")
        )
    
    def analyze_object_extraction(
        self,
        object_name: str,
        extraction_method: str,
        documentation_available: bool = True
    ) -> Dict[str, Any]:
        """
        Analyze engineering cost for extracting a specific object.
        
        Args:
            object_name: Name of the object
            extraction_method: Extraction method used
            documentation_available: Whether documentation is available
            
        Returns:
            Dictionary with cost assessment
        """
        # Simplified heuristic-based analysis
        complexity = 0.3  # Base complexity
        
        if not documentation_available:
            complexity += 0.3
        
        if "GraphQL" in extraction_method:
            complexity += 0.2  # GraphQL can be more complex
        
        if "SOAP" in extraction_method:
            complexity += 0.2  # SOAP is more complex
        
        effort = "LOW" if complexity < 0.4 else "MEDIUM" if complexity < 0.7 else "HIGH"
        maintenance = "LOW" if documentation_available else "MEDIUM"
        
        return {
            "object_name": object_name,
            "complexity_score": complexity,
            "implementation_effort": effort,
            "maintenance_burden": maintenance,
            "risk_factors": [] if documentation_available else ["Undocumented API"]
        }

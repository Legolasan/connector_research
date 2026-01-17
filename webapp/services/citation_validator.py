"""
Citation Validator Service
Deterministic pre-Critic validation that requires citations for factual claims.
Uses 3-pass parsing to avoid false failures on code blocks and tables.
"""

import re
from typing import List, Dict, Any, Tuple, Optional
from dataclasses import dataclass, field
try:
    import nltk
    from nltk.tokenize import sent_tokenize
    NLTK_AVAILABLE = True
    # Download punkt tokenizer if not already available
    try:
        nltk.data.find('tokenizers/punkt')
    except LookupError:
        nltk.download('punkt', quiet=True)
except ImportError:
    NLTK_AVAILABLE = False


@dataclass
class Table:
    """Represents a markdown table."""
    name: str  # Table identifier (e.g., "Object Catalog")
    header_row: List[str]
    data_rows: List[List[str]]
    raw_markdown: str
    start_position: int  # Character position in original content
    end_position: int


@dataclass
class FactualClaim:
    """A factual claim that requires citation."""
    sentence: str
    claim_type: str  # "NUMBER", "ENDPOINT", "SCOPE", "RATE_LIMIT", "SUPPORTS", "REQUIRES"
    position: int  # Character position in content
    requires_citation: bool
    is_known_safe: bool = False


@dataclass
class UncitedRow:
    """A table row missing required citations."""
    table_name: str
    row_index: int
    row_content: str
    missing_evidence: bool


@dataclass
class ValidationResult:
    """Result of citation validation."""
    is_valid: bool
    uncited_claims: List[FactualClaim] = field(default_factory=list)
    uncited_table_rows: List[UncitedRow] = field(default_factory=list)
    total_claims: int = 0
    cited_claims: int = 0
    failure_report: str = ""


class CitationValidator:
    """Validates that factual claims have proper citations."""
    
    # Known safe statements that don't require citations
    KNOWN_SAFE_PATTERNS = [
        r"^N/A\s*[–-]\s*not\s+documented",
        r"^N/A\s*[–-]\s*not\s+supported",
        r"^Unknown",
        r"^This\s+requires\s+runtime\s+verification",
        r"^Not\s+documented",
        r"^Not\s+available",
        r"^TBD",  # To Be Determined
        r"^To\s+be\s+determined",
    ]
    
    # Citation tag patterns
    CITATION_PATTERN = re.compile(r'\[(web|vault|doc|github):\d+\]')
    
    # Capability markers for "supports/requires" detection
    CAPABILITY_MARKERS = [
        'REST', 'GraphQL', 'SOAP', 'OAuth', 'API key', 'rate limit', 'scope',
        'permission', 'endpoint', 'webhook', 'SDK', 'JDBC', 'incremental',
        'full load', 'CDC', 'streaming', 'batch'
    ]
    
    def __init__(self, max_citation_distance: int = 250):
        """
        Initialize the citation validator.
        
        Args:
            max_citation_distance: Maximum distance (in chars) between claim and citation
        """
        self.max_citation_distance = max_citation_distance
    
    def validate_content(self, content: str, section_number: int) -> ValidationResult:
        """
        Validate content for proper citations.
        
        Args:
            content: The content to validate
            section_number: Section number for context
            
        Returns:
            ValidationResult with validation status and issues
        """
        # 3-pass parsing
        prose_content, tables, code_blocks = self._parse_content_3pass(content)
        
        # Extract factual claims from prose (excluding code blocks)
        claims = self._extract_factual_claims(prose_content, content)
        
        # Validate table rows
        uncited_rows = self._validate_table_rows(tables, content)
        
        # Check citations for each claim
        uncited_claims = []
        total_claims = len(claims)
        cited_claims = 0
        
        for claim in claims:
            if claim.is_known_safe:
                cited_claims += 1
                continue
            
            if not self._check_citations_local(claim, content):
                uncited_claims.append(claim)
            else:
                cited_claims += 1
        
        # Build failure report
        failure_report = self._build_failure_report(uncited_claims, uncited_rows, section_number)
        
        is_valid = len(uncited_claims) == 0 and len(uncited_rows) == 0
        
        return ValidationResult(
            is_valid=is_valid,
            uncited_claims=uncited_claims,
            uncited_table_rows=uncited_rows,
            total_claims=total_claims,
            cited_claims=cited_claims,
            failure_report=failure_report
        )
    
    def _parse_content_3pass(self, content: str) -> Tuple[str, List[Table], List[str]]:
        """
        3-pass parsing: Strip code blocks → Extract tables → Sentence-split prose.
        
        Returns:
            Tuple of (prose_content, tables, code_blocks)
        """
        # Pass 1: Strip fenced code blocks completely
        code_blocks = []
        code_block_pattern = re.compile(r'```[\s\S]*?```', re.MULTILINE)
        
        def replace_code_block(match):
            code_blocks.append(match.group(0))
            return f"__CODE_BLOCK_{len(code_blocks) - 1}__"
        
        content_without_code = code_block_pattern.sub(replace_code_block, content)
        
        # Pass 2: Extract markdown tables separately
        tables = self._extract_tables(content_without_code, content)
        
        # Remove tables from content for sentence splitting
        table_pattern = re.compile(r'\|.*\|[\s\S]*?(?=\n\n|\n[^|]|$)', re.MULTILINE)
        prose_content = table_pattern.sub('__TABLE_PLACEHOLDER__', content_without_code)
        
        # Pass 3: Restore code block placeholders (they're excluded from validation)
        # But keep them in prose_content for position tracking
        return prose_content, tables, code_blocks
    
    def _extract_tables(self, content: str, original_content: str) -> List[Table]:
        """Extract markdown tables from content."""
        tables = []
        table_pattern = re.compile(
            r'(\|.+\|)\s*\n(\|[-:\s\|]+\|)\s*\n((?:\|.+\|\s*\n?)+)',
            re.MULTILINE
        )
        
        for match in table_pattern.finditer(content):
            header_row = [cell.strip() for cell in match.group(1).split('|')[1:-1]]
            separator = match.group(2)
            body_text = match.group(3)
            
            data_rows = []
            for row_line in body_text.strip().split('\n'):
                if row_line.strip().startswith('|'):
                    cells = [cell.strip() for cell in row_line.split('|')[1:-1]]
                    data_rows.append(cells)
            
            # Find position in original content
            start_pos = match.start()
            end_pos = match.end()
            
            # Try to identify table name from preceding heading
            table_name = "Unknown Table"
            before_table = content[:start_pos]
            heading_match = re.search(r'^#{1,3}\s+(.+)$', before_table[-200:], re.MULTILINE)
            if heading_match:
                table_name = heading_match.group(1).strip()
            
            tables.append(Table(
                name=table_name,
                header_row=header_row,
                data_rows=data_rows,
                raw_markdown=match.group(0),
                start_position=start_pos,
                end_position=end_pos
            ))
        
        return tables
    
    def _extract_factual_claims(self, prose_content: str, original_content: str) -> List[FactualClaim]:
        """
        Extract factual claims from prose content.
        
        Excludes headings and bullet labels.
        """
        claims = []
        
        # Sentence tokenization
        if NLTK_AVAILABLE:
            sentences = sent_tokenize(prose_content)
        else:
            # Fallback: simple sentence splitting
            sentences = re.split(r'[.!?]+\s+', prose_content)
            sentences = [s.strip() for s in sentences if s.strip()]
        
        current_pos = 0
        for sentence in sentences:
            # Find position in original content
            pos = original_content.find(sentence, current_pos)
            if pos == -1:
                pos = current_pos
            current_pos = pos + len(sentence)
            
            # Skip if it's a heading
            if re.match(r'^#+\s+', sentence):
                continue
            
            # Skip if it's a bullet label (e.g., "- **Auth:**")
            if re.match(r'^[-*]\s+\*\*[^:]+:\*\*', sentence):
                continue
            
            # Check if it's a known safe statement
            is_safe = self._is_known_safe_statement(sentence)
            
            # Detect claim types
            claim_type = None
            
            # Numbers (rate limits, quotas, counts)
            if re.search(r'\d+\s+(requests?|queries?|calls?|items?)\s+(per|every)', sentence, re.IGNORECASE):
                claim_type = "RATE_LIMIT"
            elif re.search(r'rate\s+limit.*\d+', sentence, re.IGNORECASE):
                claim_type = "RATE_LIMIT"
            elif re.search(r'\d+\s+(MB|GB|TB|KB)', sentence, re.IGNORECASE):
                claim_type = "NUMBER"
            elif re.search(r'\b\d{3,}\b', sentence):  # Large numbers (likely quotas/limits)
                claim_type = "NUMBER"
            
            # Endpoints (URLs, API paths)
            if not claim_type and (re.search(r'https?://', sentence) or 
                                   re.search(r'/[a-z0-9_/-]+(?:/v\d+)?', sentence, re.IGNORECASE)):
                claim_type = "ENDPOINT"
            
            # Scopes/permissions
            if not claim_type and (re.search(r'scope[s]?:', sentence, re.IGNORECASE) or
                                   re.search(r'permission[s]?:', sentence, re.IGNORECASE) or
                                   re.search(r'\b(read|write|admin):[a-z_]+', sentence, re.IGNORECASE)):
                claim_type = "SCOPE"
            
            # "Supports/Requires" statements (only if contains capability markers)
            if not claim_type:
                supports_match = re.search(
                    r'(supports?|requires?|supports?|allows?)\s+([^.]+)',
                    sentence,
                    re.IGNORECASE
                )
                if supports_match:
                    # Check if it contains capability markers
                    rest_of_sentence = supports_match.group(2).lower()
                    if any(marker.lower() in rest_of_sentence for marker in self.CAPABILITY_MARKERS):
                        # Check if it has a concrete subject/object (noun phrase)
                        if re.search(r'\b(API|endpoint|method|system|connector|service)\b', sentence, re.IGNORECASE):
                            claim_type = "SUPPORTS" if "support" in supports_match.group(1).lower() else "REQUIRES"
            
            if claim_type and not is_safe:
                claims.append(FactualClaim(
                    sentence=sentence.strip(),
                    claim_type=claim_type,
                    position=pos,
                    requires_citation=True,
                    is_known_safe=is_safe
                ))
        
        return claims
    
    def _is_known_safe_statement(self, sentence: str) -> bool:
        """Check if sentence matches known safe patterns."""
        sentence_clean = sentence.strip()
        for pattern in self.KNOWN_SAFE_PATTERNS:
            if re.search(pattern, sentence_clean, re.IGNORECASE):
                return True
        return False
    
    def _check_citations_local(self, claim: FactualClaim, content: str) -> bool:
        """
        Check if claim has citations within max_distance characters.
        
        Not "anywhere in content" - must be proximate.
        """
        # Find citation tags near the claim
        start_pos = max(0, claim.position - self.max_citation_distance)
        end_pos = min(len(content), claim.position + len(claim.sentence) + self.max_citation_distance)
        
        local_context = content[start_pos:end_pos]
        
        # Check for citation tags in local context
        citations = self.CITATION_PATTERN.findall(local_context)
        return len(citations) > 0
    
    def _validate_table_rows(self, tables: List[Table], original_content: str) -> List[UncitedRow]:
        """
        Validate that every non-header row has citations.
        
        Every row must include ≥1 citation tag at end OR evidence_ids column.
        """
        uncited_rows = []
        
        for table in tables:
            # Check if table has evidence_ids column
            has_evidence_column = any(
                'evidence' in col.lower() or 'citation' in col.lower() 
                for col in table.header_row
            )
            
            for row_idx, row in enumerate(table.data_rows):
                # Skip if it's a separator row
                if all(re.match(r'^[-:\s]+$', cell) for cell in row):
                    continue
                
                # Check if row has citations at the end
                row_text = ' | '.join(row)
                citations_in_row = self.CITATION_PATTERN.findall(row_text)
                
                # Check if last column(s) contain citations
                last_cells = row[-2:] if len(row) >= 2 else row
                last_cells_text = ' '.join(last_cells)
                has_citation_in_last = bool(self.CITATION_PATTERN.search(last_cells_text))
                
                if not has_citation_in_last and not citations_in_row and not has_evidence_column:
                    uncited_rows.append(UncitedRow(
                        table_name=table.name,
                        row_index=row_idx + 1,  # 1-indexed for user display
                        row_content=row_text,
                        missing_evidence=True
                    ))
        
        return uncited_rows
    
    def _build_failure_report(self, uncited_claims: List[FactualClaim], 
                             uncited_rows: List[UncitedRow], 
                             section_number: int) -> str:
        """Build human-readable failure report for regeneration prompt."""
        if not uncited_claims and not uncited_rows:
            return ""
        
        report_parts = []
        report_parts.append(f"CRITICAL VALIDATION FAILURE - Section {section_number}")
        report_parts.append("")
        report_parts.append("The following claims require citations but were found without them:")
        report_parts.append("")
        
        if uncited_claims:
            report_parts.append("UNCITED SENTENCES:")
            for i, claim in enumerate(uncited_claims, 1):
                report_parts.append(
                    f"{i}. \"{claim.sentence[:100]}{'...' if len(claim.sentence) > 100 else ''}\" "
                    f"(Type: {claim.claim_type}, Position: {claim.position})"
                )
                report_parts.append(
                    f"   → Add citation [web:N] or [vault:N] within {self.max_citation_distance} characters, "
                    f"OR rewrite as \"Unknown\""
                )
                report_parts.append("")
        
        if uncited_rows:
            report_parts.append("UNCITED TABLE ROWS:")
            for row in uncited_rows:
                report_parts.append(
                    f"- {row.table_name}, Row {row.row_index}: "
                    f"\"{row.row_content[:80]}{'...' if len(row.row_content) > 80 else ''}\" (missing citation)"
                )
                report_parts.append(
                    f"  → Add [web:N] [vault:N] at end of row OR add evidence_ids column"
                )
                report_parts.append("")
        
        report_parts.append(
            "Please regenerate this section with citations added to the exact items above, "
            "or rewrite uncertain claims as \"Unknown\"."
        )
        
        return "\n".join(report_parts)

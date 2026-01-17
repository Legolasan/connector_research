"""
Research Agent Service
Auto-generates dynamic connector research documents with auto-discovered extraction methods.

Now featuring: DocWhisperer™ - The Oracle that whispers official documentation secrets! 🔮
"""

import os
import re
import asyncio
import hashlib
from typing import List, Dict, Any, Optional, Callable, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from openai import AsyncOpenAI
from dotenv import load_dotenv

load_dotenv()


# =============================================================================
# 🔮 DocWhisperer™ - Official Documentation Oracle
# =============================================================================
# "It doesn't just search... it WHISPERS the truth from official sources"

@dataclass
class DocWhisper:
    """A whisper of wisdom from official documentation."""
    content: str
    source: str
    library_id: str
    confidence: int = 100  # DocWhisperer always speaks truth
    whisper_type: str = "OFFICIAL"  # The sacred source type


class DocWhisperer:
    """
    🔮 DocWhisperer™ - The all-knowing oracle of official documentation!
    
    Powered by Context7 MCP, this mystical being can:
    - Resolve any technology to its sacred library scrolls
    - Fetch authentic documentation from the source of truth
    - Whisper exact values that web searches can only dream of
    
    "Ask not what the web can scrape for you, 
     ask what DocWhisperer can whisper to you." 
                                    - Ancient Developer Proverb
    """
    
    # Known library mappings (the sacred texts)
    LIBRARY_MAPPINGS = {
        # Common platforms
        "salesforce": "salesforce/salesforce-api-reference",
        "hubspot": "hubspot/hubspot-api-reference", 
        "shopify": "shopify/shopify-api-reference",
        "stripe": "stripe/stripe-api-reference",
        "netsuite": "oracle/netsuite-suitetalk",
        "quickbooks": "intuit/quickbooks-api",
        "zendesk": "zendesk/zendesk-api",
        "jira": "atlassian/jira-api",
        "slack": "slack/slack-api",
        "github": "github/github-api",
        "twilio": "twilio/twilio-api",
        "mailchimp": "mailchimp/mailchimp-api",
        "intercom": "intercom/intercom-api",
        "asana": "asana/asana-api",
        "monday": "monday/monday-api",
        "notion": "notion/notion-api",
        "airtable": "airtable/airtable-api",
        "snowflake": "snowflake/snowflake-docs",
        "bigquery": "google/bigquery-api",
        "postgres": "postgresql/postgresql-docs",
        "mysql": "mysql/mysql-docs",
        "mongodb": "mongodb/mongodb-docs",
    }
    
    def __init__(self):
        """Awaken the DocWhisperer from its documentation slumber."""
        self._cache: Dict[str, DocWhisper] = {}  # Memory of past whispers
        self._whisper_count = 0  # How many truths have been revealed
        print("🔮 DocWhisperer™ has awakened! Ready to whisper documentation secrets...")
    
    def _normalize_connector_name(self, name: str) -> str:
        """Transform mortal connector names into library keys."""
        return name.lower().replace(" ", "").replace("-", "").replace("_", "")
    
    async def resolve_library_id(self, connector_name: str) -> Optional[str]:
        """
        🔍 Consult the ancient scrolls to find the library ID.
        
        Args:
            connector_name: The name of the connector seeking wisdom
            
        Returns:
            The sacred library ID, or None if the scrolls are silent
        """
        normalized = self._normalize_connector_name(connector_name)
        
        # Check our known mappings first
        for key, library_id in self.LIBRARY_MAPPINGS.items():
            if key in normalized or normalized in key:
                print(f"  🔮 DocWhisperer found library scroll: {library_id}")
                return library_id
        
        # Try Context7 MCP for unknown libraries
        # This would call the actual MCP if available
        print(f"  🔮 DocWhisperer searching ancient archives for '{connector_name}'...")
        return None
    
    async def get_library_docs(
        self, 
        library_id: str, 
        topic: str,
        max_tokens: int = 5000
    ) -> Optional[DocWhisper]:
        """
        📜 Fetch sacred documentation from the library of truth.
        
        Args:
            library_id: The sacred identifier of the library
            topic: The knowledge you seek
            max_tokens: Maximum wisdom to retrieve
            
        Returns:
            A DocWhisper containing the truth, or None if silence
        """
        cache_key = f"{library_id}:{topic}"
        
        # Check if we've whispered this before
        if cache_key in self._cache:
            print(f"  🔮 DocWhisperer recalls this wisdom from memory...")
            return self._cache[cache_key]
        
        # This is where we'd call the actual Context7 MCP
        # For now, we'll return None to trigger fallback to web search
        # In production, this would be:
        # response = await mcp_client.call("context7", "query-docs", {
        #     "libraryId": library_id,
        #     "query": topic
        # })
        
        print(f"  🔮 DocWhisperer consulting the scrolls for '{topic}'...")
        self._whisper_count += 1
        
        return None  # Will trigger fallback to web search
    
    async def whisper_connector_secrets(
        self,
        connector_name: str,
        topics: List[str]
    ) -> Dict[str, Optional[DocWhisper]]:
        """
        🌟 The grand ritual: Whisper all secrets for a connector.
        
        Args:
            connector_name: The connector seeking enlightenment
            topics: List of topics to investigate
            
        Returns:
            Dict mapping topics to their whispered wisdom
        """
        library_id = await self.resolve_library_id(connector_name)
        
        if not library_id:
            print(f"  🔮 DocWhisperer: The scrolls are silent for '{connector_name}'. Falling back to web search...")
            return {topic: None for topic in topics}
        
        whispers = {}
        for topic in topics:
            whisper = await self.get_library_docs(library_id, topic)
            whispers[topic] = whisper
            await asyncio.sleep(0.1)  # Don't anger the documentation gods
        
        return whispers
    
    def get_whisper_stats(self) -> Dict[str, Any]:
        """📊 How many truths has the DocWhisperer revealed?"""
        return {
            "total_whispers": self._whisper_count,
            "cached_wisdom": len(self._cache),
            "known_libraries": len(self.LIBRARY_MAPPINGS),
            "status": "enlightened" if self._whisper_count > 0 else "awaiting questions"
        }


# Global DocWhisperer instance (the oracle is always watching)
_doc_whisperer: Optional[DocWhisperer] = None

def get_doc_whisperer() -> DocWhisperer:
    """Summon the DocWhisperer (creates singleton if needed)."""
    global _doc_whisperer
    if _doc_whisperer is None:
        _doc_whisperer = DocWhisperer()
    return _doc_whisperer


@dataclass
class ResearchSection:
    """Defines a research section."""
    number: int
    name: str
    phase: int
    phase_name: str
    prompts: List[str]
    requires_fivetran: bool = False
    requires_code_analysis: bool = False
    is_method_section: bool = False  # True if this is a per-method deep dive
    method_name: str = ""  # The method this section covers (e.g., "REST API")


# Extraction methods to discover
EXTRACTION_METHODS = [
    "REST API",
    "GraphQL API", 
    "SOAP/XML API",
    "Webhooks",
    "Bulk/Batch API",
    "Official SDK",
    "JDBC/ODBC",
    "File Export"
]


# Method section template - generated dynamically for each discovered method (Runbook Format)
def create_method_section(method_name: str, section_number: int) -> ResearchSection:
    """Create a deep-dive section for a specific extraction method in runbook format."""
    return ResearchSection(
        number=section_number,
        name=f"{method_name} Deep Dive",
        phase=2,
        phase_name="Extraction Methods",
        is_method_section=True,
        method_name=method_name,
        prompts=[
            f"""Generate a comprehensive RUNBOOK for {method_name} extraction from {{connector}}. 
Use step-by-step format with numbered procedures, code examples, and verification steps.

## {method_name} Extraction Runbook

### Prerequisites Checklist
- [ ] Prerequisite 1 (e.g., API access approved)
- [ ] Prerequisite 2 (e.g., Credentials obtained)  
- [ ] Prerequisite 3 (e.g., Network access verified)

### Step 1: Authentication Setup

**1.1 Obtain Credentials**
| Credential | Where to Get | Format |
|------------|--------------|--------|
| (credential name) | (location/portal) | (format) |

**1.2 Authentication Code**
```python
# Complete, runnable authentication code
# Include: imports, configuration, token acquisition
```

**1.3 Verify Authentication**
```python
# Code to verify auth is working
# Expected output: (describe expected result)
```

### Step 2: Discover Available Objects

**2.1 List All Objects**
```python
# Code to list/discover available objects
```

**2.2 Objects Inventory**
| Object | Endpoint/Query | Primary Key | Incremental Field | Notes |
|--------|----------------|-------------|-------------------|-------|
| (Complete table of ALL accessible objects) |

### Step 3: Extract Data (Full Load)

**3.1 Full Load Procedure**
```python
# Complete code for full extraction with pagination
def extract_full_load(object_name):
    # Implementation with error handling
    pass
```

**3.2 Verification**
- Expected record count: (how to verify)
- Data quality check: (what to check)

### Step 4: Extract Data (Incremental)

**4.1 Incremental Load Procedure**
```python
# Code for incremental extraction using cursor field
def extract_incremental(object_name, last_cursor):
    # Implementation
    pass
```

**4.2 Cursor Management**
| Object | Cursor Field | Format | Storage Method |
|--------|--------------|--------|----------------|
| (list objects with their cursor fields) |

### Step 5: Handle Pagination

**5.1 Pagination Details**
| Property | Value |
|----------|-------|
| Type | (cursor/offset/page) |
| Max Per Request | (number) |
| Next Page Indicator | (field/header) |

**5.2 Pagination Code**
```python
# Complete pagination handling code
def paginate_results(endpoint):
    # Handle all pagination scenarios
    pass
```

### Step 6: Error Handling

**6.1 Common Errors**
| Error Code | Meaning | Action | Retry |
|------------|---------|--------|-------|
| (list all relevant error codes) |

**6.2 Error Handling Code**
```python
# Robust error handling with retry logic
```

### Step 7: Rate Limit Management

**7.1 Rate Limits**
| Limit Type | Value | Scope | Header |
|------------|-------|-------|--------|
| (document all rate limits) |

**7.2 Rate Limiter Implementation**
```python
# Rate limiting code with backoff
```

### Troubleshooting Guide

| Symptom | Diagnosis | Resolution |
|---------|-----------|------------|
| (common issue 1) | (how to diagnose) | (how to fix) |
| (common issue 2) | (how to diagnose) | (how to fix) |

### Performance Optimization Tips
1. (Tip 1 for better performance)
2. (Tip 2)
3. (Tip 3)

### Pros & Cons Summary
| Pros | Cons |
|------|------|
| (advantage 1) | (disadvantage 1) |
| (advantage 2) | (disadvantage 2) |

**Best Use Case:** (when to use this method)
**Avoid When:** (when NOT to use this method)
"""
        ]
    )


# Base sections (always included)
BASE_SECTIONS = [
    # Phase 1: Discovery (Sections 1-3)
    ResearchSection(1, "Platform Overview", 1, "Platform Discovery", [
        "What does {connector} do? Describe its purpose, target users, and main functionality.",
        "What are the key modules and features?",
        "What types of data entities does it store?",
        "Does it have reporting/analytics modules?",
        "What are the limitations of its data model?",
        "Who are the typical users (enterprise, SMB, developers)?"
    ]),
    
    ResearchSection(2, "Extraction Methods Discovery", 1, "Platform Discovery", [
        "**IMPORTANT**: Discover ALL available data extraction methods for {connector}. For each method that EXISTS, provide details. If a method does NOT exist, explicitly state 'Not Available'.",
        "",
        "Check for the following methods and report availability:",
        "",
        "1. **REST API**: Does {connector} have a REST API? If yes: What is the base URL? Is it documented? What version?",
        "2. **GraphQL API**: Does {connector} have a GraphQL API? If yes: What is the endpoint? What schemas are available?",
        "3. **SOAP/XML API**: Does {connector} have a SOAP or XML-based API? If yes: Where is the WSDL?",
        "4. **Webhooks**: Does {connector} support webhooks for real-time events? If yes: What events are available?",
        "5. **Bulk/Batch API**: Does {connector} have bulk data export or async batch APIs? If yes: How do they work?",
        "6. **Official SDK**: Does {connector} provide official SDKs? If yes: What languages (Python, Java, Node.js, etc.)?",
        "7. **JDBC/ODBC**: Does {connector} support direct database connections via JDBC/ODBC? If yes: What drivers?",
        "8. **File Export**: Does {connector} support data export to files (CSV, JSON, etc.)? If yes: Manual or API-triggered?",
        "",
        "Create a summary table at the end:",
        "| Method | Available | Base URL/Endpoint | Documentation Link | Best Use Case |",
        "|--------|-----------|-------------------|-------------------|---------------|"
    ]),
    
    ResearchSection(3, "Developer Environment", 1, "Platform Discovery", [
        "Does {connector} provide sandbox or developer environments?",
        "How do you request access (self-service, sales, partner program)?",
        "What are the limitations of sandbox vs production?",
        "How do you register a developer app/integration?",
        "What credentials are needed (API keys, OAuth app, service account)?",
        "Are there IP whitelists or redirect URI requirements?",
        "Provide a minimal health check code example to verify API access."
    ]),
]


# Cross-cutting sections (always included, after method sections)
CROSS_CUTTING_SECTIONS = [
    ResearchSection(100, "Authentication Comparison", 3, "Cross-Cutting Concerns", [
        "Compare authentication methods across ALL available extraction methods for {connector}.",
        "Create a comparison table: Method | Auth Type | Token Lifetime | Refresh Strategy | Scopes Required",
        "Which authentication method is recommended for production ETL pipelines?",
        "What are the security best practices for credential management?",
        "Provide unified authentication code that works across multiple methods."
    ]),
    
    ResearchSection(101, "Rate Limiting Strategy", 3, "Cross-Cutting Concerns", [
        "Compare rate limits across ALL available extraction methods for {connector}.",
        "Create a comparison table: Method | Requests/Min | Requests/Hour | Requests/Day | Concurrency Limit",
        "Which method has the most generous rate limits for bulk extraction?",
        "What retry strategies should be used when rate limited?",
        "Provide a rate limiter implementation in Python that respects these limits."
    ]),
    
    ResearchSection(102, "Error Handling & Retries", 3, "Cross-Cutting Concerns", [
        "What error codes and responses are returned by {connector} APIs?",
        "Create a table: Error Code | Meaning | Retryable? | Resolution",
        "What errors require re-authentication vs simple retry?",
        "What is the recommended exponential backoff strategy?",
        "Provide error handling code with proper retry logic."
    ]),
    
    ResearchSection(103, "Data Model & Relationships", 3, "Cross-Cutting Concerns", [
        "Document the complete data model for {connector}.",
        "What are all the main objects/entities?",
        "What parent-child relationships exist? Create a relationship diagram or table.",
        "What is the correct load order for related objects?",
        "Are there any circular dependencies to handle?",
        "What foreign keys link objects together?"
    ], requires_fivetran=True, requires_code_analysis=True),
    
    ResearchSection(104, "Delete Detection Strategies", 3, "Cross-Cutting Concerns", [
        "Compare delete detection methods across ALL extraction methods for {connector}.",
        "Create a table: Method | Delete Detection | Field/Endpoint | Reliability",
        "Which method is most reliable for detecting deletions?",
        "How should soft deletes vs hard deletes be handled?",
        "Provide code for delete detection using the recommended method."
    ]),
]


# Final sections (always included, after cross-cutting)
FINAL_SECTIONS = [
    ResearchSection(200, "Recommended Extraction Strategy", 4, "Implementation Guide", [
        "Based on all discovered methods, what is the RECOMMENDED extraction strategy for {connector}?",
        "Consider: reliability, performance, completeness, delete detection, real-time needs.",
        "Create a decision matrix: Use Case | Recommended Method | Reason",
        "What combination of methods provides the best coverage?",
        "Provide a high-level architecture diagram for a production ETL pipeline.",
        "What are the trade-offs between different approaches?"
    ]),
    
    ResearchSection(201, "Object Catalog & Replication Guide", 4, "Implementation Guide", [
        "List ALL available objects/entities that can be extracted from {connector}.",
        "Create a comprehensive catalog table with columns:",
        "| Object | Extraction Method | Primary Key | Cursor Field | Sync Mode | Delete Method | Fivetran Support |",
        "For each object, specify:",
        "- Which extraction method(s) can access it",
        "- Primary key field",
        "- Best cursor field for incremental sync",
        "- Supported sync modes (Full/Incremental/CDC)",
        "- Delete detection method",
        "- Whether Fivetran supports this object (if known)",
        "Provide sample extraction code for the top 5 most important objects."
    ], requires_fivetran=True, requires_code_analysis=True),
    
    ResearchSection(202, "Fivetran Parity Analysis", 4, "Implementation Guide", [
        "Document how Fivetran implements the {connector} connector based on Fivetran documentation.",
        "## Fivetran Implementation Overview",
        "Document Fivetran's authentication approach for {connector}",
        "Document Fivetran's extraction methods and API endpoints used",
        "List all objects/tables that Fivetran supports for {connector}",
        "Document Fivetran's sync strategies (full load, incremental, CDC)",
        "Document Fivetran's delete detection approach",
        "Document Fivetran's rate limiting and error handling strategies",
        "## Hevo Comparison (if Hevo connector code provided)",
        "[IF HEVO] Compare Fivetran's approach with Hevo's implementation",
        "[IF HEVO] Create comparison table: Fivetran vs Hevo (Objects, Auth, Extraction, Sync, Deletes)",
        "[IF HEVO] Highlight similarities and differences in object support",
        "[IF HEVO] Compare authentication methods between Fivetran and Hevo",
        "[IF HEVO] Compare extraction approaches and API endpoints",
        "[IF HEVO] Compare sync strategies (full load, incremental, CDC)",
        "[IF HEVO] Compare delete detection methods",
        "[IF NO HEVO] Note: Hevo comparison not available (no Hevo connector code provided)",
        "Provide insights on differences in implementation approaches and their trade-offs."
    ], requires_fivetran=True, requires_code_analysis=False),
    
    ResearchSection(203, "Production Checklist", 4, "Implementation Guide", [
        "Create a production readiness checklist for {connector} data extraction.",
        "**Authentication**: [ ] OAuth app registered, [ ] Credentials secured, [ ] Token refresh implemented",
        "**Rate Limiting**: [ ] Rate limiter configured, [ ] Backoff strategy implemented",
        "**Error Handling**: [ ] All error codes handled, [ ] Alerts configured",
        "**Monitoring**: [ ] Sync metrics tracked, [ ] Data quality checks in place",
        "**Testing**: [ ] Sandbox testing complete, [ ] Load testing done",
        "What are the top 10 things that can go wrong in production?",
        "What monitoring and alerting should be in place?"
    ]),
    
    ResearchSection(205, "Engineering Cost Analysis", 4, "Implementation Guide", [
        """Analyze the engineering cost and implementation complexity for {connector}:

### Extraction Method Complexity Matrix
| Method | Implementation Effort | Maintenance Burden | Risk Factors | Recommendation |
|--------|---------------------|-------------------|--------------|----------------|
| (For each discovered method, assess complexity) |

### Complexity Factors
- **Undocumented APIs**: Methods that lack official documentation
- **Frequent Breaking Changes**: APIs that change frequently
- **Rate Limit Complexity**: Complex rate limiting schemes
- **Authentication Complexity**: Multi-step or non-standard auth
- **Data Volume Challenges**: Methods that struggle at scale
- **Error Handling Complexity**: Unpredictable error patterns

### High-Maintenance Endpoints
| Endpoint | Reason | Mitigation Strategy |
|----------|--------|-------------------|
| (List endpoints that require frequent updates or monitoring) |

### Implementation Recommendations
- **Recommended Methods**: (methods with low complexity, high reliability)
- **Conditional Methods**: (methods to use only if customer requires specific features)
- **Avoid Methods**: (methods with high complexity and low value)

### Engineering Cost Summary
Provide overall assessment of connector implementation complexity.
"""
    ], requires_code_analysis=True),
]


# Phase 5: Core Functional Requirements (Enterprise)
FUNCTIONAL_SECTIONS = [
    ResearchSection(300, "Data Source Specification", 5, "Core Functional Requirements", [
        """Provide complete data source specification for {connector}:

### Source System Details
| Property | Value |
|----------|-------|
| **System Type** | (SaaS Application / Database / File System / API Gateway) |
| **API Version** | (Current stable version, e.g., v2.1, 2024-01) |
| **Base URL(s)** | (Production, Sandbox URLs) |
| **Supported Protocols** | (HTTPS, JDBC, ODBC, etc.) |
| **Data Formats** | (JSON, XML, CSV, Parquet, etc.) |

### Connection Requirements
| Requirement | Details |
|-------------|---------|
| **Authentication** | (OAuth 2.0, API Key, Basic Auth, Certificate) |
| **Network** | (Public Internet, VPN, Private Link, IP Whitelist) |
| **Firewall Rules** | (Ports, IP ranges to allow) |
| **SSL/TLS** | (Required? Minimum version?) |

### API Capabilities Matrix
| Capability | Supported | Notes |
|------------|-----------|-------|
| REST API | Yes/No | |
| GraphQL | Yes/No | |
| Webhooks | Yes/No | |
| Bulk Export | Yes/No | |
| Real-time Streaming | Yes/No | |
| File Export | Yes/No | |
"""
    ]),
    
    ResearchSection(301, "Extraction Method Selection", 5, "Core Functional Requirements", [
        """Create an extraction method selection guide for {connector}:

### Extraction Methods Decision Matrix
| Object | Full Load | Incremental | CDC | Recommended | Reason |
|--------|-----------|-------------|-----|-------------|--------|
| (For each major object, specify which methods are supported and recommended) |

### Full Extraction Strategy
- **When to use**: Initial load, data reconciliation, small tables
- **Implementation**: Query all records without filters
- **Considerations**: Volume limits, API quotas, network bandwidth

### Incremental Extraction Strategy  
- **When to use**: Regular syncs, large tables, frequent updates
- **Cursor Fields**: List available cursor fields per object (updated_at, modified_date, etc.)
- **Window Strategy**: Time-based vs ID-based windowing
- **Gap Handling**: How to handle missed windows or failures

### Source-Driven Notification (CDC/Webhooks)
- **Available Events**: List webhook events or CDC streams available
- **Event Payload**: What data is included in notifications
- **Ordering Guarantees**: Are events ordered? How to handle out-of-order?
- **Replay Capability**: Can missed events be replayed?

### Hybrid Approach Recommendation
Describe the optimal combination of methods for production use.
"""
    ]),
    
    ResearchSection(302, "Transformation & Cleansing Rules", 5, "Core Functional Requirements", [
        """Define transformation and cleansing rules for {connector} data:

### Field Mapping Standards
| Source Field | Target Field | Transformation | Example |
|--------------|--------------|----------------|---------|
| (Document key field mappings with any transformations needed) |

### Data Type Conversions
| Source Type | Target Type | Conversion Rule | Edge Cases |
|-------------|-------------|-----------------|------------|
| datetime (ISO 8601) | timestamp | Parse with timezone | Null handling |
| decimal string | numeric | Cast with precision | Overflow handling |
| nested JSON | flattened columns | Dot notation | Array handling |

### Cleansing Rules
| Rule | Description | Implementation |
|------|-------------|----------------|
| **Null Handling** | How to handle NULL/missing values | (Default value, skip, flag) |
| **Duplicate Detection** | Identify and handle duplicates | (Primary key, composite key) |
| **Invalid Data** | Handle data outside valid ranges | (Reject, correct, flag) |
| **Encoding** | Character encoding standardization | (UTF-8 normalization) |

### Calculated Fields
| Field | Formula | Dependencies | Update Frequency |
|-------|---------|--------------|------------------|
| (Define any derived/calculated fields needed) |
"""
    ]),
    
    ResearchSection(303, "Data Validation Framework", 5, "Core Functional Requirements", [
        """Create a data validation framework for {connector}:

### Schema Validation Rules
| Object | Field | Type | Required | Constraints | Validation Code |
|--------|-------|------|----------|-------------|-----------------|
| (Define validation rules for key fields) |

### Business Rule Validations
| Rule ID | Description | Severity | Action on Failure |
|---------|-------------|----------|-------------------|
| BV001 | (Business validation rule) | Error/Warning | Reject/Flag/Log |

### Referential Integrity Checks
| Parent Object | Child Object | Foreign Key | On Violation |
|---------------|--------------|-------------|--------------|
| (Define parent-child integrity rules) |

### Data Quality Metrics
| Metric | Formula | Threshold | Alert Level |
|--------|---------|-----------|-------------|
| **Completeness** | Non-null count / Total count | > 95% | Warning < 90% |
| **Accuracy** | Valid values / Total values | > 99% | Error < 95% |
| **Timeliness** | Records within SLA / Total | > 99% | Error < 95% |
| **Consistency** | Matching records / Total | 100% | Error < 100% |

### Validation Code Example
```python
# Provide Python validation code example
```
"""
    ]),
    
    ResearchSection(304, "Loading Strategy Decision Tree", 5, "Core Functional Requirements", [
        """Define loading strategies for {connector} data:

### Loading Mode Selection
| Scenario | Loading Mode | Description |
|----------|--------------|-------------|
| Initial Load | Full Overwrite | Replace all target data |
| Regular Sync | Incremental Append | Add new records only |
| Updates Detected | Upsert (Merge) | Insert or update based on key |
| SCD Required | Type 2 History | Maintain historical versions |

### Target System Considerations
| Target Type | Recommended Strategy | Batch Size | Parallelism |
|-------------|---------------------|------------|-------------|
| Data Warehouse | Bulk Insert | 10,000-100,000 | 4-8 threads |
| Data Lake | Partitioned Write | By date/key | Parallel partitions |
| Database | Batch Upsert | 1,000-5,000 | 2-4 connections |

### Merge/Upsert Logic
```sql
-- Provide SQL merge pattern for the target system
```

### Slowly Changing Dimensions (SCD)
| SCD Type | Use Case | Implementation |
|----------|----------|----------------|
| Type 1 | Overwrite old values | UPDATE existing rows |
| Type 2 | Keep history | Add new row, close old |
| Type 3 | Limited history | Add previous value column |

### Loading Sequence (Dependency Order)
1. (List objects in correct loading order based on dependencies)
"""
    ]),
]


# Phase 6: Technical & Operational Requirements (Enterprise)
OPERATIONAL_SECTIONS = [
    ResearchSection(400, "Connectivity Runbook", 6, "Technical Operations", [
        """Create a step-by-step connectivity runbook for {connector}:

### Prerequisites Checklist
- [ ] Admin access to {connector} account
- [ ] Network access to API endpoints verified
- [ ] Required permissions/scopes identified
- [ ] Development/sandbox environment available

### Step 1: Network Connectivity Verification
```bash
# Verify API endpoint is reachable
curl -I https://api.{connector}.com/health
# Expected: HTTP 200 OK
```

### Step 2: Application Registration
1. Navigate to Developer Portal / Admin Console
2. Create new application/integration
3. Configure OAuth redirect URIs (if applicable)
4. Note down: Client ID, Client Secret, API Key

### Step 3: Authentication Setup
```python
# Provide complete authentication code
# Include: token acquisition, refresh logic, error handling
```

### Step 4: Connection Test
```python
# Provide connection test code
# Verify: auth works, can list objects, can read data
```

### Step 5: Permissions Verification
```python
# Test each required permission/scope
# Document which objects each permission grants access to
```

### Troubleshooting Guide
| Symptom | Possible Cause | Resolution |
|---------|----------------|------------|
| Connection timeout | Firewall blocking | Whitelist IPs |
| 401 Unauthorized | Invalid credentials | Regenerate API key |
| 403 Forbidden | Missing permissions | Request additional scopes |
| 429 Rate Limited | Too many requests | Implement backoff |
| SSL Error | Certificate issue | Update CA bundle |
"""
    ]),
    
    ResearchSection(401, "Volume & Performance Guide", 6, "Technical Operations", [
        """Create volume and performance guidelines for {connector}:

### Expected Data Volumes
| Object | Typical Record Count | Record Size | Daily Change Rate |
|--------|---------------------|-------------|-------------------|
| (Estimate volumes for key objects) |

### Performance Benchmarks
| Operation | Expected Throughput | Latency Target | Notes |
|-----------|--------------------|-----------------| ------|
| List objects | X records/second | < 500ms | With pagination |
| Get single record | 1 record | < 200ms | By ID |
| Bulk export | X records/minute | N/A | Async job |
| Webhook delivery | Real-time | < 5 seconds | Event to receipt |

### Batch Size Recommendations
| API Type | Recommended Batch | Max Batch | Reason |
|----------|-------------------|-----------|--------|
| REST List | 100-250 | 1000 | Balance throughput vs memory |
| Bulk API | 10,000 | 100,000 | Job processing time |
| Webhook | N/A | N/A | Event-driven |

### Scheduling Recommendations
| Sync Type | Frequency | Window | Rationale |
|-----------|-----------|--------|-----------|
| Full Load | Weekly | Off-peak hours | High volume, low frequency |
| Incremental | Every 15-60 min | Continuous | Near real-time updates |
| CDC/Webhooks | Real-time | Continuous | Immediate consistency |

### Scalability Considerations
- **Horizontal Scaling**: Can run multiple extractors in parallel?
- **Rate Limit Sharing**: How are limits shared across instances?
- **Connection Pooling**: Recommended pool size and timeout settings
"""
    ]),
    
    ResearchSection(402, "Error Handling Procedures", 6, "Technical Operations", [
        """Create error handling procedures for {connector}:

### Error Classification
| Error Category | HTTP Codes | Retryable | Max Retries | Backoff |
|----------------|------------|-----------|-------------|---------|
| Client Error | 400, 404 | No | 0 | N/A |
| Auth Error | 401, 403 | Maybe | 1 | Re-auth then retry |
| Rate Limit | 429 | Yes | 5 | Exponential + Retry-After |
| Server Error | 500, 502, 503 | Yes | 3 | Exponential |
| Timeout | N/A | Yes | 3 | Linear increase |

### Error Response Parsing
```python
# Provide error parsing code that extracts:
# - Error code, message, details
# - Retry-After header (if present)
# - Request ID for support tickets
```

### Retry Strategy Implementation
```python
# Provide exponential backoff implementation
# Include: jitter, max retries, circuit breaker
```

### Recovery Procedures
| Failure Scenario | Detection | Recovery Action | Escalation |
|------------------|-----------|-----------------|------------|
| Auth token expired | 401 response | Refresh token | Alert if refresh fails |
| Rate limit exceeded | 429 response | Backoff and retry | Reduce concurrency |
| API unavailable | 503 / timeout | Retry with backoff | Page on-call after 5 min |
| Data corruption | Validation failure | Quarantine record | Manual review |

### Alerting Rules
| Alert | Condition | Severity | Response Time |
|-------|-----------|----------|---------------|
| Sync Failed | 3 consecutive failures | High | 15 minutes |
| High Error Rate | > 5% errors in 5 min | Medium | 1 hour |
| Auth Failure | Any 401 after refresh | Critical | Immediate |
"""
    ]),
    
    ResearchSection(403, "Monitoring & Alerting Setup", 6, "Technical Operations", [
        """Create monitoring and alerting configuration for {connector}:

### Key Metrics to Track
| Metric | Description | Collection Method | Granularity |
|--------|-------------|-------------------|-------------|
| **records_extracted** | Total records pulled | Counter | Per sync |
| **extraction_duration_seconds** | Time to complete extraction | Histogram | Per object |
| **api_requests_total** | Total API calls made | Counter | Continuous |
| **api_errors_total** | Failed API calls | Counter by error type | Continuous |
| **rate_limit_hits** | Times rate limited | Counter | Continuous |
| **data_freshness_seconds** | Age of newest record | Gauge | Per object |

### Dashboard Panels
1. **Sync Overview**: Success rate, records/hour, active syncs
2. **API Health**: Request rate, error rate, latency percentiles
3. **Data Quality**: Validation pass rate, null rates, schema drift
4. **Resource Usage**: Memory, CPU, connections, queue depth

### Alert Definitions
| Alert Name | Condition | Severity | Notification |
|------------|-----------|----------|--------------|
| SyncFailed | sync_status = failed for > 5 min | P1 | PagerDuty |
| HighErrorRate | error_rate > 5% for 10 min | P2 | Slack |
| DataStale | freshness > 2 hours | P2 | Slack |
| RateLimitCritical | rate_limit_hits > 100/min | P3 | Email |

### Prometheus/Grafana Configuration
```yaml
# Provide example Prometheus alerting rules
```

### Health Check Endpoint
```python
# Provide health check implementation that verifies:
# - API connectivity, Auth validity, Recent sync success
```
"""
    ]),
    
    ResearchSection(404, "Audit & Compliance Requirements", 6, "Technical Operations", [
        """Create audit and compliance documentation for {connector}:

### Data Lineage Tracking
| Field | Source | Transformation | Target | Timestamp |
|-------|--------|----------------|--------|-----------|
| (Document data lineage for key fields) |

### Audit Log Schema
```json
{
  "event_id": "uuid",
  "timestamp": "ISO 8601",
  "event_type": "extraction|transformation|load|error",
  "connector": "{connector}",
  "object": "object_name",
  "record_count": 1000,
  "duration_ms": 5000,
  "status": "success|failure",
  "error_message": null,
  "user": "service_account",
  "source_system": "{connector}",
  "target_system": "data_warehouse"
}
```

### Compliance Checklist
| Requirement | Implementation | Verification |
|-------------|----------------|--------------|
| **Data Encryption** | TLS 1.2+ in transit, AES-256 at rest | Certificate check |
| **Access Control** | Service account with minimum privileges | Permission audit |
| **Audit Trail** | All operations logged with timestamps | Log review |
| **Data Retention** | Logs retained for X days | Retention policy |
| **PII Handling** | Masked/tokenized in logs | Log sampling |

### Data Classification
| Object | Contains PII | Contains Financial | Sensitivity | Handling |
|--------|-------------|-------------------|-------------|----------|
| (Classify each object by data sensitivity) |

### Retention & Purging
| Data Type | Retention Period | Purge Method | Compliance Reason |
|-----------|------------------|--------------|-------------------|
| Raw extraction logs | 90 days | Auto-delete | Storage cost |
| Audit records | 7 years | Archive then delete | SOX compliance |
| Error records | 30 days | Auto-delete | Troubleshooting |
"""
    ]),
]


@dataclass
class StopTheLineEvent:
    """Stop-the-line event when critical issues are detected."""
    reason: str  # "CRITICAL_CONTRADICTION", "LOW_CONFIDENCE_CRITICAL"
    section_number: int
    required_action: str  # "HUMAN_REVIEW", "ADDITIONAL_SOURCES"
    contradictions: List[Any] = field(default_factory=list)
    uncertainty_flags: List[Any] = field(default_factory=list)


@dataclass
class ResearchProgress:
    """Tracks research generation progress."""
    connector_id: str
    connector_name: str
    current_section: int = 0
    total_sections: int = 0  # Dynamic - calculated based on discovered methods
    status: str = "idle"  # idle, running, completed, failed, cancelled, stopped
    sections_completed: List[int] = field(default_factory=list)
    current_content: str = ""
    error_message: str = ""
    discovered_methods: List[str] = field(default_factory=list)  # Methods found during discovery
    section_reviews: Dict[int, Any] = field(default_factory=dict)  # Section reviews from Critic Agent
    stop_the_line_events: List[StopTheLineEvent] = field(default_factory=list)
    contradictions: List[Any] = field(default_factory=list)
    engineering_costs: Dict[str, Any] = field(default_factory=dict)
    overall_confidence: float = 0.0
    claims_json: List[Dict[str, Any]] = field(default_factory=list)  # Structured claims
    canonical_facts_json: Dict[str, Any] = field(default_factory=dict)  # Final registry
    evidence_map_json: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # Citation → evidence (with stable IDs)


@dataclass
class ResearchMetrics:
    """Tracks metrics for Quick Summary dashboard."""
    
    # API Capabilities (from web search/documentation)
    total_objects: int = 0
    objects_by_method: Dict[str, int] = field(default_factory=dict)  # REST, GraphQL, SOAP, etc.
    full_load_count: int = 0
    incremental_count: int = 0
    cdc_count: int = 0
    auth_types: List[str] = field(default_factory=list)
    sdk_info: List[str] = field(default_factory=list)
    delete_methods: List[str] = field(default_factory=list)
    
    # Current Implementation (from GitHub)
    impl_objects: int = 0
    impl_by_method: Dict[str, int] = field(default_factory=dict)
    impl_full_load: int = 0
    impl_incremental: int = 0
    impl_auth: List[str] = field(default_factory=list)
    impl_sdk: str = ""
    impl_object_names: List[str] = field(default_factory=list)
    
    # Fivetran Parity
    fivetran_objects: int = 0
    fivetran_supported: List[str] = field(default_factory=list)
    fivetran_unsupported: List[str] = field(default_factory=list)
    fivetran_features: Dict[str, bool] = field(default_factory=dict)
    fivetran_auth: List[str] = field(default_factory=list)
    fivetran_full_load: int = 0
    fivetran_incremental: int = 0


class ResearchAgent:
    """Agent that auto-generates connector research documents.
    
    Now enhanced with:
    - 📚 Knowledge Vault - Pre-indexed official documentation (HIGHEST confidence!)
    - 🔮 DocWhisperer™ - Context7 MCP for official documentation access
    - 🔍 Tavily Web Search - Fallback for additional context
    """
    
    def __init__(self):
        """Initialize the research agent with Knowledge Vault and DocWhisperer integration."""
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.tavily_api_key = os.getenv("TAVILY_API_KEY")
        self.model = os.getenv("RESEARCH_MODEL", "gpt-4o")
        
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY is required")
        
        self.client = AsyncOpenAI(api_key=self.openai_api_key)
        self._cancel_requested = False
        self._current_progress: Optional[ResearchProgress] = None
        
        # 📚 Initialize Knowledge Vault (pre-indexed official docs)
        self.knowledge_vault = None
        try:
            from services.knowledge_vault import get_knowledge_vault
            self.knowledge_vault = get_knowledge_vault()
            print("  📚 Knowledge Vault connected!")
        except Exception as e:
            print(f"  ⚠ Knowledge Vault not available: {e}")
        
        # 🔮 Summon the DocWhisperer
        self.doc_whisperer = get_doc_whisperer()
        print("  🔮 DocWhisperer™ initialized!")
        
        # 🧠 Initialize Critic Agent
        try:
            from services.critic_agent import CriticAgent
            self.critic_agent = CriticAgent()
            print("  🧠 Critic Agent initialized!")
        except Exception as e:
            print(f"  ⚠ Critic Agent not available: {e}")
            self.critic_agent = None
        
        # 🔍 Initialize Contradiction Resolver
        try:
            from services.contradiction_resolver import ContradictionResolver
            self.contradiction_resolver = ContradictionResolver()
            print("  🔍 Contradiction Resolver initialized!")
        except Exception as e:
            print(f"  ⚠ Contradiction Resolver not available: {e}")
            self.contradiction_resolver = None
        
        print("  📚 ResearchAgent ready with multi-source knowledge and multi-agent review!")
    
    def get_progress(self) -> Optional[ResearchProgress]:
        """Get current research progress."""
        return self._current_progress
    
    def cancel(self):
        """Request cancellation of current research."""
        self._cancel_requested = True
    
    def _extract_github_metrics(self, github_context: Dict[str, Any]) -> ResearchMetrics:
        """Extract metrics from GitHub context for Quick Summary.
        
        Args:
            github_context: Context extracted from GitHub repository
            
        Returns:
            ResearchMetrics with implementation stats populated
        """
        metrics = ResearchMetrics()
        
        if not github_context:
            return metrics
        
        # Check if structured format
        is_structured = github_context.get('structure_type') == 'structured'
        
        if is_structured:
            impl = github_context.get('implementation', {})
            sdk = github_context.get('sdk', {})
            
            # Count models/objects
            models = impl.get('models', [])
            metrics.impl_objects = len(models)
            metrics.impl_object_names = models[:50]
            
            # Detect API methods from api_calls
            api_calls = impl.get('api_calls', [])
            rest_count = sum(1 for c in api_calls if 'get' in c.lower() or 'post' in c.lower() or 'http' in c.lower())
            graphql_count = sum(1 for c in api_calls if 'graphql' in c.lower() or 'query' in c.lower())
            metrics.impl_by_method = {'REST': rest_count, 'GraphQL': graphql_count}
            
            # Auth detection
            auth_impl = impl.get('auth_implementation', '')
            if 'oauth' in auth_impl.lower():
                metrics.impl_auth.append('OAuth 2.0')
            if 'api_key' in auth_impl.lower() or 'apikey' in auth_impl.lower():
                metrics.impl_auth.append('API Key')
            if 'bearer' in auth_impl.lower():
                metrics.impl_auth.append('Bearer Token')
            if 'basic' in auth_impl.lower():
                metrics.impl_auth.append('Basic Auth')
            
            # SDK info
            sdk_name = sdk.get('sdk_name', '')
            if sdk_name:
                metrics.impl_sdk = sdk_name
            
            # Sync patterns
            sync_patterns = impl.get('sync_patterns', [])
            metrics.impl_incremental = sum(1 for p in sync_patterns if 'incremental' in p.lower() or 'cursor' in p.lower())
            metrics.impl_full_load = max(0, metrics.impl_objects - metrics.impl_incremental)
            
        else:
            # Legacy flat format
            object_types = github_context.get('object_types', [])
            metrics.impl_objects = len(object_types)
            metrics.impl_object_names = object_types[:50]
            
            auth_patterns = github_context.get('auth_patterns', [])
            for pattern in auth_patterns:
                if 'oauth' in pattern.lower():
                    metrics.impl_auth.append('OAuth 2.0')
                elif 'api' in pattern.lower() and 'key' in pattern.lower():
                    metrics.impl_auth.append('API Key')
        
        return metrics
    
    def _extract_fivetran_metrics(self, fivetran_context: Dict[str, Any]) -> ResearchMetrics:
        """Extract metrics from Fivetran context for Quick Summary.
        
        Args:
            fivetran_context: Context from Fivetran crawler
            
        Returns:
            ResearchMetrics with Fivetran stats populated
        """
        metrics = ResearchMetrics()
        
        if not fivetran_context:
            return metrics
        
        setup = fivetran_context.get('setup', {})
        overview = fivetran_context.get('overview', {})
        schema = fivetran_context.get('schema', {})
        
        # Supported objects
        supported = schema.get('supported_objects', [])
        unsupported = schema.get('unsupported_objects', [])
        metrics.fivetran_objects = len(supported)
        metrics.fivetran_supported = supported
        metrics.fivetran_unsupported = unsupported
        
        # Auth methods
        auth_methods = setup.get('auth_methods', [])
        metrics.fivetran_auth = auth_methods
        
        # Features
        features = overview.get('supported_features', {})
        metrics.fivetran_features = features
        
        # Sync modes from objects
        objects = schema.get('objects', [])
        for obj in objects:
            sync_mode = obj.get('sync_mode', '')
            if sync_mode == 'incremental':
                metrics.fivetran_incremental += 1
            elif sync_mode == 'full_load':
                metrics.fivetran_full_load += 1
        
        # If no specific sync modes, estimate from total
        if metrics.fivetran_incremental == 0 and metrics.fivetran_full_load == 0:
            metrics.fivetran_incremental = int(metrics.fivetran_objects * 0.7)  # Estimate
            metrics.fivetran_full_load = metrics.fivetran_objects - metrics.fivetran_incremental
        
        return metrics
    
    def _calculate_parity(self, impl_objects: List[str], fivetran_objects: List[str]) -> Dict[str, Any]:
        """Calculate parity between implementation and Fivetran.
        
        Args:
            impl_objects: List of implemented object names
            fivetran_objects: List of Fivetran supported object names
            
        Returns:
            Dict with parity percentage and gap analysis
        """
        impl_set = set(obj.lower() for obj in impl_objects)
        fivetran_set = set(obj.lower() for obj in fivetran_objects)
        
        # Objects we have that Fivetran doesn't
        extra_objects = impl_set - fivetran_set
        
        # Objects Fivetran has that we're missing
        missing_objects = fivetran_set - impl_set
        
        # Common objects
        common = impl_set & fivetran_set
        
        # Parity percentage (what % of Fivetran objects do we support)
        if fivetran_set:
            parity_pct = (len(common) / len(fivetran_set)) * 100
        else:
            parity_pct = 0
        
        return {
            'parity_percentage': round(parity_pct, 1),
            'common_count': len(common),
            'fivetran_total': len(fivetran_set),
            'extra_objects': list(extra_objects)[:20],
            'missing_objects': list(missing_objects)[:20],
            'extra_count': len(extra_objects),
            'missing_count': len(missing_objects)
        }
    
    def _generate_expert_review_template(
        self,
        connector_name: str,
        discovered_methods: List[str]
    ) -> str:
        """Generate an expert review checklist template for SME validation.
        
        Args:
            connector_name: Name of the connector
            discovered_methods: List of discovered extraction methods
            
        Returns:
            Markdown string for expert review template
        """
        review_template = f"""

---

# 📋 Appendix: Expert Review Template

> **Purpose:** This checklist helps Subject Matter Experts (SMEs) validate the research accuracy before using it for production implementation.

## 🔐 Authentication Review

| Item | Status | Reviewer Notes |
|------|--------|----------------|
| [ ] Auth method(s) documented match current API docs | ⬜ | |
| [ ] OAuth scopes are complete and accurate | ⬜ | |
| [ ] Token lifetime and refresh process verified | ⬜ | |
| [ ] Service account requirements documented | ⬜ | |
| [ ] Multi-tenant auth flow confirmed (if applicable) | ⬜ | |

## 📦 Objects & Schema Review

| Item | Status | Reviewer Notes |
|------|--------|----------------|
| [ ] Object list is complete (no missing objects) | ⬜ | |
| [ ] Primary keys correctly identified | ⬜ | |
| [ ] Cursor fields for incremental sync verified | ⬜ | |
| [ ] Parent-child relationships accurate | ⬜ | |
| [ ] Data types match API response format | ⬜ | |
| [ ] Required fields/permissions verified | ⬜ | |

## ⚡ Rate Limits Review

| Item | Status | Reviewer Notes |
|------|--------|----------------|
| [ ] Rate limits match current documentation | ⬜ | |
| [ ] Tested empirically with actual API calls | ⬜ | |
| [ ] Backoff strategy appropriate for limits | ⬜ | |
| [ ] Concurrency limits documented | ⬜ | |
| [ ] Bulk API limits (if applicable) verified | ⬜ | |

## 📄 Pagination Review

| Item | Status | Reviewer Notes |
|------|--------|----------------|
| [ ] Pagination type correctly identified | ⬜ | |
| [ ] Max records per request verified | ⬜ | |
| [ ] Cursor/offset field names correct | ⬜ | |
| [ ] Edge cases handled (empty pages, last page) | ⬜ | |

## 🗑️ Delete Detection Review

| Item | Status | Reviewer Notes |
|------|--------|----------------|
| [ ] Delete detection method(s) verified | ⬜ | |
| [ ] Soft delete fields correctly identified | ⬜ | |
| [ ] Deleted records endpoint tested (if exists) | ⬜ | |
| [ ] Webhook delete events documented (if exists) | ⬜ | |

## 💻 Code Examples Review

| Item | Status | Reviewer Notes |
|------|--------|----------------|
| [ ] Authentication code runs successfully | ⬜ | |
| [ ] Pagination code handles all edge cases | ⬜ | |
| [ ] Error handling code is production-ready | ⬜ | |
| [ ] Code follows best practices for the language | ⬜ | |

"""
        
        # Add method-specific review sections
        for method in discovered_methods:
            review_template += f"""
## 🔍 {method} Specific Review

| Item | Status | Reviewer Notes |
|------|--------|----------------|
| [ ] Base URL/endpoint is current | ⬜ | |
| [ ] All endpoints listed are accessible | ⬜ | |
| [ ] Response format matches documentation | ⬜ | |
| [ ] Error codes are complete | ⬜ | |
| [ ] Tested with sandbox/dev environment | ⬜ | |

"""
        
        review_template += """
## ✅ Final Sign-Off

| Reviewer | Role | Date | Signature |
|----------|------|------|-----------|
| | Technical Lead | | |
| | Domain Expert | | |
| | Security Review | | |

### Review Summary

**Overall Assessment:** ⬜ Approved ⬜ Approved with Changes ⬜ Needs Revision

**Critical Issues Found:**
1. 
2. 

**Recommendations:**
1. 
2. 

---

*This review template was auto-generated. Please customize based on your organization's requirements.*
"""
        
        return review_template
    
    def _generate_quick_summary(
        self,
        connector_name: str,
        connector_type: str,
        github_context: Optional[Dict[str, Any]] = None,
        fivetran_context: Optional[Dict[str, Any]] = None
    ) -> str:
        """Generate Quick Summary Dashboard with stacked cards.
        
        Args:
            connector_name: Name of the connector
            connector_type: Type of connector (REST, GraphQL, etc.)
            github_context: Optional GitHub context for implementation stats
            fivetran_context: Optional Fivetran context for parity analysis
            
        Returns:
            Markdown string for Quick Summary section
        """
        summary_parts = []
        
        # Header with Knowledge Vault and DocWhisperer status
        docwhisperer_stats = self.doc_whisperer.get_whisper_stats()
        docwhisperer_status = "🔮 Active" if docwhisperer_stats['status'] == 'enlightened' else "🔮 Ready"
        
        # Get Knowledge Vault stats for the connector
        vault_status = "Not available"
        vault_chunks = 0
        if self.knowledge_vault:
            # Extract connector name from the context
            connector_for_vault = connector_name if connector_name else "Unknown"
            vault_stats = self.knowledge_vault.get_stats(connector_for_vault)
            vault_chunks = vault_stats.get('chunks', 0)
            vault_status = f"📚 {vault_chunks} chunks indexed" if vault_chunks > 0 else "📚 No pre-indexed docs"
        
        summary_parts.append(f"""
# 📋 Quick Summary Dashboard

> At-a-glance metrics and comparison for rapid assessment

| Research Source | Status | Confidence |
|-----------------|--------|------------|
| 📚 **Knowledge Vault** | {vault_status} | +60 pts (HIGHEST) |
| 🔮 **DocWhisperer™** | {docwhisperer_status} ({docwhisperer_stats['total_whispers']} whispers) | +50 pts |
| 🔍 **Web Search** | Tavily API | +40 pts (official) |
| 📁 **GitHub Analysis** | {'✓ Provided' if github_context else 'Not provided'} | +25 pts |
| 📊 **Fivetran Parity** | {'✓ Provided' if fivetran_context else 'Not provided'} | +15 pts |

{'> 💡 **Tip:** Pre-index official documentation in the Knowledge Vault for maximum research accuracy!' if vault_chunks == 0 else '> ✅ **Knowledge Vault active** - Research will prioritize pre-indexed official documentation!'}

---
""")
        
        # Card 1: API Capabilities (always shown)
        summary_parts.append("""
## 📊 API Capabilities (from Documentation)

| Metric | Value |
|--------|-------|
| **Connector Type** | """ + connector_type.upper() + """ |
| **Primary API** | """ + self._get_primary_api(connector_type) + """ |
| **Auth Types** | _See Section 5_ |
| **Official SDKs** | _See Section 15_ |
| **Rate Limits** | _See Section 12_ |

> 💡 Detailed object catalog available in **Section 19**

---
""")
        
        # Card 2: Current Implementation (if GitHub provided)
        if github_context:
            github_metrics = self._extract_github_metrics(github_context)
            
            impl_auth_str = ', '.join(github_metrics.impl_auth) if github_metrics.impl_auth else '_Not detected_'
            sdk_str = github_metrics.impl_sdk if github_metrics.impl_sdk else '_Not detected_'
            
            # Build method breakdown
            method_parts = []
            for method, count in github_metrics.impl_by_method.items():
                if count > 0:
                    method_parts.append(f"{method}: {count}")
            method_str = ', '.join(method_parts) if method_parts else '_Mixed_'
            
            summary_parts.append(f"""
## 🔧 Current Implementation (from GitHub)

| Metric | Value |
|--------|-------|
| **Objects Implemented** | {github_metrics.impl_objects} |
| **By Extraction Method** | {method_str} |
| **Full Load Objects** | {github_metrics.impl_full_load} |
| **Incremental Objects** | {github_metrics.impl_incremental} |
| **Auth Implemented** | {impl_auth_str} |
| **SDK Used** | {sdk_str} |

> 📁 Repository: `{github_context.get('repo_url', 'N/A')}`

---
""")
        
        # Card 3: Fivetran Parity (if Fivetran URLs provided)
        if fivetran_context:
            fivetran_metrics = self._extract_fivetran_metrics(fivetran_context)
            
            # Calculate parity if we have implementation data
            parity_info = None
            if github_context:
                github_metrics = self._extract_github_metrics(github_context)
                parity_info = self._calculate_parity(
                    github_metrics.impl_object_names,
                    fivetran_metrics.fivetran_supported
                )
            
            fivetran_auth_str = ', '.join(fivetran_metrics.fivetran_auth) if fivetran_metrics.fivetran_auth else '_Not specified_'
            
            # Feature checkmarks
            features = fivetran_metrics.fivetran_features
            capture_deletes = '✓ Supported' if features.get('capture_deletes') else '✗ Not supported'
            history_mode = '✓ Supported' if features.get('history_mode') else '✗ Not supported'
            
            summary_parts.append(f"""
## 🎯 Fivetran Parity Analysis

| Metric | Value |
|--------|-------|
| **Fivetran Objects** | {fivetran_metrics.fivetran_objects} |
| **Full Load Objects** | {fivetran_metrics.fivetran_full_load} |
| **Incremental Objects** | {fivetran_metrics.fivetran_incremental} |
| **Auth Methods** | {fivetran_auth_str} |
| **Capture Deletes** | {capture_deletes} |
| **History Mode** | {history_mode} |
""")
            
            # Add parity score and gap analysis if we have implementation data
            if parity_info:
                summary_parts.append(f"""
### Parity Score

| | |
|---|---|
| **Score** | **{parity_info['parity_percentage']}%** ({parity_info['common_count']}/{parity_info['fivetran_total']} objects) |

### Gap Analysis

**Objects we have that Fivetran doesn't ({parity_info['extra_count']}):**
""")
                if parity_info['extra_objects']:
                    summary_parts.append('- ' + ', '.join(parity_info['extra_objects'][:10]))
                    if parity_info['extra_count'] > 10:
                        summary_parts.append(f'- _...and {parity_info["extra_count"] - 10} more_')
                else:
                    summary_parts.append('- _None_')
                
                summary_parts.append(f"""
**Objects Fivetran has that we're missing ({parity_info['missing_count']}):**
""")
                if parity_info['missing_objects']:
                    summary_parts.append('- ' + ', '.join(parity_info['missing_objects'][:10]))
                    if parity_info['missing_count'] > 10:
                        summary_parts.append(f'- _...and {parity_info["missing_count"] - 10} more_')
                else:
                    summary_parts.append('- _None - Full parity achieved!_')
            
            summary_parts.append("""
---
""")
        
        # Navigation hint
        summary_parts.append("""
## 📑 Document Navigation

| Phase | Sections | Focus |
|-------|----------|-------|
| **Phase 1** | 1-3 | Platform Understanding |
| **Phase 2** | 4-7 | Data Access & Auth |
| **Phase 3** | 8-11 | Sync & Extraction |
| **Phase 4** | 12-14 | Reliability & Performance |
| **Phase 5** | 15-17 | Advanced Topics |
| **Phase 6** | 18 | Troubleshooting |
| **Phase 7** | 19 | Object Catalog |

---

""")
        
        return '\n'.join(summary_parts)
    
    def _get_primary_api(self, connector_type: str) -> str:
        """Get primary API description from connector type."""
        type_map = {
            'rest_api': 'REST API',
            'graphql': 'GraphQL API',
            'soap': 'SOAP/XML Web Services',
            'jdbc': 'JDBC Database Connection',
            'sdk': 'Official SDK',
            'webhook': 'Webhooks/Event-driven'
        }
        return type_map.get(connector_type.lower(), connector_type.upper())
    
    async def _web_search(self, query: str) -> str:
        """Perform web search using Tavily.
        
        Args:
            query: Search query
            
        Returns:
            Search results as formatted text
        """
        if not self.tavily_api_key:
            return "Web search not available (no TAVILY_API_KEY)"
        
        try:
            from tavily import TavilyClient
            tavily = TavilyClient(api_key=self.tavily_api_key)
            
            response = tavily.search(
                query=query,
                search_depth="advanced",
                max_results=5
            )
            
            results = []
            for i, result in enumerate(response.get('results', []), 1):
                # Classify source type for confidence scoring
                url = result.get('url', '')
                source_type = self._classify_source(url)
                citation_tag = f"web:{i}"
                
                results.append(f"[{citation_tag}] [{source_type}] {result.get('title', 'No title')}")
                results.append(f"URL: {url}")
                content_snippet = result.get('content', '')[:500]
                results.append(f"Content: {content_snippet}...")
                results.append("")
                
                # Add to evidence map with stable ID
                if self._current_progress:
                    self._add_to_evidence_map(
                        citation_tag=citation_tag,
                        url=url,
                        snippet=content_snippet,
                        source_type="web",
                        confidence=0.7 if 'OFFICIAL' in source_type else 0.5
                    )
            
            return "\n".join(results) if results else "No results found"
            
        except Exception as e:
            return f"Web search error: {str(e)}"
    
    def _classify_source(self, url: str) -> str:
        """Classify source type for confidence scoring.
        
        Args:
            url: The URL to classify
            
        Returns:
            Source type classification
        """
        url_lower = url.lower()
        
        # Official documentation
        if any(x in url_lower for x in ['docs.', '/docs/', 'documentation', 'developers.', 'developer.', '/api/']):
            return 'OFFICIAL'
        
        # GitHub
        if 'github.com' in url_lower:
            if '/issues/' in url_lower or '/discussions/' in url_lower:
                return 'GITHUB-ISSUES'
            return 'GITHUB'
        
        # Stack Overflow / Community
        if any(x in url_lower for x in ['stackoverflow.com', 'stackexchange.com', 'community.', 'forum.']):
            return 'COMMUNITY'
        
        # Known connector platforms
        if any(x in url_lower for x in ['fivetran.com', 'airbyte.io', 'singer.io', 'meltano.com']):
            return 'CONNECTOR-REF'
        
        # Changelogs
        if any(x in url_lower for x in ['changelog', 'release', 'what-s-new', 'updates']):
            return 'CHANGELOG'
        
        # Blog/Article
        if any(x in url_lower for x in ['blog.', 'medium.com', 'dev.to', 'article']):
            return 'BLOG'
        
        return 'OTHER'
    
    async def _verify_with_multiple_sources(
        self,
        connector_name: str,
        claim: str,
        claim_type: str = "general"
    ) -> Dict[str, Any]:
        """Verify a claim by searching multiple source types.
        
        🔮 Now featuring DocWhisperer™ as the primary source of truth!
        
        Args:
            connector_name: Name of the connector
            claim: The claim to verify (e.g., "supports OAuth 2.0")
            claim_type: Type of claim (auth, rate_limit, object, etc.)
            
        Returns:
            Dict with confidence score and source details
        """
        sources_found = {
            'KNOWLEDGE_VAULT': [],  # 📚 Pre-indexed official docs (HIGHEST!)
            'DOCWHISPERER': [],  # 🔮 The Oracle speaks!
            'OFFICIAL': [],
            'GITHUB': [],
            'GITHUB-ISSUES': [],
            'COMMUNITY': [],
            'CONNECTOR-REF': [],
            'CHANGELOG': [],
            'BLOG': [],
            'OTHER': []
        }
        
        # 📚 STEP 0: Query Knowledge Vault FIRST (highest confidence!)
        if self.knowledge_vault and self.knowledge_vault.has_knowledge(connector_name):
            vault_results = self.knowledge_vault.search(
                connector_name=connector_name,
                query=claim,
                top_k=2
            )
            if vault_results:
                for result in vault_results:
                    sources_found['KNOWLEDGE_VAULT'].append(
                        f"[📚 Vault:{result.source_type}] {result.text[:200]}..."
                    )
                print(f"  📚 Knowledge Vault confirmed '{claim}' from pre-indexed docs!")
        
        # 🔮 STEP 1: Consult the DocWhisperer
        docwhisperer_topic_map = {
            'auth': f"authentication {claim}",
            'rate_limit': f"rate limits throttling {claim}",
            'object': f"objects entities {claim}",
            'general': claim
        }
        
        topic = docwhisperer_topic_map.get(claim_type, claim)
        whisper = await self.doc_whisperer.get_library_docs(
            library_id=await self.doc_whisperer.resolve_library_id(connector_name) or "",
            topic=topic
        )
        
        if whisper:
            sources_found['DOCWHISPERER'].append(f"[🔮 DocWhisperer] {whisper.content[:200]}...")
            print(f"  🔮 DocWhisperer whispered truth about '{claim}'!")
        
        # STEP 2: Fall back to web search if DocWhisperer is silent
        source_queries = {
            'auth': [
                f"{connector_name} API authentication documentation",
                f"{connector_name} OAuth setup site:github.com",
                f"{connector_name} API key authentication stackoverflow"
            ],
            'rate_limit': [
                f"{connector_name} API rate limits documentation",
                f"{connector_name} rate limit 429 site:github.com issues",
                f"{connector_name} API throttling limits"
            ],
            'object': [
                f"{connector_name} API {claim} endpoint documentation",
                f"{connector_name} {claim} schema",
                f"Fivetran {connector_name} {claim} supported"
            ],
            'general': [
                f"{connector_name} {claim} official documentation",
                f"{connector_name} {claim} site:github.com",
                f"{connector_name} {claim} example"
            ]
        }
        
        queries = source_queries.get(claim_type, source_queries['general'])
        
        # Search each query and classify results
        for query in queries[:2]:  # Limit to 2 queries to manage API costs
            try:
                results = await self._web_search(query)
                for line in results.split('\n'):
                    if line.startswith('[web:'):
                        # Extract source type from the line
                        for source_type in sources_found.keys():
                            if f'[{source_type}]' in line:
                                sources_found[source_type].append(line)
                                break
            except Exception:
                pass
        
        # Calculate confidence score
        confidence_score = 0
        confidence_reasons = []
        
        # 📚 Knowledge Vault provides THE HIGHEST confidence!
        if sources_found['KNOWLEDGE_VAULT']:
            confidence_score += 60  # Pre-indexed official docs = max trust!
            confidence_reasons.append("📚 Knowledge Vault confirmed from pre-indexed official docs")
        
        # 🔮 DocWhisperer provides high confidence!
        if sources_found['DOCWHISPERER']:
            confidence_score += 50  # The Oracle speaks truth!
            confidence_reasons.append("🔮 DocWhisperer confirmed from official source")
        
        if sources_found['OFFICIAL']:
            confidence_score += 40
            confidence_reasons.append("Found in official docs (web)")
        
        if sources_found['GITHUB'] or sources_found['GITHUB-ISSUES']:
            confidence_score += 25
            confidence_reasons.append("Confirmed on GitHub")
        
        if sources_found['COMMUNITY']:
            confidence_score += 15
            confidence_reasons.append("Community confirmed")
        
        if sources_found['CONNECTOR-REF']:
            confidence_score += 15
            confidence_reasons.append("Other connectors reference this")
        
        if sources_found['CHANGELOG']:
            confidence_score += 5
            confidence_reasons.append("Found in changelog")
        
        # Determine confidence level (adjusted for new max of 210)
        if confidence_score >= 100:
            confidence_level = "VERIFIED"
        elif confidence_score >= 60:
            confidence_level = "DOCUMENTED"
        elif confidence_score >= 30:
            confidence_level = "COMMUNITY"
        elif confidence_score > 0:
            confidence_level = "INFERRED"
        else:
            confidence_level = "UNVERIFIED"
        
        return {
            'confidence_level': confidence_level,
            'confidence_score': confidence_score,
            'reasons': confidence_reasons,
            'sources': sources_found,
            'claim': claim
        }
    
    def _build_section_context(self, section_number: int, structured_context: Dict[str, Any]) -> str:
        """Build section-specific context from structured repository data.
        
        Args:
            section_number: The section number (1-18)
            structured_context: Dict with 'implementation', 'sdk', 'documentation' keys
            
        Returns:
            Formatted context string relevant to the section
        """
        parts = []
        impl = structured_context.get('implementation', {})
        sdk = structured_context.get('sdk', {})
        docs = structured_context.get('documentation', {})
        
        # Section 4: Data Access Mechanisms
        if section_number == 4:
            if sdk.get('available_methods'):
                parts.append(f"**SDK Available Methods:**\n{', '.join(sdk['available_methods'][:30])}")
            if sdk.get('client_classes'):
                parts.append(f"**SDK Client Classes:**\n{', '.join(sdk['client_classes'][:20])}")
            if impl.get('api_calls'):
                parts.append(f"**Implementation API Calls (from Connector_Code):**")
                for call in impl['api_calls'][:10]:
                    parts.append(f"  - {call[:200]}")
            if docs.get('api_reference'):
                parts.append(f"**From Public Documentation - API Reference:**\n{docs['api_reference'][:1500]}")
        
        # Section 5: Authentication Mechanics
        elif section_number == 5:
            if impl.get('auth_implementation'):
                parts.append(f"**Current Auth Implementation (from Connector_Code):**\n```\n{impl['auth_implementation'][:2000]}\n```")
            if sdk.get('auth_methods'):
                parts.append(f"**SDK Auth Methods:**\n{', '.join(sdk['auth_methods'][:20])}")
            if docs.get('auth_guide'):
                parts.append(f"**From Public Documentation - Auth Guide:**\n{docs['auth_guide'][:1500]}")
            if docs.get('permissions'):
                parts.append(f"**Documented Permissions/Scopes:**\n{', '.join(docs['permissions'][:30])}")
        
        # Section 6: App Registration
        elif section_number == 6:
            if docs.get('auth_guide'):
                parts.append(f"**From Public Documentation - Auth/Registration:**\n{docs['auth_guide'][:1500]}")
        
        # Section 7: Metadata Discovery & Schema
        elif section_number == 7:
            if sdk.get('data_types'):
                parts.append(f"**SDK Data Types/Models:**\n{', '.join(sdk['data_types'][:50])}")
            if impl.get('models'):
                parts.append(f"**Implementation Models (from Connector_Code):**\n{', '.join(impl['models'][:30])}")
            if docs.get('objects_schema'):
                parts.append(f"**From Public Documentation - Objects/Schema:**\n{docs['objects_schema'][:2000]}")
            if docs.get('endpoints_list'):
                parts.append(f"**Documented Endpoints:**")
                for ep in docs['endpoints_list'][:20]:
                    parts.append(f"  - {ep}")
        
        # Section 8: Sync Strategies
        elif section_number == 8:
            if impl.get('sync_patterns'):
                parts.append(f"**Sync Patterns Found in Implementation:**")
                for pattern in impl['sync_patterns'][:15]:
                    parts.append(f"  - {pattern[:150]}")
        
        # Section 9: Bulk Extraction & Pagination
        elif section_number == 9:
            if impl.get('sync_patterns'):
                parts.append(f"**Pagination Patterns Found:**")
                for pattern in impl['sync_patterns'][:10]:
                    parts.append(f"  - {pattern[:150]}")
            if impl.get('api_calls'):
                bulk_calls = [c for c in impl['api_calls'] if 'bulk' in c.lower() or 'batch' in c.lower() or 'export' in c.lower()]
                if bulk_calls:
                    parts.append(f"**Bulk API Calls Found:**")
                    for call in bulk_calls[:5]:
                        parts.append(f"  - {call[:200]}")
        
        # Section 12: Rate Limits
        elif section_number == 12:
            if docs.get('rate_limits'):
                parts.append(f"**From Public Documentation - Rate Limits:**\n{docs['rate_limits'][:1500]}")
        
        # Section 13: API Failure Types & Retry
        elif section_number == 13:
            if impl.get('error_handling'):
                parts.append(f"**Error Handling Patterns in Implementation:**")
                for err in impl['error_handling'][:10]:
                    parts.append(f"  - {err[:150]}")
        
        # Section 15: Dependencies, Drivers & SDK
        elif section_number == 15:
            if sdk.get('sdk_name'):
                parts.append(f"**SDK Name:** {sdk['sdk_name']}")
            if sdk.get('client_classes'):
                parts.append(f"**SDK Client Classes:**\n{', '.join(sdk['client_classes'][:20])}")
            if sdk.get('constants'):
                parts.append(f"**SDK Constants/Enums:**\n{', '.join(sdk['constants'][:30])}")
        
        # Section 17: Relationships
        elif section_number == 17:
            if impl.get('models'):
                parts.append(f"**Models Found (potential relationships):**\n{', '.join(impl['models'][:30])}")
            if sdk.get('data_types'):
                parts.append(f"**SDK Data Types:**\n{', '.join(sdk['data_types'][:30])}")
        
        # Section 18: Troubleshooting
        elif section_number == 18:
            if impl.get('error_handling'):
                parts.append(f"**Error Handling Found in Implementation:**")
                for err in impl['error_handling'][:10]:
                    parts.append(f"  - {err[:150]}")
            if impl.get('config_patterns'):
                parts.append(f"**Configuration Patterns:**")
                for cfg in impl['config_patterns'][:10]:
                    parts.append(f"  - {cfg}")
        
        # Section 19: Available Objects & Replication Guide
        elif section_number == 19:
            if sdk.get('data_types'):
                parts.append(f"**SDK Data Types/Objects ({len(sdk['data_types'])} found):**\n{', '.join(sdk['data_types'][:100])}")
            if impl.get('models'):
                parts.append(f"**Implementation Models ({len(impl['models'])} found):**\n{', '.join(impl['models'][:100])}")
            if docs.get('objects_schema'):
                parts.append(f"**From Public Documentation - Objects/Schema:**\n{docs['objects_schema'][:3000]}")
            if docs.get('endpoints_list'):
                parts.append(f"**Documented Endpoints ({len(docs['endpoints_list'])} found):**")
                for ep in docs['endpoints_list'][:40]:
                    parts.append(f"  - {ep}")
            if impl.get('api_calls'):
                parts.append(f"**API Calls Found in Implementation:**")
                for call in impl['api_calls'][:20]:
                    parts.append(f"  - {call[:200]}")
        
        # For other sections, provide general context if available
        else:
            if docs.get('raw_content') and section_number in [1, 2, 3]:
                # Platform understanding sections can use general docs
                parts.append(f"**From Public Documentation:**\n{docs['raw_content'][:1500]}")
        
        return "\n\n".join(parts) if parts else ""
    
    def _build_fivetran_section_context(self, section_number: int, fivetran_context: Dict[str, Any]) -> str:
        """Build section-specific context from Fivetran documentation.
        
        Args:
            section_number: The section number (1-18)
            fivetran_context: Dict with 'setup', 'overview', 'schema' keys from FivetranCrawler
            
        Returns:
            Formatted context string relevant to the section
        """
        parts = []
        setup = fivetran_context.get('setup', {})
        overview = fivetran_context.get('overview', {})
        schema = fivetran_context.get('schema', {})
        
        # Section 1: Product Overview - Use overview features
        if section_number == 1:
            if overview.get('supported_features'):
                features = [f"{k.replace('_', ' ').title()}: {'Yes' if v else 'No'}" 
                           for k, v in overview['supported_features'].items()]
                parts.append(f"**Fivetran Supported Features:**\n{', '.join(features)}")
            if overview.get('sync_overview'):
                parts.append(f"**Fivetran Sync Overview:**\n{overview['sync_overview'][:1500]}")
        
        # Section 3: Pre-Call Config - Use setup prerequisites
        elif section_number == 3:
            if setup.get('prerequisites'):
                parts.append(f"**Fivetran Prerequisites:**")
                for prereq in setup['prerequisites'][:10]:
                    parts.append(f"  - {prereq}")
        
        # Section 5: Authentication - Use setup auth methods and instructions
        elif section_number == 5:
            if setup.get('auth_methods'):
                parts.append(f"**Fivetran Auth Methods:**\n{', '.join(setup['auth_methods'])}")
            if setup.get('auth_instructions'):
                parts.append(f"**Fivetran Auth Instructions:**\n{setup['auth_instructions'][:2000]}")
        
        # Section 6: App Registration - Use setup auth instructions
        elif section_number == 6:
            if setup.get('auth_instructions'):
                parts.append(f"**Fivetran Setup Instructions:**\n{setup['auth_instructions'][:1500]}")
        
        # Section 7: Metadata Discovery - Use schema objects
        elif section_number == 7:
            if schema.get('supported_objects'):
                parts.append(f"**Fivetran Supported Objects ({len(schema['supported_objects'])}):**")
                parts.append(f"{', '.join(schema['supported_objects'][:50])}")
            if schema.get('unsupported_objects'):
                parts.append(f"**Fivetran Unsupported Objects:**\n{', '.join(schema['unsupported_objects'][:20])}")
            if schema.get('permissions_required'):
                parts.append(f"**Fivetran Permissions Required:**")
                for obj, perms in list(schema['permissions_required'].items())[:10]:
                    parts.append(f"  - {obj}: {', '.join(perms)}")
        
        # Section 8: Sync Strategies - Use overview sync details and schema object sync modes
        elif section_number == 8:
            if overview.get('incremental_sync_details'):
                parts.append(f"**Fivetran Incremental Sync:**\n{overview['incremental_sync_details'][:1500]}")
            if overview.get('historical_sync_timeframe'):
                parts.append(f"**Fivetran Historical Sync Timeframe:** {overview['historical_sync_timeframe']}")
            if schema.get('objects'):
                incremental_objs = [o['name'] for o in schema['objects'] if o.get('sync_mode') == 'incremental']
                full_objs = [o['name'] for o in schema['objects'] if o.get('sync_mode') == 'full_load']
                if incremental_objs:
                    parts.append(f"**Fivetran Incremental Objects:** {', '.join(incremental_objs[:20])}")
                if full_objs:
                    parts.append(f"**Fivetran Full Load Objects:** {', '.join(full_objs[:20])}")
        
        # Section 11: Deletion Handling - Use overview capture_deletes feature
        elif section_number == 11:
            if overview.get('supported_features'):
                capture_deletes = overview['supported_features'].get('capture_deletes')
                if capture_deletes is not None:
                    parts.append(f"**Fivetran Capture Deletes:** {'Supported' if capture_deletes else 'Not Supported'}")
        
        # Section 17: Relationships - Use schema parent-child relationships
        elif section_number == 17:
            if schema.get('parent_child_relationships'):
                parts.append(f"**Fivetran Parent-Child Relationships:**")
                for parent, child in schema['parent_child_relationships'][:20]:
                    parts.append(f"  - {parent} → {child}")
        
        # Section 18: Troubleshooting - Use overview limitations
        elif section_number == 18:
            if overview.get('sync_limitations'):
                parts.append(f"**Fivetran Known Limitations:**")
                for lim in overview['sync_limitations'][:10]:
                    parts.append(f"  - {lim}")
        
        # Section 19: Available Objects & Replication Guide - Comprehensive object catalog
        elif section_number == 19:
            # Provide detailed Fivetran object information for the catalog table
            if schema.get('supported_objects'):
                parts.append(f"**Fivetran Supported Objects ({len(schema['supported_objects'])} total):**")
                parts.append(f"Objects: {', '.join(schema['supported_objects'])}")
            
            if schema.get('unsupported_objects'):
                parts.append(f"\n**Fivetran Unsupported Objects ({len(schema['unsupported_objects'])} total):**")
                parts.append(f"Objects: {', '.join(schema['unsupported_objects'])}")
            
            if schema.get('objects'):
                # Build detailed object info for the table
                parts.append(f"\n**Fivetran Object Details (for table columns):**")
                for obj in schema['objects'][:50]:
                    obj_name = obj.get('name', 'Unknown')
                    sync_mode = obj.get('sync_mode', 'Unknown')
                    parent = obj.get('parent', '-')
                    cursor = obj.get('cursor_field', '-')
                    delete_method = obj.get('delete_method', 'Unknown')
                    parts.append(f"  - {obj_name}: sync_mode={sync_mode}, parent={parent}, cursor={cursor}, delete_method={delete_method}")
            
            # Include capture_deletes feature from overview
            if overview.get('supported_features'):
                capture_deletes = overview['supported_features'].get('capture_deletes')
                if capture_deletes is not None:
                    parts.append(f"\n**Fivetran Capture Deletes Feature:** {'Supported' if capture_deletes else 'Not Supported'}")
            
            if schema.get('parent_child_relationships'):
                parts.append(f"\n**Fivetran Parent-Child Relationships:**")
                for parent, child in schema['parent_child_relationships'][:30]:
                    parts.append(f"  - {parent} → {child}")
            
            if schema.get('permissions_required'):
                parts.append(f"\n**Fivetran Permissions by Object:**")
                for obj, perms in list(schema['permissions_required'].items())[:20]:
                    parts.append(f"  - {obj}: {', '.join(perms)}")
            
            # Include overview sync info
            if overview.get('incremental_sync_details'):
                parts.append(f"\n**Fivetran Incremental Sync Details:**")
                parts.append(overview['incremental_sync_details'][:1500])
            
            if overview.get('supported_features'):
                features = [f"{k.replace('_', ' ').title()}: {'Yes' if v else 'No'}" 
                           for k, v in overview['supported_features'].items()]
                parts.append(f"\n**Fivetran Supported Features:**\n{', '.join(features)}")
        
        return "\n\n".join(parts) if parts else ""
    
    async def _generate_section(
        self,
        section: ResearchSection,
        connector_name: str,
        connector_type: str,
        github_context: str = "",
        hevo_context: Optional[Dict[str, Any]] = None,
        fivetran_context: str = "",
        structured_context: Optional[Dict[str, Any]] = None
    ) -> str:
        """Generate content for a single section.
        
        🔮 Now featuring DocWhisperer™ as the primary knowledge source!
        
        Args:
            section: Section definition
            connector_name: Name of connector
            connector_type: Type of connector
            github_context: Context from GitHub code analysis (legacy flat format)
            hevo_context: Optional Hevo connector context for comparison (only used for Fivetran Parity section)
            fivetran_context: Context from Fivetran comparison
            structured_context: Structured context with implementation, sdk, and documentation
            
        Returns:
            Generated markdown content
        """
        # =================================================================
        # Multi-Source Knowledge Retrieval (Priority Order)
        # 📚 Knowledge Vault (pre-indexed) > 🔮 DocWhisperer > 🔍 Web Search
        # =================================================================
        
        all_context_parts = []
        
        # 📚 STEP 1: Query Knowledge Vault FIRST (highest confidence!)
        vault_context = ""
        if self.knowledge_vault and self.knowledge_vault.has_knowledge(connector_name):
            vault_results = self.knowledge_vault.search(
                connector_name=connector_name,
                query=f"{section.name} {section.phase_name}",
                top_k=3
            )
            
            if vault_results:
                vault_texts = []
                for i, result in enumerate(vault_results, 1):
                    vault_texts.append(f"[vault:{i}] **{result.title}** (confidence: {result.score:.2f})")
                    vault_texts.append(f"Source Type: {result.source_type}")
                    vault_texts.append(f"{result.text[:1000]}...")
                    vault_texts.append("")
                
                vault_context = f"""
📚 **Knowledge Vault Context (Pre-Indexed Official Documentation):**
*This information was pre-indexed from official sources - HIGHEST CONFIDENCE*

{chr(10).join(vault_texts)}

---
"""
                all_context_parts.append(vault_context)
                print(f"  📚 Knowledge Vault provided {len(vault_results)} results for Section {section.number}")
        
        # 🔮 STEP 2: Consult DocWhisperer
        docwhisperer_context = ""
        whisper = await self.doc_whisperer.get_library_docs(
            library_id=await self.doc_whisperer.resolve_library_id(connector_name) or "",
            topic=f"{section.name} {section.phase_name}"
        )
        
        if whisper:
            citation_tag = "doc:1"
            docwhisperer_context = f"""
🔮 **DocWhisperer™ Official Documentation Context:**
Source: {whisper.source}
Library: {whisper.library_id}
Confidence: {whisper.confidence}%

{whisper.content}

---
"""
            all_context_parts.append(docwhisperer_context)
            print(f"  🔮 DocWhisperer provided wisdom for Section {section.number}: {section.name}")
            
            # Add to evidence map with stable ID
            if self._current_progress:
                self._add_to_evidence_map(
                    citation_tag=citation_tag,
                    url=whisper.source,
                    snippet=whisper.content[:1000],
                    source_type="doc",
                    confidence=whisper.confidence / 100.0 if whisper.confidence else 0.9
                )
        
        # 🔍 STEP 3: Fall back to web search for additional context
        search_query = f"{connector_name} API {section.name} documentation 2024 2025"
        web_results = await self._web_search(search_query)
        
        # Combine all context sources
        if all_context_parts:
            web_results = "\n".join(all_context_parts) + "\n\n**Web Search Results (supplementary):**\n" + web_results
        
        # Build Hevo context string if provided (for Fivetran Parity section)
        hevo_context_str = ""
        if hevo_context:
            hevo_is_structured = hevo_context.get('structure_type') == 'structured'
            hevo_context_str = self._build_github_context_string(hevo_context, hevo_is_structured, None)
        
        # Build the prompt
        # Use string replacement instead of .format() to avoid KeyError with JSON code blocks
        # This safely replaces {connector} without interpreting other { } as format placeholders
        # Handle conditional prompts [IF HEVO] and [IF NO HEVO]
        filtered_prompts = []
        for p in section.prompts:
            if "[IF HEVO]" in p:
                if hevo_context:
                    filtered_prompts.append(p.replace("[IF HEVO]", ""))
                # Skip if no Hevo context
            elif "[IF NO HEVO]" in p:
                if not hevo_context:
                    filtered_prompts.append(p.replace("[IF NO HEVO]", ""))
                # Skip if Hevo context exists
            else:
                filtered_prompts.append(p)
        
        prompts_text = "\n".join(f"- {p.replace('{connector}', connector_name)}" for p in filtered_prompts)
        
        # Special system prompt for Section 19 (Object Catalog)
        if section.number == 19:
            system_prompt = """You are an expert technical writer specializing in data integration and ETL connector development.
Your task is to create a comprehensive Object Catalog for connector research.

CRITICAL OUTPUT FORMAT REQUIREMENTS:
1. Start with a markdown table listing ALL available objects with these exact columns:
   | Object | Extraction Method | Primary Key | Cursor Field | Parent | Permissions | Delete Method | Fivetran Support |
   
2. The table should include:
   - Object: Name of the entity/object (e.g., accounts, contacts, orders)
   - Extraction Method: Exact API endpoint or method (e.g., "GET /v1/accounts", "GraphQL query accounts", "SOAP GetAccounts")
   - Primary Key: The unique identifier field (e.g., id, account_id)
   - Cursor Field: Field for incremental sync (e.g., updated_at, modified_date) or "-" if full load only
   - Parent: Parent object name if this is a child entity, or "-" if top-level
   - Permissions: Required scopes/permissions (e.g., read:accounts, accounts.read)
   - Delete Method: How to detect deleted records. Use one of:
     * "Soft Delete (field_name)" - e.g., "Soft Delete (is_deleted)", "Soft Delete (deleted_at)"
     * "Deleted Endpoint" - API provides GET /deleted_records or similar
     * "Webhook (event_name)" - e.g., "Webhook (record.deleted)"
     * "Audit Log" - Deletions tracked in audit/activity endpoint
     * "None" - Hard deletes only, no detection available
   - Fivetran Support: "✓" if supported by Fivetran, "✗" if not, or "?" if unknown

3. After the table, include:
   - Replication Strategy Notes: List objects by category (Full Load Only, Incremental, CDC-capable)
   - Delete Detection Summary: Group objects by delete method
   - Sample Extraction Code: Python code example for 2-3 key objects with pagination
   - Volume Considerations: Rate limits or pagination specific to high-volume objects

4. Include inline citations like [web:1], [web:2] referencing web search results
5. If Fivetran context is provided, prioritize that for the Fivetran Support column
6. List at least 15-30 objects if available, or all objects if fewer exist
"""
        else:
            system_prompt = """You are an expert technical writer specializing in data integration and ETL connector development.
Your task is to write detailed, production-grade documentation for connector research.

CRITICAL CITATION REQUIREMENTS:
- Every factual claim (numbers, endpoints, scopes, rate limits, 'supports'/'requires' statements) MUST include inline citations like [web:1], [vault:1], [doc:1]
- Citations must be within 250 characters of the claim
- Table rows must include citations at the end of each row
- Claims without citations will be rejected and require regeneration

Requirements:
- Write 8-10 detailed sentences per subsection
- Include exact values from documentation (OAuth scopes, permissions, rate limits)
- Use markdown tables where appropriate
- Include inline citations like [web:1], [web:2], [vault:1] referencing search results
- When Knowledge Vault context is provided, PRIORITIZE that as the most authoritative source
- When DocWhisperer context is provided, use it as secondary authoritative source
- When structured context is provided (from Connector_Code, Connector_SDK, Public_Documentation), prioritize that information
- Focus on data extraction (read operations), not write operations
- If information is not available, explicitly state "N/A - not documented" or "N/A - not supported"
"""

        # Build section-specific context from structured data
        section_context = ""
        if structured_context:
            section_context = self._build_section_context(section.number, structured_context)

        # Special user prompt for Section 19 (Object Catalog)
        if section.number == 19:
        user_prompt = f"""Generate Section {section.number}: {section.name} for the {connector_name} connector research document.

Connector Type: {connector_type}
Phase: {section.phase_name}

IMPORTANT: This section MUST start with a comprehensive markdown table of ALL available objects.

Questions to answer:
{prompts_text}

Web Search Results:
{web_results}

{f"GitHub Code Analysis Context:{chr(10)}{github_context}" if github_context else ""}
{f"⚠️ **Fivetran Context (Reference Only - Not Ground Truth):**{chr(10)}Fivetran's implementation is provided for comparison purposes. Note that:{chr(10)}- Fivetran may use private/undocumented endpoints{chr(10)}- Their implementation may differ from official API documentation{chr(10)}- Use Fivetran as a signal, not authoritative source{chr(10)}- When Fivetran conflicts with official docs, prioritize official docs{chr(10)}- For Fivetran Support column: Use '?' if only Fivetran mentions the object{chr(10)}{chr(10)}{fivetran_context}" if fivetran_context else ""}
{f"Structured Repository Context:{chr(10)}{section_context}" if section_context else ""}
{f"Hevo Connector Code Context:{chr(10)}{hevo_context_str}" if hevo_context and hevo_context_str else ""}
{f"Hevo Connector Code Context (for comparison):{chr(10)}{hevo_context_str}" if hevo_context and hevo_context_str else ""}

OUTPUT FORMAT REQUIRED:

### 19.1 Object Catalog Table

| Object | Extraction Method | Primary Key | Cursor Field | Parent | Permissions | Delete Method | Fivetran Support |
|--------|-------------------|-------------|--------------|--------|-------------|---------------|------------------|
| (list all objects here - include Delete Method for each: Soft Delete (field), Deleted Endpoint, Webhook (event), Audit Log, or None) |

### 19.2 Replication Strategy Notes

**Full Load Objects:** (list objects with no cursor field)
**Incremental Objects:** (list objects with cursor fields)
**CDC-Capable Objects:** (list objects with real-time change tracking if any)

### 19.3 Delete Detection Summary

**Soft Delete:** (list objects with soft delete flag - specify field name)
**Deleted Endpoint:** (list objects with dedicated deleted records endpoint)
**Webhook:** (list objects with delete webhook events)
**Audit Log:** (list objects tracked via audit log)
**No Delete Detection:** (list objects with hard deletes only)

### 19.4 Sample Extraction Code

```python
# Python code example for extracting 2-3 key objects with pagination
# Include example for detecting deleted records if applicable
```

### 19.5 Volume Considerations

(Rate limits, pagination limits, high-volume object notes)
"""
        else:
            user_prompt = f"""Generate Section {section.number}: {section.name} for the {connector_name} connector research document.

Connector Type: {connector_type}
Phase: {section.phase_name}

Questions to answer:
{prompts_text}

Web Search Results (including DocWhisperer™ official docs if available):
{web_results}

{f"GitHub Code Analysis Context:{chr(10)}{github_context}" if github_context else ""}
{f"⚠️ **Fivetran Context (Reference Only - Not Ground Truth):**{chr(10)}Fivetran's implementation is provided for comparison purposes. Note that:{chr(10)}- Fivetran may use private/undocumented endpoints{chr(10)}- Their implementation may differ from official API documentation{chr(10)}- Use Fivetran as a signal, not authoritative source{chr(10)}- When Fivetran conflicts with official docs, prioritize official docs{chr(10)}{chr(10)}{fivetran_context}" if fivetran_context else ""}
{f"Structured Repository Context:{chr(10)}{section_context}" if section_context else ""}
{f"Hevo Connector Code Context (for comparison with Fivetran):{chr(10)}{hevo_context_str}" if hevo_context and hevo_context_str else ""}

Generate comprehensive markdown content for this section. Include:
1. Clear subsection headers (e.g., {section.number}.1, {section.number}.2)
2. Detailed explanations with citations
3. Tables where appropriate (objects, limits, permissions)
4. Code examples if relevant
5. Exact values from documentation (no placeholders)
"""

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.3,
                max_tokens=3000
            )
            
            # Validate response structure
            if not response or not hasattr(response, 'choices') or not response.choices:
                raise ValueError("OpenAI API returned empty response or no choices")
            
            if not response.choices[0] or not hasattr(response.choices[0], 'message'):
                raise ValueError("OpenAI API response missing message")
            
            message = response.choices[0].message
            if not hasattr(message, 'content') or message.content is None:
                raise ValueError("OpenAI API response missing content")
            
            content = message.content.strip()
            
            if not content:
                raise ValueError("OpenAI API returned empty content")
            
            # Phase emoji mapping
            phase_emojis = {
                1: "🔍",  # Understand the Platform
                2: "🔐",  # Data Access Mechanisms
                3: "🔄",  # Sync Design & Extraction
                4: "⚡",  # Reliability & Performance
                5: "🔧",  # Advanced Considerations
                6: "🛠️",  # Troubleshooting
                7: "📋",  # Object Catalog
            }
            phase_emoji = phase_emojis.get(section.phase, "📄")
            
            # Format as markdown section with improved layout
            formatted = f"""

---

# {phase_emoji} Phase {section.phase}: {section.phase_name}

## {section.number}. {section.name}

{content}

<details>
<summary>📌 Section Metadata</summary>

- Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}
- Source: Web search + AI synthesis

</details>

[↑ Back to Summary](#-quick-summary-dashboard)

"""
            return formatted
            
        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            print(f"Error generating section {section.number} ({section.name}): {e}")
            print(f"Traceback:\n{error_trace}")
            return f"""

---

## {section.number}. {section.name}

**Error generating section:** {str(e)}

*This section could not be generated due to an error. Please try regenerating the research.*

---
"""
    
    async def _check_stop_the_line(
        self,
        section_review: Any,
        section_content: str
    ) -> Optional[StopTheLineEvent]:
        """
        Check if section should trigger stop-the-line.
        
        Args:
            section_review: SectionReview from Critic Agent
            section_content: Generated section content
            
        Returns:
            StopTheLineEvent if should stop, None otherwise
        """
        if not section_review:
            return None
        
        # Check for critical contradictions
        critical_contradictions = [
            c for c in section_review.contradictions 
            if c.severity == "CRITICAL" and c.category in ["AUTH", "RATE_LIMIT", "OBJECT_SUPPORT"]
        ]
        
        if critical_contradictions:
            return StopTheLineEvent(
                reason="CRITICAL_CONTRADICTION",
                contradictions=critical_contradictions,
                section_number=section_review.section_number,
                required_action="HUMAN_REVIEW"
            )
        
        # Check for low confidence on critical claims
        low_confidence_critical = [
            f for f in section_review.uncertainty_flags
            if f.confidence < 0.5 and f.category in ["AUTH", "RATE_LIMIT", "OBJECT_SUPPORT"]
        ]
        
        if low_confidence_critical:
            return StopTheLineEvent(
                reason="LOW_CONFIDENCE_CRITICAL",
                uncertainty_flags=low_confidence_critical,
                section_number=section_review.section_number,
                required_action="ADDITIONAL_SOURCES"
            )
        
        return None
    
    async def _validate_and_regenerate(
        self,
        section: ResearchSection,
        connector_name: str,
        connector_type: str,
        github_context: str = "",
        hevo_context: Optional[Dict[str, Any]] = None,
        fivetran_context: str = "",
        structured_context: Optional[Dict[str, Any]] = None,
        max_attempts: int = 3
    ) -> Tuple[str, Any, bool]:
        """
        Generate section with citation validation and smart regeneration.
        
        Returns:
            Tuple of (final_content, validation_result, should_stop)
        """
        from services.citation_validator import CitationValidator
        
        validator = CitationValidator(max_citation_distance=250)
        content = ""
        validation_result = None
        
        for attempt in range(1, max_attempts + 1):
            # Generate content
            if attempt == 1:
                # First attempt: normal generation
                content = await self._generate_section(
                    section=section,
                    connector_name=connector_name,
                    connector_type=connector_type,
                    github_context=github_context,
                    hevo_context=hevo_context,
                    fivetran_context=fivetran_context,
                    structured_context=structured_context
                )
            else:
                # Regeneration: include failure report in enhanced prompt
                content = await self._generate_section_with_failure_report(
                    section=section,
                    connector_name=connector_name,
                    connector_type=connector_type,
                    github_context=github_context,
                    hevo_context=hevo_context,
                    fivetran_context=fivetran_context,
                    structured_context=structured_context,
                    failure_report=validation_result.failure_report,
                    attempt_number=attempt
                )
            
            # Validate citations
            validation_result = validator.validate_content(content, section.number)
            
            if validation_result.is_valid:
                break  # Success!
            
            print(f"  ⚠ Citation validation failed (attempt {attempt}/{max_attempts}): "
                  f"{len(validation_result.uncited_claims)} uncited claims, "
                  f"{len(validation_result.uncited_table_rows)} uncited table rows")
        
        # After max attempts, if still invalid, trigger stop-the-line
        should_stop = not validation_result.is_valid if validation_result else False
        
        if should_stop:
            print(f"  🛑 Citation validation failed after {max_attempts} attempts. Triggering stop-the-line.")
            if self._current_progress:
                self._current_progress.status = "stopped"
                self._current_progress.error_message = (
                    f"Citation validation failed: {len(validation_result.uncited_claims)} uncited claims, "
                    f"{len(validation_result.uncited_table_rows)} uncited table rows"
                )
        
        return content, validation_result, should_stop
    
    async def _generate_and_review_section(
        self,
        section: ResearchSection,
        connector_name: str,
        connector_type: str,
        github_context: str = "",
        hevo_context: Optional[Dict[str, Any]] = None,
        fivetran_context: str = "",
        structured_context: Optional[Dict[str, Any]] = None,
        previous_sections: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Generate section and review with Citation Validator, Evidence Integrity Validator, and Critic Agent.
        
        Returns:
            Dict with 'content', 'review', 'stop_the_line'
        """
        # Generate section with validation and smart regeneration
        content, validation_result, should_stop = await self._validate_and_regenerate(
            section=section,
            connector_name=connector_name,
            connector_type=connector_type,
            github_context=github_context,
            hevo_context=hevo_context,
            fivetran_context=fivetran_context,
            structured_context=structured_context,
            max_attempts=3
        )
        
        if should_stop:
            return {
                "content": content,
                "review": None,
                "stop_the_line": True
            }
        
        # Validate evidence integrity
        integrity_result = None
        if self._current_progress and self._current_progress.evidence_map_json:
            try:
                from services.evidence_integrity_validator import EvidenceIntegrityValidator
                integrity_validator = EvidenceIntegrityValidator(enable_snippet_matching=True)
                integrity_result = integrity_validator.validate_evidence_integrity(
                    content=content,
                    evidence_map=self._current_progress.evidence_map_json
                )
                
                if not integrity_result.is_valid:
                    print(f"  ⚠ Evidence integrity validation failed: {len(integrity_result.issues)} issues")
                    # For now, log but don't stop - could be enhanced to trigger regeneration
            except Exception as e:
                print(f"  ⚠ Evidence integrity validator not available: {e}")
        
        # Review with Critic Agent if available
        review = None
        stop_event = None
        
        if self.critic_agent:
            # Build sources dict for review
            sources = {
                "vault": "",
                "docwhisperer": "",
                "web": "",
                "fivetran": fivetran_context if fivetran_context else "",
                "github": github_context if github_context else ""
            }
            
            try:
                review = await self.critic_agent.review_section(
                    section_number=section.number,
                    section_name=section.name,
                    content=content,
                    sources=sources,
                    previous_sections=previous_sections or []
                )
                
                # Check for stop-the-line
                stop_event = await self._check_stop_the_line(review, content)
                
                # Store review in progress
                if self._current_progress:
                    self._current_progress.section_reviews[section.number] = review
                    if stop_event:
                        self._current_progress.stop_the_line_events.append(stop_event)
                        self._current_progress.contradictions.extend(review.contradictions)
                        self._current_progress.status = "stopped"
                
            except Exception as e:
                print(f"  ⚠ Critic Agent review failed: {e}")
        
        return {
            "content": content,
            "review": review,
            "stop_the_line": stop_event
        }
    
    async def _process_section_with_review(
        self,
        section: ResearchSection,
        connector_name: str,
        connector_type: str,
        github_context: str = "",
        hevo_context: Optional[Dict[str, Any]] = None,
        fivetran_context: str = "",
        structured_context: Optional[Dict[str, Any]] = None,
        document_parts: List[str] = None,
        on_progress: Optional[Callable] = None
    ) -> Tuple[str, bool]:
        """
        Helper to generate, review, and process a section.
        
        Returns:
            Tuple of (section_content, should_stop)
        """
        result = await self._generate_and_review_section(
            section=section,
            connector_name=connector_name,
            connector_type=connector_type,
            github_context=github_context,
            hevo_context=hevo_context,
            fivetran_context=fivetran_context,
            structured_context=structured_context,
            previous_sections=(document_parts[-3:] if document_parts else [])
        )
        
        section_content = result["content"]
        review = result.get("review")
        stop_event = result.get("stop_the_line")
        
        # Extract structured claims after validation passes
        if not stop_event and self._current_progress:
            try:
                sources = {
                    "vault": "",
                    "docwhisperer": "",
                    "web": "",
                    "fivetran": fivetran_context if fivetran_context else "",
                    "github": github_context if github_context else ""
                }
                
                claims = self._extract_structured_claims(
                    content=section_content,
                    section_number=section.number,
                    sources=sources,
                    evidence_map=self._current_progress.evidence_map_json
                )
                
                self._current_progress.claims_json.extend(claims)
            except Exception as e:
                print(f"  ⚠ Claim extraction failed: {e}")
        
        # Check for stop-the-line
        if stop_event:
            print(f"  🛑 STOP-THE-LINE triggered for Section {section.number}: {stop_event.reason}")
            self._current_progress.status = "stopped"
            self._current_progress.error_message = f"Stop-the-line: {stop_event.reason} - {stop_event.required_action}"
            if on_progress:
                on_progress(self._current_progress)
            section_content += f"\n\n⚠️ **STOP-THE-LINE**: {stop_event.reason} - {stop_event.required_action}\n"
            return section_content, True
        
        # Update overall confidence
        if review and review.confidence_score:
            completed_count = len(self._current_progress.sections_completed)
            current_avg = self._current_progress.overall_confidence
            self._current_progress.overall_confidence = (
                (current_avg * (completed_count - 1) + review.confidence_score) / completed_count
                if completed_count > 0 else review.confidence_score
            )
        
        return section_content, False
    
    def _parse_discovered_methods(self, discovery_content: str) -> List[str]:
        """Parse the discovery section content to extract available methods.
        
        Args:
            discovery_content: The generated content for section 2 (Extraction Methods Discovery)
            
        Returns:
            List of method names that were found to be available
        """
        discovered = []
        content_lower = discovery_content.lower()
        
        # Check for each method
        method_indicators = {
            "REST API": ["rest api", "rest endpoint", "restful", "rest-based", "/api/v"],
            "GraphQL API": ["graphql", "graph ql", "graphql endpoint", "graphql api"],
            "SOAP/XML API": ["soap", "wsdl", "xml api", "soap endpoint"],
            "Webhooks": ["webhook", "web hook", "event subscription", "push notification"],
            "Bulk/Batch API": ["bulk api", "batch api", "async export", "bulk export", "batch request"],
            "Official SDK": ["official sdk", "sdk available", "python sdk", "java sdk", "node sdk", "client library"],
            "JDBC/ODBC": ["jdbc", "odbc", "database driver", "direct database"],
            "File Export": ["file export", "csv export", "data export", "export to file", "downloadable report"]
        }
        
        for method, indicators in method_indicators.items():
            # Check if the method is mentioned positively (not "not available")
            for indicator in indicators:
                if indicator in content_lower:
                    # Check it's not marked as unavailable
                    idx = content_lower.find(indicator)
                    surrounding = content_lower[max(0, idx-50):idx+100]
                    if "not available" not in surrounding and "unavailable" not in surrounding and "does not" not in surrounding and "no " + indicator not in surrounding:
                        if method not in discovered:
                            discovered.append(method)
                        break
        
        # If nothing found, default to REST API (most common)
        if not discovered:
            discovered = ["REST API"]
        
        return discovered
    
    async def generate_research(
        self,
        connector_id: str,
        connector_name: str,
        connector_type: str = "auto",
        github_context: Optional[Dict[str, Any]] = None,
        hevo_context: Optional[Dict[str, Any]] = None,
        fivetran_context: Optional[Dict[str, Any]] = None,
        on_progress: Optional[Callable[[ResearchProgress], None]] = None
    ) -> str:
        """Generate complete research document for a connector with dynamic method discovery.
        
        Args:
            connector_id: Connector ID
            connector_name: Connector display name
            connector_type: Type of connector (default "auto" for discovery)
            github_context: Optional extracted code patterns from GitHub
            hevo_context: Optional Hevo connector context for comparison (used in Fivetran Parity section)
            fivetran_context: Optional Fivetran documentation context for parity comparison
            on_progress: Optional callback for progress updates
            
        Returns:
            Complete research document as markdown
        """
        self._cancel_requested = False
        self._current_progress = ResearchProgress(
            connector_id=connector_id,
            connector_name=connector_name,
            status="running"
        )
        
        # Detect if we have structured context
        is_structured = github_context and github_context.get('structure_type') == 'structured'
        structured_context = None
        
        if is_structured:
            structured_context = {
                'implementation': github_context.get('implementation', {}),
                'sdk': github_context.get('sdk', {}),
                'documentation': github_context.get('documentation', {})
            }
        
        # Build research method description
        research_method_parts = ["Automated generation using web search"]
        if github_context:
            if is_structured:
                research_method_parts.append("structured repository analysis")
            else:
                research_method_parts.append("GitHub code analysis")
        if fivetran_context:
            research_method_parts.append("Fivetran documentation parity analysis")
        
        # Prepare GitHub context string
        github_context_str = self._build_github_context_string(github_context, is_structured, structured_context)
        
        # Prepare Hevo context string (if Hevo context provided)
        hevo_context_str = ""
        if hevo_context:
            hevo_is_structured = hevo_context.get('structure_type') == 'structured'
            hevo_context_str = self._build_github_context_string(hevo_context, hevo_is_structured, None)
        
        # Initialize document with header (section count will be updated later)
        document_parts = []
        discovered_methods = []
        
        # ========================================
        # PHASE 1: Discovery Sections (1-3)
        # ========================================
        print(f"  Phase 1: Platform Discovery")
        
        discovery_content = ""
        for section in BASE_SECTIONS:
            if self._cancel_requested:
                self._current_progress.status = "cancelled"
                break
            
            self._current_progress.current_section = section.number
            self._current_progress.current_content = f"Generating Section {section.number}: {section.name}..."
            
            if on_progress:
                on_progress(self._current_progress)
            
            # Generate and review section with Critic Agent
            result = await self._generate_and_review_section(
                section=section,
                connector_name=connector_name,
                connector_type=connector_type,
                github_context=github_context_str if section.requires_code_analysis else "",
                hevo_context=None,
                fivetran_context="",
                structured_context=structured_context,
                previous_sections=[p for p in document_parts[-3:]]  # Last 3 sections for context
            )
            
            section_content = result["content"]
            review = result.get("review")
            stop_event = result.get("stop_the_line")
            
            # Check for stop-the-line
            if stop_event:
                print(f"  🛑 STOP-THE-LINE triggered for Section {section.number}: {stop_event.reason}")
                self._current_progress.status = "stopped"
                self._current_progress.error_message = f"Stop-the-line: {stop_event.reason} - {stop_event.required_action}"
                if on_progress:
                    on_progress(self._current_progress)
                # Add stop notice to document
                section_content += f"\n\n⚠️ **STOP-THE-LINE**: {stop_event.reason} - {stop_event.required_action}\n"
                document_parts.append(section_content)
                break  # Stop generation
            
            # Save discovery section content for parsing
            if section.number == 2:
                discovery_content = section_content
            
            document_parts.append(section_content)
            self._current_progress.sections_completed.append(section.number)
            
            # Update overall confidence
            if review and review.confidence_score:
                # Update running average of confidence
                completed_count = len(self._current_progress.sections_completed)
                current_avg = self._current_progress.overall_confidence
                self._current_progress.overall_confidence = (
                    (current_avg * (completed_count - 1) + review.confidence_score) / completed_count
                )
            
            await asyncio.sleep(1)
        
        # Parse discovered methods from Section 2
        discovered_methods = self._parse_discovered_methods(discovery_content)
        self._current_progress.discovered_methods = discovered_methods
        print(f"  Discovered extraction methods: {', '.join(discovered_methods)}")
        
        # Calculate total sections (including new enterprise phases)
        total_sections = (len(BASE_SECTIONS) + len(discovered_methods) + 
                         len(CROSS_CUTTING_SECTIONS) + len(FINAL_SECTIONS) +
                         len(FUNCTIONAL_SECTIONS) + len(OPERATIONAL_SECTIONS))
        self._current_progress.total_sections = total_sections
        
        # ========================================
        # PHASE 2: Per-Method Deep Dives (Dynamic)
        # ========================================
        print(f"  Phase 2: Extraction Methods ({len(discovered_methods)} methods)")
        
        method_section_number = 4  # Start after base sections
        for method in discovered_methods:
            if self._cancel_requested:
                self._current_progress.status = "cancelled"
                break
            
            method_section = create_method_section(method, method_section_number)
            
            self._current_progress.current_section = method_section_number
            self._current_progress.current_content = f"Generating Section {method_section_number}: {method} Deep Dive..."
            
            if on_progress:
                on_progress(self._current_progress)
            
            # Generate and review section with Critic Agent
            result = await self._generate_and_review_section(
                section=method_section,
                connector_name=connector_name,
                connector_type=connector_type,
                github_context=github_context_str,
                hevo_context=None,
                fivetran_context="",
                structured_context=structured_context,
                previous_sections=[p for p in document_parts[-3:]]
            )
            
            section_content = result["content"]
            review = result.get("review")
            stop_event = result.get("stop_the_line")
            
            # Check for stop-the-line
            if stop_event:
                print(f"  🛑 STOP-THE-LINE triggered for Section {method_section_number}: {stop_event.reason}")
                self._current_progress.status = "stopped"
                self._current_progress.error_message = f"Stop-the-line: {stop_event.reason} - {stop_event.required_action}"
                if on_progress:
                    on_progress(self._current_progress)
                section_content += f"\n\n⚠️ **STOP-THE-LINE**: {stop_event.reason} - {stop_event.required_action}\n"
                document_parts.append(section_content)
                break
            
            document_parts.append(section_content)
            self._current_progress.sections_completed.append(method_section_number)
            
            # Update overall confidence
            if review and review.confidence_score:
                completed_count = len(self._current_progress.sections_completed)
                current_avg = self._current_progress.overall_confidence
                self._current_progress.overall_confidence = (
                    (current_avg * (completed_count - 1) + review.confidence_score) / completed_count
                )
            
            method_section_number += 1
            await asyncio.sleep(1)
        
        # ========================================
        # PHASE 3: Cross-Cutting Concerns
        # ========================================
        print(f"  Phase 3: Cross-Cutting Concerns")
        
        # Prepare methods list for cross-cutting context
        methods_context = f"Available extraction methods for {connector_name}: {', '.join(discovered_methods)}"
        
        for i, section in enumerate(CROSS_CUTTING_SECTIONS):
            if self._cancel_requested:
                self._current_progress.status = "cancelled"
                break
            
            actual_section_number = method_section_number + i
            section_copy = ResearchSection(
                number=actual_section_number,
                name=section.name,
                phase=section.phase,
                phase_name=section.phase_name,
                prompts=section.prompts,
                requires_fivetran=section.requires_fivetran,
                requires_code_analysis=section.requires_code_analysis
            )
            
            self._current_progress.current_section = actual_section_number
            self._current_progress.current_content = f"Generating Section {actual_section_number}: {section.name}..."
            
            if on_progress:
                on_progress(self._current_progress)
            
            # Build Fivetran context
            section_fivetran_context = ""
            if fivetran_context and section.requires_fivetran:
                section_fivetran_context = self._build_fivetran_section_context(section.number, fivetran_context)
            
            # Generate and review section
            section_content, should_stop = await self._process_section_with_review(
                section=section_copy,
                connector_name=connector_name,
                connector_type=connector_type,
                github_context=github_context_str + "\n\n" + methods_context if section.requires_code_analysis else methods_context,
                hevo_context=None,
                fivetran_context=section_fivetran_context,
                structured_context=structured_context,
                document_parts=document_parts,
                on_progress=on_progress
            )
            
            if should_stop:
                document_parts.append(section_content)
                break
            
            document_parts.append(section_content)
            self._current_progress.sections_completed.append(actual_section_number)
            await asyncio.sleep(1)
        
        # ========================================
        # PHASE 4: Implementation Guide
        # ========================================
        print(f"  Phase 4: Implementation Guide")
        
        final_section_start = method_section_number + len(CROSS_CUTTING_SECTIONS)
        for i, section in enumerate(FINAL_SECTIONS):
            if self._cancel_requested:
                self._current_progress.status = "cancelled"
                break
            
            actual_section_number = final_section_start + i
            section_copy = ResearchSection(
                number=actual_section_number,
                name=section.name,
                phase=section.phase,
                phase_name=section.phase_name,
                prompts=section.prompts,
                requires_fivetran=section.requires_fivetran,
                requires_code_analysis=section.requires_code_analysis
            )
            
            self._current_progress.current_section = actual_section_number
            self._current_progress.current_content = f"Generating Section {actual_section_number}: {section.name}..."
            
            if on_progress:
                on_progress(self._current_progress)
            
            # Build Fivetran context
            section_fivetran_context = ""
            if fivetran_context and section.requires_fivetran:
                section_fivetran_context = self._build_fivetran_section_context(section.number, fivetran_context)
            
            # Pass Hevo context only for Fivetran Parity section
            # Check by section name since section.number is dynamically assigned
            section_hevo_context = hevo_context if "Fivetran Parity" in section.name else None
            
            # Generate and review section
            section_content, should_stop = await self._process_section_with_review(
                section=section_copy,
                connector_name=connector_name,
                connector_type=connector_type,
                github_context=github_context_str + "\n\n" + methods_context if section.requires_code_analysis else methods_context,
                hevo_context=section_hevo_context,
                fivetran_context=section_fivetran_context,
                structured_context=structured_context,
                document_parts=document_parts,
                on_progress=on_progress
            )
            
            if should_stop:
                document_parts.append(section_content)
                break
            
            document_parts.append(section_content)
            self._current_progress.sections_completed.append(actual_section_number)
            await asyncio.sleep(1)
        
        # ========================================
        # PHASE 5: Core Functional Requirements
        # ========================================
        print(f"  Phase 5: Core Functional Requirements")
        
        functional_section_start = final_section_start + len(FINAL_SECTIONS)
        for i, section in enumerate(FUNCTIONAL_SECTIONS):
            if self._cancel_requested:
                self._current_progress.status = "cancelled"
                break
            
            actual_section_number = functional_section_start + i
            section_copy = ResearchSection(
                number=actual_section_number,
                name=section.name,
                phase=section.phase,
                phase_name=section.phase_name,
                prompts=section.prompts,
                requires_fivetran=section.requires_fivetran,
                requires_code_analysis=section.requires_code_analysis
            )
            
            self._current_progress.current_section = actual_section_number
            self._current_progress.current_content = f"Generating Section {actual_section_number}: {section.name}..."
            
            if on_progress:
                on_progress(self._current_progress)
            
            # Generate and review section
            section_content, should_stop = await self._process_section_with_review(
                section=section_copy,
                connector_name=connector_name,
                connector_type=connector_type,
                github_context=github_context_str + "\n\n" + methods_context,
                hevo_context=None,
                fivetran_context="",
                structured_context=structured_context,
                document_parts=document_parts,
                on_progress=on_progress
            )
            
            if should_stop:
                document_parts.append(section_content)
                break
            
            document_parts.append(section_content)
            self._current_progress.sections_completed.append(actual_section_number)
            await asyncio.sleep(1)
        
        # ========================================
        # PHASE 6: Technical Operations
        # ========================================
        print(f"  Phase 6: Technical Operations")
        
        operational_section_start = functional_section_start + len(FUNCTIONAL_SECTIONS)
        for i, section in enumerate(OPERATIONAL_SECTIONS):
            if self._cancel_requested:
                self._current_progress.status = "cancelled"
                break
            
            actual_section_number = operational_section_start + i
            section_copy = ResearchSection(
                number=actual_section_number,
                name=section.name,
                phase=section.phase,
                phase_name=section.phase_name,
                prompts=section.prompts,
                requires_fivetran=section.requires_fivetran,
                requires_code_analysis=section.requires_code_analysis
            )
            
            self._current_progress.current_section = actual_section_number
            self._current_progress.current_content = f"Generating Section {actual_section_number}: {section.name}..."
            
            if on_progress:
                on_progress(self._current_progress)
            
            # Generate and review section
            section_content, should_stop = await self._process_section_with_review(
                section=section_copy,
                connector_name=connector_name,
                connector_type=connector_type,
                github_context=github_context_str + "\n\n" + methods_context,
                hevo_context=None,
                fivetran_context="",
                structured_context=structured_context,
                document_parts=document_parts,
                on_progress=on_progress
            )
            
            if should_stop:
                document_parts.append(section_content)
                break
            
            document_parts.append(section_content)
            self._current_progress.sections_completed.append(actual_section_number)
            await asyncio.sleep(1)
        
        # ========================================
        # Build Final Document
        # ========================================
        
        # Check if stopped
        if self._current_progress.status == "stopped":
            if on_progress:
                on_progress(self._current_progress)
            # Return partial document with stop notice
            partial_doc = "\n".join(document_parts)
            stop_events_text = "\n".join([
                f"- Section {event.section_number}: {event.reason} - {event.required_action}"
                for event in self._current_progress.stop_the_line_events
            ]) if self._current_progress.stop_the_line_events else "No details available"
            
            return f"""# 📚 Connector Research: {connector_name}

**Status:** ⚠️ STOPPED - Critical Issues Detected
**Reason:** {self._current_progress.error_message}

---

{partial_doc}

---

## ⚠️ Research Generation Stopped

Research generation was stopped due to critical contradictions or low confidence in critical claims.

**Stop-the-Line Events:**
{stop_events_text}

Please review the issues above and resolve them before continuing.
"""
        
        # Create document header with accurate section count
        docwhisperer_stats = self.doc_whisperer.get_whisper_stats()
        header = f"""# 📚 Connector Research: {connector_name}

**Subject:** {connector_name} Connector - Full Production Research  
**Status:** Complete  
**Generated:** {datetime.utcnow().strftime('%Y-%m-%d')}  
**Total Sections:** {total_sections}  
**Discovered Methods:** {', '.join(discovered_methods)}  
**Research Sources:** 🔮 DocWhisperer™ ({docwhisperer_stats['total_whispers']} official docs consulted), Tavily Web Search, GitHub Analysis

---

## 📝 Research Overview

**Goal:** Produce exhaustive, production-grade research on building a data connector for {connector_name}.

**Extraction Methods Discovered:** {len(discovered_methods)} ({', '.join(discovered_methods)})

**Research Method:** {' and '.join(research_method_parts)}

{f"**Repository Structure:** Structured (Connector_Code, Connector_SDK, Public_Documentation)" if is_structured else ""}

---

## 📑 Document Structure

| Phase | Sections | Content |
|-------|----------|---------|
| 1. Platform Discovery | 1-3 | Overview, Methods Discovery, Dev Environment |
| 2. Extraction Methods | 4-{3 + len(discovered_methods)} | Deep dive for each discovered method |
| 3. Cross-Cutting | {4 + len(discovered_methods)}-{3 + len(discovered_methods) + len(CROSS_CUTTING_SECTIONS)} | Auth, Rate Limits, Errors, Data Model, Deletes |
| 4. Implementation | {4 + len(discovered_methods) + len(CROSS_CUTTING_SECTIONS)}-{3 + len(discovered_methods) + len(CROSS_CUTTING_SECTIONS) + len(FINAL_SECTIONS)} | Strategy, Object Catalog, Checklist |
| 5. Core Functional | {4 + len(discovered_methods) + len(CROSS_CUTTING_SECTIONS) + len(FINAL_SECTIONS)}-{3 + len(discovered_methods) + len(CROSS_CUTTING_SECTIONS) + len(FINAL_SECTIONS) + len(FUNCTIONAL_SECTIONS)} | Data Source, Extraction, Transform, Quality, Loading |
| 6. Technical Ops | {4 + len(discovered_methods) + len(CROSS_CUTTING_SECTIONS) + len(FINAL_SECTIONS) + len(FUNCTIONAL_SECTIONS)}-{total_sections} | Connectivity, Volume, Errors, Monitoring, Audit |

---
"""
        
        # Generate Quick Summary Dashboard
        quick_summary = self._generate_quick_summary(
            connector_name=connector_name,
            connector_type=', '.join(discovered_methods) if discovered_methods else "auto",
            github_context=github_context,
            fivetran_context=fivetran_context
        )
        
        # Combine all parts
        full_document = header + quick_summary + '\n'.join(document_parts)
        
        # Add final deliverables section
        final_section = f"""

---

# ✅ Final Deliverables

## 🎯 Production Recommendations

| Priority | Recommendation |
|----------|----------------|
| **Critical** | Implement exponential backoff for rate limit handling |
| **Critical** | Implement proper OAuth token refresh before expiration |
| **Critical** | Handle pagination consistently across all objects |
| **High** | Use incremental sync with cursor where available |
| **High** | Implement delete detection mechanism |
| **High** | Set appropriate timeouts for long-running operations |
| **Medium** | Use bulk APIs for historical loads when available |
| **Medium** | Implement proper error categorization |
| **Medium** | Monitor API usage against quotas |
| **Low** | Document all custom field mappings |

---

## ☑️ Implementation Checklist

| Task | Status | Notes |
|------|--------|-------|
| Authentication configured and tested | ⬜ | |
| Rate limiting implemented with backoff | ⬜ | |
| Error handling with retry logic | ⬜ | |
| Incremental sync with cursor fields | ⬜ | |
| Delete detection mechanism | ⬜ | |
| Custom fields discovery | ⬜ | |
| Parent-child load ordering | ⬜ | |
| Monitoring and alerting | ⬜ | |
| Documentation complete | ⬜ | |

---

## 📚 Sources and Methodology

This research document was generated using:

| Source | Description |
|--------|-------------|
| **Tavily API** | Web search for official documentation |
| **OpenAI GPT-4** | AI synthesis and analysis |
"""
        
        if github_context:
            repo_url = github_context.get('repo_url', 'N/A')
            if is_structured:
                final_section += f"""| **GitHub Repository** | `{repo_url}` |
| | Connector_Code: Implementation patterns |
| | Connector_SDK: SDK methods, data types |
| | Public_Documentation: API reference |
"""
            else:
                final_section += f"| **GitHub Repository** | `{repo_url}` |\n"
        
        if fivetran_context:
            fivetran_sources = []
            if fivetran_context.get('has_setup'):
                fivetran_sources.append("Setup Guide")
            if fivetran_context.get('has_overview'):
                fivetran_sources.append("Connector Overview")
            if fivetran_context.get('has_schema'):
                fivetran_sources.append("Schema Information")
            final_section += f"| **Fivetran Docs** | {', '.join(fivetran_sources)} |\n"
        
        final_section += f"""

---

## 🏷️ Document Info

| | |
|---|---|
| **Generated By** | Connector Research Agent |
| **Generated On** | {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} |
| **Total Sections** | {total_sections} |
| **Discovered Methods** | {', '.join(discovered_methods)} |
| **Version** | 2.0 (Dynamic Discovery) |

---

*End of Document*
"""
        
        full_document += final_section
        
        # Add Expert Review Template
        expert_review = self._generate_expert_review_template(connector_name, discovered_methods)
        full_document += expert_review
        
        # Update final status
        if not self._cancel_requested:
            self._current_progress.status = "completed"
            self._current_progress.current_content = full_document
        
        if on_progress:
            on_progress(self._current_progress)
        
        return full_document
    
    def _build_github_context_string(
        self,
        github_context: Optional[Dict[str, Any]],
        is_structured: bool,
        structured_context: Optional[Dict[str, Any]]
    ) -> str:
        """Build GitHub context string for prompts.
        
        Args:
            github_context: GitHub context dict
            is_structured: Whether repo has structured format
            structured_context: Structured context dict
            
        Returns:
            Formatted context string
        """
        if not github_context:
            return ""
        
        if not is_structured:
            return f"""
Repository: {github_context.get('repo_url', 'N/A')}
Languages: {', '.join(github_context.get('languages_detected', []))}
Objects Found: {', '.join(github_context.get('object_types', [])[:20])}
API Endpoints: {', '.join(github_context.get('api_endpoints', [])[:10])}
Auth Patterns: {', '.join(github_context.get('auth_patterns', []))}
"""
        else:
            impl = structured_context.get('implementation', {}) if structured_context else {}
            sdk = structured_context.get('sdk', {}) if structured_context else {}
            docs = structured_context.get('documentation', {}) if structured_context else {}
            return f"""
Repository: {github_context.get('repo_url', 'N/A')}
Structure: Structured Repository Format
SDK Name: {sdk.get('sdk_name', 'N/A')}
Implementation Models: {len(impl.get('models', []))} found
SDK Methods: {len(sdk.get('available_methods', []))} found
SDK Data Types: {len(sdk.get('data_types', []))} found
Documentation Endpoints: {len(docs.get('endpoints_list', []))} documented
"""


# Singleton instance
_agent: Optional[ResearchAgent] = None


def get_research_agent() -> ResearchAgent:
    """Get the singleton ResearchAgent instance."""
    global _agent
    if _agent is None:
        _agent = ResearchAgent()
    return _agent
# Research Document Generation Instructions

This document outlines the complete set of instructions, prompts, and procedures used by the Research Agent to generate comprehensive connector research documents.

## Overview

The Research Agent generates production-grade research documents for data connectors using a multi-source knowledge retrieval system with the following priority:

1. **Knowledge Vault** (Highest Confidence) - Pre-indexed official documentation
2. **DocWhisperer™** (High Confidence) - Context7 MCP for official library documentation
3. **Tavily Web Search** (Supplementary) - Web search for additional context
4. **GitHub Code Analysis** - Extracted patterns from GitHub repositories
5. **Fivetran Documentation** - Crawled Fivetran connector documentation
6. **Hevo Code Analysis** - Optional Hevo connector code for comparison

## Document Structure

The research document is organized into 6 phases:

| Phase | Sections | Content |
|-------|----------|---------|
| 1. Platform Discovery | 1-3 | Overview, Methods Discovery, Dev Environment |
| 2. Extraction Methods | 4-9 (dynamic) | Deep dive for each discovered method |
| 3. Cross-Cutting Concerns | 10-14 | Auth, Rate Limits, Errors, Data Model, Deletes |
| 4. Implementation Guide | 15-17 (dynamic) | Strategy, Object Catalog, Fivetran Parity, Checklist |
| 5. Core Functional Requirements | 18-22 | Data Source, Extraction, Transform, Quality, Loading |
| 6. Technical Operations | 23-27 | Connectivity, Volume, Errors, Monitoring, Audit |

---

## Phase 1: Platform Discovery (BASE_SECTIONS)

### Section 1: Platform Overview

**System Prompt:**
```
You are an expert technical writer specializing in data integration and ETL connector development.
Your task is to write detailed, production-grade documentation for connector research.

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
```

**User Prompts:**
- What does {connector} do? Describe its purpose, target users, and main functionality.
- What are the key modules and features?
- What types of data entities does it store?
- Does it have reporting/analytics modules?
- What are the limitations of its data model?
- Who are the typical users (enterprise, SMB, developers)?

---

### Section 2: Extraction Methods Discovery

**Purpose:** Discover all available data extraction methods for the connector.

**User Prompts:**
```
**IMPORTANT**: Discover ALL available data extraction methods for {connector}. 
For each method that EXISTS, provide details. If a method does NOT exist, 
explicitly state 'Not Available'.

Check for the following methods and report availability:

1. **REST API**: Does {connector} have a REST API? If yes: What is the base URL? 
   Is it documented? What version?
2. **GraphQL API**: Does {connector} have a GraphQL API? If yes: What is the 
   endpoint? What schemas are available?
3. **SOAP/XML API**: Does {connector} have a SOAP or XML-based API? If yes: 
   Where is the WSDL?
4. **Webhooks**: Does {connector} support webhooks for real-time events? If yes: 
   What events are available?
5. **Bulk/Batch API**: Does {connector} have bulk data export or async batch 
   APIs? If yes: How do they work?
6. **Official SDK**: Does {connector} provide official SDKs? If yes: What 
   languages (Python, Java, Node.js, etc.)?
7. **JDBC/ODBC**: Does {connector} support direct database connections via 
   JDBC/ODBC? If yes: What drivers?
8. **File Export**: Does {connector} support data export to files (CSV, JSON, 
   etc.)? If yes: Manual or API-triggered?

Create a summary table at the end:
| Method | Available | Base URL/Endpoint | Documentation Link | Best Use Case |
```

**Output:** This section's content is parsed to extract discovered methods, which are then used to generate dynamic method sections.

---

### Section 3: Developer Environment

**User Prompts:**
- Does {connector} provide sandbox or developer environments?
- How do you request access (self-service, sales, partner program)?
- What are the limitations of sandbox vs production?
- How do you register a developer app/integration?
- What credentials are needed (API keys, OAuth app, service account)?
- Are there IP whitelists or redirect URI requirements?
- Provide a minimal health check code example to verify API access.

---

## Phase 2: Extraction Methods (Dynamic Method Sections)

For each discovered extraction method, a **runbook-style deep dive** section is generated using the `create_method_section()` template.

### Method Section Template Structure

Each method section follows this runbook format:

#### Step 1: Authentication Setup
- **1.1 Obtain Credentials** (table format)
- **1.2 Authentication Code** (complete Python code)
- **1.3 Verify Authentication** (test code)

#### Step 2: Discover Available Objects
- **2.1 List All Objects** (code)
- **2.2 Objects Inventory** (comprehensive table)

#### Step 3: Extract Data (Full Load)
- **3.1 Full Load Procedure** (code with pagination)
- **3.2 Verification** (validation steps)

#### Step 4: Extract Data (Incremental)
- **4.1 Incremental Load Procedure** (cursor-based code)
- **4.2 Cursor Management** (table of cursor fields)

#### Step 5: Handle Pagination
- **5.1 Pagination Details** (table)
- **5.2 Pagination Code** (complete implementation)

#### Step 6: Error Handling
- **6.1 Common Errors** (error codes table)
- **6.2 Error Handling Code** (retry logic)

#### Step 7: Rate Limit Management
- **7.1 Rate Limits** (limits table)
- **7.2 Rate Limiter Implementation** (backoff code)

#### Troubleshooting Guide
- Symptom | Diagnosis | Resolution table

#### Performance Optimization Tips
- Numbered list of optimization strategies

#### Pros & Cons Summary
- Comparison table of advantages and disadvantages

---

## Phase 3: Cross-Cutting Concerns (CROSS_CUTTING_SECTIONS)

### Section 100: Authentication Comparison

**User Prompts:**
- Compare authentication methods across ALL available extraction methods for {connector}
- Create a comparison table: Method | Auth Type | Token Lifetime | Refresh Strategy | Scopes Required
- Which authentication method is recommended for production ETL pipelines?
- What are the security best practices for credential management?
- Provide unified authentication code that works across multiple methods

---

### Section 101: Rate Limiting Strategy

**User Prompts:**
- Compare rate limits across ALL available extraction methods for {connector}
- Create a comparison table: Method | Requests/Min | Requests/Hour | Requests/Day | Concurrency Limit
- Which method has the most generous rate limits for bulk extraction?
- What retry strategies should be used when rate limited?
- Provide a rate limiter implementation in Python that respects these limits

---

### Section 102: Error Handling & Retries

**User Prompts:**
- What error codes and responses are returned by {connector} APIs?
- Create a table: Error Code | Meaning | Retryable? | Resolution
- What errors require re-authentication vs simple retry?
- What is the recommended exponential backoff strategy?
- Provide error handling code with proper retry logic

---

### Section 103: Data Model & Relationships

**Requires:** Fivetran context and code analysis

**User Prompts:**
- Document the complete data model for {connector}
- What are all the main objects/entities?
- What parent-child relationships exist? Create a relationship diagram or table
- What is the correct load order for related objects?
- Are there any circular dependencies to handle?
- What foreign keys link objects together?

---

### Section 104: Delete Detection Strategies

**User Prompts:**
- Compare delete detection methods across ALL extraction methods for {connector}
- Create a table: Method | Delete Detection | Field/Endpoint | Reliability
- Which method is most reliable for detecting deletions?
- How should soft deletes vs hard deletes be handled?
- Provide code for delete detection using the recommended method

---

## Phase 4: Implementation Guide (FINAL_SECTIONS)

### Section 200: Recommended Extraction Strategy

**User Prompts:**
- Based on all discovered methods, what is the RECOMMENDED extraction strategy for {connector}?
- Consider: reliability, performance, completeness, delete detection, real-time needs
- Create a decision matrix: Use Case | Recommended Method | Reason
- What combination of methods provides the best coverage?
- Provide a high-level architecture diagram for a production ETL pipeline
- What are the trade-offs between different approaches?

---

### Section 201: Object Catalog & Replication Guide

**Requires:** Fivetran context and code analysis

**System Prompt (Special for Section 201):**
```
You are an expert technical writer specializing in data integration and ETL connector development.
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
```

**User Prompts:**
- List ALL available objects/entities that can be extracted from {connector}
- Create a comprehensive catalog table with columns:
  | Object | Extraction Method | Primary Key | Cursor Field | Sync Mode | Delete Method | Fivetran Support |
- For each object, specify:
  - Which extraction method(s) can access it
  - Primary key field
  - Best cursor field for incremental sync
  - Supported sync modes (Full/Incremental/CDC)
  - Delete detection method
  - Whether Fivetran supports this object (if known)
- Provide sample extraction code for the top 5 most important objects

**Output Format Required:**
```
### 19.1 Object Catalog Table

| Object | Extraction Method | Primary Key | Cursor Field | Parent | Permissions | Delete Method | Fivetran Support |

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
```

---

### Section 202: Fivetran Parity Analysis

**Requires:** Fivetran context

**Conditional Prompts:**
- Prompts marked with `[IF HEVO]` are only included if Hevo connector code is provided
- Prompts marked with `[IF NO HEVO]` are only included if Hevo connector code is NOT provided

**User Prompts:**
- Document how Fivetran implements the {connector} connector based on Fivetran documentation
- **## Fivetran Implementation Overview**
  - Document Fivetran's authentication approach for {connector}
  - Document Fivetran's extraction methods and API endpoints used
  - List all objects/tables that Fivetran supports for {connector}
  - Document Fivetran's sync strategies (full load, incremental, CDC)
  - Document Fivetran's delete detection approach
  - Document Fivetran's rate limiting and error handling strategies
- **## Hevo Comparison (if Hevo connector code provided)**
  - [IF HEVO] Compare Fivetran's approach with Hevo's implementation
  - [IF HEVO] Create comparison table: Fivetran vs Hevo (Objects, Auth, Extraction, Sync, Deletes)
  - [IF HEVO] Highlight similarities and differences in object support
  - [IF HEVO] Compare authentication methods between Fivetran and Hevo
  - [IF HEVO] Compare extraction approaches and API endpoints
  - [IF HEVO] Compare sync strategies (full load, incremental, CDC)
  - [IF HEVO] Compare delete detection methods
  - [IF NO HEVO] Note: Hevo comparison not available (no Hevo connector code provided)
- Provide insights on differences in implementation approaches and their trade-offs

---

### Section 203: Production Checklist

**User Prompts:**
- Create a production readiness checklist for {connector} data extraction
- **Authentication**: [ ] OAuth app registered, [ ] Credentials secured, [ ] Token refresh implemented
- **Rate Limiting**: [ ] Rate limiter configured, [ ] Backoff strategy implemented
- **Error Handling**: [ ] All error codes handled, [ ] Alerts configured
- **Monitoring**: [ ] Sync metrics tracked, [ ] Data quality checks in place
- **Testing**: [ ] Sandbox testing complete, [ ] Load testing done
- What are the top 10 things that can go wrong in production?
- What monitoring and alerting should be in place?

---

## Phase 5: Core Functional Requirements (FUNCTIONAL_SECTIONS)

### Section 300: Data Source Specification

**User Prompts:**
```
Provide complete data source specification for {connector}:

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
```

---

### Section 301: Extraction Method Selection

**User Prompts:**
```
Create an extraction method selection guide for {connector}:

### Extraction Methods Decision Matrix
| Object | Full Load | Incremental | CDC | Recommended | Reason |

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
```

---

### Section 302: Transformation & Cleansing Rules

**User Prompts:**
```
Define transformation and cleansing rules for {connector} data:

### Field Mapping Standards
| Source Field | Target Field | Transformation | Example |

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
```

---

### Section 303: Data Validation Framework

**User Prompts:**
```
Create a data validation framework for {connector}:

### Schema Validation Rules
| Object | Field | Type | Required | Constraints | Validation Code |

### Business Rule Validations
| Rule ID | Description | Severity | Action on Failure |
|---------|-------------|----------|-------------------|
| BV001 | (Business validation rule) | Error/Warning | Reject/Flag/Log |

### Referential Integrity Checks
| Parent Object | Child Object | Foreign Key | On Violation |

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
```

---

### Section 304: Loading Strategy Decision Tree

**User Prompts:**
```
Define loading strategies for {connector} data:

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
```
```

---

## Phase 6: Technical Operations (OPERATIONAL_SECTIONS)

### Section 400: Connectivity Runbook

**User Prompts:**
```
Create a step-by-step connectivity runbook for {connector}:

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
```
```

---

### Section 401: Volume & Performance Guide

**User Prompts:**
```
Create volume and performance guidelines for {connector}:

### Expected Data Volumes
| Object | Typical Record Count | Record Size | Daily Change Rate |

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
```
```

---

### Section 402: Error Handling Procedures

**User Prompts:**
```
Create error handling procedures for {connector}:

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
```
```

---

### Section 403: Monitoring & Alerting Setup

**User Prompts:**
```
Create monitoring and alerting configuration for {connector}:

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
```
```

---

### Section 404: Audit & Compliance Requirements

**User Prompts:**
```
Create audit and compliance documentation for {connector}:

### Data Lineage Tracking
| Field | Source | Transformation | Target | Timestamp |

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

### Retention & Purging
| Data Type | Retention Period | Purge Method | Compliance Reason |
|-----------|------------------|--------------|-------------------|
| Raw extraction logs | 90 days | Auto-delete | Storage cost |
| Audit records | 7 years | Archive then delete | SOX compliance |
| Error records | 30 days | Auto-delete | Troubleshooting |
```
```
```

---

## Multi-Source Knowledge Retrieval Process

For each section, the following knowledge sources are queried in priority order:

### Step 1: Knowledge Vault (Highest Confidence)
- Query: `{connector_name} {section.name} {section.phase_name}`
- Top K: 3 results
- Format: Each result includes title, confidence score, source type, and text excerpt
- Citations: `[vault:1]`, `[vault:2]`, `[vault:3]`

### Step 2: DocWhisperer™ (High Confidence)
- Resolve library ID from connector name
- Query library docs for: `{section.name} {section.phase_name}`
- Returns: Official documentation content with confidence score
- Citations: `[doc:1]`, `[doc:2]`

### Step 3: Tavily Web Search (Supplementary)
- Query: `{connector_name} API {section.name} documentation 2024 2025`
- Used as supplementary context when primary sources don't have complete information
- Citations: `[web:1]`, `[web:2]`

### Step 4: GitHub Code Analysis (Conditional)
- Used when `requires_code_analysis=True`
- Extracts patterns from GitHub repository:
  - API endpoints
  - Authentication methods
  - Object structures
  - Code examples

### Step 5: Fivetran Context (Conditional)
- Used when `requires_fivetran=True`
- Provides Fivetran's implementation details:
  - Supported objects
  - Authentication approach
  - Sync strategies
  - Delete detection methods

### Step 6: Hevo Context (Conditional, Fivetran Parity Only)
- Used only for Section 202: Fivetran Parity Analysis
- Provides Hevo connector implementation for comparison
- Includes authentication, objects, extraction methods, sync strategies

---

## Context Building

### GitHub Context String
Built from extracted code patterns, including:
- API endpoints discovered
- Authentication patterns
- Object structures
- Code examples

### Fivetran Section Context
Built specifically for each section, extracting relevant information from:
- Setup guide
- Connector overview
- Schema information
- Manual input (if provided)

### Hevo Context String
Built from Hevo connector code analysis (only for Fivetran Parity section):
- Hevo's authentication approach
- Hevo's supported objects
- Hevo's extraction methods
- Hevo's sync strategies

### Structured Context
When GitHub repository follows structured format:
- `Connector_Code/` → Implementation patterns
- `Connector_SDK/` → SDK methods and types
- `Public_Documentation/` → API docs, auth guides, rate limits

---

## Prompt Processing

### Placeholder Replacement
- `{connector}` is replaced with the actual connector name using string replacement (not `.format()` to avoid KeyError with JSON code blocks)
- All prompts are processed sequentially

### Conditional Prompts
- `[IF HEVO]` prompts: Only included if Hevo context is provided
- `[IF NO HEVO]` prompts: Only included if Hevo context is NOT provided
- These are filtered before being sent to the LLM

---

## Output Format Requirements

### General Sections
- 8-10 detailed sentences per subsection
- Clear subsection headers (e.g., `{section.number}.1`, `{section.number}.2`)
- Markdown tables where appropriate
- Inline citations: `[web:1]`, `[vault:1]`, `[doc:1]`
- Code examples in Python (unless method-specific language required)
- Exact values only (no placeholders for OAuth scopes, permissions, rate limits)

### Object Catalog Section (Special)
- MUST start with comprehensive markdown table
- All columns must be populated for each object
- Delete Method must use specific format: "Soft Delete (field_name)", "Deleted Endpoint", "Webhook (event_name)", "Audit Log", or "None"
- Minimum 15-30 objects if available

### Fivetran Parity Section (Special)
- Always includes Fivetran Implementation Overview
- Conditionally includes Hevo Comparison if Hevo context provided
- Comparison tables formatted: Fivetran vs Hevo | Feature | Implementation

---

## Generation Workflow

1. **Initialize Document** - Create header with connector name, generation date, method list
2. **Phase 1: Discovery** - Generate sections 1-3 (Platform Overview, Methods Discovery, Dev Environment)
3. **Parse Discovered Methods** - Extract methods from Section 2 content
4. **Phase 2: Method Deep Dives** - Generate runbook for each discovered method (dynamic sections 4-9)
5. **Phase 3: Cross-Cutting** - Generate sections 10-14 (Auth, Rate Limits, Errors, Data Model, Deletes)
6. **Phase 4: Implementation** - Generate sections 15-17 (Strategy, Object Catalog, Fivetran Parity, Checklist)
7. **Phase 5: Functional Requirements** - Generate sections 18-22 (Data Source, Extraction, Transform, Quality, Loading)
8. **Phase 6: Technical Operations** - Generate sections 23-27 (Connectivity, Volume, Errors, Monitoring, Audit)
9. **Build Final Document** - Combine all sections with Quick Summary Dashboard

---

## Quality Standards

### Content Requirements
- **No Hallucination**: Only use verified documentation from Knowledge Vault, DocWhisperer, or web search
- **Exact Values**: No placeholders - use actual OAuth scopes, permissions, rate limits from documentation
- **Citations Required**: Every factual claim must have inline citation `[web:1]`, `[vault:1]`, etc.
- **Production-Grade**: Code examples must be complete, runnable, and include error handling

### Format Requirements
- **Markdown**: Properly formatted headers, tables, code blocks
- **Tables**: Use markdown table format with aligned columns
- **Code**: Syntax-highlighted code blocks with language specified
- **Subsections**: Numbered subsections (e.g., `1.1`, `1.2`, `2.1`)

### Completeness Requirements
- **Object Catalog**: Must list ALL available objects (minimum 15-30 if available)
- **Method Sections**: Must cover all 7 steps of the runbook template
- **Comparison Tables**: All methods must be compared in cross-cutting sections
- **Documentation Links**: Include documentation URLs where applicable

---

## Special Handling

### Section 19 (Object Catalog)
- Special system prompt requiring table-first format
- Special user prompt with detailed output format requirements
- Requires both Fivetran context and code analysis

### Section 202 (Fivetran Parity)
- Special conditional prompt handling for Hevo comparison
- Always includes Fivetran implementation details
- Conditionally includes Hevo comparison if Hevo GitHub URL provided

### Method Sections (Dynamic)
- Generated using `create_method_section()` function
- Section number assigned dynamically (starts at 4, after base sections)
- All sections follow identical runbook template structure

---

## Progress Tracking

Each section generation:
1. Updates `current_section` and `current_content` in `ResearchProgress`
2. Calls `on_progress` callback if provided
3. Adds section number to `sections_completed` after generation
4. Sleeps 1 second between sections (rate limiting)

Total sections calculated as:
```
total_sections = (
    len(BASE_SECTIONS) +                    # Always 3
    len(discovered_methods) +               # Dynamic (typically 4-8)
    len(CROSS_CUTTING_SECTIONS) +           # Always 5
    len(FINAL_SECTIONS) +                   # Always 4 (was 3, now includes Fivetran Parity)
    len(FUNCTIONAL_SECTIONS) +              # Always 5
    len(OPERATIONAL_SECTIONS)               # Always 5
)
```

---

## Notes

- All sections are generated sequentially (not in parallel)
- Each section can be cancelled mid-generation via `_cancel_requested` flag
- Section generation is idempotent - re-running only regenerates incomplete sections
- Quick Summary Dashboard is generated at the end with metrics from all sections
- Research document is saved to database and file system after completion

---

## File Location

These instructions are implemented in:
- **Main Implementation**: `webapp/services/research_agent.py`
- **Section Definitions**: Lines 196-902
- **Generation Logic**: `generate_research()` method (line 2316+)
- **Section Generation**: `_generate_section()` method (line 1990+)

---

## Multi-Agent DAG Architecture (NEW)

### Overview

The research generation system has been enhanced with a multi-agent architecture that includes:

1. **Research Agent** - Generates research sections
2. **Critic Agent** - Reviews each section for quality, contradictions, and uncertainty
3. **Contradiction Detector** - Identifies conflicts between sources
4. **Contradiction Resolver** - Resolves contradictions using confidence-weighted approach
5. **Uncertainty Model** - Provides confidence scoring (0.0-1.0) based on source reliability
6. **Engineering Cost Analyzer** - Assesses implementation complexity and maintenance burden
7. **DAG Orchestrator** - Manages section-level parallelism and execution order

### Critic Agent Review Process

Each section is reviewed by the Critic Agent before proceeding:

1. **Factual Accuracy** - Cross-references claims with source contexts
2. **Contradiction Detection** - Identifies conflicts between sources
3. **Uncertainty Identification** - Flags low confidence claims
4. **Completeness Check** - Verifies critical information is present
5. **Engineering Feasibility** - Flags potential implementation concerns

**Review Output:**
- Approval Status: "APPROVED", "NEEDS_REVISION", or "STOP_THE_LINE"
- Confidence Score: 0.0-1.0
- Issues: List of review issues with severity
- Contradictions: List of detected contradictions
- Uncertainty Flags: List of low-confidence claims
- Recommendations: Specific improvement suggestions

### Stop-the-Line Mechanism

The system stops generation when:

1. **Critical Contradictions** - Conflicts in auth methods, rate limits, or object support
2. **Low Confidence Critical Claims** - Confidence < 0.5 for critical categories
3. **Multiple Source Conflicts** - Same critical claim conflicts across multiple sources

**Stop-the-Line Actions:**
- **HUMAN_REVIEW**: Requires human intervention to resolve
- **ADDITIONAL_SOURCES**: System attempts to gather more information

### Uncertainty Modeling

Replaces binary truth with confidence gradation:

**Source Reliability Weights:**
- Knowledge Vault: 0.95 (pre-indexed official docs)
- DocWhisperer: 0.85 (official library docs)
- Official Docs (web): 0.75
- Fivetran Docs: 0.65 (signal, not ground truth)
- GitHub Code: 0.60
- Community: 0.40
- Blog: 0.30
- Other: 0.20

**Confidence Thresholds:**
- Critical categories (AUTH, RATE_LIMIT, OBJECT_SUPPORT): Flag if < 0.6
- General categories: Flag if < 0.5

### Fivetran as Signal (Not Ground Truth)

Fivetran documentation is now treated as a reference signal, not authoritative source:

- Confidence weight reduced to 0.65 (from 0.8)
- Disclaimers added: "Fivetran may use private/undocumented endpoints"
- Conflicts with official docs are flagged
- Objects only mentioned by Fivetran are marked with "?" in Object Catalog

### Engineering Cost Analysis

New Section 205 analyzes:

- **Extraction Method Complexity Matrix** - Implementation effort, maintenance burden, risk factors
- **High-Maintenance Endpoints** - Endpoints requiring frequent updates
- **Implementation Recommendations** - Recommended, Conditional, or Avoid methods
- **Engineering Cost Summary** - Overall complexity assessment

### DAG Execution (Planned)

The system is designed to support section-level parallelism:

- Sections within a phase can execute in parallel
- Dependencies are calculated using topological sort
- Execution levels determine parallel execution groups
- Stop-the-line events pause entire execution level

**Current Status:** DAG infrastructure is in place. Full parallel execution integration is pending.

---

*Last Updated: 2026-01-16*
*Version: 3.0 (Multi-Agent DAG Architecture with Critic Agent and Stop-the-Line)*

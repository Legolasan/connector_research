# Connector Research: `<CONNECTOR_NAME>`

**Subject:** `<CONNECTOR_NAME>` Connector - Full Production Research  
**Status:** In Progress  
**Started:** `<DATE>`  
**Last Updated:** `<DATE>`

---

## Research Overview

**Goal:** Produce exhaustive, production-grade research on how to build a connector for `<CONNECTOR_NAME>`.

**Connector Type:** (Select applicable)
- [ ] API-based (REST/GraphQL/SOAP)
- [ ] Driver-based (JDBC/ODBC/ADO.NET)
- [ ] SDK-based (official vendor SDK)
- [ ] Schema-less (NoSQL/document/key-value)
- [ ] File/Drive-based (FTP/SFTP/cloud drives)
- [ ] Object Storage (S3/Azure Blob/GCS)
- [ ] Productivity Tool (Jira/Notion/Asana)
- [ ] Developer Tool (GitHub/GitLab)
- [ ] Messaging/Streaming (Kafka/Pub-Sub/Webhooks)
- [ ] Advertising Platform (Facebook Ads/Google Ads)
- [ ] Warehouse/Datalake (Snowflake/BigQuery)
- [ ] Webhook-only

---

# Phase 1 - Understand the Platform

## 1. Product Overview

### 1.1 What does `<CONNECTOR_NAME>` do?
<!-- 8-10 sentences describing the product, its purpose, and target users -->

### 1.2 Key Modules
<!-- List major modules/features of the platform -->

| Module | Description | Data Available |
|--------|-------------|----------------|
| | | |

### 1.3 Data Domains
<!-- Categories of extractable data -->

| Domain | Entity Types | Notes |
|--------|--------------|-------|
| | | |

### 1.4 Reporting/Analytics Modules
<!-- Yes/No and description -->

### 1.5 Data Model Limitations
<!-- Known constraints or gaps -->

---

## 2. Sandbox / Dev Environments

### 2.1 Sandbox Availability
- **Available:** Yes/No
- **Access Method:** Self-service / Sales / Partner request
- **URL:** 

### 2.2 Sandbox Nature
- **Type:** Permanent / Temporary / Trial
- **Refresh Rules:** 
- **Data Copy Behavior:** 

### 2.3 Sandbox Tier
- **Full vs Limited:** 
- **Restrictions:** 

### 2.4 Alternatives
<!-- If sandbox is paid/limited, list alternatives -->

---

## 3. Required Pre-Call Configurations

### 3.1 Prerequisites Checklist

| Prerequisite | UI Path / Config | Required |
|--------------|------------------|----------|
| | | |

### 3.2 Feature Toggles
<!-- List any features that must be enabled -->

### 3.3 Integration Registration
<!-- Steps to register app/integration -->

### 3.4 Network Configuration
- **IP Whitelist Required:** Yes/No
- **Redirect URIs:** 
- **Domain Differences (Sandbox vs Prod):** 

### 3.5 Pre-flight Health Check (Java)

```java
// REST API Health Check
// TODO: Add minimal REST call example

// JDBC Health Check (if applicable)
// TODO: Add minimal JDBC query example
```

---

# Phase 2 - Data Access Mechanisms

## 4. Data Access Mechanisms

### 4.1 Available Methods

| Access Method | Official Name | Auth Type | Rate Limits | Pros | Cons | Best For |
|---------------|---------------|-----------|-------------|------|------|----------|
| REST API | | | | | | |
| SOAP API | | | | | | |
| Bulk API | | | | | | |
| JDBC/ODBC | | | | | | |
| Webhooks | | | | | | |
| Official SDK | | | | | | |

### 4.2 Recommended Method by Use Case

| Use Case | Recommended Method | Reason |
|----------|-------------------|--------|
| Historical Extraction | | |
| Incremental Sync | | |
| High-Volume Analytics | | |

---

## 5. Authentication Mechanics

### 5.1 Supported Auth Methods

| Method | Official Name | Use Case |
|--------|---------------|----------|
| | | |

### 5.2 OAuth Scopes (Exact)
```
# List exact OAuth scope strings from documentation
```

### 5.3 Required Roles/Permissions (Exact)
<!-- List exact permission names as shown in UI/docs -->

| Permission | Description | Required For |
|------------|-------------|--------------|
| | | |

### 5.4 Java Examples

```java
// REST API Authentication
// TODO: Add complete auth flow example

// JDBC Connection String (if applicable)
// TODO: Add connection string with all properties
```

---

## 6. App Registration & User Consent

### 6.1 App Registration Steps
<!-- Step-by-step UI flow -->

1. 
2. 
3. 

### 6.2 Configuration Settings
- **Callback URLs:** 
- **Secrets/Certificates:** 

### 6.3 Multi-Tenant Consent
- **One App Across Accounts:** Yes/No
- **Per-Account Consent Flow:** 

---

## 7. Metadata Discovery & Schema Introspection

### 7.1 Objects & Modules Catalog
**Applicability:** Yes/No

| Object Name | API Endpoint / Table | Category | Notes |
|-------------|---------------------|----------|-------|
| | | | |

### 7.2 OpenAPI/WSDL Locations
**Applicability:** Yes/No

- **Schema URL:** 
- **Introspection Endpoint:** 

### 7.3 REST Metadata Endpoints
**Applicability:** Yes/No

```http
# Example metadata request
```

### 7.4 JDBC DatabaseMetaData
**Applicability:** Yes/No

```java
// Java example for schema discovery
```

### 7.5 Custom Fields Discovery
**Applicability:** Yes/No

<!-- How to discover custom fields -->

### 7.6 Fivetran Parity (MANDATORY)

| Metric | Fivetran | Our Implementation | Gap |
|--------|----------|-------------------|-----|
| Total Objects | | | |
| Transaction Objects | | | |
| Entity Objects | | | |
| Custom Objects | | | |

**Objects in Fivetran not available via API:**
- 

---

# Phase 3 - Sync Design & Data Extraction Strategy

## 8. Sync Strategies

### 8.1 Object Sync Configuration

| Object | Cursor Field | Why Chosen | Window Strategy | Conflict Handling | Load Modes |
|--------|--------------|------------|-----------------|-------------------|------------|
| | | | | | |

**Load Mode Legend:**
- `FL` = Full Load Only
- `FL+I` = Full Load + Incremental
- `RH` = Reverse Historical
- `CDC` = CDC/Webhook-driven

### 8.2 Historical Sync
- **Method:** 
- **Duration:** 
- **Reverse-Historical Recommended:** Yes/No

---

## 9. Bulk Extraction & Billions of Rows

### 9.1 Bulk/Async APIs
<!-- Document all bulk mechanisms -->

| API | Max Records | Timeout | Format | Notes |
|-----|-------------|---------|--------|-------|
| | | | | |

### 9.2 Pagination Rules
- **Cursor Field:** 
- **Page Size Limits:** 
- **Continuation Token:** 

### 9.3 JDBC Streaming Properties (if applicable)
```properties
# Key JDBC properties for large extracts
fetchSize=
defaultRowPrefetch=
```

---

## 10. Async Capabilities, Job Queues & Webhooks

### 10.1 Async Job Mechanisms
<!-- Bulk jobs, export tasks, report generation -->

| Job Type | Endpoint | Polling | Max Duration |
|----------|----------|---------|--------------|
| | | | |

### 10.2 Webhook Events

| Event Type | Trigger | Payload | Ordering |
|------------|---------|---------|----------|
| | | | |

### 10.3 Webhook for Incremental/Delete Detection
<!-- How to use webhooks -->

---

## 11. Deletion Handling

### 11.1 Delete Representation

| Interface | Hard Delete | Soft Delete | Archive | Audit Log |
|-----------|-------------|-------------|---------|-----------|
| REST | | | | |
| SOAP | | | | |
| JDBC | | | | |

### 11.2 Delete Detection Methods
- **Listing Endpoint:** 
- **Deleted Items Endpoint:** 
- **Webhook Events:** 
- **Snapshot Comparison:** 

---

# Phase 4 - Reliability, Limits, Failures & Performance

## 12. Rate Limits, Quotas & Concurrency

### 12.1 Documented Rate Limits

| Scope | Per Minute | Per Hour | Per Day | Notes |
|-------|------------|----------|---------|-------|
| User | | | | |
| Account | | | | |
| App | | | | |

### 12.2 Concurrency Limits
- **API Calls:** 
- **JDBC Connections:** 
- **Recommended Bulk Concurrency:** 

---

## 13. API Failure Types & Retry Strategy

### 13.1 Error Codes

| Code | Message | Category | Retry Strategy |
|------|---------|----------|----------------|
| | | Retryable | |
| | | Non-Retryable | |
| | | Re-auth Required | |

---

## 14. Timeouts

### 14.1 Timeout Settings

| Setting | Default | Configurable | Notes |
|---------|---------|--------------|-------|
| HTTP Connect | | | |
| HTTP Read | | | |
| API Execution | | | |
| JDBC Socket | | | |

### 14.2 Empirical Limits
<!-- Community-measured limits -->

---

# Phase 5 - Advanced & Vendor-Specific Considerations

## 15. Dependencies, Drivers & SDK Versions

### 15.1 Official SDKs

| Language | Package | Version | Maven/NPM |
|----------|---------|---------|-----------|
| Java | | | |
| Node.js | | | |

### 15.2 JDBC/ODBC Drivers (if applicable)

| Driver | Class Name | Maven | Properties |
|--------|------------|-------|------------|
| | | | |

### 15.3 Version Compatibility Matrix

| API Version | SDK Version | Driver Version | Notes |
|-------------|-------------|----------------|-------|
| | | | |

---

## 16. Operational Test Data & Runbooks

### 16.1 Test Data Generation

| Operation | Steps | Limitations |
|-----------|-------|-------------|
| Historical Data | | |
| Insert New | | |
| Update Existing | | |
| Delete | | |
| Custom Fields | | |

### 16.2 Objects Without Test Data Generation
<!-- List objects that cannot have realistic test data -->

---

## 17. Relationships, Refresher Tasks & Multi-Account

### 17.1 Parent-Child Relationships
**Has Relationships:** Yes/No

| Parent | Child | FK Field | Load Order |
|--------|-------|----------|------------|
| | | | |

### 17.2 API Access Patterns

| Pattern | Endpoints | Dependency Logic | Tables Produced |
|---------|-----------|------------------|-----------------|
| Event Stream | | | |
| Parent ID Required | | | |
| Virtual Parent | | | |
| Nested Extraction | | | |

### 17.3 Refresher Task Requirement
**Required:** Yes/No

| Setting | Value |
|---------|-------|
| Purpose | |
| Frequency | |
| Lookback Window | |
| Objects Requiring Refresher | |

### 17.4 Multi-Account Setup
**Supported:** Yes/No

| Aspect | Details |
|--------|---------|
| Connection Method | |
| Rate Limit Isolation | |
| Scheduling Rules | |

---

# Phase 6 - Common Problems, Errors & Resolutions

## 18. Common Issues & Troubleshooting

### Issue 1: `<ISSUE_NAME>`
- **Error Code/Message:** 
- **Root Cause:** 
- **Resolution:** 
- **Documentation:** 

<!-- Repeat for top 10 issues -->

---

# Final Deliverables

## Production Recommendations
1. 
2. 
3. 
4. 
5. 
6. 
7. 
8. 
9. 
10. 
11. 
12. 

## Implementation Checklist

- [ ] Authentication configured
- [ ] Rate limiting implemented
- [ ] Error handling with retries
- [ ] Incremental sync with cursor fields
- [ ] Delete detection mechanism
- [ ] Custom fields discovery
- [ ] Parent-child load ordering
- [ ] Refresher tasks (if required)
- [ ] Multi-account support (if required)
- [ ] Monitoring and alerting

---

## Sources and Methodology

| Section | Research Method | Sources |
|---------|-----------------|---------|
| 1 | | |
| 2 | | |
| ... | | |
| 18 | | |

**Primary Documentation:**
- 

**Fivetran Reference:**
- 

---

*Document generated by Connector Research Agent v1.0*

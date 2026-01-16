# Agent Instructions: Connector Research Workflow

## Goal
Produce exhaustive, production-grade research documentation for data connectors following the master template.

## Directory Structure
```
research_docs/connectors/
├── _agent/
│   ├── AGENT_INSTRUCTIONS.md    # This file
│   └── research_tracker.json    # Tracks progress per connector
├── _templates/
│   └── connector-research-template.md
├── netsuite/
│   └── netsuite-research.md
├── salesforce/
│   └── salesforce-research.md
└── ... (other connectors)
```

## Workflow

### 1. Setup Phase
1. **Input:** User provides connector name (e.g., "NetSuite", "Salesforce")
2. **Create Directory:** `research_docs/connectors/<connector-name>/`
3. **Initialize File:** Create `<connector-name>-research.md` with header from template
4. **Initialize Tracker:** Add entry to `research_tracker.json`

### 2. Research Process (Per Section)

#### Primary Research Method: context7 MCP
```
1. Use resolve-library-id to find official documentation library
2. Use get-library-docs to fetch specific sections
3. Extract exact values, quotes, and tables from official docs
```

#### Fallback Protocol (If context7 fails)
```
1. STOP immediately
2. Inform user: "context7 MCP did not return sufficient data for Section X"
3. Ask user for:
   - Official documentation root URL
   - Explicit approval to use web search for this section
4. Only proceed with web search after explicit approval
```

#### Fivetran Parity (Mandatory Web Search)
For Section 7 (Fivetran Parity), web search is REQUIRED:
- Search: "Fivetran <connector-name> connector ERD"
- Search: "Fivetran <connector-name> supported objects"
- Document object coverage differences

### 3. Section-by-Section Execution

**CRITICAL: Execute ONE section at a time**

For each section (1-18):
```
1. Announce: "Starting Section X: <Section Name>"
2. Research using context7 MCP or approved fallback
3. Write content meeting these requirements:
   - 8-10 detailed sentences minimum
   - Inline citations [web:X] or [doc:X]
   - Include exact strings (OAuth scopes, permissions, API paths)
   - Markdown tables where applicable
   - Exact cursor field names for sync strategies
4. Present to user for review
5. WAIT for approval
6. After approval, APPEND to <connector>-research.md
7. Update research_tracker.json
8. Announce completion: "Section X complete. Proceed to Section Y? (Y/N)"
```

### 4. Tracking Progress

Update `research_tracker.json` after each section:
```json
{
  "netsuite": {
    "status": "in_progress",
    "current_section": 5,
    "sections_completed": [1, 2, 3, 4],
    "sections_pending": [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
    "research_method": {
      "1": "context7_mcp",
      "2": "context7_mcp",
      "3": "web_search_approved",
      "4": "context7_mcp"
    },
    "sources": [
      "https://docs.oracle.com/en/cloud/saas/netsuite/ns-online-help/..."
    ],
    "last_updated": "2026-01-16T12:00:00Z"
  }
}
```

## Section Reference

| Phase | Section | Name | Key Deliverables |
|-------|---------|------|------------------|
| 1 | 1 | Product Overview | Data domains, modules, limitations |
| 1 | 2 | Sandbox/Dev Environments | Access methods, refresh rules |
| 1 | 3 | Pre-Call Configurations | Prerequisites, health check code |
| 2 | 4 | Data Access Mechanisms | Comparison table, recommendations |
| 2 | 5 | Authentication Mechanics | Auth methods, Java examples |
| 2 | 6 | App Registration | UI flow, multi-tenant consent |
| 2 | 7 | Metadata Discovery | Objects catalog, Fivetran parity |
| 3 | 8 | Sync Strategies | Cursor fields, load modes table |
| 3 | 9 | Bulk Extraction | Pagination, JDBC properties |
| 3 | 10 | Async Capabilities | Job mechanisms, webhooks |
| 3 | 11 | Deletion Handling | Delete detection methods |
| 4 | 12 | Rate Limits | Exact limits, concurrency |
| 4 | 13 | API Failures | Error codes, retry strategy |
| 4 | 14 | Timeouts | Timeout settings, empirical limits |
| 5 | 15 | Dependencies | SDKs, drivers, versions |
| 5 | 16 | Test Data | Runbooks, test generation |
| 5 | 17 | Relationships | Parent-child, refresher tasks |
| 6 | 18 | Troubleshooting | Top 10 issues, resolutions |

## Final Documentation Requirements

After all 18 sections complete:
1. Add "Sources and Methodology" appendix
2. Add "Production Recommendations" (10-12 bullets)
3. Add "Implementation Checklist"
4. Vectorize into Pinecone index

## Constraints

1. **NO hallucination** - Only use verified documentation
2. **Exact values only** - No placeholders for OAuth scopes, permissions, etc.
3. **One section at a time** - Always wait for approval
4. **APPEND only** - Never overwrite the research document
5. **Track sources** - Record which tool/URL was used for each section

## Commands

User can use these commands:
- `start <connector>` - Begin new connector research
- `continue <connector>` - Resume from last section
- `section <N>` - Jump to specific section
- `status` - Show progress for all connectors
- `approve` - Approve current section and continue
- `revise` - Request changes to current section

# Context7 MCP Setup Guide

## What is Context7?

Context7 is an MCP (Model Context Protocol) server that provides access to official library documentation. It allows Claude to fetch authentic, up-to-date documentation directly from official sources.

## Installation

### Option 1: NPX (Recommended)
Add to your Cursor MCP settings (`~/.cursor/mcp.json`):

```json
{
  "mcpServers": {
    "context7": {
      "command": "npx",
      "args": ["-y", "@upstash/context7-mcp"]
    }
  }
}
```

### Option 2: Global Install
```bash
npm install -g @upstash/context7-mcp
```

Then add to MCP settings:
```json
{
  "mcpServers": {
    "context7": {
      "command": "context7-mcp"
    }
  }
}
```

## Available Tools

### 1. resolve-library-id
Finds the library ID for a given technology name.

**Example:**
```
Input: "netsuite suitetalk"
Output: Library ID for NetSuite SuiteTalk documentation
```

### 2. get-library-docs
Fetches documentation for a specific topic from a library.

**Example:**
```
Input: {
  "libraryId": "netsuite-suitetalk",
  "topic": "authentication oauth"
}
Output: Official documentation about OAuth authentication
```

## Usage in Research Workflow

### Step 1: Resolve Library
```
resolve-library-id("netsuite soap api")
```

### Step 2: Fetch Documentation
```
get-library-docs({
  "libraryId": "<resolved_id>",
  "topic": "rate limits concurrency governance"
})
```

### Step 3: Extract Information
- Pull exact values (OAuth scopes, permission names, etc.)
- Note documentation URLs for citations
- Verify information is current (2024-2025)

## Fallback Protocol

If context7 returns insufficient data:

1. **Stop immediately**
2. Inform user with exact message:
   ```
   "Context7 MCP did not return sufficient data for Section X: <Section Name>.
   
   To proceed, I need:
   1. The official documentation URL for this topic
   2. Your explicit approval to use web search
   
   Official NetSuite docs base: https://docs.oracle.com/en/cloud/saas/netsuite/ns-online-help/"
   ```

3. **Wait for user response**
4. Only proceed with web search after explicit approval

## Supported Libraries (Examples)

| Technology | Possible Library IDs |
|------------|---------------------|
| NetSuite SOAP | netsuite-suitetalk, netsuite-soap |
| NetSuite REST | netsuite-rest-api |
| Salesforce | salesforce-api |
| HubSpot | hubspot-api |
| Stripe | stripe-api |
| Snowflake | snowflake-docs |

## Verification Checklist

Before using context7 data in research:

- [ ] Verify the source is official vendor documentation
- [ ] Check documentation date/version is 2024-2025
- [ ] Extract exact strings (don't paraphrase OAuth scopes, permissions)
- [ ] Note the documentation URL for citation
- [ ] Cross-reference with web search if uncertain

## Troubleshooting

### MCP Server Not Starting
1. Check Node.js is installed: `node --version`
2. Verify MCP config syntax in `~/.cursor/mcp.json`
3. Restart Cursor after config changes

### Library Not Found
1. Try alternative library names
2. Fall back to web search with user approval
3. Document the library gap

### Incomplete Documentation
1. Try more specific topic queries
2. Break into smaller sub-queries
3. Fall back to web search for gaps

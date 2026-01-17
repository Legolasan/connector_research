# Connector Research Platform

A modern, enterprise-ready platform for generating comprehensive research documents for data integration connectors. Features multi-source knowledge retrieval, intelligent validation, and full auditability.

## Features

### 🔍 Multi-Source Knowledge Retrieval
- **Knowledge Vault**: Pre-indexed official documentation with highest confidence scoring
- **DocWhisperer™**: Official library documentation via Context7 MCP integration
- **Web Search**: Supplementary context from web search (Tavily)
- **GitHub Analysis**: Code pattern extraction from existing implementations
- **Fivetran Parity**: Comparison with Fivetran's implementation for reference

### 🛡️ Hallucination Prevention & Validation
- **Cite-or-Refuse Validator**: Deterministic pre-Critic validation requiring citations for all factual claims
  - 3-pass parsing (strips code blocks, parses tables separately, sentence-splits prose)
  - Local citation checking (within 250 chars of claim)
  - Strict table row validation (every row must have citations)
  - Known safe statements allowlist (N/A, Unknown, etc.)
- **Evidence Integrity Validator**: Validates citation tags exist in evidence_map and snippets support claims
- **Smart Regeneration**: Up to 3 attempts with failure reports fed back to model
- **Stop-the-Line Mechanism**: Pauses generation on critical contradictions or low-confidence claims

### 📊 Claim Graph Storage & Auditability
- **Structured Claims**: All claims stored with evidence tags, confidence scores, and timestamps
- **Canonical Facts Registry**: Final aggregated facts with contradiction resolution
- **Evidence Map**: Stable SHA256-based evidence IDs for reliable citation tracking
- **Full Audit Trail**: Complete history of claims, sources, and validation decisions

### 🧠 Multi-Agent Review System
- **Critic Agent**: Reviews generated sections for quality, contradictions, and engineering feasibility
- **Contradiction Detector**: Identifies conflicts between sources
- **Uncertainty Model**: Assigns confidence scores and flags low-confidence claims
- **Contradiction Resolver**: Resolves conflicts using confidence-weighted approach
- **Engineering Cost Analyzer**: Assesses implementation complexity and maintenance burden

### 📚 Knowledge Vault Management
- **Bulk PDF Upload**: Support for 500+ PDF documents
- **Pre-Indexing**: Documents indexed into pgvector for fast similarity search
- **Per-Connector Storage**: Organized knowledge base per connector
- **Source Type Tracking**: Official docs, SDK references, ERD schemas, changelogs, etc.

### 🧪 Comprehensive Testing
- **Unit Tests**: Full coverage of validators and claim extraction
- **Integration Tests**: End-to-end research generation flow
- **Hallucination Regression Tests**: 7 scenarios covering common failure modes
  - Missing rate limits
  - Contradicting scopes
  - Fivetran objects not in docs
  - GitHub endpoint mismatches
  - Table rows without citations
  - Citation spam
  - Cross-section inconsistencies

## Architecture

### Core Components

- **Research Agent** (`webapp/services/research_agent.py`): Main orchestration for research generation
- **Citation Validator** (`webapp/services/citation_validator.py`): Pre-Critic citation validation
- **Evidence Integrity Validator** (`webapp/services/evidence_integrity_validator.py`): Citation-to-evidence mapping validation
- **Critic Agent** (`webapp/services/critic_agent.py`): Section quality review
- **Knowledge Vault** (`webapp/services/knowledge_vault.py`): Pre-indexed documentation management
- **DocWhisperer** (`webapp/services/research_agent.py`): Official library documentation retrieval

### Data Flow

```
1. User creates connector → Connector Manager
2. Research generation starts → Research Agent
3. For each section:
   a. Multi-source knowledge retrieval (Vault → DocWhisperer → Web)
   b. Section generation with LLM
   c. Citation validation (3 attempts with regeneration)
   d. Evidence integrity validation
   e. Critic Agent review
   f. Claim extraction and storage
4. Canonical facts aggregation
5. Document saved with full claim graph
```

## Installation

### Prerequisites
- Python 3.11+
- PostgreSQL with pgvector extension (optional, falls back to JSON storage)
- OpenAI API key
- Tavily API key (for web search)
- Railway/PostgreSQL database (or local PostgreSQL)

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd connector_research
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your API keys and database URL
```

Required environment variables:
- `OPENAI_API_KEY`: OpenAI API key for LLM
- `TAVILY_API_KEY`: Tavily API key for web search
- `DATABASE_URL`: PostgreSQL connection string
- `RESEARCH_MODEL`: OpenAI model (default: gpt-4o)

4. Initialize database:
```bash
alembic upgrade head
```

5. Run the application:
```bash
uvicorn webapp.main:app --reload
```

## Usage

### Creating a Connector

1. Navigate to the web interface
2. Click "Create New Connector"
3. Fill in:
   - Connector name
   - Connector type (or "auto" for discovery)
   - GitHub URL (optional, for code analysis)
   - Fivetran URLs (optional, for parity comparison)
   - Hevo GitHub URL (optional, for comparison)
4. Click "Create Connector"

### Pre-Indexing Documentation (Knowledge Vault)

1. Navigate to "Knowledge Vault" section
2. Select connector
3. Upload PDFs or index from URL
4. Documents are automatically indexed into pgvector

### Generating Research

1. Click "Generate Research" on a connector
2. Monitor progress in real-time
3. If citation validation fails:
   - Review missing citations in the intervention modal
   - Choose action: Remove, Rewrite to Unknown, Attach Citation, or Approve as Assumption
   - Apply overrides and resume
4. If stop-the-line is triggered:
   - Review contradictions and uncertainty flags
   - Provide additional context or approve assumptions
   - Resume research generation

### Viewing Research

1. Click "View Research" on a completed connector
2. Navigate through sections using the table of contents
3. Interactive method cards show extraction details
4. Download as Markdown for offline use

## API Endpoints

### Connectors
- `GET /api/connectors` - List all connectors
- `POST /api/connectors` - Create new connector
- `GET /api/connectors/{id}` - Get connector details
- `POST /api/connectors/{id}/generate` - Start research generation
- `GET /api/connectors/{id}/status` - Get generation progress
- `POST /api/connectors/{id}/cancel` - Cancel generation
- `GET /api/connectors/{id}/research` - Get research document

### Citation Validation
- `POST /api/connectors/{id}/citation-report` - Get citation validation report
- `POST /api/connectors/{id}/citation-override` - Apply citation overrides

### Knowledge Vault
- `GET /api/vault/stats` - Get vault statistics
- `GET /api/vault/{connector_name}/stats` - Get connector-specific stats
- `POST /api/vault/index` - Index document from text
- `POST /api/vault/index-url` - Index document from URL
- `POST /api/vault/index-pdf` - Index PDF document
- `POST /api/vault/bulk-upload` - Bulk upload PDFs

### Search
- `POST /api/search` - Search across research documents
- `POST /api/chat` - Chat with research documents

## Testing

Run the full test suite:
```bash
pytest tests/ -v
```

Run specific test files:
```bash
pytest tests/test_citation_validator.py -v
pytest tests/test_evidence_integrity_validator.py -v
pytest tests/test_research_agent_integration.py -v
```

Run with coverage:
```bash
pytest tests/ --cov=webapp/services --cov-report=html
```

## Database Migrations

The project uses Alembic for database migrations:

```bash
# Create a new migration
alembic revision --autogenerate -m "description"

# Apply migrations
alembic upgrade head

# Rollback
alembic downgrade -1
```

## Project Structure

```
connector_research/
├── webapp/
│   ├── main.py                 # FastAPI application
│   ├── services/
│   │   ├── research_agent.py   # Main research generation
│   │   ├── citation_validator.py
│   │   ├── evidence_integrity_validator.py
│   │   ├── critic_agent.py
│   │   ├── knowledge_vault.py
│   │   ├── connector_manager.py
│   │   ├── database.py
│   │   └── ...
│   ├── templates/
│   │   └── index.html          # Main UI
│   └── static/
│       └── app.js              # Frontend logic
├── alembic/
│   └── versions/               # Database migrations
├── tests/
│   ├── fixtures/               # Test scenarios
│   ├── test_citation_validator.py
│   ├── test_evidence_integrity_validator.py
│   └── test_research_agent_integration.py
├── requirements.txt
├── pytest.ini
└── README.md
```

## Key Features in Detail

### Citation Validation

The Cite-or-Refuse Validator ensures all factual claims have proper citations:

- **3-Pass Parsing**: Strips code blocks, extracts tables separately, then sentence-splits prose
- **Local Citation Checking**: Citations must be within 250 characters of the claim
- **Table Row Validation**: Every non-header row must include ≥1 citation tag
- **Known Safe Statements**: Allowlist for "N/A", "Unknown", "This requires runtime verification"
- **Smart Regeneration**: Up to 3 attempts with detailed failure reports

### Evidence Integrity

The Evidence Integrity Validator ensures citations are valid:

- **Citation Tag Validation**: All citation tags must exist in evidence_map
- **Required Fields Check**: Evidence entries must have url, snippet, source_type, timestamp
- **Snippet-Keyword Matching**: Optional lightweight matching to verify snippet supports claim
- **Missing Citation Detection**: Catches citation spam (citations that don't exist)

### Claim Graph Storage

Full auditability through structured claim storage:

- **Structured Claims**: Each claim includes:
  - Claim text and type
  - Evidence tags and stable IDs
  - Confidence score
  - Sources and timestamp
  - Assumption flag (if approved as assumption)
- **Canonical Facts**: Aggregated facts with contradiction resolution
- **Evidence Map**: Stable SHA256-based IDs prevent retry instability

### Human Intervention

When validation fails or stop-the-line is triggered:

- **Citation Intervention Modal**: Review and fix missing citations
- **Actions Available**:
  - Remove: Delete the claim
  - Rewrite to Unknown: Change to "Unknown" or "N/A - not documented"
  - Attach Citation: Link to existing evidence (validated)
  - Approve as Assumption: Renders in dedicated "Assumptions" section
- **Security**: All inputs sanitized, citations validated against evidence_map

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new features
5. Run the test suite
6. Submit a pull request

## License

[Add your license here]

## Support

For issues, questions, or contributions, please open an issue on GitHub.

# Connector Research Platform

A modern, enterprise-ready platform for generating comprehensive research documents for data integration connectors. Features multi-source knowledge retrieval, intelligent validation, parallel processing, and full auditability.

## ✨ Key Features

### 🖥️ Interactive CLI for Research Generation (NEW)
- **Section-by-Section Review**: Review and approve each section before proceeding
- **Refinement Mode**: Provide additional URLs or context to improve sections
- **Real-time Logging**: See detailed progress for each step (crawling, indexing, generation)
- **Flexible Workflow**: Approve, reject, refine, or skip sections as needed
- **Automatic Documentation Crawling**: Uses `llm-crawler` for smart content extraction
- **Rich Terminal UI**: Beautiful formatting with `rich` library for better readability

### 🔍 Multi-Source Knowledge Retrieval
- **Knowledge Vault**: Pre-indexed official documentation with highest confidence scoring
- **DocWhisperer™**: Official library documentation via Context7 MCP integration
- **Official Doc Crawler**: Automated pre-crawling of API documentation with ethical compliance
- **Web Search**: Supplementary context from web search (Tavily)
- **GitHub Analysis**: Code pattern extraction from existing implementations
- **Fivetran Parity**: Comparison with Fivetran's implementation for reference
- **Hevo Comparison**: Optional comparison with Hevo connector implementations

### ⚡ DAG-Based Parallel Research (NEW)
- **Capability-Based Tasks**: Research split by function (search, fetch, summarize, synthesize)
- **Shared Artifact Store**: Redis-backed fact registry prevents redundant work
- **Smart Caching**: Semantic cache keys for web search and LLM responses
- **Convergence Checking**: Early-exit when research has gathered sufficient information
- **Progress Events**: Real-time phase-by-phase progress tracking

### 🌐 Official Documentation Pre-Crawling (NEW)
- **Two-Gate Filtering**: Pattern-based allow/deny + keyword-based ranking
- **llms.txt Priority**: Checks for LLM-optimized content first
- **robots.txt Compliance**: Respects crawler directives
- **Sitemap Support**: Parses sitemap indexes recursively
- **URL Normalization**: Prevents duplicate crawling
- **Ethical Rate Limiting**: Polite delays between requests

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
- **GitHub Repo Indexing**: Auto-indexes Java, Python, JS files from cloned repos
- **Fivetran Doc Indexing**: Auto-indexes crawled Fivetran documentation
- **Pre-Indexing**: Documents indexed into pgvector for fast similarity search
- **Per-Connector Storage**: Organized knowledge base per connector
- **Source Type Tracking**: Official docs, SDK references, ERD schemas, changelogs, etc.

### 🔒 Enterprise Security (NEW)
- **API Key Authentication**: Secure endpoints with API key middleware
- **Rate Limiting**: Configurable limits per endpoint (slowapi)
- **CORS Configuration**: Controlled cross-origin access
- **Input Sanitization**: Protection against HTML, JS, SQL injection
- **Request Size Limits**: Configurable file upload and JSON limits

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

## 🏗️ Architecture

### System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         FastAPI App                              │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │  Connector  │  │  Knowledge  │  │   Research Generation   │  │
│  │   Manager   │  │    Vault    │  │     (DAG-based)         │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│                      Celery Workers                              │
│  ┌──────────┐  ┌──────────┐  ┌───────────┐  ┌───────────────┐  │
│  │  Search  │  │  Fetch   │  │ Summarize │  │  Synthesize   │  │
│  │  Tasks   │  │  Tasks   │  │   Tasks   │  │    Tasks      │  │
│  └──────────┘  └──────────┘  └───────────┘  └───────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                     Redis                                    ││
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────────┐ ││
│  │  │ Artifact │  │  Search  │  │   LLM    │  │  Progress   │ ││
│  │  │  Store   │  │  Cache   │  │  Cache   │  │   Events    │ ││
│  │  └──────────┘  └──────────┘  └──────────┘  └─────────────┘ ││
│  └─────────────────────────────────────────────────────────────┘│
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────────┐│
│  │              PostgreSQL + pgvector                           ││
│  │  ┌──────────┐  ┌──────────┐  ┌──────────────────────────┐  ││
│  │  │Connectors│  │ Research │  │ Document Chunks (vectors)│  ││
│  │  │          │  │   Docs   │  │                          │  ││
│  │  └──────────┘  └──────────┘  └──────────────────────────┘  ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

### Core Components

| Component | File | Purpose |
|-----------|------|---------|
| **Research Agent** | `research_agent.py` | Main orchestration for research generation |
| **DAG Orchestrator** | `research_dag_orchestrator.py` | Capability-based task DAG execution |
| **Artifact Store** | `artifact_store.py` | Redis-backed shared artifact + facts registry |
| **Research Cache** | `cache.py` | Semantic cache with proper keys |
| **Celery Tasks** | `tasks.py` | Capability-based parallel tasks |
| **Convergence Checker** | `convergence.py` | Early-exit detection |
| **Citation Validator** | `citation_validator.py` | Pre-Critic citation validation |
| **Evidence Validator** | `evidence_integrity_validator.py` | Citation-to-evidence mapping |
| **Critic Agent** | `critic_agent.py` | Section quality review |
| **Knowledge Vault** | `knowledge_vault.py` | Pre-indexed documentation management |
| **Doc Crawler** | `doc_crawler.py` | Official documentation pre-crawling |
| **Doc Registry** | `doc_registry.py` | Connector documentation URL registry |

### Data Flow

```
1. User creates connector → Connector Manager
2. Official docs pre-crawled → Doc Crawler → Knowledge Vault
3. Research generation starts → DAG Orchestrator
4. Phase 1: Web Search Tasks (parallel)
   → Results cached → Artifact Store
5. Phase 2: Source Fetch Tasks (parallel)
   → Page content cached → Artifact Store
6. Phase 3: Summarize Tasks (parallel)
   → Facts extracted → Facts Registry
7. Convergence Check
   → If not converged, loop back to Phase 1
8. Phase 4: Synthesis Supervisor
   → Deduplicate claims
   → Resolve conflicts
   → Generate final document
9. For each section:
   a. Citation validation (3 attempts with regeneration)
   b. Evidence integrity validation
   c. Critic Agent review
   d. Claim extraction and storage
10. Document saved with full claim graph
```

## 🚀 Installation

### Prerequisites
- Python 3.11+
- PostgreSQL with pgvector extension
- Redis (for caching and task queue)
- OpenAI API key
- Tavily API key (for web search)

### Quick Start

1. **Clone the repository:**
```bash
git clone https://github.com/Legolasan/connector_research.git
cd connector_research
```

2. **Install dependencies:**
```bash
pip install -r requirements.txt
playwright install chromium  # For JS-rendered page crawling
```

3. **Set up environment variables:**

**Option A: Using `.env` file (Recommended)**
```bash
# Copy the example file
cp .env.example .env

# Edit .env with your actual keys
nano .env  # or use your preferred editor
```

The `.env` file should contain:
```bash
# Required
OPENAI_API_KEY=your-openai-key
TAVILY_API_KEY=your-tavily-key
DATABASE_URL=postgresql://user:pass@host:5432/dbname
REDIS_URL=redis://localhost:6379/0

# Optional
RESEARCH_MODEL=gpt-5-mini-2025-08-07  # Default model
API_KEY=your-api-key  # For API authentication
```

**Option B: Export in terminal**
```bash
# Required
export OPENAI_API_KEY="your-openai-key"
export TAVILY_API_KEY="your-tavily-key"
export DATABASE_URL="postgresql://user:pass@host:5432/dbname"
export REDIS_URL="redis://localhost:6379/0"

# Optional
export RESEARCH_MODEL="gpt-5-mini-2025-08-07"  # Default model
export API_KEY="your-api-key"   # For API authentication
```

**Note:** The application automatically loads `.env` files using `python-dotenv`. Make sure `.env` is in your `.gitignore` to avoid committing secrets!

4. **Initialize database:**
```bash
cd webapp
python migrate.py upgrade
```

5. **Run the application:**
```bash
# Web server
uvicorn webapp.main:app --host 0.0.0.0 --port 8000

# Celery worker (in separate terminal)
cd webapp && celery -A services.celery_app worker --loglevel=info --concurrency=4
```

### Railway Deployment

The project includes Railway-specific configuration:

**Procfile:**
```
release: cd webapp && python migrate.py upgrade
web: playwright install chromium && cd webapp && uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
worker: cd webapp && celery -A services.celery_app worker --loglevel=info --concurrency=4
```

**Required Railway Services:**
- PostgreSQL (with pgvector extension)
- Redis

**Environment Variables to Set:**
- `OPENAI_API_KEY`
- `TAVILY_API_KEY`
- `DATABASE_URL` (auto-set by Railway PostgreSQL)
- `REDIS_URL` (reference from Redis service)

## 📖 Usage

### Creating a Connector (CLI Only)

Connectors are created using the interactive CLI tool. The web UI is for viewing and managing existing connectors only.

**Basic Usage:**
```bash
python scripts/research_cli.py <connector_name> --interactive
```

**With GitHub Repository:**
```bash
python scripts/research_cli.py Shopify --interactive --github-url https://github.com/shopify/shopify-api-ruby
```

**With Custom Documentation URLs:**
```bash
python scripts/research_cli.py Zendesk --interactive --doc-urls https://developer.zendesk.com/api-reference https://developer.zendesk.com/documentation
```

**With Output File:**
```bash
python scripts/research_cli.py Shopify --interactive --output shopify_research.md
```

**Full Options:**
```bash
python scripts/research_cli.py <connector_name> \
    --interactive \
    --connector-type auto \
    --github-url <github_repo_url> \
    --doc-urls <url1> <url2> ... \
    --output <output_file.md>
```

The interactive CLI workflow:

1. **Documentation Crawling & Indexing**
   - Automatically crawls official documentation URLs using `llm-crawler`
   - Indexes content into Knowledge Vault for fast retrieval
   - Falls back to legacy crawler if `llm-crawler` is unavailable

2. **GitHub Repository Analysis** (if provided)
   - Clones and analyzes repository structure
   - Extracts code patterns and implementation details
   - Auto-indexes code into Knowledge Vault

3. **Section-by-Section Generation**
   - Generates each research section sequentially
   - Shows detailed logs for each step (crawling, indexing, generation)
   - Displays the generated content in a formatted panel

4. **Interactive Review**
   After each section is generated, you can:
   - **`y` (approve)**: Accept the section and continue
   - **`n` (reject)**: Regenerate the section
   - **`r` (refine)**: Provide additional URLs or context to improve the section
   - **`s` (skip)**: Skip this section and move to the next

5. **Refinement Mode**
   When choosing `r` (refine):
   - Provide additional documentation URLs to crawl
   - Add custom context or notes
   - The section will be regenerated with the new information

6. **Final Document**
   - All approved sections are combined into a complete research document
   - Saved to the specified output file (or default: `{connector_name}_research.md`)
   - Connector is created and appears in the web UI for viewing

**Example Interactive Session:**
```
🔄 Generating Section 2: Extraction Methods Discovery
  📚 Querying Knowledge Vault...
  ✅ Found 15 relevant chunks
  🔍 Performing targeted web search...
  ✅ Generated section content

[Section 2 content displayed in formatted panel]

Section Review Options:
  y - Approve and continue
  n - Reject and regenerate
  r - Refine (provide additional URLs/context)
  s - Skip this section

Action [y]: r
Provide additional documentation URLs:
URL: https://developer.example.com/api-reference
URL: [Enter to finish]

Additional context or notes: Include GraphQL endpoint details
✅ Regenerated section with additional context
```

After creation, connectors will appear in the web UI for viewing and management.

### Pre-Indexing Documentation (Knowledge Vault)

**Option 1: Automatic Pre-Crawling**
- Official documentation URLs are automatically crawled when creating a connector
- Supports `llms.txt`, sitemaps, and pattern-based filtering

**Option 2: Manual Upload**
1. Navigate to "Knowledge Vault" section
2. Select connector
3. Upload PDFs or paste documentation text
4. Documents are automatically chunked and indexed

### Generating Research (Web UI)

1. Click "Generate Research" on a connector (created via CLI)
2. Monitor real-time progress:
   - Phase indicators (search → fetch → summarize → synthesize)
   - Fact counts by category
   - Convergence status
3. If citation validation fails:
   - Review missing citations in the intervention modal
   - Choose action: Remove, Rewrite to Unknown, Attach Citation, or Approve as Assumption
4. If stop-the-line is triggered:
   - Review contradictions and uncertainty flags
   - Provide additional context or approve assumptions

### Viewing Research

1. Click "View Research" on a completed connector
2. Navigate through sections using the table of contents
3. Interactive method cards show extraction details per method
4. Object catalog with full schema information
5. Download as Markdown for offline use

## 🔌 API Endpoints

### Connectors
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/connectors` | List all connectors |
| POST | `/api/connectors` | Create new connector (CLI can use, UI disabled) |
| POST | `/api/connectors/upload` | Create with file upload (CLI can use, UI disabled) |
| GET | `/api/connectors/{id}` | Get connector details |
| DELETE | `/api/connectors/{id}` | Delete connector |
| POST | `/api/connectors/{id}/generate` | Start research generation |
| GET | `/api/connectors/{id}/status` | Get generation progress |
| GET | `/api/connectors/{id}/progress` | Get DAG progress (phases) |
| POST | `/api/connectors/{id}/cancel` | Cancel generation |
| GET | `/api/connectors/{id}/research` | Get research document |

**Note:** Connector creation endpoints remain functional for CLI use, but the web UI no longer provides a create form. Use the interactive CLI (`scripts/research_cli.py`) to create connectors.

### Citation Validation
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/connectors/{id}/citation-report` | Get validation report |
| POST | `/api/connectors/{id}/citation-override` | Apply citation overrides |

### Knowledge Vault
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/vault/stats` | Get vault statistics |
| GET | `/api/vault/{connector}/stats` | Get connector-specific stats |
| POST | `/api/vault/index` | Index document from text |
| POST | `/api/vault/index-url` | Index document from URL |
| POST | `/api/vault/index-pdf` | Index PDF document |
| POST | `/api/vault/bulk-upload` | Bulk upload PDFs |
| GET | `/api/vault/bulk-progress/{job_id}` | Get bulk upload progress |

### Search
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/search` | Search across research documents |
| POST | `/api/chat` | Chat with research documents |

### Health
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/api/health` | Detailed health status |

## 🧪 Testing

```bash
# Run all tests
pytest tests/ -v

# Run specific test files
pytest tests/test_citation_validator.py -v
pytest tests/test_evidence_integrity_validator.py -v
pytest tests/test_research_agent_integration.py -v

# Run with coverage
pytest tests/ --cov=webapp/services --cov-report=html
```

### Hallucination Regression Scenarios

| Scenario | Expected Behavior |
|----------|-------------------|
| Docs missing rate limit | Mark as "Unknown" |
| Docs contradict scopes | Flag contradiction, use confidence-weighted resolution |
| Fivetran mentions object not in docs | Flag with "?" in Fivetran Support column |
| GitHub uses endpoint docs don't mention | Flag as low confidence |
| Table rows without citations | Auto-fail, regenerate with failure report |
| Citation spam (tags without evidence) | Evidence Integrity Validator fails |
| Inconsistent claims across sections | Contradiction detector flags, stop-the-line if critical |

## 📁 Project Structure

```
connector_research/
├── webapp/
│   ├── main.py                          # FastAPI application
│   ├── migrate.py                       # Database migration script
│   ├── services/
│   │   ├── research_agent.py            # Main research generation
│   │   ├── research_dag_orchestrator.py # DAG-based orchestration
│   │   ├── artifact_store.py            # Redis artifact store
│   │   ├── cache.py                     # Research cache
│   │   ├── tasks.py                     # Celery tasks
│   │   ├── celery_app.py                # Celery configuration
│   │   ├── convergence.py               # Convergence checking
│   │   ├── citation_validator.py        # Citation validation
│   │   ├── evidence_integrity_validator.py
│   │   ├── critic_agent.py              # Section review
│   │   ├── contradiction_detector.py
│   │   ├── contradiction_resolver.py
│   │   ├── uncertainty_model.py
│   │   ├── engineering_cost_analyzer.py
│   │   ├── knowledge_vault.py           # Document indexing
│   │   ├── doc_crawler.py               # Official doc crawler (legacy)
│   │   ├── llm_crawler_service.py       # llm-crawler wrapper (primary)
│   │   ├── doc_registry.py              # Connector doc URLs
│   │   ├── fivetran_crawler.py          # Fivetran doc crawler
│   │   ├── github_cloner.py             # GitHub repo analysis
│   │   ├── connector_manager.py
│   │   ├── database.py
│   │   ├── vector_manager.py
│   │   ├── security.py                  # API security
│   │   └── __init__.py
│   ├── cli/                             # Interactive CLI (NEW)
│   │   ├── __init__.py
│   │   ├── research_interactive.py      # Main interactive workflow
│   │   └── interactive_prompts.py       # User prompts and input
│   ├── templates/
│   │   ├── index.html                   # Main dashboard (view-only)
│   │   ├── base.html
│   │   └── research_view.html           # Research viewer
│   └── static/
│       └── app.js                       # Frontend logic
├── scripts/
│   └── research_cli.py                # CLI entry point (NEW)
├── alembic/
│   ├── env.py
│   └── versions/                        # Database migrations
│       ├── 001_initial_schema.py
│       ├── 002_add_pgvector_embedding.py
│       ├── 003_add_citation_validation.py
│       ├── 004_add_claim_graph_storage.py
│       └── 005_add_hevo_github_url.py
├── tests/
│   ├── conftest.py
│   ├── fixtures/
│   │   └── hallucination_scenarios.py
│   ├── test_citation_validator.py
│   ├── test_evidence_integrity_validator.py
│   └── test_research_agent_integration.py
├── connectors/
│   └── _agent/
│       ├── AGENT_INSTRUCTIONS.md
│       └── connectors_registry.json
├── requirements.txt
├── pytest.ini
├── Procfile
├── alembic.ini
├── .env.example                         # Environment variables template (NEW)
└── README.md
```

## ⚙️ Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `OPENAI_API_KEY` | Yes | - | OpenAI API key |
| `TAVILY_API_KEY` | Yes | - | Tavily search API key |
| `DATABASE_URL` | Yes | - | PostgreSQL connection string |
| `REDIS_URL` | No | `redis://localhost:6379/0` | Redis connection string |
| `RESEARCH_MODEL` | No | `gpt-4o` | OpenAI model for generation |
| `API_KEY` | No | - | API authentication key |
| `MAX_CONTENT_LENGTH` | No | `104857600` | Max upload size (100MB) |

### Doc Crawler Configuration

The doc crawler can be configured per-connector in `doc_registry.py`:

```python
"shopify": ConnectorDocConfig(
    name="Shopify",
    official_docs=[
        "https://shopify.dev/docs/api",
        "https://shopify.dev/docs/api/admin-rest",
    ],
    domain="shopify.dev",
    url_patterns=[
        "/docs/api/*",
        "/docs/api/admin-rest/*",
        "/changelog/*",
    ],
    exclude_patterns=[
        "/docs/api/shipping-partner-platform/*",
        "*/beta/*",
    ]
)
```

## 🔧 Database Migrations

```bash
# Check migration status
python webapp/migrate.py check

# Apply migrations
python webapp/migrate.py upgrade

# Show current version
python webapp/migrate.py current

# Show history
python webapp/migrate.py history

# Rollback (use with caution)
python webapp/migrate.py downgrade -1
```

## 📈 Performance

### Expected Performance with DAG Architecture

| Metric | Before | After |
|--------|--------|-------|
| Web Searches | Sequential | Parallel (6 concurrent) |
| Source Fetching | Sequential | Parallel (10-20 concurrent) |
| Cache Hit Rate | ~20% | ~60-80% |
| Early Exit | Never | When converged |
| **Total Time** | **5-10 min** | **1.5-3 min** |

### Cache TTLs

| Cache Type | TTL | Purpose |
|------------|-----|---------|
| Web Search | 24 hours | Search results |
| LLM Response | 6 hours | Generated content |
| Page Content | 12 hours | Crawled pages |

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests for new features
5. Run the test suite (`pytest tests/ -v`)
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## 📄 License

[Add your license here]

## 🆘 Support

For issues, questions, or contributions, please open an issue on GitHub.

---

**Built with ❤️ for enterprise connector research**

# Connector Research Platform

A multi-connector research platform that auto-generates production-grade documentation for any data connector (Facebook Ads, Google Ads, Zoho, Amazon Ads, etc.).

## Features

- **Multi-Connector Support**: Research any connector type (REST, GraphQL, SOAP, JDBC, SDK, Webhooks)
- **Automated Research**: 18-section research documents generated via web search
- **GitHub Integration**: Clone and analyze existing SDKs for code patterns
- **Per-Connector Indices**: Separate Pinecone vector index per connector
- **RAG Chat**: Chat with your research documents
- **Semantic Search**: Search across all connectors or within specific ones

## Architecture

```
connector_research/
├── connectors/
│   ├── _agent/              # Agent framework and instructions
│   │   ├── AGENT_INSTRUCTIONS.md
│   │   ├── CONTEXT7_SETUP.md
│   │   └── connectors_registry.json
│   ├── _templates/          # Research document templates
│   │   └── connector-research-template.md
│   ├── facebook-ads/        # Per-connector research (auto-created)
│   ├── google-ads/
│   └── ...
├── webapp/
│   ├── main.py              # FastAPI application
│   ├── services/
│   │   ├── connector_manager.py
│   │   ├── github_cloner.py
│   │   ├── research_agent.py
│   │   ├── pinecone_manager.py
│   │   └── ...
│   ├── templates/
│   │   └── index.html
│   └── static/
│       └── app.js
├── requirements.txt
├── Procfile
└── .env.example
```

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/Legolasan/connector_research.git
cd connector_research
```

### 2. Install Dependencies

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Set Up Environment Variables

```bash
cp .env.example .env
# Edit .env with your API keys
```

Required keys:
- `OPENAI_API_KEY` - For embeddings and research generation
- `PINECONE_API_KEY` - For vector storage
- `TAVILY_API_KEY` - For web search

### 4. Run the Application

```bash
cd webapp
uvicorn main:app --reload --port 8000
```

Visit http://localhost:8000

## Usage

### Creating a New Connector

1. Click **"New Connector"** on the Dashboard
2. Enter connector name (e.g., "Facebook Ads")
3. Select connector type (REST API, GraphQL, etc.)
4. Optionally provide GitHub SDK URL for code analysis
5. Click **"Create"** and then **"Start Research"**

### Research Generation

The agent automatically:
1. Clones GitHub repo (if provided)
2. Extracts code patterns (APIs, auth, objects)
3. Generates all 18 research sections via web search
4. Creates a dedicated Pinecone index
5. Makes the research searchable and chat-ready

### 18 Research Sections

| Phase | Sections |
|-------|----------|
| 1. Platform Understanding | Product Overview, Sandbox/Dev, Pre-Call Config |
| 2. Data Access | Access Mechanisms, Authentication, App Registration, Metadata Discovery |
| 3. Sync Design | Sync Strategies, Bulk Extraction, Async/Webhooks, Deletion Handling |
| 4. Reliability | Rate Limits, API Failures, Timeouts |
| 5. Advanced | Dependencies, Test Data, Relationships |
| 6. Troubleshooting | Common Issues |

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/connectors` | GET | List all connectors |
| `/api/connectors` | POST | Create new connector |
| `/api/connectors/{id}` | GET | Get connector details |
| `/api/connectors/{id}` | DELETE | Delete connector |
| `/api/connectors/{id}/generate` | POST | Start research generation |
| `/api/connectors/{id}/status` | GET | Get generation progress |
| `/api/connectors/{id}/cancel` | POST | Cancel generation |
| `/api/connectors/{id}/research` | GET | Get research document |
| `/api/connectors/{id}/search` | POST | Search within connector |
| `/api/connectors/search-all` | POST | Search across all connectors |
| `/api/search` | POST | Legacy search endpoint |
| `/api/chat` | POST | RAG chat endpoint |

## Deployment (Railway)

1. Push to GitHub
2. Connect Railway to the repo
3. Set environment variables in Railway dashboard
4. Deploy

The `Procfile` is configured for Railway deployment.

## Environment Variables

```env
# OpenAI
OPENAI_API_KEY=sk-xxx

# Pinecone
PINECONE_API_KEY=xxx

# Tavily (Web Search)
TAVILY_API_KEY=xxx

# Optional
RESEARCH_MODEL=gpt-4o
EMBEDDING_MODEL=text-embedding-3-small
```

## License

MIT

## Contributing

1. Fork the repo
2. Create a feature branch
3. Submit a PR

---

Built for ETL connector development teams.

"""
Connector Research Platform - FastAPI Web Application

A modern platform for creating, managing, and searching connector research documents.
Supports multi-connector research with per-connector Pinecone indices.
"""

import os
import asyncio
import re
import html
from pathlib import Path
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Request, HTTPException, BackgroundTasks, File, UploadFile, Form, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel
from sqlalchemy import text
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
import json

from services.connector_manager import get_connector_manager, ConnectorManager, ConnectorStatus, FivetranUrls, ManualInput
from services.github_cloner import get_github_cloner, GitHubCloner
from services.research_agent import get_research_agent, ResearchAgent
from services.vector_manager import get_vector_manager, VectorManager
from services.fivetran_crawler import get_fivetran_crawler, FivetranCrawler
from services.knowledge_vault import get_knowledge_vault, KnowledgeVault, KnowledgeSourceType
from services.doc_crawler import get_doc_crawler, DocCrawler
from services.security import verify_api_key, InputSanitizer, get_client_ip


# =====================
# Request/Response Models
# =====================

class FivetranUrlsRequest(BaseModel):
    """Fivetran documentation URLs for parity comparison."""
    setup_guide_url: Optional[str] = None
    connector_overview_url: Optional[str] = None
    schema_info_url: Optional[str] = None


# =====================
# 📚 Knowledge Vault Models
# =====================

class VaultIndexRequest(BaseModel):
    """Request to index documentation into the Knowledge Vault."""
    connector_name: str
    title: str
    content: str
    source_type: str = "official_docs"  # official_docs, sdk_reference, erd_schema, changelog, fivetran_docs, airbyte_docs, custom
    source_url: Optional[str] = None
    version: Optional[str] = None


class VaultIndexFromUrlRequest(BaseModel):
    """Request to index documentation from a URL."""
    connector_name: str
    url: str
    source_type: str = "official_docs"


class VaultSearchRequest(BaseModel):
    """Request to search the Knowledge Vault."""
    connector_name: str
    query: str
    top_k: int = 5


class VaultDocumentResponse(BaseModel):
    """Response for indexed document."""
    id: str
    connector_name: str
    title: str
    source_type: str
    source_url: Optional[str] = None
    chunk_count: int


class VaultSearchResultResponse(BaseModel):
    """Search result from the vault."""
    text: str
    score: float
    source_type: str
    title: str
    connector_name: str


class VaultStatsResponse(BaseModel):
    """Knowledge Vault statistics."""
    available: bool
    connector_count: int = 0
    total_chunks: int = 0
    connectors: List[Dict[str, Any]] = []


class ConnectorCreateRequest(BaseModel):
    name: str
    connector_type: Optional[str] = "auto"  # Auto-discovered during research
    github_url: Optional[str] = None
    hevo_github_url: Optional[str] = None  # Optional Hevo connector GitHub URL for comparison
    official_doc_urls: Optional[List[str]] = None  # User-provided official documentation URLs for pre-crawl
    fivetran_urls: Optional[FivetranUrlsRequest] = None
    description: str = ""
    manual_text: Optional[str] = None  # Manual object list as text


class ConnectorProgressResponse(BaseModel):
    current_section: int
    total_sections: int
    current_phase: int
    sections_completed: List[int]
    percentage: float
    current_section_name: str
    discovered_methods: List[str] = []


class FivetranUrlsResponse(BaseModel):
    """Fivetran URLs in response."""
    setup_guide_url: Optional[str] = None
    connector_overview_url: Optional[str] = None
    schema_info_url: Optional[str] = None


class ConnectorResponse(BaseModel):
    id: str
    name: str
    connector_type: str
    status: str
    github_url: Optional[str]
    hevo_github_url: Optional[str]
    fivetran_urls: Optional[FivetranUrlsResponse]
    description: str
    discovered_methods: List[str] = []  # Auto-discovered extraction methods
    objects_count: int
    vectors_count: int
    fivetran_parity: Optional[float]
    progress: ConnectorProgressResponse
    created_at: str
    updated_at: str
    completed_at: Optional[str]
    pinecone_index: str


class ConnectorListResponse(BaseModel):
    connectors: List[ConnectorResponse]
    total: int


class SearchRequest(BaseModel):
    query: str
    top_k: int = 5
    connector_id: Optional[str] = None


class SearchResultItem(BaseModel):
    id: str
    score: float
    text: str
    section: str
    source_type: str
    connector_name: str


class SearchResponse(BaseModel):
    query: str
    results: List[SearchResultItem]
    total_results: int


class ChatRequest(BaseModel):
    message: str
    connector_id: Optional[str] = None
    top_k: int = 5


class ChatResponse(BaseModel):
    question: str
    answer: str
    sources: List[Dict[str, Any]]


# Services (initialized on startup)
connector_manager: Optional[ConnectorManager] = None
github_cloner: Optional[GitHubCloner] = None
research_agent: Optional[ResearchAgent] = None
vector_manager: Optional[VectorManager] = None
fivetran_crawler: Optional[FivetranCrawler] = None
knowledge_vault: Optional[KnowledgeVault] = None
doc_crawler: Optional[DocCrawler] = None

# Background tasks tracking
_running_research_tasks: Dict[str, asyncio.Task] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize services on startup."""
    global connector_manager, github_cloner, research_agent, vector_manager, fivetran_crawler, knowledge_vault, doc_crawler
    
    # Download NLTK data for sentence tokenization (used by citation validator)
    try:
        import nltk
        nltk.download('punkt_tab', quiet=True)
        nltk.download('punkt', quiet=True)
        print("✓ NLTK data downloaded (punkt tokenizer)")
    except Exception as e:
        print(f"⚠ NLTK data download failed: {e}")
    
    # Initialize database first (if DATABASE_URL is set)
    if os.getenv("DATABASE_URL"):
        try:
            from services.database import init_database
            if init_database():
                print("✓ Database initialized")
            else:
                print("⚠ Database initialization returned False, using file storage")
        except Exception as e:
            print(f"⚠ Database initialization failed: {e}")
    else:
        print("ℹ DATABASE_URL not set, using file-based storage")
    
    # Initialize connector services
    try:
        connector_manager = get_connector_manager()
        print("✓ Connector Manager initialized")
    except Exception as e:
        print(f"⚠ Connector Manager not available: {e}")
        connector_manager = None
    
    try:
        github_cloner = get_github_cloner()
        print("✓ GitHub Cloner initialized")
    except Exception as e:
        print(f"⚠ GitHub Cloner not available: {e}")
        github_cloner = None
    
    try:
        research_agent = get_research_agent()
        print("✓ Research Agent initialized")
    except Exception as e:
        print(f"⚠ Research Agent not available: {e}")
        research_agent = None
    
    try:
        vector_manager = get_vector_manager()
        print("✓ Vector Manager initialized (pgvector)")
    except Exception as e:
        print(f"⚠ Vector Manager not available: {e}")
        vector_manager = None
    
    try:
        fivetran_crawler = get_fivetran_crawler()
        print("✓ Fivetran Crawler initialized")
    except Exception as e:
        print(f"⚠ Fivetran Crawler not available: {e}")
        fivetran_crawler = None
    
    try:
        knowledge_vault = get_knowledge_vault()
        vault_stats = knowledge_vault.get_stats()
        print(f"📚 Knowledge Vault initialized ({vault_stats.get('connector_count', 0)} connectors, {vault_stats.get('total_chunks', 0)} chunks)")
    except Exception as e:
        print(f"⚠ Knowledge Vault not available: {e}")
        knowledge_vault = None
    
    try:
        doc_crawler = get_doc_crawler()
        print("🕷️ Documentation Crawler initialized")
    except Exception as e:
        print(f"⚠ Documentation Crawler not available: {e}")
        doc_crawler = None
    
    yield
    
    # Cancel any running research tasks
    for task in _running_research_tasks.values():
        task.cancel()
    
    print("Shutting down services...")


# Create FastAPI app
app = FastAPI(
    title="Connector Research Platform",
    description="Multi-connector research platform with per-connector Pinecone indices",
    version="1.0.0",
    lifespan=lifespan
)

# =====================
# Security Configuration
# =====================

# Rate Limiting
limiter = Limiter(key_func=get_client_ip)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# CORS Configuration
cors_origins = os.getenv("CORS_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins if "*" not in cors_origins else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-API-Key", "X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset"]
)

# Trusted Host Middleware (optional, for production)
trusted_hosts = os.getenv("TRUSTED_HOSTS")
if trusted_hosts:
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=trusted_hosts.split(",")
    )

# Request Size Limits (configured via middleware)
MAX_UPLOAD_SIZE = int(os.getenv("MAX_UPLOAD_SIZE_MB", "100")) * 1024 * 1024  # Default 100MB
MAX_JSON_SIZE = int(os.getenv("MAX_JSON_SIZE_MB", "10")) * 1024 * 1024  # Default 10MB

# Mount static files
static_path = Path(__file__).parent / "static"
static_path.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

# Setup templates
templates_path = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(templates_path))


# =====================
# Base Routes
# =====================

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Render the main dashboard."""
    return templates.TemplateResponse("index.html", {
        "request": request,
        "title": "Connector Research"
    })


@app.get("/connectors/{connector_id}/view", response_class=HTMLResponse)
async def view_research_page(request: Request, connector_id: str):
    """Render research document as a beautiful HTML page."""
    import markdown
    
    if not connector_manager:
        raise HTTPException(status_code=503, detail="Connector Manager not initialized")
    
    connector = connector_manager.get_connector(connector_id)
    if not connector:
        raise HTTPException(status_code=404, detail=f"Connector '{connector_id}' not found")
    
    # Get research content
    content = connector_manager.get_research_document(connector_id)
    if not content:
        raise HTTPException(status_code=404, detail=f"Research document not found for '{connector_id}'")
    
    # Convert markdown to HTML with proper code block handling
    md = markdown.Markdown(extensions=['tables', 'fenced_code', 'toc', 'nl2br'])
    html_content = md.convert(content)
    
    # Ensure code blocks have proper structure (fenced_code should handle this, but verify)
    # The fenced_code extension generates: <pre><code class="language-{lang}">code</code></pre>
    
    # Extract table of contents from headings
    toc_items = []
    heading_pattern = re.compile(r'^(#{1,3})\s+(.+)$', re.MULTILINE)
    for match in heading_pattern.finditer(content):
        level = len(match.group(1))
        title = match.group(2).strip()
        # Create anchor from title
        anchor = re.sub(r'[^\w\s-]', '', title.lower())
        anchor = re.sub(r'[-\s]+', '-', anchor).strip('-')
        toc_items.append({
            'level': level,
            'title': title,
            'anchor': anchor
        })
    
    return templates.TemplateResponse("research_view.html", {
        "request": request,
        "title": f"{connector.name} Research",
        "connector": connector,
        "content": html_content,
        "raw_content": content,
        "toc_items": toc_items
    })


@app.get("/api/connectors/{connector_id}/download")
@limiter.limit("50/minute")
async def download_research(
    request: Request,
    connector_id: str,
    api_key: str = Depends(verify_api_key)
):
    """Download research document as markdown file."""
    from fastapi.responses import Response
    
    if not connector_manager:
        raise HTTPException(status_code=503, detail="Connector Manager not initialized")
    
    connector = connector_manager.get_connector(connector_id)
    if not connector:
        raise HTTPException(status_code=404, detail=f"Connector '{connector_id}' not found")
    
    content = connector_manager.get_research_document(connector_id)
    if not content:
        raise HTTPException(status_code=404, detail=f"Research document not found")
    
    return Response(
        content=content,
        media_type="text/markdown",
        headers={
            "Content-Disposition": f"attachment; filename={connector_id}-research.md"
        }
    )


@app.get("/health")
async def health_check():
    """Health check endpoint for Railway."""
    from services.database import is_database_available, get_db_session
    
    db_url_set = bool(os.getenv("DATABASE_URL"))
    db_available = is_database_available()
    
    # Try to actually connect
    db_connected = False
    db_error = None
    if db_available:
        try:
            session = get_db_session()
            if session:
                session.execute(text("SELECT 1"))
                session.close()
                db_connected = True
        except Exception as e:
            db_error = str(e)
    
    return {
        "status": "healthy",
        "services": {
            "connector_manager": connector_manager is not None,
            "github_cloner": github_cloner is not None,
            "research_agent": research_agent is not None,
            "vector_manager": vector_manager is not None,
            "fivetran_crawler": fivetran_crawler is not None,
            "knowledge_vault": knowledge_vault is not None
        },
        "database": {
            "url_set": db_url_set,
            "available": db_available,
            "connected": db_connected,
            "error": db_error
        }
    }


# =====================
# Connector API Endpoints
# =====================

def _connector_to_response(connector) -> ConnectorResponse:
    """Convert Connector object to response model."""
    fivetran_urls_response = None
    if connector.fivetran_urls:
        fivetran_urls_response = FivetranUrlsResponse(
            setup_guide_url=connector.fivetran_urls.setup_guide_url,
            connector_overview_url=connector.fivetran_urls.connector_overview_url,
            schema_info_url=connector.fivetran_urls.schema_info_url
        )
    
    return ConnectorResponse(
        id=connector.id,
        name=connector.name,
        connector_type=connector.connector_type,
        status=connector.status,
        github_url=connector.github_url,
        hevo_github_url=connector.hevo_github_url,
        fivetran_urls=fivetran_urls_response,
        description=connector.description,
        discovered_methods=connector.discovered_methods or [],
        objects_count=connector.objects_count,
        vectors_count=connector.vectors_count,
        fivetran_parity=connector.fivetran_parity,
        progress=ConnectorProgressResponse(
            current_section=connector.progress.current_section,
            total_sections=connector.progress.total_sections,
            current_phase=connector.progress.current_phase,
            sections_completed=connector.progress.sections_completed,
            percentage=connector.progress.percentage,
            current_section_name=connector.progress.current_section_name,
            discovered_methods=connector.progress.discovered_methods if hasattr(connector.progress, 'discovered_methods') else []
        ),
        created_at=connector.created_at,
        updated_at=connector.updated_at,
        completed_at=connector.completed_at,
        pinecone_index=connector.pinecone_index
    )


@app.get("/api/connectors", response_model=ConnectorListResponse)
async def list_connectors():
    """List all connector research projects."""
    if not connector_manager:
        raise HTTPException(status_code=503, detail="Connector Manager not initialized")
    
    connectors = connector_manager.list_connectors()
    return ConnectorListResponse(
        connectors=[_connector_to_response(c) for c in connectors],
        total=len(connectors)
    )


@app.post("/api/connectors", response_model=ConnectorResponse)
@limiter.limit("20/minute")
async def create_connector(
    request: Request,
    connector_request: ConnectorCreateRequest,
    api_key: str = Depends(verify_api_key)
):
    """Create a new connector research project."""
    if not connector_manager:
        raise HTTPException(status_code=503, detail="Connector Manager not initialized")
    
    try:
        # Convert FivetranUrlsRequest to FivetranUrls if provided
        fivetran_urls = None
        if connector_request.fivetran_urls:
            fivetran_urls = FivetranUrls(
                setup_guide_url=connector_request.fivetran_urls.setup_guide_url,
                connector_overview_url=connector_request.fivetran_urls.connector_overview_url,
                schema_info_url=connector_request.fivetran_urls.schema_info_url
            )
        
        connector = connector_manager.create_connector(
            name=connector_request.name,
            connector_type=connector_request.connector_type,
            github_url=connector_request.github_url,
            hevo_github_url=connector_request.hevo_github_url,
            official_doc_urls=connector_request.official_doc_urls,
            fivetran_urls=fivetran_urls,
            description=connector_request.description,
            manual_text=connector_request.manual_text
        )
        return _connector_to_response(connector)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/connectors/upload", response_model=ConnectorResponse)
@limiter.limit("10/minute")
async def create_connector_with_file(
    request: Request,
    name: str = Form(...),
    connector_type: Optional[str] = Form("auto"),  # Auto-discovered during research
    github_url: Optional[str] = Form(None),
    hevo_github_url: Optional[str] = Form(None),
    official_doc_urls: Optional[str] = Form(None),  # JSON string of URLs
    fivetran_urls: Optional[str] = Form(None),  # JSON string
    manual_text: Optional[str] = Form(None),
    manual_file: Optional[UploadFile] = File(None),
    api_key: str = Depends(verify_api_key)
):
    """Create a new connector research project with optional file upload."""
    if not connector_manager:
        raise HTTPException(status_code=503, detail="Connector Manager not initialized")
    
    try:
        # Parse fivetran_urls from JSON string
        fivetran_urls_obj = None
        if fivetran_urls:
            try:
                urls_dict = json.loads(fivetran_urls)
                fivetran_urls_obj = FivetranUrls(
                    setup_guide_url=urls_dict.get('setup_guide_url'),
                    connector_overview_url=urls_dict.get('connector_overview_url'),
                    schema_info_url=urls_dict.get('schema_info_url')
                )
            except json.JSONDecodeError:
                pass
        
        # Parse official_doc_urls from JSON string
        official_doc_urls_list = None
        if official_doc_urls:
            try:
                official_doc_urls_list = json.loads(official_doc_urls)
            except json.JSONDecodeError:
                pass
        
        # Read file content if provided
        manual_file_content = None
        manual_file_type = None
        if manual_file and manual_file.filename:
            content = await manual_file.read()
            filename_lower = manual_file.filename.lower()
            
            if filename_lower.endswith('.csv'):
                manual_file_content = content.decode('utf-8')
                manual_file_type = 'csv'
            elif filename_lower.endswith('.pdf'):
                manual_file_content = content  # Keep as bytes for PDF
                manual_file_type = 'pdf'
            else:
                raise HTTPException(status_code=400, detail="Only CSV and PDF files are supported")
        
        connector = connector_manager.create_connector(
            name=name,
            connector_type=connector_type,
            github_url=github_url,
            hevo_github_url=hevo_github_url,
            official_doc_urls=official_doc_urls_list,
            fivetran_urls=fivetran_urls_obj,
            description="",
            manual_text=manual_text,
            manual_file_content=manual_file_content,
            manual_file_type=manual_file_type
        )
        return _connector_to_response(connector)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/connectors/{connector_id}", response_model=ConnectorResponse)
@limiter.limit("200/minute")
async def get_connector(
    request: Request,
    connector_id: str,
    api_key: str = Depends(verify_api_key)
):
    """Get a specific connector by ID."""
    if not connector_manager:
        raise HTTPException(status_code=503, detail="Connector Manager not initialized")
    
    connector = connector_manager.get_connector(connector_id)
    if not connector:
        raise HTTPException(status_code=404, detail=f"Connector '{connector_id}' not found")
    
    return _connector_to_response(connector)


@app.delete("/api/connectors/{connector_id}")
@limiter.limit("20/minute")
async def delete_connector(
    request: Request,
    connector_id: str,
    api_key: str = Depends(verify_api_key)
):
    """Delete a connector research project."""
    if not connector_manager:
        raise HTTPException(status_code=503, detail="Connector Manager not initialized")
    
    # Cancel any running research
    if connector_id in _running_research_tasks:
        _running_research_tasks[connector_id].cancel()
        del _running_research_tasks[connector_id]
    
    success = connector_manager.delete_connector(connector_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"Connector '{connector_id}' not found")
    
    # Optionally delete Pinecone index
    if vector_manager:
        vector_manager.delete_index(connector_id)
    
    return {"message": f"Connector '{connector_id}' deleted"}


@app.post("/api/connectors/{connector_id}/generate")
@limiter.limit("5/minute")
async def generate_research(
    request: Request,
    connector_id: str,
    background_tasks: BackgroundTasks,
    api_key: str = Depends(verify_api_key)
):
    """Start research generation for a connector (runs in background)."""
    if not connector_manager:
        raise HTTPException(status_code=503, detail="Connector Manager not initialized")
    if not research_agent:
        raise HTTPException(status_code=503, detail="Research Agent not initialized")
    
    connector = connector_manager.get_connector(connector_id)
    if not connector:
        raise HTTPException(status_code=404, detail=f"Connector '{connector_id}' not found")
    
    # Check if already running
    if connector_id in _running_research_tasks:
        return {"message": "Research generation already in progress", "status": "running"}
    
    # Update status
    connector_manager.update_connector(connector_id, status=ConnectorStatus.RESEARCHING.value)
    
    async def run_research():
        """Background task to run research generation."""
        try:
            github_context = None
            hevo_context = None
            fivetran_context = None
            
            # Clone GitHub repo if URL provided
            if connector.github_url and github_cloner:
                connector_manager.update_connector(connector_id, status=ConnectorStatus.CLONING.value)
                extracted = await github_cloner.clone_and_extract(connector.github_url, connector_id)
                if extracted:
                    github_context = extracted.to_dict()
                    
                    # Auto-index GitHub code into Knowledge Vault for research context
                    if knowledge_vault and extracted.clone_path:
                        try:
                            print(f"📦 Auto-indexing GitHub repo into Knowledge Vault...")
                            index_stats = knowledge_vault.index_github_repo(
                                connector_name=connector.name,
                                repo_path=extracted.clone_path,
                                source_type="github_implementation"
                            )
                            print(f"  ✓ Indexed {index_stats.get('files_indexed', 0)} files into Knowledge Vault")
                        except Exception as e:
                            print(f"  ⚠ Failed to index GitHub repo to Knowledge Vault: {e}")
                else:
                    print(f"⚠ GitHub cloning skipped for {connector.name}, continuing with web search only")
            
            # Clone Hevo repository if Hevo GitHub URL provided
            if connector.hevo_github_url and github_cloner:
                print(f"Cloning Hevo repository for comparison: {connector.hevo_github_url}")
                hevo_extracted = await github_cloner.clone_and_extract(connector.hevo_github_url, f"{connector_id}-hevo")
                if hevo_extracted:
                    hevo_context = hevo_extracted.to_dict()
                    print(f"✓ Hevo repository analyzed successfully")
                else:
                    print(f"⚠ Hevo repository cloning skipped, continuing without Hevo comparison")
            
            # Pre-crawl official documentation and index into Knowledge Vault
            if doc_crawler and knowledge_vault:
                try:
                    # Get user-provided URLs or use registry/auto-discovery
                    user_doc_urls = getattr(connector, 'official_doc_urls', None)
                    
                    print(f"📚 Pre-crawling official documentation for {connector.name}...")
                    connector_manager.update_connector(
                        connector_id, 
                        status="crawling_docs"
                    )
                    
                    crawl_result = await doc_crawler.crawl_official_docs(
                        connector_name=connector.name,
                        user_provided_urls=user_doc_urls,
                        max_depth=2  # Medium depth: 2-3 levels
                    )
                    
                    if crawl_result.total_content:
                        # Index crawled content into Knowledge Vault
                        knowledge_vault.index_text(
                            connector_name=connector.name,
                            title=f"Official {connector.name} API Documentation",
                            content=crawl_result.total_content,
                            source_type="official_docs"
                        )
                        
                        # Update connector with crawl stats
                        connector_manager.update_connector(
                            connector_id,
                            doc_crawl_status="indexed",
                            doc_crawl_urls=crawl_result.urls_crawled,
                            doc_crawl_pages=len(crawl_result.pages),
                            doc_crawl_words=crawl_result.total_words
                        )
                        
                        print(f"  ✓ Pre-crawled {len(crawl_result.pages)} pages, {crawl_result.total_words} words indexed")
                    else:
                        connector_manager.update_connector(connector_id, doc_crawl_status="no_content")
                        print(f"  ⚠ No documentation content found for pre-crawl")
                        
                except Exception as e:
                    connector_manager.update_connector(connector_id, doc_crawl_status="failed")
                    print(f"  ⚠ Documentation pre-crawl failed: {e}")
            
            # Crawl Fivetran documentation if URLs provided, or use manual input
            has_fivetran_urls = connector.fivetran_urls and connector.fivetran_urls.has_urls()
            has_manual_input = connector.manual_input and connector.manual_input.has_input()
            
            if (has_fivetran_urls or has_manual_input) and fivetran_crawler:
                print(f"Processing Fivetran/manual input for {connector.name}...")
                
                # Prepare manual input data
                manual_csv = None
                manual_pdf_bytes = None
                manual_text = None
                
                if has_manual_input:
                    mi = connector.manual_input
                    if mi.text:
                        manual_text = mi.text
                    if mi.file_content and mi.file_type:
                        if mi.file_type == 'csv':
                            manual_csv = mi.file_content
                        elif mi.file_type == 'pdf':
                            # Decode from base64
                            import base64
                            manual_pdf_bytes = base64.b64decode(mi.file_content)
                
                fivetran_result = await fivetran_crawler.crawl_all(
                    setup_url=connector.fivetran_urls.setup_guide_url if has_fivetran_urls else None,
                    overview_url=connector.fivetran_urls.connector_overview_url if has_fivetran_urls else None,
                    schema_url=connector.fivetran_urls.schema_info_url if has_fivetran_urls else None,
                    manual_csv=manual_csv,
                    manual_pdf_bytes=manual_pdf_bytes,
                    manual_text=manual_text
                )
                fivetran_context = fivetran_result.to_dict()
                
                # Auto-index Fivetran docs into Knowledge Vault
                if knowledge_vault and fivetran_result:
                    try:
                        print(f"📚 Auto-indexing Fivetran docs into Knowledge Vault...")
                        
                        # Combine content from all Fivetran sections
                        combined_content = ""
                        
                        if fivetran_result.setup:
                            combined_content += f"## Fivetran Setup Guide\n\n{fivetran_result.setup.raw_content}\n\n"
                        if fivetran_result.overview:
                            combined_content += f"## Fivetran Connector Overview\n\n{fivetran_result.overview.raw_content}\n\n"
                        if fivetran_result.schema:
                            combined_content += f"## Fivetran Schema Information\n\n{fivetran_result.schema.raw_content}\n\n"
                        
                        if combined_content.strip():
                            knowledge_vault.index_text(
                                connector_name=connector.name,
                                title=f"Fivetran {connector.name} Documentation",
                                content=combined_content,
                                source_type="fivetran_docs"
                            )
                            print(f"  ✓ Indexed Fivetran docs into Knowledge Vault")
                        else:
                            print(f"  ⚠ No Fivetran content to index")
                    except Exception as e:
                        print(f"  ⚠ Failed to index Fivetran docs to Knowledge Vault: {e}")
            
            # Update status to researching
            connector_manager.update_connector(connector_id, status=ConnectorStatus.RESEARCHING.value)
            
            # Generate research
            def on_progress(progress):
                # Extract section name from current_content
                # Format is usually "Generating Section X: Section Name..."
                section_name = ""
                if progress.current_content:
                    # Try to extract section name from the format
                    match = re.search(r'Section \d+: (.+?)(?:\.\.\.|$)', progress.current_content)
                    if match:
                        section_name = match.group(1).strip()
                    else:
                        # Fallback to first 50 chars
                        section_name = progress.current_content[:50].strip()
                
                connector_manager.update_progress(
                    connector_id,
                    section=progress.current_section,
                    section_name=section_name,
                    completed=(progress.current_section in progress.sections_completed),
                    total_sections=progress.total_sections,
                    discovered_methods=progress.discovered_methods if hasattr(progress, 'discovered_methods') else None,
                    research_progress=progress  # Pass full ResearchProgress object for new fields
                )
            
            research_content = await research_agent.generate_research(
                connector_id=connector_id,
                connector_name=connector.name,
                connector_type=connector.connector_type,
                github_context=github_context,
                hevo_context=hevo_context,
                fivetran_context=fivetran_context,
                on_progress=on_progress
            )
            
            # Save research document with claim graph data
            progress = research_agent.get_progress()
            connector_manager.save_research_document(
                connector_id=connector_id,
                content=research_content,
                claims_json=progress.claims_json if progress else None,
                canonical_facts_json=progress.canonical_facts_json if progress else None,
                evidence_map_json=progress.evidence_map_json if progress else None,
                validation_attempts=len([e for e in (progress.stop_the_line_events or []) if 'citation' in str(e).lower()]) if progress else None
            )
            
            # Vectorize into Pinecone
            vectors_count = 0
            if vector_manager:
                vectors_count = vector_manager.vectorize_research(
                    connector_id=connector_id,
                    connector_name=connector.name,
                    research_content=research_content
                )
            
            # Ensure final progress is persisted (mark all sections complete)
            if progress and progress.total_sections > 0:
                # Mark all sections as completed
                for section_num in range(1, progress.total_sections + 1):
                    if section_num not in progress.sections_completed:
                        progress.sections_completed.append(section_num)
                progress.sections_completed.sort()
                
                # Update progress with final state
                connector_manager.update_progress(
                    connector_id,
                    section=progress.total_sections,
                    section_name="Complete",
                    completed=True,
                    total_sections=progress.total_sections,
                    discovered_methods=progress.discovered_methods,
                    research_progress=progress
                )
            
            # Update connector with final stats
            connector_manager.update_connector(
                connector_id,
                status=ConnectorStatus.COMPLETE.value,
                vectors_count=vectors_count
            )
            
        except asyncio.CancelledError:
            connector_manager.update_connector(connector_id, status=ConnectorStatus.CANCELLED.value)
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            print(f"Research generation failed: {e}")
            print(f"Full traceback:\n{error_details}")
            connector_manager.update_connector(connector_id, status=ConnectorStatus.FAILED.value)
        finally:
            if connector_id in _running_research_tasks:
                del _running_research_tasks[connector_id]
    
    # Start background task
    task = asyncio.create_task(run_research())
    _running_research_tasks[connector_id] = task
    
    return {"message": "Research generation started", "status": "started", "connector_id": connector_id}


@app.get("/api/connectors/{connector_id}/status")
@limiter.limit("100/minute")
async def get_research_status(
    request: Request,
    connector_id: str,
    api_key: str = Depends(verify_api_key)
):
    """Get research generation status for a connector."""
    if not connector_manager:
        raise HTTPException(status_code=503, detail="Connector Manager not initialized")
    
    connector = connector_manager.get_connector(connector_id)
    if not connector:
        raise HTTPException(status_code=404, detail=f"Connector '{connector_id}' not found")
    
    is_running = connector_id in _running_research_tasks
    
    # Convert progress to dict, handling new fields
    progress_dict = {
        "current_section": connector.progress.current_section,
        "total_sections": connector.progress.total_sections,
        "sections_completed": connector.progress.sections_completed,
        "percentage": connector.progress.percentage,
        "current_section_name": connector.progress.current_section_name,
        "discovered_methods": connector.progress.discovered_methods,
        "overall_confidence": getattr(connector.progress, 'overall_confidence', 0.0),
        "stop_the_line_events": getattr(connector.progress, 'stop_the_line_events', []),
        "contradictions": getattr(connector.progress, 'contradictions', []),
        "section_reviews": getattr(connector.progress, 'section_reviews', {})
    }
    
    return {
        "connector_id": connector_id,
        "status": connector.status,
        "is_running": is_running,
        "progress": progress_dict
    }


@app.get("/api/connectors/{connector_id}/progress")
@limiter.limit("100/minute")
async def get_research_progress_dag(
    request: Request,
    connector_id: str,
    api_key: str = Depends(verify_api_key)
):
    """
    Get structured progress of DAG-based research generation.
    
    Returns detailed progress including:
    - Phase-by-phase progress (web_search, fetch, summarize, synthesis)
    - Recent events
    - Convergence status
    - Fact counts by category
    """
    try:
        from services.research_dag_orchestrator import get_research_progress
        
        progress = get_research_progress(connector_id)
        return progress
        
    except ImportError:
        # Fallback if DAG system not available
        return {
            "status": "not_available",
            "message": "DAG-based research not configured",
            "progress": 0
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "progress": 0
        }


@app.post("/api/connectors/{connector_id}/cancel")
@limiter.limit("20/minute")
async def cancel_research(
    request: Request,
    connector_id: str,
    api_key: str = Depends(verify_api_key)
):
    """Cancel research generation for a connector."""
    if connector_id not in _running_research_tasks:
        raise HTTPException(status_code=400, detail="No research generation running for this connector")
    
    _running_research_tasks[connector_id].cancel()
    
    if connector_manager:
        connector_manager.update_connector(connector_id, status=ConnectorStatus.CANCELLED.value)
    
    return {"message": "Research generation cancelled", "connector_id": connector_id}


@app.get("/api/connectors/{connector_id}/research")
@limiter.limit("100/minute")
async def get_research_document(
    request: Request,
    connector_id: str,
    api_key: str = Depends(verify_api_key)
):
    """Get the research document content for a connector."""
    if not connector_manager:
        raise HTTPException(status_code=503, detail="Connector Manager not initialized")
    
    # Sanitize connector_id
    connector_id = InputSanitizer.sanitize_connector_id(connector_id)
    
    content = connector_manager.get_research_document(connector_id)
    if content is None:
        raise HTTPException(status_code=404, detail=f"Research document not found for '{connector_id}'")
    
    return {"connector_id": connector_id, "content": content}


# =====================
# Citation Validation API Endpoints
# =====================

class CitationOverrideRequest(BaseModel):
    """Request to override citation validation decisions."""
    overrides: List[Dict[str, Any]]  # List of override actions


@app.post("/api/connectors/{connector_id}/citation-report")
@limiter.limit("50/minute")
async def get_citation_report(
    request: Request,
    connector_id: str,
    api_key: str = Depends(verify_api_key)
):
    """
    Get citation validation report for a connector.
    Exports missing citations to JSON for human review.
    """
    if not connector_manager:
        raise HTTPException(status_code=503, detail="Connector Manager not initialized")
    
    connector = connector_manager.get_connector(connector_id)
    if not connector:
        raise HTTPException(status_code=404, detail=f"Connector '{connector_id}' not found")
    
    # Get research document from database
    from services.database import get_db_session, ResearchDocumentModel
    
    session = get_db_session()
    if not session:
        raise HTTPException(status_code=503, detail="Database not available")
    
    try:
        doc = session.query(ResearchDocumentModel).filter(
            ResearchDocumentModel.connector_id == connector_id
        ).first()
        
        if not doc:
            raise HTTPException(status_code=404, detail=f"Research document not found for '{connector_id}'")
        
        citation_report = doc.citation_report_json
        
        if not citation_report:
            # Generate report from current progress if available
            research_agent = get_research_agent()
            progress = research_agent.get_progress() if research_agent else None
            
            if progress and hasattr(progress, 'stop_the_line_events'):
                # Build report from stop-the-line events
                citation_report = {
                    "connector_id": connector_id,
                    "status": "stopped" if progress.status == "stopped" else "in_progress",
                    "uncited_claims": [],
                    "uncited_table_rows": [],
                    "validation_attempts": 3,
                    "generated_at": datetime.utcnow().isoformat()
                }
            else:
                citation_report = {
                    "connector_id": connector_id,
                    "status": "no_report",
                    "message": "No citation validation issues found"
                }
        
        return {
            "connector_id": connector_id,
            "report": citation_report,
            "report_id": f"{connector_id}_citation_report"
        }
    finally:
        session.close()


@app.post("/api/connectors/{connector_id}/citation-override")
@limiter.limit("20/minute")
async def citation_override(
    http_request: Request,
    connector_id: str,
    request: CitationOverrideRequest,
    api_key: str = Depends(verify_api_key)
):
    """
    Apply citation overrides and resume research generation.
    
    Security validation:
    - Sanitize all user inputs
    - Validate citation tags exist in evidence_map
    - Check evidence entry has url + snippet + source_type
    - Optional: Lightweight snippet-keyword matching
    """
    from services.database import get_db_session, ResearchDocumentModel
    from services.evidence_integrity_validator import EvidenceIntegrityValidator
    
    if not connector_manager:
        raise HTTPException(status_code=503, detail="Connector Manager not initialized")
    
    connector = connector_manager.get_connector(connector_id)
    if not connector:
        raise HTTPException(status_code=404, detail=f"Connector '{connector_id}' not found")
    
    session = get_db_session()
    if not session:
        raise HTTPException(status_code=503, detail="Database not available")
    
    try:
        doc = session.query(ResearchDocumentModel).filter(
            ResearchDocumentModel.connector_id == connector_id
        ).first()
        
        if not doc:
            raise HTTPException(status_code=404, detail=f"Research document not found for '{connector_id}'")
        
        evidence_map = doc.evidence_map_json or {}
        integrity_validator = EvidenceIntegrityValidator(enable_snippet_matching=True)
        
        # Validate and sanitize overrides
        validated_overrides = []
        for override in request.overrides:
            claim_id = html.escape(str(override.get("claim_id", "")))
            action = override.get("action")
            
            if action not in ["remove", "rewrite_to_unknown", "attach_citation", "approve_as_assumption"]:
                raise HTTPException(status_code=400, detail=f"Invalid action: {action}")
            
            validated_override = {
                "claim_id": claim_id,
                "action": action
            }
            
            # Validate citation if attaching
            if action == "attach_citation":
                citation = override.get("citation", "")
                evidence_id = override.get("evidence_id", "")
                
                # Sanitize citation
                citation = html.escape(citation)
                
                # Extract citation tag (e.g., "web:1" from "[web:1]")
                citation_match = re.match(r'\[([^\]]+)\]', citation)
                if not citation_match:
                    raise HTTPException(status_code=400, detail=f"Invalid citation format: {citation}")
                
                citation_tag = citation_match.group(1)
                
                # Validate citation exists in evidence_map
                if citation_tag not in evidence_map:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Citation tag '{citation_tag}' not found in evidence_map"
                    )
                
                evidence_entry = evidence_map[citation_tag]
                
                # Validate evidence entry has required fields
                required_fields = ['url', 'snippet', 'source_type']
                missing_fields = [f for f in required_fields if f not in evidence_entry]
                if missing_fields:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Evidence entry for '{citation_tag}' missing fields: {', '.join(missing_fields)}"
                    )
                
                # Validate evidence_id matches
                if evidence_id and evidence_entry.get('evidence_id') != evidence_id:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Evidence ID mismatch for '{citation_tag}'"
                    )
                
                validated_override["citation"] = citation
                validated_override["evidence_id"] = evidence_entry.get('evidence_id', '')
            
            validated_overrides.append(validated_override)
        
        # Store overrides in database
        doc.citation_overrides_json = validated_overrides
        session.commit()
        
        # TODO: Apply overrides to content and resume research generation
        # This would involve:
        # 1. Loading current content
        # 2. Applying overrides (remove, rewrite, attach citations)
        # 3. Resuming research generation from where it stopped
        
        return {
            "connector_id": connector_id,
            "message": "Citation overrides applied successfully",
            "overrides_count": len(validated_overrides)
        }
    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=f"Error applying citation overrides: {str(e)}")
    finally:
        session.close()


# =====================
# Search API Endpoints
# =====================

@app.post("/api/search", response_model=SearchResponse)
@limiter.limit("100/minute")
async def search(
    http_request: Request,
    request: SearchRequest,
    api_key: str = Depends(verify_api_key)
):
    """Search across connector research documents."""
    if not vector_manager or not connector_manager:
        raise HTTPException(status_code=503, detail="Services not initialized")
    
    # Sanitize search query
    sanitized_query = InputSanitizer.sanitize_string(request.query, max_length=500)
    
    if request.connector_id:
        # Sanitize connector_id
        sanitized_connector_id = InputSanitizer.sanitize_connector_id(request.connector_id)
        # Search within specific connector
        if not vector_manager.index_exists(sanitized_connector_id):
            raise HTTPException(status_code=404, detail=f"No index found for connector '{sanitized_connector_id}'")
        
        results = vector_manager.search(
            connector_id=sanitized_connector_id,
            query=sanitized_query,
            top_k=request.top_k
        )
    else:
        # Search across all complete connectors
        connectors = connector_manager.list_connectors()
        connector_ids = [c.id for c in connectors if c.status == ConnectorStatus.COMPLETE.value]
        
        if not connector_ids:
            return SearchResponse(query=request.query, results=[], total_results=0)
        
        results = vector_manager.search_all_connectors(
            query=request.query,
            connector_ids=connector_ids,
            top_k=request.top_k
        )
    
    return SearchResponse(
        query=sanitized_query,
        results=[
            SearchResultItem(
                id=r["id"],
                score=r["score"],
                text=r["text"],
                section=r.get("section", ""),
                source_type=r.get("source_type", "research"),
                connector_name=r.get("connector_name", "")
            )
            for r in results
        ],
        total_results=len(results)
    )


@app.post("/api/chat", response_model=ChatResponse)
@limiter.limit("50/minute")
async def chat(
    http_request: Request,
    request: ChatRequest,
    api_key: str = Depends(verify_api_key)
):
    """Chat with connector research using RAG."""
    if not vector_manager or not connector_manager or not research_agent:
        raise HTTPException(status_code=503, detail="Services not initialized")
    
    # Get relevant context
    if request.connector_id:
        if not vector_manager.index_exists(request.connector_id):
            raise HTTPException(status_code=404, detail=f"No index found for connector '{request.connector_id}'")
        results = vector_manager.search(
            connector_id=request.connector_id,
            query=request.message,
            top_k=request.top_k
        )
    else:
        connectors = connector_manager.list_connectors()
        connector_ids = [c.id for c in connectors if c.status == ConnectorStatus.COMPLETE.value]
        
        if not connector_ids:
            return ChatResponse(
                question=request.message,
                answer="No connector research available yet. Create a connector and generate research first.",
                sources=[]
            )
        
        results = vector_manager.search_all_connectors(
            query=sanitized_message,
            connector_ids=connector_ids,
            top_k=request.top_k
        )
    
    # Build context
    context = "\n\n".join([
        f"[{r.get('connector_name', 'Unknown')} - {r.get('section', 'N/A')}]\n{r['text']}"
        for r in results
    ])
    
    # Generate answer using OpenAI
    from openai import OpenAI
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    
    response = client.chat.completions.create(
        model=os.getenv("RESEARCH_MODEL", "gpt-4o"),
        messages=[
            {
                "role": "system",
                "content": """You are a helpful connector research assistant. Answer questions based on the provided research context.
Be specific and cite the connector name and section when referencing information.
If the context doesn't contain relevant information, say so clearly."""
            },
            {
                "role": "user",
                "content": f"""Context from research documents:
{context}

Question: {request.message}

Answer based on the context above:"""
            }
        ],
        temperature=0.3,
        max_tokens=1000
    )
    
    answer = response.choices[0].message.content
    
    return ChatResponse(
        question=sanitized_message,
        answer=answer,
        sources=[
            {
                "connector": r.get("connector_name", "Unknown"),
                "section": r.get("section", "N/A"),
                "score": r["score"]
            }
            for r in results[:3]
        ]
    )


# =====================
# 📚 Knowledge Vault Endpoints
# =====================

@app.get("/api/vault/stats", response_model=VaultStatsResponse)
@limiter.limit("100/minute")
async def get_vault_stats(
    request: Request,
    api_key: str = Depends(verify_api_key)
):
    """Get Knowledge Vault statistics."""
    if not knowledge_vault:
        raise HTTPException(status_code=503, detail="Knowledge Vault not initialized")
    
    stats = knowledge_vault.get_stats()
    return VaultStatsResponse(
        available=stats.get("available", False),
        connector_count=stats.get("connector_count", 0),
        total_chunks=stats.get("total_chunks", 0),
        connectors=stats.get("connectors", [])
    )


@app.get("/api/vault/{connector_name}/stats")
@limiter.limit("100/minute")
async def get_connector_vault_stats(
    request: Request,
    connector_name: str,
    api_key: str = Depends(verify_api_key)
):
    """Get Knowledge Vault statistics for a specific connector."""
    if not knowledge_vault:
        raise HTTPException(status_code=503, detail="Knowledge Vault not initialized")
    
    stats = knowledge_vault.get_stats(connector_name)
    return stats


@app.post("/api/vault/index", response_model=VaultDocumentResponse)
@limiter.limit("50/minute")
async def index_vault_document(
    http_request: Request,
    request: VaultIndexRequest,
    api_key: str = Depends(verify_api_key)
):
    """Index documentation into the Knowledge Vault."""
    if not knowledge_vault:
        raise HTTPException(status_code=503, detail="Knowledge Vault not initialized")
    
    try:
        source_type = KnowledgeSourceType(request.source_type)
    except ValueError:
        source_type = KnowledgeSourceType.CUSTOM
    
    try:
        doc = knowledge_vault.index_document(
            connector_name=request.connector_name,
            title=request.title,
            content=request.content,
            source_type=source_type,
            source_url=request.source_url,
            version=request.version
        )
        
        return VaultDocumentResponse(
            id=doc.id,
            connector_name=doc.connector_name,
            title=doc.title,
            source_type=doc.source_type.value,
            source_url=doc.source_url,
            chunk_count=doc.chunk_count
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/vault/index-url", response_model=VaultDocumentResponse)
@limiter.limit("30/minute")
async def index_vault_from_url(
    http_request: Request,
    request: VaultIndexFromUrlRequest,
    api_key: str = Depends(verify_api_key)
):
    """Index documentation from a URL into the Knowledge Vault."""
    if not knowledge_vault:
        raise HTTPException(status_code=503, detail="Knowledge Vault not initialized")
    
    try:
        source_type = KnowledgeSourceType(request.source_type)
    except ValueError:
        source_type = KnowledgeSourceType.OFFICIAL_DOCS
    
    try:
        doc = knowledge_vault.index_from_url(
            connector_name=request.connector_name,
            url=request.url,
            source_type=source_type
        )
        
        return VaultDocumentResponse(
            id=doc.id,
            connector_name=doc.connector_name,
            title=doc.title,
            source_type=doc.source_type.value,
            source_url=doc.source_url,
            chunk_count=doc.chunk_count
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/vault/index-file")
@limiter.limit("30/minute")
async def index_vault_from_file(
    request: Request,
    connector_name: str = Form(...),
    title: str = Form(None),
    source_type: str = Form("official_docs"),
    source_url: Optional[str] = Form(None),
    file: UploadFile = File(...),
    api_key: str = Depends(verify_api_key)
):
    """Upload and index a file into the Knowledge Vault (supports PDF, MD, TXT, JSON, HTML)."""
    if not knowledge_vault:
        raise HTTPException(status_code=503, detail="Knowledge Vault not initialized")
    
    try:
        # Read file content
        content = await file.read()
        filename = file.filename or "document"
        
        try:
            st = KnowledgeSourceType(source_type)
        except ValueError:
            st = KnowledgeSourceType.CUSTOM
        
        # Handle PDF files
        if filename.lower().endswith('.pdf'):
            doc = knowledge_vault.index_pdf(
                connector_name=connector_name,
                pdf_content=content,
                filename=filename,
                source_type=st
            )
        else:
            # Try to decode as text
            try:
                text_content = content.decode('utf-8')
            except UnicodeDecodeError:
                try:
                    text_content = content.decode('latin-1')
                except:
                    raise HTTPException(status_code=400, detail="Could not decode file as text")
            
            doc = knowledge_vault.index_document(
                connector_name=connector_name,
                title=title or filename,
                content=text_content,
                source_type=st,
                source_url=source_url
            )
        
        return {
            "id": doc.id,
            "connector_name": doc.connector_name,
            "title": doc.title,
            "source_type": doc.source_type.value,
            "chunk_count": doc.chunk_count,
            "filename": file.filename
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/vault/search", response_model=List[VaultSearchResultResponse])
@limiter.limit("100/minute")
async def search_vault(
    http_request: Request,
    request: VaultSearchRequest,
    api_key: str = Depends(verify_api_key)
):
    """Search the Knowledge Vault for a connector."""
    if not knowledge_vault:
        raise HTTPException(status_code=503, detail="Knowledge Vault not initialized")
    
    if not knowledge_vault.has_knowledge(request.connector_name):
        return []
    
    results = knowledge_vault.search(
        connector_name=request.connector_name,
        query=request.query,
        top_k=request.top_k
    )
    
    return [
        VaultSearchResultResponse(
            text=r.text,
            score=r.score,
            source_type=r.source_type,
            title=r.title,
            connector_name=r.connector_name
        )
        for r in results
    ]


@app.delete("/api/vault/{connector_name}")
@limiter.limit("20/minute")
async def delete_vault_knowledge(
    request: Request,
    connector_name: str,
    api_key: str = Depends(verify_api_key)
):
    """Delete all knowledge for a connector from the vault."""
    if not knowledge_vault:
        raise HTTPException(status_code=503, detail="Knowledge Vault not initialized")
    
    deleted = knowledge_vault.delete_knowledge(connector_name)
    
    return {
        "deleted": deleted,
        "connector_name": connector_name,
        "message": f"Knowledge for '{connector_name}' deleted" if deleted else f"No knowledge found for '{connector_name}'"
    }


@app.get("/api/vault/connectors")
@limiter.limit("100/minute")
async def list_vault_connectors(
    request: Request,
    api_key: str = Depends(verify_api_key)
):
    """List all connectors with knowledge in the vault."""
    if not knowledge_vault:
        raise HTTPException(status_code=503, detail="Knowledge Vault not initialized")
    
    connectors = knowledge_vault.list_connectors()
    return {"connectors": connectors, "count": len(connectors)}


# =====================
# 📚 Bulk Upload Endpoints (500+ documents)
# =====================

@app.post("/api/vault/bulk-upload")
@limiter.limit("5/minute")
async def bulk_upload_files(
    request: Request,
    background_tasks: BackgroundTasks,
    connector_name: str = Form(...),
    source_type: str = Form("official_docs"),
    files: List[UploadFile] = File(...),
    api_key: str = Depends(verify_api_key)
):
    """
    Upload multiple files (PDF, MD, TXT) to the Knowledge Vault.
    
    Supports up to 500+ files with background processing.
    Returns a job_id for tracking progress.
    """
    if not knowledge_vault:
        raise HTTPException(status_code=503, detail="Knowledge Vault not initialized")
    
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")
    
    try:
        st = KnowledgeSourceType(source_type)
    except ValueError:
        st = KnowledgeSourceType.CUSTOM
    
    # Start bulk upload job
    job_id = knowledge_vault.start_bulk_upload(connector_name, len(files))
    
    # Process files in background
    async def process_files():
        for file in files:
            try:
                content = await file.read()
                knowledge_vault.process_bulk_file(
                    job_id=job_id,
                    file_content=content,
                    filename=file.filename or "unknown",
                    source_type=st
                )
            except Exception as e:
                print(f"Error processing {file.filename}: {e}")
        
        knowledge_vault.complete_bulk_upload(job_id)
    
    background_tasks.add_task(process_files)
    
    return {
        "job_id": job_id,
        "connector_name": connector_name,
        "total_files": len(files),
        "status": "processing",
        "message": f"Started processing {len(files)} files in background"
    }


@app.get("/api/vault/bulk-upload/{job_id}")
async def get_bulk_upload_progress(job_id: str):
    """Get progress for a bulk upload job."""
    if not knowledge_vault:
        raise HTTPException(status_code=503, detail="Knowledge Vault not initialized")
    
    progress = knowledge_vault.get_bulk_upload_progress(job_id)
    
    if not progress:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
    
    return progress.to_dict()


@app.get("/api/vault/bulk-upload")
@limiter.limit("100/minute")
async def list_bulk_upload_jobs(
    request: Request,
    api_key: str = Depends(verify_api_key)
):
    """List all bulk upload jobs."""
    if not knowledge_vault:
        raise HTTPException(status_code=503, detail="Knowledge Vault not initialized")
    
    jobs = knowledge_vault.list_bulk_upload_jobs()
    return {"jobs": jobs, "count": len(jobs)}


@app.post("/api/vault/bulk-upload-sync")
@limiter.limit("2/minute")
async def bulk_upload_files_sync(
    request: Request,
    connector_name: str = Form(...),
    source_type: str = Form("official_docs"),
    files: List[UploadFile] = File(...),
    api_key: str = Depends(verify_api_key)
):
    """
    Upload multiple files synchronously (for smaller batches).
    
    Waits for all files to be processed before returning.
    Recommended for < 50 files.
    """
    if not knowledge_vault:
        raise HTTPException(status_code=503, detail="Knowledge Vault not initialized")
    
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")
    
    try:
        st = KnowledgeSourceType(source_type)
    except ValueError:
        st = KnowledgeSourceType.CUSTOM
    
    # Process all files
    job_id = knowledge_vault.start_bulk_upload(connector_name, len(files))
    
    for file in files:
        try:
            content = await file.read()
            knowledge_vault.process_bulk_file(
                job_id=job_id,
                file_content=content,
                filename=file.filename or "unknown",
                source_type=st
            )
        except Exception as e:
            print(f"Error processing {file.filename}: {e}")
    
    progress = knowledge_vault.complete_bulk_upload(job_id)
    
    return progress.to_dict()


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

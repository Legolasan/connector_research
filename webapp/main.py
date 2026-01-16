"""
Connector Research Platform - FastAPI Web Application

A modern platform for creating, managing, and searching connector research documents.
Supports multi-connector research with per-connector Pinecone indices.
"""

import os
import asyncio
from pathlib import Path
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException, BackgroundTasks, File, UploadFile, Form
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import json

from services.connector_manager import get_connector_manager, ConnectorManager, ConnectorStatus, FivetranUrls, ManualInput
from services.github_cloner import get_github_cloner, GitHubCloner
from services.research_agent import get_research_agent, ResearchAgent
from services.vector_manager import get_vector_manager, VectorManager
from services.fivetran_crawler import get_fivetran_crawler, FivetranCrawler


# =====================
# Request/Response Models
# =====================

class FivetranUrlsRequest(BaseModel):
    """Fivetran documentation URLs for parity comparison."""
    setup_guide_url: Optional[str] = None
    connector_overview_url: Optional[str] = None
    schema_info_url: Optional[str] = None


class ConnectorCreateRequest(BaseModel):
    name: str
    connector_type: Optional[str] = "auto"  # Auto-discovered during research
    github_url: Optional[str] = None
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

# Background tasks tracking
_running_research_tasks: Dict[str, asyncio.Task] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize services on startup."""
    global connector_manager, github_cloner, research_agent, vector_manager, fivetran_crawler
    
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
    import re
    
    if not connector_manager:
        raise HTTPException(status_code=503, detail="Connector Manager not initialized")
    
    connector = connector_manager.get_connector(connector_id)
    if not connector:
        raise HTTPException(status_code=404, detail=f"Connector '{connector_id}' not found")
    
    # Get research content
    content = connector_manager.get_research_document(connector_id)
    if not content:
        raise HTTPException(status_code=404, detail=f"Research document not found for '{connector_id}'")
    
    # Convert markdown to HTML
    md = markdown.Markdown(extensions=['tables', 'fenced_code', 'toc', 'nl2br'])
    html_content = md.convert(content)
    
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
async def download_research(connector_id: str):
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
    return {
        "status": "healthy",
        "services": {
            "connector_manager": connector_manager is not None,
            "github_cloner": github_cloner is not None,
            "research_agent": research_agent is not None,
            "vector_manager": vector_manager is not None,
            "fivetran_crawler": fivetran_crawler is not None
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
async def create_connector(request: ConnectorCreateRequest):
    """Create a new connector research project."""
    if not connector_manager:
        raise HTTPException(status_code=503, detail="Connector Manager not initialized")
    
    try:
        # Convert FivetranUrlsRequest to FivetranUrls if provided
        fivetran_urls = None
        if request.fivetran_urls:
            fivetran_urls = FivetranUrls(
                setup_guide_url=request.fivetran_urls.setup_guide_url,
                connector_overview_url=request.fivetran_urls.connector_overview_url,
                schema_info_url=request.fivetran_urls.schema_info_url
            )
        
        connector = connector_manager.create_connector(
            name=request.name,
            connector_type=request.connector_type,
            github_url=request.github_url,
            fivetran_urls=fivetran_urls,
            description=request.description,
            manual_text=request.manual_text
        )
        return _connector_to_response(connector)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/connectors/upload", response_model=ConnectorResponse)
async def create_connector_with_file(
    name: str = Form(...),
    connector_type: Optional[str] = Form("auto"),  # Auto-discovered during research
    github_url: Optional[str] = Form(None),
    fivetran_urls: Optional[str] = Form(None),  # JSON string
    manual_text: Optional[str] = Form(None),
    manual_file: Optional[UploadFile] = File(None)
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
async def get_connector(connector_id: str):
    """Get a specific connector by ID."""
    if not connector_manager:
        raise HTTPException(status_code=503, detail="Connector Manager not initialized")
    
    connector = connector_manager.get_connector(connector_id)
    if not connector:
        raise HTTPException(status_code=404, detail=f"Connector '{connector_id}' not found")
    
    return _connector_to_response(connector)


@app.delete("/api/connectors/{connector_id}")
async def delete_connector(connector_id: str):
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
async def generate_research(connector_id: str, background_tasks: BackgroundTasks):
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
            fivetran_context = None
            
            # Clone GitHub repo if URL provided
            if connector.github_url and github_cloner:
                connector_manager.update_connector(connector_id, status=ConnectorStatus.CLONING.value)
                extracted = await github_cloner.clone_and_extract(connector.github_url, connector_id)
                if extracted:
                    github_context = extracted.to_dict()
                else:
                    print(f"⚠ GitHub cloning skipped for {connector.name}, continuing with web search only")
            
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
            
            # Update status to researching
            connector_manager.update_connector(connector_id, status=ConnectorStatus.RESEARCHING.value)
            
            # Generate research
            def on_progress(progress):
                connector_manager.update_progress(
                    connector_id,
                    section=progress.current_section,
                    section_name=progress.current_content[:50] if progress.current_content else "",
                    completed=(progress.current_section in progress.sections_completed),
                    total_sections=progress.total_sections,
                    discovered_methods=progress.discovered_methods if hasattr(progress, 'discovered_methods') else None
                )
            
            research_content = await research_agent.generate_research(
                connector_id=connector_id,
                connector_name=connector.name,
                connector_type=connector.connector_type,
                github_context=github_context,
                fivetran_context=fivetran_context,
                on_progress=on_progress
            )
            
            # Save research document (supports both database and file storage)
            connector_manager.save_research_document(connector_id, research_content)
            
            # Vectorize into Pinecone
            vectors_count = 0
            if vector_manager:
                vectors_count = vector_manager.vectorize_research(
                    connector_id=connector_id,
                    connector_name=connector.name,
                    research_content=research_content
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
            print(f"Research generation failed: {e}")
            connector_manager.update_connector(connector_id, status=ConnectorStatus.FAILED.value)
        finally:
            if connector_id in _running_research_tasks:
                del _running_research_tasks[connector_id]
    
    # Start background task
    task = asyncio.create_task(run_research())
    _running_research_tasks[connector_id] = task
    
    return {"message": "Research generation started", "status": "started", "connector_id": connector_id}


@app.get("/api/connectors/{connector_id}/status")
async def get_research_status(connector_id: str):
    """Get research generation status for a connector."""
    if not connector_manager:
        raise HTTPException(status_code=503, detail="Connector Manager not initialized")
    
    connector = connector_manager.get_connector(connector_id)
    if not connector:
        raise HTTPException(status_code=404, detail=f"Connector '{connector_id}' not found")
    
    is_running = connector_id in _running_research_tasks
    
    return {
        "connector_id": connector_id,
        "status": connector.status,
        "is_running": is_running,
        "progress": {
            "current_section": connector.progress.current_section,
            "total_sections": connector.progress.total_sections,
            "sections_completed": connector.progress.sections_completed,
            "percentage": connector.progress.percentage,
            "current_section_name": connector.progress.current_section_name
        }
    }


@app.post("/api/connectors/{connector_id}/cancel")
async def cancel_research(connector_id: str):
    """Cancel research generation for a connector."""
    if connector_id not in _running_research_tasks:
        raise HTTPException(status_code=400, detail="No research generation running for this connector")
    
    _running_research_tasks[connector_id].cancel()
    
    if connector_manager:
        connector_manager.update_connector(connector_id, status=ConnectorStatus.CANCELLED.value)
    
    return {"message": "Research generation cancelled", "connector_id": connector_id}


@app.get("/api/connectors/{connector_id}/research")
async def get_research_document(connector_id: str):
    """Get the research document content for a connector."""
    if not connector_manager:
        raise HTTPException(status_code=503, detail="Connector Manager not initialized")
    
    content = connector_manager.get_research_document(connector_id)
    if content is None:
        raise HTTPException(status_code=404, detail=f"Research document not found for '{connector_id}'")
    
    return {"connector_id": connector_id, "content": content}


# =====================
# Search API Endpoints
# =====================

@app.post("/api/search", response_model=SearchResponse)
async def search(request: SearchRequest):
    """Search across connector research documents."""
    if not vector_manager or not connector_manager:
        raise HTTPException(status_code=503, detail="Services not initialized")
    
    if request.connector_id:
        # Search within specific connector
        if not vector_manager.index_exists(request.connector_id):
            raise HTTPException(status_code=404, detail=f"No index found for connector '{request.connector_id}'")
        
        results = vector_manager.search(
            connector_id=request.connector_id,
            query=request.query,
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
        query=request.query,
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
async def chat(request: ChatRequest):
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
            query=request.message,
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
        question=request.message,
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


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

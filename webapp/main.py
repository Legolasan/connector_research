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

from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from services.connector_manager import get_connector_manager, ConnectorManager, ConnectorStatus
from services.github_cloner import get_github_cloner, GitHubCloner
from services.research_agent import get_research_agent, ResearchAgent
from services.pinecone_manager import get_pinecone_manager, PineconeManager


# =====================
# Request/Response Models
# =====================

class ConnectorCreateRequest(BaseModel):
    name: str
    connector_type: str
    github_url: Optional[str] = None
    description: str = ""


class ConnectorProgressResponse(BaseModel):
    current_section: int
    total_sections: int
    current_phase: int
    sections_completed: List[int]
    percentage: float
    current_section_name: str


class ConnectorResponse(BaseModel):
    id: str
    name: str
    connector_type: str
    status: str
    github_url: Optional[str]
    description: str
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
pinecone_manager: Optional[PineconeManager] = None

# Background tasks tracking
_running_research_tasks: Dict[str, asyncio.Task] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize services on startup."""
    global connector_manager, github_cloner, research_agent, pinecone_manager
    
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
        pinecone_manager = get_pinecone_manager()
        print("✓ Pinecone Manager initialized")
    except Exception as e:
        print(f"⚠ Pinecone Manager not available: {e}")
        pinecone_manager = None
    
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


@app.get("/health")
async def health_check():
    """Health check endpoint for Railway."""
    return {
        "status": "healthy",
        "services": {
            "connector_manager": connector_manager is not None,
            "github_cloner": github_cloner is not None,
            "research_agent": research_agent is not None,
            "pinecone_manager": pinecone_manager is not None
        }
    }


# =====================
# Connector API Endpoints
# =====================

def _connector_to_response(connector) -> ConnectorResponse:
    """Convert Connector object to response model."""
    return ConnectorResponse(
        id=connector.id,
        name=connector.name,
        connector_type=connector.connector_type,
        status=connector.status,
        github_url=connector.github_url,
        description=connector.description,
        objects_count=connector.objects_count,
        vectors_count=connector.vectors_count,
        fivetran_parity=connector.fivetran_parity,
        progress=ConnectorProgressResponse(
            current_section=connector.progress.current_section,
            total_sections=connector.progress.total_sections,
            current_phase=connector.progress.current_phase,
            sections_completed=connector.progress.sections_completed,
            percentage=connector.progress.percentage,
            current_section_name=connector.progress.current_section_name
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
        connector = connector_manager.create_connector(
            name=request.name,
            connector_type=request.connector_type,
            github_url=request.github_url,
            description=request.description
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
    if pinecone_manager:
        pinecone_manager.delete_index(connector_id)
    
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
            
            # Clone GitHub repo if URL provided
            if connector.github_url and github_cloner:
                connector_manager.update_connector(connector_id, status=ConnectorStatus.CLONING.value)
                extracted = await github_cloner.clone_and_extract(connector.github_url, connector_id)
                github_context = extracted.to_dict()
            
            # Update status to researching
            connector_manager.update_connector(connector_id, status=ConnectorStatus.RESEARCHING.value)
            
            # Generate research
            def on_progress(progress):
                connector_manager.update_progress(
                    connector_id,
                    section=progress.current_section,
                    section_name=progress.current_content[:50] if progress.current_content else "",
                    completed=(progress.current_section in progress.sections_completed)
                )
            
            research_content = await research_agent.generate_research(
                connector_id=connector_id,
                connector_name=connector.name,
                connector_type=connector.connector_type,
                github_context=github_context,
                on_progress=on_progress
            )
            
            # Save research document
            doc_path = connector_manager.get_research_document_path(connector_id)
            if doc_path:
                with open(doc_path, 'w') as f:
                    f.write(research_content)
            
            # Vectorize into Pinecone
            vectors_count = 0
            if pinecone_manager:
                vectors_count = pinecone_manager.vectorize_research(
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
    if not pinecone_manager or not connector_manager:
        raise HTTPException(status_code=503, detail="Services not initialized")
    
    if request.connector_id:
        # Search within specific connector
        if not pinecone_manager.index_exists(request.connector_id):
            raise HTTPException(status_code=404, detail=f"No index found for connector '{request.connector_id}'")
        
        results = pinecone_manager.search(
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
        
        results = pinecone_manager.search_all_connectors(
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
    if not pinecone_manager or not connector_manager or not research_agent:
        raise HTTPException(status_code=503, detail="Services not initialized")
    
    # Get relevant context
    if request.connector_id:
        if not pinecone_manager.index_exists(request.connector_id):
            raise HTTPException(status_code=404, detail=f"No index found for connector '{request.connector_id}'")
        results = pinecone_manager.search(
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
        
        results = pinecone_manager.search_all_connectors(
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

"""
Connector Manager Service
Handles CRUD operations for connector research projects.
Supports both database (PostgreSQL) and file-based storage.
"""

import os
import json
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field, asdict, fields
from enum import Enum


class ConnectorStatus(str, Enum):
    """Status of a connector research project."""
    NOT_STARTED = "not_started"
    CLONING = "cloning"
    RESEARCHING = "researching"
    COMPLETE = "complete"
    FAILED = "failed"
    CANCELLED = "cancelled"
    STOPPED = "stopped"  # Stop-the-line triggered


class ConnectorType(str, Enum):
    """Type of connector/data source."""
    REST_API = "rest_api"
    GRAPHQL = "graphql"
    SOAP = "soap"
    JDBC = "jdbc"
    SDK = "sdk"
    WEBHOOK = "webhook"
    FILE_STORAGE = "file_storage"
    OBJECT_STORAGE = "object_storage"
    MESSAGING = "messaging"
    ADVERTISING = "advertising"
    WAREHOUSE = "warehouse"


@dataclass
class ConnectorProgress:
    """Tracks research generation progress."""
    current_section: int = 0
    total_sections: int = 0  # Dynamic - calculated based on discovered methods
    current_phase: int = 0
    sections_completed: List[int] = field(default_factory=list)
    sections_failed: List[int] = field(default_factory=list)
    current_section_name: str = ""
    research_method: Dict[int, str] = field(default_factory=dict)  # section -> method used
    discovered_methods: List[str] = field(default_factory=list)  # Extraction methods found
    section_reviews: Dict[int, Any] = field(default_factory=dict)  # Section reviews from Critic Agent
    stop_the_line_events: List[Any] = field(default_factory=list)  # Stop-the-line events
    contradictions: List[Any] = field(default_factory=list)  # Detected contradictions
    engineering_costs: Dict[str, Any] = field(default_factory=dict)  # Engineering cost analysis
    overall_confidence: float = 0.0  # Overall confidence score
    
    @property
    def percentage(self) -> float:
        if self.total_sections == 0:
            # If total not yet calculated, estimate based on minimum sections
            if self.sections_completed:
                return min(95.0, len(self.sections_completed) * 5)
            return 0.0
        return (len(self.sections_completed) / self.total_sections) * 100


@dataclass
class ManualInput:
    """Manual input for object lists (CSV, PDF, or text)."""
    text: Optional[str] = None           # Text list of objects
    file_content: Optional[str] = None   # CSV content or base64 PDF
    file_type: Optional[str] = None      # 'csv' or 'pdf'
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'text': self.text,
            'file_content': self.file_content[:1000] if self.file_content else None,  # Truncate for storage
            'file_type': self.file_type
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Optional['ManualInput']:
        if data is None:
            return None
        return cls(
            text=data.get('text'),
            file_content=data.get('file_content'),
            file_type=data.get('file_type')
        )
    
    def has_input(self) -> bool:
        """Check if any manual input is provided."""
        return bool(self.text or self.file_content)


@dataclass
class FivetranUrls:
    """Fivetran documentation URLs for parity comparison."""
    setup_guide_url: Optional[str] = None        # Setup Guide page (prerequisites, auth)
    connector_overview_url: Optional[str] = None  # Connector Overview (features, sync modes)
    schema_info_url: Optional[str] = None         # Schema Information (objects, ERD)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'setup_guide_url': self.setup_guide_url,
            'connector_overview_url': self.connector_overview_url,
            'schema_info_url': self.schema_info_url
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FivetranUrls':
        if data is None:
            return None
        return cls(
            setup_guide_url=data.get('setup_guide_url'),
            connector_overview_url=data.get('connector_overview_url'),
            schema_info_url=data.get('schema_info_url')
        )
    
    def has_urls(self) -> bool:
        """Check if any Fivetran URLs are provided."""
        return any([self.setup_guide_url, self.connector_overview_url, self.schema_info_url])


@dataclass
class Connector:
    """Represents a connector research project."""
    id: str  # slug, e.g., "facebook-ads"
    name: str  # Display name, e.g., "Facebook Ads"
    connector_type: str = "auto"  # Auto-discovered during research
    status: str = ConnectorStatus.NOT_STARTED.value
    github_url: Optional[str] = None
    hevo_github_url: Optional[str] = None  # Optional Hevo connector GitHub URL for comparison
    description: str = ""
    
    # Official Documentation Pre-crawl
    official_doc_urls: Optional[List[str]] = None
    doc_crawl_status: str = "pending"
    doc_crawl_urls: Optional[List[str]] = None
    doc_crawl_pages: int = 0
    doc_crawl_words: int = 0
    
    # Fivetran parity URLs
    fivetran_urls: Optional[FivetranUrls] = None
    
    # Manual input for object lists
    manual_input: Optional[ManualInput] = None
    
    # Auto-discovered extraction methods (REST, GraphQL, Webhooks, etc.)
    discovered_methods: List[str] = field(default_factory=list)
    
    # Metadata
    objects_count: int = 0
    vectors_count: int = 0
    fivetran_parity: Optional[float] = None
    
    # Progress tracking
    progress: ConnectorProgress = field(default_factory=ConnectorProgress)
    
    # Timestamps
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    completed_at: Optional[str] = None
    
    # Research sources used
    sources: List[str] = field(default_factory=list)
    
    # Pinecone index name
    pinecone_index: str = ""
    
    def __post_init__(self):
        if not self.pinecone_index:
            self.pinecone_index = f"{self.id}-docs"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        data = asdict(self)
        # Convert progress to dict
        if isinstance(self.progress, ConnectorProgress):
            data['progress'] = asdict(self.progress)
        # Convert fivetran_urls to dict
        if isinstance(self.fivetran_urls, FivetranUrls):
            data['fivetran_urls'] = self.fivetran_urls.to_dict()
        # Convert manual_input to dict
        if isinstance(self.manual_input, ManualInput):
            data['manual_input'] = self.manual_input.to_dict()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Connector':
        """Create from dictionary."""
        # Make a copy to avoid modifying the original
        data = data.copy()
        
        # Handle progress
        progress_data = data.pop('progress', {})
        if isinstance(progress_data, dict):
            progress = ConnectorProgress(**progress_data) if progress_data else ConnectorProgress()
        else:
            progress = ConnectorProgress()
        
        # Handle fivetran_urls
        fivetran_urls_data = data.pop('fivetran_urls', None)
        fivetran_urls = FivetranUrls.from_dict(fivetran_urls_data) if fivetran_urls_data else None
        
        # Handle manual_input
        manual_input_data = data.pop('manual_input', None)
        manual_input = ManualInput.from_dict(manual_input_data) if manual_input_data else None
        
        # Filter out any unknown fields that aren't in the dataclass
        known_fields = {f.name for f in fields(cls)}
        filtered_data = {k: v for k, v in data.items() if k in known_fields}
        
        return cls(progress=progress, fivetran_urls=fivetran_urls, manual_input=manual_input, **filtered_data)


class ConnectorManager:
    """Manages connector research projects with database or file-based storage."""
    
    def __init__(self, base_dir: Optional[Path] = None):
        """Initialize the connector manager.
        
        Uses DATABASE_URL for PostgreSQL storage if available,
        otherwise falls back to file-based storage.
        """
        # Check for database availability
        self._use_database = False
        self._db_storage = None
        
        if os.getenv("DATABASE_URL"):
            try:
                from services.database import init_database, get_database_storage, is_database_available
                
                # Initialize database
                if init_database():
                    self._db_storage = get_database_storage()
                    if self._db_storage:
                        self._use_database = True
                        print("✓ ConnectorManager using PostgreSQL database storage")
            except Exception as e:
                print(f"⚠ Database initialization failed, using file storage: {e}")
        
        # Setup file-based storage as fallback
        if not self._use_database:
            if base_dir is None:
                env_dir = os.getenv("CONNECTOR_DATA_DIR")
                if env_dir:
                    base_dir = Path(env_dir)
                elif os.getenv("RAILWAY_ENVIRONMENT") or os.getenv("PORT"):
                    base_dir = Path("/tmp/connectors")
                else:
                    base_dir = Path(__file__).parent.parent.parent / "connectors"
            
            self.base_dir = Path(base_dir)
            self.registry_file = self.base_dir / "_agent" / "connectors_registry.json"
            
            try:
                self.base_dir.mkdir(parents=True, exist_ok=True)
                (self.base_dir / "_agent").mkdir(exist_ok=True)
                (self.base_dir / "_templates").mkdir(exist_ok=True)
                print(f"✓ ConnectorManager using file storage: {self.base_dir}")
            except Exception as e:
                print(f"⚠ Could not create directories at {self.base_dir}: {e}")
                self.base_dir = Path("/tmp/connectors")
                self.registry_file = self.base_dir / "_agent" / "connectors_registry.json"
                self.base_dir.mkdir(parents=True, exist_ok=True)
                (self.base_dir / "_agent").mkdir(exist_ok=True)
                (self.base_dir / "_templates").mkdir(exist_ok=True)
                print(f"✓ Using fallback directory: {self.base_dir}")
            
            # Load file-based registry
            self._registry: Dict[str, Connector] = {}
            self._load_registry()
    
    def _load_registry(self):
        """Load connector registry from file (file-based mode only)."""
        if self._use_database:
            return
        
        if self.registry_file.exists():
            try:
                with open(self.registry_file, 'r') as f:
                    data = json.load(f)
                    for connector_id, connector_data in data.get('connectors', {}).items():
                        self._registry[connector_id] = Connector.from_dict(connector_data)
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Warning: Could not load registry: {e}")
                self._registry = {}
        else:
            self._registry = {}
    
    def _save_registry(self):
        """Save connector registry to file (file-based mode only)."""
        if self._use_database:
            return
        
        data = {
            'connectors': {
                connector_id: connector.to_dict()
                for connector_id, connector in self._registry.items()
            },
            'metadata': {
                'version': '1.0.0',
                'updated_at': datetime.utcnow().isoformat()
            }
        }
        
        with open(self.registry_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def _generate_id(self, name: str) -> str:
        """Generate a URL-safe ID from connector name."""
        slug = name.lower().strip()
        slug = slug.replace(' ', '-')
        slug = ''.join(c for c in slug if c.isalnum() or c == '-')
        while '--' in slug:
            slug = slug.replace('--', '-')
        return slug.strip('-')
    
    def create_connector(
        self,
        name: str,
        connector_type: str,
        github_url: Optional[str] = None,
        hevo_github_url: Optional[str] = None,
        official_doc_urls: Optional[List[str]] = None,
        fivetran_urls: Optional[FivetranUrls] = None,
        description: str = "",
        manual_text: Optional[str] = None,
        manual_file_content: Optional[str] = None,
        manual_file_type: Optional[str] = None
    ) -> Connector:
        """Create a new connector research project."""
        connector_id = self._generate_id(name)
        
        # Create manual input if provided
        manual_input = None
        if manual_text or manual_file_content:
            # For PDF files, convert bytes to base64 string for storage
            file_content_str = None
            if manual_file_content:
                if isinstance(manual_file_content, bytes):
                    import base64
                    file_content_str = base64.b64encode(manual_file_content).decode('utf-8')
                else:
                    file_content_str = manual_file_content
            
            manual_input = ManualInput(
                text=manual_text,
                file_content=file_content_str,
                file_type=manual_file_type
            )
        
        # Prepare connector data
        connector_data = {
            'id': connector_id,
            'name': name,
            'connector_type': connector_type,
            'status': ConnectorStatus.NOT_STARTED.value,
            'github_url': github_url,
            'hevo_github_url': hevo_github_url,
            'official_doc_urls': official_doc_urls,
            'doc_crawl_status': 'pending',
            'description': description,
            'fivetran_urls': fivetran_urls.to_dict() if fivetran_urls else None,
            'manual_input': manual_input.to_dict() if manual_input else None,
            'objects_count': 0,
            'vectors_count': 0,
            'fivetran_parity': None,
            'progress': {
                'current_section': 0,
                'total_sections': 18,
                'current_phase': 0,
                'sections_completed': [],
                'sections_failed': [],
                'current_section_name': '',
                'research_method': {}
            },
            'created_at': datetime.utcnow().isoformat(),
            'updated_at': datetime.utcnow().isoformat(),
            'completed_at': None,
            'sources': [],
            'pinecone_index': f"{connector_id}-docs"
        }
        
        if self._use_database:
            # Check if exists
            existing = self._db_storage.get_connector(connector_id)
            if existing:
                raise ValueError(f"Connector '{connector_id}' already exists")
            
            # Create in database
            result = self._db_storage.create_connector(connector_data)
            if not result:
                raise ValueError("Failed to create connector in database")
            
            return Connector.from_dict(result)
        else:
            # File-based storage
            if connector_id in self._registry:
                raise ValueError(f"Connector '{connector_id}' already exists")
            
            # Create connector directory
            connector_dir = self.base_dir / connector_id
            connector_dir.mkdir(exist_ok=True)
            (connector_dir / "sources").mkdir(exist_ok=True)
            
            # Create connector object
            connector = Connector(
                id=connector_id,
                name=name,
                connector_type=connector_type,
                github_url=github_url,
                fivetran_urls=fivetran_urls,
                manual_input=manual_input,
                description=description
            )
            
            self._registry[connector_id] = connector
            self._save_registry()
            self._create_research_document(connector)
            
            return connector
    
    def _create_research_document(self, connector: Connector):
        """Create initial research document (file-based mode only)."""
        if self._use_database:
            return
        
        template_path = self.base_dir / "_templates" / "connector-research-template.md"
        output_path = self.base_dir / connector.id / f"{connector.id}-research.md"
        
        if template_path.exists():
            with open(template_path, 'r') as f:
                template = f.read()
            
            content = template.replace('<CONNECTOR_NAME>', connector.name)
            content = content.replace('<DATE>', datetime.utcnow().strftime('%Y-%m-%d'))
            
            with open(output_path, 'w') as f:
                f.write(content)
        else:
            content = f"""# Connector Research: {connector.name}

**Subject:** {connector.name} Connector - Full Production Research  
**Status:** In Progress  
**Started:** {datetime.utcnow().strftime('%Y-%m-%d')}  
**Last Updated:** {datetime.utcnow().strftime('%Y-%m-%d')}

---

## Research Overview

**Goal:** Produce exhaustive, production-grade research on how to build a data connector for {connector.name}.

**Connector Type:** {connector.connector_type}

**GitHub Source:** {connector.github_url or 'Not provided'}

---

<!-- RESEARCH SECTIONS WILL BE APPENDED BELOW -->

"""
            with open(output_path, 'w') as f:
                f.write(content)
    
    def get_connector(self, connector_id: str) -> Optional[Connector]:
        """Get a connector by ID."""
        if self._use_database:
            data = self._db_storage.get_connector(connector_id)
            if data:
                return Connector.from_dict(data)
            return None
        else:
            return self._registry.get(connector_id)
    
    def list_connectors(self) -> List[Connector]:
        """List all connectors."""
        if self._use_database:
            connectors_data = self._db_storage.list_connectors()
            return [Connector.from_dict(data) for data in connectors_data]
        else:
            return list(self._registry.values())
    
    def update_connector(self, connector_id: str, **updates) -> Optional[Connector]:
        """Update connector properties."""
        allowed_fields = {
            'name', 'description', 'status', 'objects_count', 
            'vectors_count', 'fivetran_parity', 'sources', 'completed_at',
            'doc_crawl_status', 'doc_crawl_urls', 'doc_crawl_pages', 'doc_crawl_words'
        }
        
        # Filter to allowed fields
        filtered_updates = {k: v for k, v in updates.items() if k in allowed_fields}
        filtered_updates['updated_at'] = datetime.utcnow().isoformat()
        
        if self._use_database:
            result = self._db_storage.update_connector(connector_id, filtered_updates)
            if result:
                return Connector.from_dict(result)
            return None
        else:
            connector = self._registry.get(connector_id)
            if not connector:
                return None
            
            for key, value in filtered_updates.items():
                if hasattr(connector, key):
                    setattr(connector, key, value)
            
            self._save_registry()
            return connector
    
    def update_progress(
        self,
        connector_id: str,
        section: int,
        section_name: str = "",
        method: str = "web_search",
        completed: bool = False,
        failed: bool = False,
        total_sections: int = 0,
        discovered_methods: List[str] = None,
        research_progress: Optional[Any] = None  # ResearchProgress from ResearchAgent
    ) -> Optional[Connector]:
        """Update research progress for a connector."""
        connector = self.get_connector(connector_id)
        if not connector:
            return None
        
        progress = connector.progress
        progress.current_section = section
        progress.current_section_name = section_name
        progress.research_method[section] = method
        
        # Update total_sections if provided
        if total_sections > 0:
            progress.total_sections = total_sections
        
        # Update discovered_methods if provided
        if discovered_methods:
            progress.discovered_methods = discovered_methods
            # Also update connector's discovered_methods
            connector.discovered_methods = discovered_methods
        
        # Update new fields from ResearchProgress if provided
        if research_progress:
            # CRITICAL: Copy sections_completed from research agent's progress
            if hasattr(research_progress, 'sections_completed') and research_progress.sections_completed:
                progress.sections_completed = list(research_progress.sections_completed)
            if hasattr(research_progress, 'total_sections') and research_progress.total_sections > 0:
                progress.total_sections = research_progress.total_sections
            if hasattr(research_progress, 'section_reviews'):
                progress.section_reviews = research_progress.section_reviews
            if hasattr(research_progress, 'stop_the_line_events'):
                progress.stop_the_line_events = research_progress.stop_the_line_events
            if hasattr(research_progress, 'contradictions'):
                progress.contradictions = research_progress.contradictions
            if hasattr(research_progress, 'engineering_costs'):
                progress.engineering_costs = research_progress.engineering_costs
            if hasattr(research_progress, 'overall_confidence'):
                progress.overall_confidence = research_progress.overall_confidence
        
        # Calculate phase dynamically based on section number and total
        # Phase 1: Discovery (1-3)
        # Phase 2: Method Deep Dives (4 to 3+num_methods)  
        # Phase 3: Cross-Cutting (variable)
        # Phase 4: Implementation (final sections)
        num_methods = len(progress.discovered_methods) if progress.discovered_methods else 0
        
        if section <= 3:
            progress.current_phase = 1  # Discovery
        elif num_methods > 0 and section <= 3 + num_methods:
            progress.current_phase = 2  # Method Deep Dives
        elif num_methods > 0 and section <= 3 + num_methods + 5:
            progress.current_phase = 3  # Cross-Cutting
        else:
            progress.current_phase = 4  # Implementation
        
        if completed and section not in progress.sections_completed:
            progress.sections_completed.append(section)
            progress.sections_completed.sort()
        
        if failed and section not in progress.sections_failed:
            progress.sections_failed.append(section)
        
        # Determine new status
        new_status = connector.status
        completed_at = connector.completed_at
        
        if len(progress.sections_completed) == progress.total_sections:
            new_status = ConnectorStatus.COMPLETE.value
            completed_at = datetime.utcnow().isoformat()
        elif len(progress.sections_completed) > 0 or progress.current_section > 0:
            new_status = ConnectorStatus.RESEARCHING.value
        
        if self._use_database:
            updates = {
                'progress': asdict(progress),
                'status': new_status,
                'completed_at': completed_at,
                'updated_at': datetime.utcnow().isoformat()
            }
            result = self._db_storage.update_connector(connector_id, updates)
            if result:
                return Connector.from_dict(result)
            return None
        else:
            connector.status = new_status
            connector.completed_at = completed_at
            connector.updated_at = datetime.utcnow().isoformat()
            self._save_registry()
            return connector
    
    def delete_connector(self, connector_id: str) -> bool:
        """Delete a connector."""
        if self._use_database:
            return self._db_storage.delete_connector(connector_id)
        else:
            if connector_id not in self._registry:
                return False
            
            del self._registry[connector_id]
            self._save_registry()
            return True
    
    def get_connector_dir(self, connector_id: str) -> Optional[Path]:
        """Get the directory path for a connector (file-based mode only)."""
        if self._use_database:
            # For database mode, create a temp directory if needed
            temp_dir = Path("/tmp/connectors") / connector_id
            temp_dir.mkdir(parents=True, exist_ok=True)
            return temp_dir
        
        connector = self.get_connector(connector_id)
        if connector:
            return self.base_dir / connector_id
        return None
    
    def get_research_document_path(self, connector_id: str) -> Optional[Path]:
        """Get the path to a connector's research document."""
        connector_dir = self.get_connector_dir(connector_id)
        if connector_dir:
            return connector_dir / f"{connector_id}-research.md"
        return None
    
    def get_research_document(self, connector_id: str) -> Optional[str]:
        """Get the content of a connector's research document."""
        if self._use_database:
            return self._db_storage.get_research_document(connector_id)
        else:
            doc_path = self.get_research_document_path(connector_id)
            if doc_path and doc_path.exists():
                with open(doc_path, 'r') as f:
                    return f.read()
            return None
    
    def save_research_document(
        self,
        connector_id: str,
        content: str,
        claims_json: Optional[Dict[str, Any]] = None,
        canonical_facts_json: Optional[Dict[str, Any]] = None,
        evidence_map_json: Optional[Dict[str, Any]] = None,
        citation_report_json: Optional[Dict[str, Any]] = None,
        citation_overrides_json: Optional[Dict[str, Any]] = None,
        validation_attempts: Optional[int] = None,
        assumptions_section: Optional[str] = None
    ) -> bool:
        """Save research document content with optional claim graph data."""
        if self._use_database:
            return self._db_storage.save_research_document(
                connector_id=connector_id,
                content=content,
                claims_json=claims_json,
                canonical_facts_json=canonical_facts_json,
                evidence_map_json=evidence_map_json,
                citation_report_json=citation_report_json,
                citation_overrides_json=citation_overrides_json,
                validation_attempts=validation_attempts,
                assumptions_section=assumptions_section
            )
        else:
            doc_path = self.get_research_document_path(connector_id)
            if not doc_path:
                return False
            
            try:
                doc_path.parent.mkdir(parents=True, exist_ok=True)
                with open(doc_path, 'w') as f:
                    f.write(content)
                # Note: claim graph data is not saved in file-based mode
                return True
            except Exception as e:
                print(f"Error saving research document: {e}")
                return False
    
    def append_to_research(self, connector_id: str, content: str) -> bool:
        """Append content to a connector's research document."""
        existing = self.get_research_document(connector_id) or ""
        new_content = existing + '\n\n' + content
        return self.save_research_document(connector_id, new_content)


# Singleton instance
_manager: Optional[ConnectorManager] = None


def get_connector_manager() -> ConnectorManager:
    """Get the singleton ConnectorManager instance."""
    global _manager
    if _manager is None:
        _manager = ConnectorManager()
    return _manager
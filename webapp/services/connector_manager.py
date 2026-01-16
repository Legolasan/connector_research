"""
Connector Manager Service
Handles CRUD operations for connector research projects.
"""

import os
import json
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field, asdict
from enum import Enum


class ConnectorStatus(str, Enum):
    """Status of a connector research project."""
    NOT_STARTED = "not_started"
    CLONING = "cloning"
    RESEARCHING = "researching"
    COMPLETE = "complete"
    FAILED = "failed"
    CANCELLED = "cancelled"


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
    total_sections: int = 18
    current_phase: int = 0
    sections_completed: List[int] = field(default_factory=list)
    sections_failed: List[int] = field(default_factory=list)
    current_section_name: str = ""
    research_method: Dict[int, str] = field(default_factory=dict)  # section -> method used
    
    @property
    def percentage(self) -> float:
        if self.total_sections == 0:
            return 0.0
        return (len(self.sections_completed) / self.total_sections) * 100


@dataclass
class Connector:
    """Represents a connector research project."""
    id: str  # slug, e.g., "facebook-ads"
    name: str  # Display name, e.g., "Facebook Ads"
    connector_type: str
    status: str = ConnectorStatus.NOT_STARTED.value
    github_url: Optional[str] = None
    description: str = ""
    
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
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Connector':
        """Create from dictionary."""
        progress_data = data.pop('progress', {})
        progress = ConnectorProgress(**progress_data) if progress_data else ConnectorProgress()
        return cls(progress=progress, **data)


class ConnectorManager:
    """Manages connector research projects."""
    
    def __init__(self, base_dir: Optional[Path] = None):
        """Initialize the connector manager.
        
        Args:
            base_dir: Base directory for connector research files.
                     Defaults to research_docs/connectors/
        """
        if base_dir is None:
            base_dir = Path(__file__).parent.parent.parent / "connectors"
        
        self.base_dir = Path(base_dir)
        self.registry_file = self.base_dir / "_agent" / "connectors_registry.json"
        
        # Ensure directories exist
        self.base_dir.mkdir(parents=True, exist_ok=True)
        (self.base_dir / "_agent").mkdir(exist_ok=True)
        (self.base_dir / "_templates").mkdir(exist_ok=True)
        
        # Load or initialize registry
        self._registry: Dict[str, Connector] = {}
        self._load_registry()
    
    def _load_registry(self):
        """Load connector registry from file."""
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
        """Save connector registry to file."""
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
        # Convert to lowercase, replace spaces with hyphens
        slug = name.lower().strip()
        slug = slug.replace(' ', '-')
        # Remove non-alphanumeric characters except hyphens
        slug = ''.join(c for c in slug if c.isalnum() or c == '-')
        # Remove multiple consecutive hyphens
        while '--' in slug:
            slug = slug.replace('--', '-')
        return slug.strip('-')
    
    def create_connector(
        self,
        name: str,
        connector_type: str,
        github_url: Optional[str] = None,
        description: str = ""
    ) -> Connector:
        """Create a new connector research project.
        
        Args:
            name: Display name for the connector
            connector_type: Type of connector (rest_api, graphql, etc.)
            github_url: Optional GitHub repository URL
            description: Optional description
            
        Returns:
            The created Connector object
            
        Raises:
            ValueError: If connector with same ID already exists
        """
        connector_id = self._generate_id(name)
        
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
            description=description
        )
        
        # Save to registry
        self._registry[connector_id] = connector
        self._save_registry()
        
        # Create empty research document
        self._create_research_document(connector)
        
        return connector
    
    def _create_research_document(self, connector: Connector):
        """Create initial research document from template."""
        template_path = self.base_dir / "_templates" / "connector-research-template.md"
        output_path = self.base_dir / connector.id / f"{connector.id}-research.md"
        
        if template_path.exists():
            with open(template_path, 'r') as f:
                template = f.read()
            
            # Replace placeholders
            content = template.replace('<CONNECTOR_NAME>', connector.name)
            content = content.replace('<DATE>', datetime.utcnow().strftime('%Y-%m-%d'))
            
            # Mark connector type
            type_markers = {
                'rest_api': 'API-based (REST/GraphQL/SOAP)',
                'graphql': 'API-based (REST/GraphQL/SOAP)',
                'soap': 'API-based (REST/GraphQL/SOAP)',
                'jdbc': 'Driver-based (JDBC/ODBC/ADO.NET)',
                'sdk': 'SDK-based (official vendor SDK)',
                'webhook': 'Webhook-only',
                'advertising': 'Advertising Platform (Facebook Ads/Google Ads)',
            }
            
            with open(output_path, 'w') as f:
                f.write(content)
        else:
            # Create minimal document if template doesn't exist
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
        return self._registry.get(connector_id)
    
    def list_connectors(self) -> List[Connector]:
        """List all connectors."""
        return list(self._registry.values())
    
    def update_connector(self, connector_id: str, **updates) -> Optional[Connector]:
        """Update connector properties.
        
        Args:
            connector_id: ID of connector to update
            **updates: Fields to update
            
        Returns:
            Updated Connector or None if not found
        """
        connector = self._registry.get(connector_id)
        if not connector:
            return None
        
        # Update allowed fields
        allowed_fields = {
            'name', 'description', 'status', 'objects_count', 
            'vectors_count', 'fivetran_parity', 'sources', 'completed_at'
        }
        
        for key, value in updates.items():
            if key in allowed_fields and hasattr(connector, key):
                setattr(connector, key, value)
        
        connector.updated_at = datetime.utcnow().isoformat()
        self._save_registry()
        
        return connector
    
    def update_progress(
        self,
        connector_id: str,
        section: int,
        section_name: str = "",
        method: str = "web_search",
        completed: bool = False,
        failed: bool = False
    ) -> Optional[Connector]:
        """Update research progress for a connector.
        
        Args:
            connector_id: ID of connector
            section: Section number (1-18)
            section_name: Name of current section
            method: Research method used (context7, github, web_search)
            completed: Whether section was completed
            failed: Whether section failed
            
        Returns:
            Updated Connector or None if not found
        """
        connector = self._registry.get(connector_id)
        if not connector:
            return None
        
        progress = connector.progress
        progress.current_section = section
        progress.current_section_name = section_name
        progress.research_method[section] = method
        
        # Calculate phase (sections grouped by 3-4)
        phase_map = {
            1: 1, 2: 1, 3: 1,
            4: 2, 5: 2, 6: 2, 7: 2,
            8: 3, 9: 3, 10: 3, 11: 3,
            12: 4, 13: 4, 14: 4,
            15: 5, 16: 5, 17: 5,
            18: 6
        }
        progress.current_phase = phase_map.get(section, 0)
        
        if completed and section not in progress.sections_completed:
            progress.sections_completed.append(section)
            progress.sections_completed.sort()
        
        if failed and section not in progress.sections_failed:
            progress.sections_failed.append(section)
        
        # Update status based on progress
        if len(progress.sections_completed) == progress.total_sections:
            connector.status = ConnectorStatus.COMPLETE.value
            connector.completed_at = datetime.utcnow().isoformat()
        elif len(progress.sections_completed) > 0 or progress.current_section > 0:
            connector.status = ConnectorStatus.RESEARCHING.value
        
        connector.updated_at = datetime.utcnow().isoformat()
        self._save_registry()
        
        return connector
    
    def delete_connector(self, connector_id: str) -> bool:
        """Delete a connector and its files.
        
        Args:
            connector_id: ID of connector to delete
            
        Returns:
            True if deleted, False if not found
        """
        if connector_id not in self._registry:
            return False
        
        # Remove from registry
        del self._registry[connector_id]
        self._save_registry()
        
        # Note: We don't delete the directory to preserve any work
        # User can manually delete if needed
        
        return True
    
    def get_connector_dir(self, connector_id: str) -> Optional[Path]:
        """Get the directory path for a connector."""
        if connector_id in self._registry:
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
        doc_path = self.get_research_document_path(connector_id)
        if doc_path and doc_path.exists():
            with open(doc_path, 'r') as f:
                return f.read()
        return None
    
    def append_to_research(self, connector_id: str, content: str) -> bool:
        """Append content to a connector's research document.
        
        Args:
            connector_id: ID of connector
            content: Content to append
            
        Returns:
            True if successful, False otherwise
        """
        doc_path = self.get_research_document_path(connector_id)
        if not doc_path:
            return False
        
        try:
            with open(doc_path, 'a') as f:
                f.write('\n\n' + content)
            return True
        except Exception as e:
            print(f"Error appending to research: {e}")
            return False


# Singleton instance
_manager: Optional[ConnectorManager] = None


def get_connector_manager() -> ConnectorManager:
    """Get the singleton ConnectorManager instance."""
    global _manager
    if _manager is None:
        _manager = ConnectorManager()
    return _manager

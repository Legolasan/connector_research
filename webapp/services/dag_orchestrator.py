"""
DAG Orchestrator Service
Manages section-level parallelism and agent coordination for research generation.
"""

from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from services.research_agent import ResearchSection


@dataclass
class SectionNode:
    """Node in the research DAG representing a section."""
    section: ResearchSection
    dependencies: List[int]  # Section numbers that must complete first
    status: str = "PENDING"  # "PENDING", "RUNNING", "REVIEWING", "APPROVED", "BLOCKED"
    generated_content: Optional[str] = None
    review: Optional[Any] = None
    stop_the_line: Optional[Any] = None


@dataclass
class ResearchContext:
    """Context for research generation."""
    connector_id: str
    connector_name: str
    connector_type: str
    github_context: str = ""
    hevo_context: Optional[Dict[str, Any]] = None
    fivetran_context: str = ""
    structured_context: Optional[Dict[str, Any]] = None
    completed_sections: List[str] = field(default_factory=list)
    sources: Dict[str, Any] = field(default_factory=dict)


class ResearchDAG:
    """Directed Acyclic Graph for research generation with parallel execution."""
    
    def __init__(self):
        """Initialize the DAG."""
        self.nodes: Dict[int, SectionNode] = {}
        self.edges: List[tuple] = []  # (from_section, to_section)
        self.execution_levels: List[List[int]] = []  # Levels for parallel execution
    
    def add_section(self, section: ResearchSection, dependencies: List[int]):
        """
        Add section node with dependencies.
        
        Args:
            section: ResearchSection to add
            dependencies: List of section numbers that must complete first
        """
        node = SectionNode(
            section=section,
            dependencies=dependencies
        )
        self.nodes[section.number] = node
        
        # Add edges
        for dep in dependencies:
            self.edges.append((dep, section.number))
    
    def calculate_execution_levels(self):
        """
        Calculate which sections can run in parallel.
        
        Uses topological sort to determine execution order.
        """
        self.execution_levels = []
        
        # Build in-degree map
        in_degree: Dict[int, int] = {num: 0 for num in self.nodes.keys()}
        for _, to_node in self.edges:
            in_degree[to_node] = in_degree.get(to_node, 0) + 1
        
        # Find nodes with no dependencies (level 0)
        current_level = [num for num, degree in in_degree.items() if degree == 0]
        
        while current_level:
            self.execution_levels.append(current_level)
            next_level = []
            
            # Process current level
            for node_num in current_level:
                # Find nodes that depend on this one
                for from_node, to_node in self.edges:
                    if from_node == node_num:
                        in_degree[to_node] -= 1
                        if in_degree[to_node] == 0:
                            next_level.append(to_node)
            
            current_level = next_level
    
    def get_level(self, level_index: int) -> List[SectionNode]:
        """Get sections in a specific execution level."""
        if level_index >= len(self.execution_levels):
            return []
        
        section_nums = self.execution_levels[level_index]
        return [self.nodes[num] for num in section_nums]
    
    def get_node(self, section_number: int) -> Optional[SectionNode]:
        """Get node by section number."""
        return self.nodes.get(section_number)
    
    def update_node_status(self, section_number: int, status: str):
        """Update node status."""
        if section_number in self.nodes:
            self.nodes[section_number].status = status
    
    def mark_completed(self, section_number: int, content: str, review: Optional[Any] = None):
        """Mark section as completed."""
        if section_number in self.nodes:
            node = self.nodes[section_number]
            node.status = "APPROVED"
            node.generated_content = content
            node.review = review
    
    def mark_blocked(self, section_number: int, stop_event: Any):
        """Mark section as blocked by stop-the-line."""
        if section_number in self.nodes:
            node = self.nodes[section_number]
            node.status = "BLOCKED"
            node.stop_the_line = stop_event
    
    def get_ready_sections(self) -> List[SectionNode]:
        """Get sections that are ready to execute (dependencies met)."""
        ready = []
        for node in self.nodes.values():
            if node.status == "PENDING":
                # Check if all dependencies are completed
                deps_complete = all(
                    self.nodes[dep].status == "APPROVED"
                    for dep in node.dependencies
                    if dep in self.nodes
                )
                if deps_complete:
                    ready.append(node)
        return ready
    
    def has_blocked_sections(self) -> bool:
        """Check if any sections are blocked."""
        return any(node.status == "BLOCKED" for node in self.nodes.values())
    
    def get_blocked_sections(self) -> List[SectionNode]:
        """Get all blocked sections."""
        return [node for node in self.nodes.values() if node.status == "BLOCKED"]
    
    def all_completed(self) -> bool:
        """Check if all sections are completed."""
        return all(
            node.status in ["APPROVED", "BLOCKED"]
            for node in self.nodes.values()
        )

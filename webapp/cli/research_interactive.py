"""
Interactive CLI for section-by-section research generation with review.
"""

import asyncio
import sys
import os
from typing import Optional, List, Dict, Any, Callable
from pathlib import Path

# Add project root to path for imports
webapp_dir = Path(__file__).parent.parent
project_root = webapp_dir.parent
sys.path.insert(0, str(project_root))

# Always use webapp.* imports (works from both scripts/ and webapp/ directories)
from webapp.services.research_agent import (
    get_research_agent, 
    ResearchAgent,
    ResearchSection,
    BASE_SECTIONS,
    CROSS_CUTTING_SECTIONS,
    FINAL_SECTIONS,
    FUNCTIONAL_SECTIONS,
    OPERATIONAL_SECTIONS,
    create_method_section
)
from webapp.services.connector_manager import get_connector_manager
from webapp.services.knowledge_vault import get_knowledge_vault, KnowledgeSourceType
from webapp.services.llm_crawler_service import get_llm_crawler_service
from webapp.services.doc_registry import get_official_doc_urls
from webapp.services.github_cloner import get_github_cloner
from webapp.cli.interactive_prompts import (
    prompt_section_action,
    prompt_additional_urls,
    prompt_additional_context
)

from rich.console import Console
from rich.panel import Panel
from rich.markdown import Markdown
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.live import Live
from rich.table import Table

console = Console()


class InteractiveLogger:
    """Logger that displays steps in the terminal."""
    
    def __init__(self):
        self.steps = []
    
    def log(self, step: str, details: str = ""):
        """Log a step."""
        self.steps.append((step, details))
        console.print(f"  {step}", style="cyan")
        if details:
            console.print(f"    {details}", style="dim")
    
    def clear(self):
        """Clear logged steps."""
        self.steps = []


async def run_interactive_research(
    connector_name: str,
    connector_type: str = "auto",
    github_url: Optional[str] = None,
    doc_urls: Optional[List[str]] = None,
    output_file: Optional[str] = None
):
    """Run research generation with interactive section review."""
    
    logger = InteractiveLogger()
    
    # Initialize services
    console.print(f"\n[bold blue]Initializing services for {connector_name}...[/bold blue]")
    research_agent = get_research_agent()
    connector_manager = get_connector_manager()
    knowledge_vault = get_knowledge_vault()
    llm_crawler = get_llm_crawler_service()
    github_cloner = get_github_cloner() if github_url else None
    
    # Set up logging callback
    research_agent.log_callback = logger.log
    
    # Step 1: Crawl and index documentation
    console.print("\n[bold blue]Step 1: Crawling and indexing documentation...[/bold blue]")
    
    urls_to_crawl = doc_urls or get_official_doc_urls(connector_name)
    
    if urls_to_crawl and llm_crawler and llm_crawler.is_available:
        logger.log("🕷️ Starting LLM Crawler", f"{len(urls_to_crawl)} URLs")
        
        crawl_result = await llm_crawler.crawl_urls(
            urls=urls_to_crawl,
            connector_name=connector_name,
            depth=2,
            max_pages=50
        )
        
        if crawl_result.chunks and knowledge_vault:
            logger.log("📚 Indexing chunks into Knowledge Vault", f"{len(crawl_result.chunks)} chunks")
            
            chunks_indexed = 0
            for chunk in crawl_result.chunks:
                knowledge_vault.index_text(
                    connector_name=connector_name,
                    title=chunk.heading_context or chunk.title or f"Chunk {chunk.position}",
                    content=chunk.content,
                    source_type="official_docs",
                    source_url=chunk.url
                )
                chunks_indexed += 1
            
            logger.log("✅ Indexing complete", f"{chunks_indexed} chunks indexed")
        else:
            logger.log("⚠️ No chunks to index", "")
    else:
        logger.log("⚠️ Skipping crawl", "No URLs provided or crawler unavailable")
    
    # Step 2: Clone GitHub repo if provided
    github_context = None
    github_context_str = ""
    if github_url and github_cloner:
        console.print("\n[bold blue]Step 2: Cloning GitHub repository...[/bold blue]")
        logger.log("📦 Cloning repository", github_url)
        
        extracted = await github_cloner.clone_and_extract(github_url, "interactive")
        if extracted:
            github_context = extracted.to_dict()
            # Build GitHub context string
            is_structured = github_context.get('structure_type') == 'structured'
            github_context_str = research_agent._build_github_context_string(
                github_context,
                is_structured,
                None
            )
            logger.log("✅ Repository analyzed", f"{len(github_context.get('files', []))} files")
        else:
            logger.log("⚠️ Repository cloning failed", "")
    
    # Step 3: Generate sections interactively
    console.print("\n[bold green]Step 3: Generating research sections interactively...[/bold green]")
    
    document_parts = []
    discovered_methods = []
    
    # Phase 1: Base sections
    console.print("\n[bold yellow]Phase 1: Platform Discovery[/bold yellow]")
    
    for section in BASE_SECTIONS:
        section_content = await generate_section_interactive(
            research_agent=research_agent,
            section=section,
            connector_name=connector_name,
            connector_type=connector_type,
            github_context_str=github_context_str,
            logger=logger,
            previous_sections=document_parts[-3:] if document_parts else []
        )
        
        if section_content:
            document_parts.append(section_content)
            
            # Parse discovered methods from Section 2
            if section.number == 2:
                discovered_methods = research_agent._parse_discovered_methods(section_content)
                known_methods = research_agent._get_known_connector_methods(connector_name)
                if known_methods:
                    for method in known_methods:
                        if method not in discovered_methods:
                            discovered_methods.append(method)
                    method_order = ["REST API", "GraphQL API", "Webhooks", "Bulk/Batch API", "Official SDK", "SOAP/XML API", "JDBC/ODBC", "File Export"]
                    discovered_methods = sorted(discovered_methods, key=lambda x: method_order.index(x) if x in method_order else 999)
                console.print(f"\n[bold]Discovered methods: {', '.join(discovered_methods)}[/bold]")
    
    # Phase 2: Method deep dives
    if discovered_methods:
        console.print(f"\n[bold yellow]Phase 2: Extraction Methods ({len(discovered_methods)} methods)[/bold yellow]")
        
        method_section_number = 4
        for method in discovered_methods:
            method_section = create_method_section(method, method_section_number)
            
            section_content = await generate_section_interactive(
                research_agent=research_agent,
                section=method_section,
                connector_name=connector_name,
                connector_type=connector_type,
                github_context_str=github_context_str,
                logger=logger,
                previous_sections=document_parts[-3:] if document_parts else []
            )
            
            if section_content:
                document_parts.append(section_content)
            
            method_section_number += 1
    
    # Phase 3: Cross-cutting sections
    console.print(f"\n[bold yellow]Phase 3: Cross-Cutting Sections[/bold yellow]")
    
    for section in CROSS_CUTTING_SECTIONS:
        section_content = await generate_section_interactive(
            research_agent=research_agent,
            section=section,
            connector_name=connector_name,
            connector_type=connector_type,
            github_context_str=github_context_str,
            logger=logger,
            previous_sections=document_parts[-3:] if document_parts else []
        )
        
        if section_content:
            document_parts.append(section_content)
    
    # Phase 4: Final sections
    console.print(f"\n[bold yellow]Phase 4: Final Sections[/bold yellow]")
    
    for section in FINAL_SECTIONS:
        section_content = await generate_section_interactive(
            research_agent=research_agent,
            section=section,
            connector_name=connector_name,
            connector_type=connector_type,
            github_context_str=github_context_str,
            logger=logger,
            previous_sections=document_parts[-3:] if document_parts else []
        )
        
        if section_content:
            document_parts.append(section_content)
    
    # Generate final document
    console.print("\n[bold green]Generating final document...[/bold green]")
    
    final_document = "\n\n".join(document_parts)
    
    # Save to file if specified
    if output_file:
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(final_document, encoding='utf-8')
        console.print(f"\n[bold green]✅ Document saved to: {output_file}[/bold green]")
    else:
        # Default output
        default_output = f"{connector_name.lower().replace(' ', '_')}_research.md"
        Path(default_output).write_text(final_document, encoding='utf-8')
        console.print(f"\n[bold green]✅ Document saved to: {default_output}[/bold green]")
    
    console.print(f"\n[bold]Research generation complete![/bold]")
    console.print(f"Total sections: {len(document_parts)}")
    console.print(f"Total length: {len(final_document)} characters")


async def generate_section_interactive(
    research_agent: ResearchAgent,
    section: ResearchSection,
    connector_name: str,
    connector_type: str,
    github_context_str: str,
    logger: InteractiveLogger,
    previous_sections: List[str],
    additional_urls: Optional[List[str]] = None,
    additional_context: Optional[str] = None
) -> Optional[str]:
    """Generate a section with interactive review."""
    
    console.print(f"\n[bold yellow]Generating Section {section.number}: {section.name}[/bold yellow]")
    
    logger.clear()
    
    # Generate section
    result = await research_agent._generate_and_review_section(
        section=section,
        connector_name=connector_name,
        connector_type=connector_type,
        github_context=github_context_str,
        hevo_context=None,
        fivetran_context="",
        structured_context=None,
        previous_sections=previous_sections
    )
    
    section_content = result.get("content", "")
    
    if not section_content:
        console.print("[red]⚠️ Section generation failed[/red]")
        return None
    
    # Show section content
    console.print("\n" + "="*80)
    console.print(Panel(
        Markdown(section_content[:5000]),  # Limit display to first 5000 chars
        title=f"Section {section.number}: {section.name}",
        border_style="blue"
    ))
    
    if len(section_content) > 5000:
        console.print(f"[dim]... (showing first 5000 of {len(section_content)} characters)[/dim]")
    
    console.print("="*80)
    
    # Interactive prompt
    while True:
        action = prompt_section_action()
        
        if action == "approve":
            console.print("[green]✅ Section approved[/green]")
            return section_content
        
        elif action == "reject":
            console.print("[yellow]🔄 Regenerating section...[/yellow]")
            logger.clear()
            
            result = await research_agent._generate_and_review_section(
                section=section,
                connector_name=connector_name,
                connector_type=connector_type,
                github_context=github_context_str,
                hevo_context=None,
                fivetran_context="",
                structured_context=None,
                previous_sections=previous_sections
            )
            
            section_content = result.get("content", "")
            
            if section_content:
                console.print(Panel(
                    Markdown(section_content[:5000]),
                    title=f"Section {section.number}: {section.name} (Regenerated)",
                    border_style="yellow"
                ))
            continue
        
        elif action == "refine":
            console.print("[yellow]🔧 Refining section with additional context...[/yellow]")
            
            urls = prompt_additional_urls()
            context = prompt_additional_context()
            
            # TODO: Implement refinement with additional URLs/context
            # For now, regenerate with note about additional context
            logger.clear()
            logger.log("🔍 Crawling additional URLs", f"{len(urls)} URLs")
            
            # Crawl additional URLs if provided
            if urls and research_agent.llm_crawler and research_agent.llm_crawler.is_available:
                for url in urls:
                    crawl_result = await research_agent.llm_crawler.crawl_single_url(
                        url=url,
                        connector_name=connector_name,
                        depth=0
                    )
                    if crawl_result and crawl_result.total_content:
                        # Index into vault
                        if research_agent.knowledge_vault:
                            for chunk in crawl_result.chunks:
                                research_agent.knowledge_vault.index_text(
                                    connector_name=connector_name,
                                    title=chunk.title,
                                    content=chunk.content,
                                    source_type="official_docs",
                                    source_url=chunk.url
                                )
            
            # Regenerate with additional context
            logger.log("🤖 Regenerating with additional context", "")
            
            result = await research_agent._generate_and_review_section(
                section=section,
                connector_name=connector_name,
                connector_type=connector_type,
                github_context=github_context_str,
                hevo_context=None,
                fivetran_context=context if context else "",
                structured_context=None,
                previous_sections=previous_sections
            )
            
            section_content = result.get("content", "")
            
            if section_content:
                console.print(Panel(
                    Markdown(section_content[:5000]),
                    title=f"Section {section.number}: {section.name} (Refined)",
                    border_style="green"
                ))
            continue
        
        elif action == "skip":
            console.print("[blue]⏭️ Section skipped[/blue]")
            return "[Section skipped by user]"

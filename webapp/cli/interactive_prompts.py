"""
Interactive prompt handlers for section-by-section research review.
"""

from typing import List
from rich.prompt import Prompt
from rich.console import Console

console = Console()


def prompt_section_action() -> str:
    """Prompt user for section action."""
    console.print("\n[bold]Section Review Options:[/bold]")
    console.print("  [green]y[/green] - Approve and continue")
    console.print("  [red]n[/red] - Reject and regenerate")
    console.print("  [yellow]r[/yellow] - Refine (provide additional URLs/context)")
    console.print("  [blue]s[/blue] - Skip this section")
    
    choice = Prompt.ask(
        "\n[bold]Action[/bold]",
        choices=["y", "n", "r", "s"],
        default="y"
    )
    
    return {
        "y": "approve",
        "n": "reject", 
        "r": "refine",
        "s": "skip"
    }[choice]


def prompt_additional_urls() -> List[str]:
    """Prompt for additional documentation URLs."""
    console.print("\n[bold yellow]Provide additional documentation URLs:[/bold yellow]")
    console.print("(Enter one URL per line, empty line to finish)")
    
    urls = []
    while True:
        url = Prompt.ask("URL", default="")
        if not url:
            break
        urls.append(url)
    
    return urls


def prompt_additional_context() -> str:
    """Prompt for additional context/notes."""
    context = Prompt.ask(
        "\n[bold yellow]Additional context or notes:[/bold yellow]",
        default=""
    )
    
    return context

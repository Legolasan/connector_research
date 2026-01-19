#!/usr/bin/env python3
"""
Interactive CLI for connector research generation.

Usage:
    python scripts/research_cli.py Zendesk --interactive
    python scripts/research_cli.py Shopify --interactive --github-url https://github.com/...
    python scripts/research_cli.py GitHub --interactive --doc-urls https://docs.github.com/...
"""

import asyncio
import argparse
import sys
import os
from pathlib import Path

# Add webapp to path and change to project root
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
os.chdir(project_root)

# Import after path setup
from webapp.cli.research_interactive import run_interactive_research


def main():
    parser = argparse.ArgumentParser(
        description="Interactive connector research generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Interactive mode with default settings
  python scripts/research_cli.py Zendesk --interactive
  
  # With GitHub repository
  python scripts/research_cli.py Shopify --interactive --github-url https://github.com/shopify/shopify-api-ruby
  
  # With custom documentation URLs
  python scripts/research_cli.py CustomConnector --interactive --doc-urls https://docs.example.com/api https://docs.example.com/auth
  
  # Specify output file
  python scripts/research_cli.py Zendesk --interactive --output zendesk_research.md
        """
    )
    
    parser.add_argument(
        "connector_name",
        help="Name of the connector to research (e.g., Zendesk, Shopify)"
    )
    
    parser.add_argument(
        "--interactive",
        action="store_true",
        default=True,
        help="Enable interactive mode (default: True)"
    )
    
    parser.add_argument(
        "--connector-type",
        default="auto",
        help="Connector type (default: auto)"
    )
    
    parser.add_argument(
        "--github-url",
        help="GitHub repository URL for code analysis"
    )
    
    parser.add_argument(
        "--doc-urls",
        nargs="+",
        help="Documentation URLs to crawl (space-separated)"
    )
    
    parser.add_argument(
        "--output",
        "-o",
        help="Output file path (default: {connector_name}_research.md)"
    )
    
    args = parser.parse_args()
    
    if args.interactive:
        asyncio.run(run_interactive_research(
            connector_name=args.connector_name,
            connector_type=args.connector_type,
            github_url=args.github_url,
            doc_urls=args.doc_urls,
            output_file=args.output
        ))
    else:
        print("Non-interactive mode not yet implemented. Use --interactive (default).")
        sys.exit(1)


if __name__ == "__main__":
    main()

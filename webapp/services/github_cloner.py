"""
GitHub Cloner Service
Clones repositories and extracts relevant code patterns for connector research.
"""

import os
import re
import subprocess
import shutil
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class CodePattern:
    """Represents a code pattern extracted from source files."""
    file_path: str
    pattern_type: str  # 'class', 'method', 'enum', 'api_endpoint', 'auth', 'object'
    name: str
    content: str
    line_number: int = 0
    language: str = "unknown"
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExtractedCode:
    """Results of code extraction from a repository."""
    repo_url: str
    repo_name: str
    clone_path: str
    languages_detected: List[str] = field(default_factory=list)
    patterns: List[CodePattern] = field(default_factory=list)
    api_endpoints: List[str] = field(default_factory=list)
    object_types: List[str] = field(default_factory=list)
    auth_patterns: List[str] = field(default_factory=list)
    readme_content: str = ""
    extracted_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'repo_url': self.repo_url,
            'repo_name': self.repo_name,
            'clone_path': self.clone_path,
            'languages_detected': self.languages_detected,
            'patterns': [
                {
                    'file_path': p.file_path,
                    'pattern_type': p.pattern_type,
                    'name': p.name,
                    'content': p.content[:500] + '...' if len(p.content) > 500 else p.content,
                    'line_number': p.line_number,
                    'language': p.language
                }
                for p in self.patterns[:50]  # Limit to first 50 patterns
            ],
            'api_endpoints': self.api_endpoints[:100],
            'object_types': self.object_types[:100],
            'auth_patterns': self.auth_patterns,
            'readme_summary': self.readme_content[:2000] if self.readme_content else "",
            'extracted_at': self.extracted_at
        }


class GitHubCloner:
    """Clones GitHub repositories and extracts code patterns."""
    
    # File extensions to analyze
    CODE_EXTENSIONS = {
        '.java': 'java',
        '.py': 'python',
        '.js': 'javascript',
        '.ts': 'typescript',
        '.go': 'go',
        '.rb': 'ruby',
        '.cs': 'csharp',
        '.php': 'php'
    }
    
    # Patterns to look for
    API_ENDPOINT_PATTERNS = [
        r'["\']/(api|v\d+)/[\w/{}]+["\']',  # /api/v1/users
        r'https?://[^\s"\']+/[\w/{}]+',  # Full URLs
        r'@(Get|Post|Put|Delete|Patch)Mapping\(["\']([^"\']+)',  # Spring annotations
        r'@app\.(get|post|put|delete|patch)\(["\']([^"\']+)',  # Flask/FastAPI
        r'router\.(get|post|put|delete)\(["\']([^"\']+)',  # Express.js
    ]
    
    AUTH_PATTERNS = [
        r'oauth',
        r'api[_-]?key',
        r'bearer',
        r'authorization',
        r'token',
        r'client[_-]?id',
        r'client[_-]?secret',
        r'credentials',
    ]
    
    OBJECT_PATTERNS = [
        r'class\s+(\w+)(Record|Entity|Model|Object|Resource|Type)',
        r'interface\s+(\w+)(Record|Entity|Model|Object|Resource)',
        r'type\s+(\w+)(Record|Entity|Model|Object|Resource)',
        r'enum\s+(\w+)(Type|Status|Category)',
    ]
    
    def __init__(self, base_dir: Optional[Path] = None):
        """Initialize the cloner.
        
        Args:
            base_dir: Base directory for cloned repos.
        """
        if base_dir is None:
            base_dir = Path(__file__).parent.parent.parent / "connectors"
        self.base_dir = Path(base_dir)
    
    def _parse_github_url(self, url: str) -> Tuple[str, str]:
        """Parse GitHub URL to extract owner and repo name.
        
        Args:
            url: GitHub repository URL
            
        Returns:
            Tuple of (owner, repo_name)
        """
        # Handle various GitHub URL formats
        patterns = [
            r'github\.com[/:]([^/]+)/([^/\.]+)',  # https://github.com/owner/repo or git@github.com:owner/repo
            r'github\.com/([^/]+)/([^/]+)\.git',  # With .git extension
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1), match.group(2).replace('.git', '')
        
        raise ValueError(f"Could not parse GitHub URL: {url}")
    
    def clone_repo(self, github_url: str, connector_id: str) -> Path:
        """Clone a GitHub repository.
        
        Args:
            github_url: URL of the GitHub repository
            connector_id: ID of the connector (for directory naming)
            
        Returns:
            Path to the cloned repository
        """
        owner, repo_name = self._parse_github_url(github_url)
        
        # Create target directory
        target_dir = self.base_dir / connector_id / "sources" / repo_name
        
        # Remove existing directory if present
        if target_dir.exists():
            shutil.rmtree(target_dir)
        
        target_dir.parent.mkdir(parents=True, exist_ok=True)
        
        # Clone the repository (shallow clone for speed)
        try:
            subprocess.run(
                ['git', 'clone', '--depth', '1', github_url, str(target_dir)],
                check=True,
                capture_output=True,
                text=True
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to clone repository: {e.stderr}")
        
        return target_dir
    
    def extract_patterns(self, repo_path: Path) -> ExtractedCode:
        """Extract code patterns from a cloned repository.
        
        Args:
            repo_path: Path to the cloned repository
            
        Returns:
            ExtractedCode object with all extracted patterns
        """
        repo_name = repo_path.name
        
        result = ExtractedCode(
            repo_url="",
            repo_name=repo_name,
            clone_path=str(repo_path)
        )
        
        # Read README if present
        for readme_name in ['README.md', 'README.rst', 'README.txt', 'README']:
            readme_path = repo_path / readme_name
            if readme_path.exists():
                try:
                    result.readme_content = readme_path.read_text(errors='ignore')
                except Exception:
                    pass
                break
        
        # Walk through all files
        for file_path in repo_path.rglob('*'):
            if not file_path.is_file():
                continue
            
            # Skip hidden directories and common non-code directories
            if any(part.startswith('.') for part in file_path.parts):
                continue
            if any(part in ['node_modules', 'vendor', 'dist', 'build', '__pycache__', '.git'] 
                   for part in file_path.parts):
                continue
            
            # Check file extension
            ext = file_path.suffix.lower()
            if ext not in self.CODE_EXTENSIONS:
                continue
            
            language = self.CODE_EXTENSIONS[ext]
            if language not in result.languages_detected:
                result.languages_detected.append(language)
            
            # Read and analyze file
            try:
                content = file_path.read_text(errors='ignore')
                self._extract_from_file(
                    content, 
                    str(file_path.relative_to(repo_path)),
                    language,
                    result
                )
            except Exception as e:
                print(f"Warning: Could not read {file_path}: {e}")
        
        return result
    
    def _extract_from_file(
        self, 
        content: str, 
        file_path: str, 
        language: str,
        result: ExtractedCode
    ):
        """Extract patterns from a single file.
        
        Args:
            content: File content
            file_path: Relative path to file
            language: Programming language
            result: ExtractedCode object to populate
        """
        lines = content.split('\n')
        
        # Extract API endpoints
        for pattern in self.API_ENDPOINT_PATTERNS:
            for match in re.finditer(pattern, content, re.IGNORECASE):
                endpoint = match.group(0)
                if endpoint not in result.api_endpoints:
                    result.api_endpoints.append(endpoint)
        
        # Extract auth patterns
        for pattern in self.AUTH_PATTERNS:
            if re.search(pattern, content, re.IGNORECASE):
                if pattern not in result.auth_patterns:
                    result.auth_patterns.append(pattern)
        
        # Extract object/class names
        for pattern in self.OBJECT_PATTERNS:
            for match in re.finditer(pattern, content):
                obj_name = match.group(1) + (match.group(2) if len(match.groups()) > 1 else '')
                if obj_name not in result.object_types:
                    result.object_types.append(obj_name)
        
        # Language-specific extraction
        if language == 'java':
            self._extract_java_patterns(content, file_path, lines, result)
        elif language == 'python':
            self._extract_python_patterns(content, file_path, lines, result)
        elif language in ['javascript', 'typescript']:
            self._extract_js_patterns(content, file_path, lines, result)
    
    def _extract_java_patterns(
        self, 
        content: str, 
        file_path: str, 
        lines: List[str],
        result: ExtractedCode
    ):
        """Extract Java-specific patterns."""
        # Find class definitions
        class_pattern = r'(?:public\s+)?(?:abstract\s+)?(?:class|interface|enum)\s+(\w+)'
        for match in re.finditer(class_pattern, content):
            line_num = content[:match.start()].count('\n') + 1
            class_name = match.group(1)
            
            # Get class content (simplified - up to closing brace)
            start_idx = match.start()
            brace_count = 0
            end_idx = start_idx
            found_brace = False
            
            for i, char in enumerate(content[start_idx:start_idx + 5000]):
                if char == '{':
                    brace_count += 1
                    found_brace = True
                elif char == '}':
                    brace_count -= 1
                    if found_brace and brace_count == 0:
                        end_idx = start_idx + i + 1
                        break
            
            class_content = content[start_idx:end_idx] if end_idx > start_idx else content[start_idx:start_idx + 500]
            
            result.patterns.append(CodePattern(
                file_path=file_path,
                pattern_type='class',
                name=class_name,
                content=class_content,
                line_number=line_num,
                language='java'
            ))
        
        # Find enum values (often represent object types)
        enum_pattern = r'enum\s+(\w+)\s*\{([^}]+)\}'
        for match in re.finditer(enum_pattern, content):
            enum_name = match.group(1)
            enum_values = [v.strip().split('(')[0] for v in match.group(2).split(',') if v.strip()]
            
            for value in enum_values:
                if value and not value.startswith('//'):
                    clean_value = value.strip()
                    if clean_value and clean_value not in result.object_types:
                        result.object_types.append(f"{enum_name}.{clean_value}")
    
    def _extract_python_patterns(
        self, 
        content: str, 
        file_path: str, 
        lines: List[str],
        result: ExtractedCode
    ):
        """Extract Python-specific patterns."""
        # Find class definitions
        class_pattern = r'class\s+(\w+)(?:\([^)]*\))?:'
        for match in re.finditer(class_pattern, content):
            line_num = content[:match.start()].count('\n') + 1
            class_name = match.group(1)
            
            # Get class docstring and methods
            class_start = match.start()
            next_class = re.search(r'\nclass\s+', content[class_start + 10:])
            class_end = class_start + next_class.start() if next_class else min(class_start + 2000, len(content))
            
            result.patterns.append(CodePattern(
                file_path=file_path,
                pattern_type='class',
                name=class_name,
                content=content[class_start:class_end],
                line_number=line_num,
                language='python'
            ))
        
        # Find API endpoint decorators
        endpoint_pattern = r'@\w+\.(get|post|put|delete|patch)\(["\']([^"\']+)'
        for match in re.finditer(endpoint_pattern, content, re.IGNORECASE):
            endpoint = f"{match.group(1).upper()} {match.group(2)}"
            if endpoint not in result.api_endpoints:
                result.api_endpoints.append(endpoint)
    
    def _extract_js_patterns(
        self, 
        content: str, 
        file_path: str, 
        lines: List[str],
        result: ExtractedCode
    ):
        """Extract JavaScript/TypeScript patterns."""
        # Find class and interface definitions
        class_pattern = r'(?:export\s+)?(?:class|interface|type)\s+(\w+)'
        for match in re.finditer(class_pattern, content):
            line_num = content[:match.start()].count('\n') + 1
            name = match.group(1)
            
            result.patterns.append(CodePattern(
                file_path=file_path,
                pattern_type='class',
                name=name,
                content=content[match.start():match.start() + 500],
                line_number=line_num,
                language='javascript'
            ))
        
        # Find exports (often API objects)
        export_pattern = r'export\s+(?:const|let|var|function)\s+(\w+)'
        for match in re.finditer(export_pattern, content):
            name = match.group(1)
            if name not in result.object_types:
                result.object_types.append(name)
    
    async def clone_and_extract(
        self, 
        github_url: str, 
        connector_id: str
    ) -> ExtractedCode:
        """Clone a repository and extract all patterns.
        
        Args:
            github_url: URL of the GitHub repository
            connector_id: ID of the connector
            
        Returns:
            ExtractedCode object with all extracted information
        """
        # Clone the repository
        repo_path = self.clone_repo(github_url, connector_id)
        
        # Extract patterns
        result = self.extract_patterns(repo_path)
        result.repo_url = github_url
        
        return result


# Singleton instance
_cloner: Optional[GitHubCloner] = None


def get_github_cloner() -> GitHubCloner:
    """Get the singleton GitHubCloner instance."""
    global _cloner
    if _cloner is None:
        _cloner = GitHubCloner()
    return _cloner

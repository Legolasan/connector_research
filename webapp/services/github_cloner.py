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
class ImplementationContext:
    """Context extracted from Connector_Code folder - your implementation."""
    api_calls: List[str] = field(default_factory=list)           # HTTP calls made
    models: List[str] = field(default_factory=list)              # Data models/classes
    auth_implementation: str = ""                                 # How auth is implemented
    sync_patterns: List[str] = field(default_factory=list)       # Sync/pagination patterns found
    config_patterns: List[str] = field(default_factory=list)     # Configuration patterns
    error_handling: List[str] = field(default_factory=list)      # Error handling patterns
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'api_calls': self.api_calls[:50],
            'models': self.models[:50],
            'auth_implementation': self.auth_implementation[:2000],
            'sync_patterns': self.sync_patterns[:20],
            'config_patterns': self.config_patterns[:20],
            'error_handling': self.error_handling[:20]
        }


@dataclass
class SDKContext:
    """Context extracted from Connector_SDK folder - vendor SDK source."""
    sdk_name: str = ""
    available_methods: List[str] = field(default_factory=list)   # Public methods
    data_types: List[str] = field(default_factory=list)          # Types/models exposed
    auth_methods: List[str] = field(default_factory=list)        # Auth helpers
    client_classes: List[str] = field(default_factory=list)      # Client/service classes
    constants: List[str] = field(default_factory=list)           # Constants/enums
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'sdk_name': self.sdk_name,
            'available_methods': self.available_methods[:100],
            'data_types': self.data_types[:100],
            'auth_methods': self.auth_methods[:50],
            'client_classes': self.client_classes[:50],
            'constants': self.constants[:50]
        }


@dataclass
class DocumentationContext:
    """Context extracted from Public_Documentation folder - official docs."""
    api_reference: str = ""             # Extracted API docs
    auth_guide: str = ""                # Auth documentation
    rate_limits: str = ""               # Rate limit info
    objects_schema: str = ""            # Object definitions
    endpoints_list: List[str] = field(default_factory=list)  # Documented endpoints
    permissions: List[str] = field(default_factory=list)     # Required permissions
    raw_content: str = ""               # Full raw content for reference
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'api_reference': self.api_reference[:3000],
            'auth_guide': self.auth_guide[:2000],
            'rate_limits': self.rate_limits[:1000],
            'objects_schema': self.objects_schema[:3000],
            'endpoints_list': self.endpoints_list[:100],
            'permissions': self.permissions[:50],
            'raw_content': self.raw_content[:5000]
        }


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
    
    # New structured fields for organized repositories
    structure_type: str = "flat"  # 'flat' or 'structured'
    implementation: Optional[ImplementationContext] = None
    sdk: Optional[SDKContext] = None
    documentation: Optional[DocumentationContext] = None
    
    def to_dict(self) -> Dict[str, Any]:
        result = {
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
            'extracted_at': self.extracted_at,
            'structure_type': self.structure_type
        }
        
        # Add structured context if available
        if self.implementation:
            result['implementation'] = self.implementation.to_dict()
        if self.sdk:
            result['sdk'] = self.sdk.to_dict()
        if self.documentation:
            result['documentation'] = self.documentation.to_dict()
        
        return result


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
    
    # Structured repository folder names
    STRUCTURED_FOLDERS = {
        'implementation': ['Connector_Code', 'connector_code', 'src', 'implementation'],
        'sdk': ['Connector_SDK', 'connector_sdk', 'sdk', 'vendor'],
        'documentation': ['Public_Documentation', 'public_documentation', 'docs', 'documentation']
    }
    
    # Patterns for implementation code analysis
    HTTP_CALL_PATTERNS = [
        r'requests\.(get|post|put|delete|patch)\s*\(',
        r'http[cC]lient\.(get|post|put|delete|patch)\s*\(',
        r'fetch\s*\(["\'][^"\']+["\']',
        r'axios\.(get|post|put|delete|patch)\s*\(',
        r'HttpClient\.new[A-Z]\w*\(',
        r'\.execute\s*\(\s*["\']?(GET|POST|PUT|DELETE|PATCH)',
        r'WebClient\.\w+\(\)',
        r'RestTemplate\.\w+\(',
    ]
    
    SYNC_PATTERN_MARKERS = [
        r'(next_?page|nextPage|page_?token|pageToken)',
        r'(offset|limit|skip|take)',
        r'(cursor|after|before)',
        r'(hasMore|has_more|moreResults)',
        r'(lastModified|last_modified|updated_?at|updatedAt)',
        r'(sync|incremental|delta|checkpoint)',
    ]
    
    ERROR_HANDLING_PATTERNS = [
        r'except\s+(\w+Error|\w+Exception)',
        r'catch\s*\(\s*(\w+Error|\w+Exception)',
        r'on\s+(\w+Error|\w+Exception)',
        r'\.catch\s*\(',
        r'try\s*{',
        r'retry|backoff|exponential',
    ]
    
    # Documentation keyword patterns
    DOC_SECTION_KEYWORDS = {
        'auth': ['authentication', 'authorization', 'oauth', 'api key', 'token', 'credentials', 'login', 'sign in'],
        'rate_limits': ['rate limit', 'throttl', 'quota', 'requests per', 'calls per', 'limit exceed'],
        'objects': ['object', 'entity', 'resource', 'schema', 'model', 'field', 'property', 'attribute'],
        'endpoints': ['endpoint', 'api reference', 'method', 'request', 'response', 'url', 'path'],
        'permissions': ['permission', 'scope', 'role', 'access', 'privilege', 'grant']
    }
    
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
    
    def _check_git_available(self) -> bool:
        """Check if git is available on the system."""
        try:
            subprocess.run(
                ['git', '--version'],
                check=True,
                capture_output=True,
                text=True
            )
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False
    
    def clone_repo(self, github_url: str, connector_id: str) -> Optional[Path]:
        """Clone a GitHub repository.
        
        Args:
            github_url: URL of the GitHub repository
            connector_id: ID of the connector (for directory naming)
            
        Returns:
            Path to the cloned repository, or None if git is not available
        """
        # Check if git is available
        if not self._check_git_available():
            print("⚠ Git is not installed. Skipping repository clone.")
            print("  Research will continue using web search only.")
            return None
        
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
            print(f"⚠ Failed to clone repository: {e.stderr}")
            return None
        
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
    
    # =====================
    # Structured Repository Detection & Extraction
    # =====================
    
    def _detect_structure(self, repo_path: Path) -> Tuple[str, Dict[str, Optional[Path]]]:
        """Detect if repository follows the structured format.
        
        Args:
            repo_path: Path to the cloned repository
            
        Returns:
            Tuple of (structure_type, folder_paths)
        """
        folder_paths = {
            'implementation': None,
            'sdk': None,
            'documentation': None
        }
        
        # Check for each folder type
        for folder_type, possible_names in self.STRUCTURED_FOLDERS.items():
            for name in possible_names:
                folder_path = repo_path / name
                if folder_path.exists() and folder_path.is_dir():
                    folder_paths[folder_type] = folder_path
                    break
        
        # Determine structure type
        found_count = sum(1 for p in folder_paths.values() if p is not None)
        structure_type = 'structured' if found_count >= 2 else 'flat'
        
        return structure_type, folder_paths
    
    def _extract_implementation_context(self, impl_path: Path) -> ImplementationContext:
        """Extract context from Connector_Code folder.
        
        Args:
            impl_path: Path to the implementation folder
            
        Returns:
            ImplementationContext with extracted patterns
        """
        context = ImplementationContext()
        auth_code_snippets = []
        
        for file_path in impl_path.rglob('*'):
            if not file_path.is_file():
                continue
            
            ext = file_path.suffix.lower()
            if ext not in self.CODE_EXTENSIONS:
                continue
            
            try:
                content = file_path.read_text(errors='ignore')
                rel_path = str(file_path.relative_to(impl_path))
                
                # Extract HTTP API calls
                for pattern in self.HTTP_CALL_PATTERNS:
                    for match in re.finditer(pattern, content, re.IGNORECASE):
                        call_context = content[max(0, match.start()-50):match.end()+100]
                        if call_context not in context.api_calls:
                            context.api_calls.append(f"{rel_path}: {call_context.strip()}")
                
                # Extract data models/classes
                class_pattern = r'(?:class|interface|type|struct)\s+(\w+)'
                for match in re.finditer(class_pattern, content):
                    model_name = match.group(1)
                    if model_name not in context.models:
                        context.models.append(model_name)
                
                # Extract auth implementation
                auth_keywords = ['auth', 'oauth', 'token', 'credential', 'api_key', 'apikey']
                if any(kw in content.lower() for kw in auth_keywords):
                    if any(kw in file_path.name.lower() for kw in auth_keywords):
                        auth_code_snippets.append(f"--- {rel_path} ---\n{content[:2000]}")
                
                # Extract sync/pagination patterns
                for pattern in self.SYNC_PATTERN_MARKERS:
                    if re.search(pattern, content, re.IGNORECASE):
                        match = re.search(pattern, content, re.IGNORECASE)
                        if match:
                            pattern_context = content[max(0, match.start()-30):match.end()+50]
                            if pattern_context not in context.sync_patterns:
                                context.sync_patterns.append(f"{rel_path}: {pattern_context.strip()}")
                
                # Extract error handling patterns
                for pattern in self.ERROR_HANDLING_PATTERNS:
                    for match in re.finditer(pattern, content, re.IGNORECASE):
                        error_context = content[max(0, match.start()-20):match.end()+80]
                        if error_context not in context.error_handling:
                            context.error_handling.append(error_context.strip())
                
                # Extract config patterns
                config_pattern = r'(config|setting|option|param)\s*[=:]\s*["\']?[\w.]+["\']?'
                for match in re.finditer(config_pattern, content, re.IGNORECASE):
                    if match.group(0) not in context.config_patterns:
                        context.config_patterns.append(match.group(0))
                        
            except Exception as e:
                print(f"Warning: Could not process {file_path}: {e}")
        
        # Combine auth snippets
        context.auth_implementation = "\n\n".join(auth_code_snippets[:5])
        
        return context
    
    def _extract_sdk_context(self, sdk_path: Path) -> SDKContext:
        """Extract context from Connector_SDK folder.
        
        Args:
            sdk_path: Path to the SDK folder
            
        Returns:
            SDKContext with extracted patterns
        """
        context = SDKContext()
        
        # Try to determine SDK name from folder structure
        for child in sdk_path.iterdir():
            if child.is_dir() and not child.name.startswith('.'):
                context.sdk_name = child.name
                break
        
        if not context.sdk_name:
            context.sdk_name = sdk_path.name
        
        for file_path in sdk_path.rglob('*'):
            if not file_path.is_file():
                continue
            
            ext = file_path.suffix.lower()
            if ext not in self.CODE_EXTENSIONS:
                continue
            
            try:
                content = file_path.read_text(errors='ignore')
                
                # Extract public methods
                method_patterns = [
                    r'public\s+\w+\s+(\w+)\s*\(',  # Java public methods
                    r'def\s+(\w+)\s*\(',           # Python methods
                    r'(?:export\s+)?(?:async\s+)?function\s+(\w+)',  # JS functions
                    r'func\s+(\w+)\s*\(',          # Go functions
                ]
                for pattern in method_patterns:
                    for match in re.finditer(pattern, content):
                        method_name = match.group(1)
                        if not method_name.startswith('_') and method_name not in context.available_methods:
                            context.available_methods.append(method_name)
                
                # Extract data types
                type_patterns = [
                    r'(?:class|interface|type|struct)\s+(\w+)',
                    r'@dataclass\s*\n\s*class\s+(\w+)',
                    r'type\s+(\w+)\s*=',
                ]
                for pattern in type_patterns:
                    for match in re.finditer(pattern, content):
                        type_name = match.group(1)
                        if type_name not in context.data_types:
                            context.data_types.append(type_name)
                
                # Extract auth-related methods
                auth_method_pattern = r'(?:def|function|public\s+\w+)\s+(\w*(?:auth|login|token|credential|oauth)\w*)\s*\('
                for match in re.finditer(auth_method_pattern, content, re.IGNORECASE):
                    method = match.group(1)
                    if method not in context.auth_methods:
                        context.auth_methods.append(method)
                
                # Extract client/service classes
                client_pattern = r'(?:class|interface)\s+(\w*(?:Client|Service|Api|Connection|Session)\w*)'
                for match in re.finditer(client_pattern, content):
                    client = match.group(1)
                    if client not in context.client_classes:
                        context.client_classes.append(client)
                
                # Extract constants/enums
                const_patterns = [
                    r'(?:final\s+)?(?:static\s+)?(?:final\s+)?\w+\s+([A-Z][A-Z_0-9]+)\s*=',
                    r'enum\s+(\w+)',
                    r'([A-Z][A-Z_0-9]{2,})\s*=\s*["\'\d]',
                ]
                for pattern in const_patterns:
                    for match in re.finditer(pattern, content):
                        const = match.group(1)
                        if const not in context.constants:
                            context.constants.append(const)
                            
            except Exception as e:
                print(f"Warning: Could not process {file_path}: {e}")
        
        return context
    
    def _html_to_text(self, html_content: str) -> str:
        """Convert HTML to plain text by stripping tags.
        
        Args:
            html_content: HTML content
            
        Returns:
            Plain text content
        """
        # Remove script and style elements
        html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', html_content)
        
        # Decode HTML entities
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'&lt;', '<', text)
        text = re.sub(r'&gt;', '>', text)
        text = re.sub(r'&amp;', '&', text)
        text = re.sub(r'&quot;', '"', text)
        
        # Clean up whitespace
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    
    def _categorize_docs(self, content: str) -> Dict[str, str]:
        """Categorize documentation content by keywords.
        
        Args:
            content: Full documentation content
            
        Returns:
            Dict with categorized content sections
        """
        categories = {
            'auth': [],
            'rate_limits': [],
            'objects': [],
            'endpoints': [],
            'permissions': []
        }
        
        # Split content into paragraphs
        paragraphs = re.split(r'\n\s*\n', content)
        
        for para in paragraphs:
            para_lower = para.lower()
            for category, keywords in self.DOC_SECTION_KEYWORDS.items():
                if any(kw in para_lower for kw in keywords):
                    categories[category].append(para.strip())
        
        return {k: '\n\n'.join(v[:10]) for k, v in categories.items()}
    
    def _extract_documentation_context(self, doc_path: Path) -> DocumentationContext:
        """Extract context from Public_Documentation folder.
        
        Args:
            doc_path: Path to the documentation folder
            
        Returns:
            DocumentationContext with extracted content
        """
        context = DocumentationContext()
        content_parts = []
        
        for file_path in doc_path.rglob('*'):
            if not file_path.is_file():
                continue
            
            try:
                file_content = ""
                ext = file_path.suffix.lower()
                
                if ext == '.md':
                    file_content = file_path.read_text(errors='ignore')
                elif ext in ['.html', '.htm']:
                    raw_html = file_path.read_text(errors='ignore')
                    file_content = self._html_to_text(raw_html)
                elif ext == '.txt':
                    file_content = file_path.read_text(errors='ignore')
                elif ext == '.rst':
                    file_content = file_path.read_text(errors='ignore')
                elif ext == '.json':
                    # JSON might be API spec (OpenAPI, etc.)
                    file_content = file_path.read_text(errors='ignore')
                elif ext in ['.yaml', '.yml']:
                    file_content = file_path.read_text(errors='ignore')
                
                if file_content:
                    content_parts.append(f"--- {file_path.name} ---\n{file_content}")
                    
            except Exception as e:
                print(f"Warning: Could not read {file_path}: {e}")
        
        # Combine all content
        full_content = '\n\n'.join(content_parts)
        context.raw_content = full_content
        
        # Categorize content
        categorized = self._categorize_docs(full_content)
        context.auth_guide = categorized.get('auth', '')
        context.rate_limits = categorized.get('rate_limits', '')
        context.objects_schema = categorized.get('objects', '')
        context.api_reference = categorized.get('endpoints', '')
        
        # Extract permissions
        perm_pattern = r'(?:permission|scope|role)[:\s]+["\']?(\w+(?:[:\.\-_]\w+)*)["\']?'
        for match in re.finditer(perm_pattern, full_content, re.IGNORECASE):
            perm = match.group(1)
            if perm not in context.permissions:
                context.permissions.append(perm)
        
        # Extract endpoint URLs
        endpoint_pattern = r'(?:GET|POST|PUT|DELETE|PATCH)\s+(/[\w/{}\-\.]+)'
        for match in re.finditer(endpoint_pattern, full_content):
            endpoint = f"{match.group(0)}"
            if endpoint not in context.endpoints_list:
                context.endpoints_list.append(endpoint)
        
        # Also look for URL patterns
        url_pattern = r'["\']/(api|v\d+)/[\w/{\}\-\.]+["\']'
        for match in re.finditer(url_pattern, full_content):
            endpoint = match.group(0).strip('"\'')
            if endpoint not in context.endpoints_list:
                context.endpoints_list.append(endpoint)
        
        return context
    
    def extract_structured_patterns(self, repo_path: Path) -> ExtractedCode:
        """Extract patterns from a structured repository.
        
        Args:
            repo_path: Path to the cloned repository
            
        Returns:
            ExtractedCode with structured context
        """
        # First, do the standard extraction
        result = self.extract_patterns(repo_path)
        
        # Detect structure
        structure_type, folder_paths = self._detect_structure(repo_path)
        result.structure_type = structure_type
        
        if structure_type == 'structured':
            print(f"Detected structured repository format")
            
            # Extract from each folder if present
            if folder_paths['implementation']:
                print(f"  - Extracting from Connector_Code: {folder_paths['implementation']}")
                result.implementation = self._extract_implementation_context(folder_paths['implementation'])
            
            if folder_paths['sdk']:
                print(f"  - Extracting from Connector_SDK: {folder_paths['sdk']}")
                result.sdk = self._extract_sdk_context(folder_paths['sdk'])
            
            if folder_paths['documentation']:
                print(f"  - Extracting from Public_Documentation: {folder_paths['documentation']}")
                result.documentation = self._extract_documentation_context(folder_paths['documentation'])
        else:
            print(f"Flat repository structure detected, using standard extraction")
        
        return result
    
    async def clone_and_extract(
        self, 
        github_url: str, 
        connector_id: str
    ) -> Optional[ExtractedCode]:
        """Clone a repository and extract all patterns.
        
        Automatically detects if the repository follows the structured format:
        - Connector_Code/     → Implementation patterns
        - Connector_SDK/      → SDK methods and types
        - Public_Documentation/ → API docs, auth guides, rate limits
        
        Args:
            github_url: URL of the GitHub repository
            connector_id: ID of the connector
            
        Returns:
            ExtractedCode object with all extracted information, or None if git unavailable
        """
        # Clone the repository
        repo_path = self.clone_repo(github_url, connector_id)
        
        # If git is not available, return None
        if repo_path is None:
            return None
        
        # Extract patterns with structure detection
        result = self.extract_structured_patterns(repo_path)
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

"""
Security utilities for API endpoints.

Provides:
- API key authentication
- Input sanitization
- Request validation
"""

import os
import re
import html
from typing import Optional, List, Dict, Any
from fastapi import HTTPException, Request, Header, status
from fastapi.security import APIKeyHeader


# API Key Header
API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)


def get_api_key_from_env() -> Optional[str]:
    """Get API key from environment variable."""
    return os.getenv("API_KEY")


def validate_api_key(api_key: Optional[str] = None) -> bool:
    """
    Validate API key.
    
    Args:
        api_key: API key to validate
        
    Returns:
        True if valid, False otherwise
    """
    expected_key = get_api_key_from_env()
    
    # If no API key is configured, allow all requests (development mode)
    if not expected_key:
        return True
    
    # If API key is configured, require it
    if not api_key:
        return False
    
    return api_key == expected_key


async def verify_api_key(
    request: Request,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key")
) -> str:
    """
    Dependency to verify API key for protected endpoints.
    
    Usage:
        @app.post("/api/endpoint")
        async def endpoint(api_key: str = Depends(verify_api_key)):
            ...
    """
    # Check if API key is required
    expected_key = get_api_key_from_env()
    
    if expected_key:
        if not x_api_key:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="API key required. Provide X-API-Key header."
            )
        
        if not validate_api_key(x_api_key):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid API key"
            )
    
    return x_api_key or ""


class InputSanitizer:
    """Utility class for sanitizing user inputs."""
    
    # Dangerous patterns to remove
    DANGEROUS_PATTERNS = [
        r'<script[^>]*>.*?</script>',  # Script tags
        r'javascript:',  # JavaScript protocol
        r'on\w+\s*=',  # Event handlers (onclick, onerror, etc.)
        r'<iframe[^>]*>',  # Iframes
        r'<object[^>]*>',  # Objects
        r'<embed[^>]*>',  # Embeds
    ]
    
    # SQL injection patterns (for additional validation)
    SQL_INJECTION_PATTERNS = [
        r"(\b(SELECT|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|EXEC|EXECUTE)\b)",
        r"(--|#|\/\*|\*\/)",  # SQL comments
        r"(\b(UNION|OR|AND)\s+\d+\s*=\s*\d+)",  # SQL injection attempts
    ]
    
    @staticmethod
    def sanitize_string(value: str, max_length: Optional[int] = None) -> str:
        """
        Sanitize a string input.
        
        Args:
            value: String to sanitize
            max_length: Maximum allowed length
            
        Returns:
            Sanitized string
        """
        if not isinstance(value, str):
            value = str(value)
        
        # Remove dangerous patterns
        for pattern in InputSanitizer.DANGEROUS_PATTERNS:
            value = re.sub(pattern, '', value, flags=re.IGNORECASE | re.DOTALL)
        
        # HTML escape
        value = html.escape(value)
        
        # Trim whitespace
        value = value.strip()
        
        # Enforce max length
        if max_length and len(value) > max_length:
            value = value[:max_length]
        
        return value
    
    @staticmethod
    def sanitize_dict(data: Dict[str, Any], max_string_length: Optional[int] = None) -> Dict[str, Any]:
        """
        Recursively sanitize a dictionary.
        
        Args:
            data: Dictionary to sanitize
            max_string_length: Maximum length for string values
            
        Returns:
            Sanitized dictionary
        """
        sanitized = {}
        
        for key, value in data.items():
            # Sanitize key
            sanitized_key = InputSanitizer.sanitize_string(str(key), max_length=100)
            
            # Sanitize value based on type
            if isinstance(value, str):
                sanitized[sanitized_key] = InputSanitizer.sanitize_string(value, max_string_length)
            elif isinstance(value, dict):
                sanitized[sanitized_key] = InputSanitizer.sanitize_dict(value, max_string_length)
            elif isinstance(value, list):
                sanitized[sanitized_key] = InputSanitizer.sanitize_list(value, max_string_length)
            else:
                # For other types (int, float, bool, None), keep as-is
                sanitized[sanitized_key] = value
        
        return sanitized
    
    @staticmethod
    def sanitize_list(data: List[Any], max_string_length: Optional[int] = None) -> List[Any]:
        """
        Recursively sanitize a list.
        
        Args:
            data: List to sanitize
            max_string_length: Maximum length for string values
            
        Returns:
            Sanitized list
        """
        sanitized = []
        
        for item in data:
            if isinstance(item, str):
                sanitized.append(InputSanitizer.sanitize_string(item, max_string_length))
            elif isinstance(item, dict):
                sanitized.append(InputSanitizer.sanitize_dict(item, max_string_length))
            elif isinstance(item, list):
                sanitized.append(InputSanitizer.sanitize_list(item, max_string_length))
            else:
                sanitized.append(item)
        
        return sanitized
    
    @staticmethod
    def validate_no_sql_injection(value: str) -> bool:
        """
        Check if string contains SQL injection patterns.
        
        Args:
            value: String to check
            
        Returns:
            True if safe, False if suspicious
        """
        if not isinstance(value, str):
            return True
        
        for pattern in InputSanitizer.SQL_INJECTION_PATTERNS:
            if re.search(pattern, value, re.IGNORECASE):
                return False
        
        return True
    
    @staticmethod
    def sanitize_connector_id(connector_id: str) -> str:
        """
        Sanitize connector ID (alphanumeric, hyphens, underscores only).
        
        Args:
            connector_id: Connector ID to sanitize
            
        Returns:
            Sanitized connector ID
        """
        # Only allow alphanumeric, hyphens, underscores
        sanitized = re.sub(r'[^a-zA-Z0-9_-]', '', connector_id)
        
        # Limit length
        if len(sanitized) > 255:
            sanitized = sanitized[:255]
        
        return sanitized
    
    @staticmethod
    def sanitize_url(url: str) -> str:
        """
        Sanitize and validate URL.
        
        Args:
            url: URL to sanitize
            
        Returns:
            Sanitized URL
        """
        if not url:
            return ""
        
        # Basic URL validation
        url_pattern = re.compile(
            r'^https?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE
        )
        
        if not url_pattern.match(url):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid URL format: {url}"
            )
        
        # HTML escape
        return html.escape(url)


def get_client_ip(request: Request) -> str:
    """
    Get client IP address from request.
    
    Args:
        request: FastAPI request object
        
    Returns:
        Client IP address
    """
    # Check for forwarded IP (from proxy/load balancer)
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        # Take the first IP (original client)
        return forwarded_for.split(",")[0].strip()
    
    # Check for real IP header
    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        return real_ip.strip()
    
    # Fallback to direct client IP
    if request.client:
        return request.client.host
    
    return "unknown"

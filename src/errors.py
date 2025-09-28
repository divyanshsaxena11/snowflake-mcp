"""
Custom exceptions for Snowflake MCP operations.
"""

class SnowflakeMCPError(Exception):
    """Base exception for Snowflake MCP operations."""
    pass

class ConnectionError(SnowflakeMCPError):
    """Raised when database connection fails."""
    pass

class AuthenticationError(SnowflakeMCPError):
    """Raised when authentication fails."""
    pass

class QueryError(SnowflakeMCPError):
    """Raised when query execution fails."""
    pass

class ConfigurationError(SnowflakeMCPError):
    """Raised when configuration is invalid."""
    pass

class ValidationError(SnowflakeMCPError):
    """Raised when input validation fails."""
    pass

class CortexError(SnowflakeMCPError):
    """Base exception for Cortex service operations."""
    pass

class CortexCompleteError(CortexError):
    """Raised when Cortex Complete operations fail."""
    pass

class CortexSearchError(CortexError):
    """Raised when Cortex Search operations fail."""
    pass

class CortexAnalystError(CortexError):
    """Raised when Cortex Analyst operations fail."""
    pass

class CortexServiceNotFoundError(CortexError):
    """Raised when a Cortex service is not found in configuration."""
    pass

class CortexModelNotSupportedError(CortexError):
    """Raised when an unsupported model is requested."""
    pass
"""
Input validation utilities for Snowflake MCP operations.
"""

import re
from typing import Any, Dict, Optional
from .errors import ValidationError

class QueryValidator:
    """Validates SQL queries and parameters."""
    
    # Dangerous SQL keywords that should be avoided
    DANGEROUS_KEYWORDS = [
        'DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE',
        'GRANT', 'REVOKE', 'EXEC', 'EXECUTE', 'CALL', 'MERGE'
    ]
    
    @classmethod
    def validate_query(cls, query: str, allow_ddl: bool = False) -> None:
        """
        Validate a SQL query for safety.
        
        Args:
            query: SQL query to validate
            allow_ddl: Whether to allow DDL operations
            
        Raises:
            ValidationError: If query is invalid or unsafe
        """
        if not query or not query.strip():
            raise ValidationError("Query cannot be empty")
        
        query_upper = query.upper().strip()
        
        # Check for dangerous keywords
        if not allow_ddl:
            for keyword in cls.DANGEROUS_KEYWORDS:
                if keyword in query_upper:
                    raise ValidationError(f"Query contains potentially dangerous keyword: {keyword}")
        
        # Basic SQL injection patterns
        dangerous_patterns = [
            r';\s*DROP\s+',
            r';\s*DELETE\s+',
            r';\s*TRUNCATE\s+',
            r';\s*ALTER\s+',
            r';\s*CREATE\s+',
            r';\s*INSERT\s+',
            r';\s*UPDATE\s+',
            r'UNION\s+SELECT',
            r'--',
            r'/\*.*\*/',
            r'EXEC\s*\(',
            r'EXECUTE\s*\(',
        ]
        
        for pattern in dangerous_patterns:
            if re.search(pattern, query_upper, re.IGNORECASE):
                raise ValidationError(f"Query contains potentially dangerous pattern: {pattern}")
    
    @classmethod
    def validate_identifier(cls, identifier: str, identifier_type: str = "identifier") -> None:
        """
        Validate database identifiers (table names, column names, etc.).
        
        Args:
            identifier: Identifier to validate
            identifier_type: Type of identifier for error messages
            
        Raises:
            ValidationError: If identifier is invalid
        """
        if not identifier or not identifier.strip():
            raise ValidationError(f"{identifier_type} cannot be empty")
        
        identifier = identifier.strip()
        
        # Check for valid identifier pattern
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
            raise ValidationError(f"Invalid {identifier_type}: {identifier}")
        
        # Check length
        if len(identifier) > 255:
            raise ValidationError(f"{identifier_type} too long: {identifier}")

class ParameterValidator:
    """Validates query parameters."""
    
    @classmethod
    def validate_params(cls, params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate and sanitize query parameters.
        
        Args:
            params: Parameters to validate
            
        Returns:
            Validated parameters
            
        Raises:
            ValidationError: If parameters are invalid
        """
        if params is None:
            return {}
        
        if not isinstance(params, dict):
            raise ValidationError("Parameters must be a dictionary")
        
        validated_params = {}
        for key, value in params.items():
            if not isinstance(key, str):
                raise ValidationError("Parameter keys must be strings")
            
            # Validate parameter name
            QueryValidator.validate_identifier(key, "parameter name")
            
            # Basic value validation
            if value is None:
                validated_params[key] = None
            elif isinstance(value, (str, int, float, bool)):
                validated_params[key] = value
            else:
                raise ValidationError(f"Invalid parameter value type for '{key}': {type(value)}")
        
        return validated_params

class ConnectionValidator:
    """Validates connection parameters."""
    
    @classmethod
    def validate_connection_params(cls, params: Dict[str, Any]) -> None:
        """
        Validate Snowflake connection parameters.
        
        Args:
            params: Connection parameters to validate
            
        Raises:
            ValidationError: If parameters are invalid
        """
        required_params = ['user', 'password', 'account', 'database', 'warehouse']
        
        for param in required_params:
            if not params.get(param):
                raise ValidationError(f"Missing required parameter: {param}")
        
        # Validate account format
        account = params.get('account')
        if account and not re.match(r'^[a-zA-Z0-9_-]+$', account):
            raise ValidationError("Invalid account format")
        
        # Validate user format
        user = params.get('user')
        if user and not re.match(r'^[a-zA-Z0-9_.-]+$', user):
            raise ValidationError("Invalid user format")

class CortexValidator:
    """Validates Cortex service parameters."""
    
    # Valid Cortex Complete models
    VALID_MODELS = [
        "snowflake-llama-3.3-70b",
        "snowflake-llama-3.1-8b", 
        "snowflake-llama-3.1-70b"
    ]
    
    @classmethod
    def validate_cortex_complete_params(cls, prompt: str, model: Optional[str] = None, 
                                      temperature: Optional[float] = None, 
                                      max_tokens: Optional[int] = None) -> None:
        """
        Validate Cortex Complete parameters.
        
        Args:
            prompt: Input prompt
            model: Model name
            temperature: Temperature parameter
            max_tokens: Maximum tokens parameter
            
        Raises:
            ValidationError: If parameters are invalid
        """
        if not prompt or not prompt.strip():
            raise ValidationError("Prompt cannot be empty")
        
        if len(prompt) > 10000:  # Reasonable limit
            raise ValidationError("Prompt too long (max 10000 characters)")
        
        if model and model not in cls.VALID_MODELS:
            raise ValidationError(f"Invalid model: {model}. Valid models: {', '.join(cls.VALID_MODELS)}")
        
        if temperature is not None:
            if not isinstance(temperature, (int, float)):
                raise ValidationError("Temperature must be a number")
            if temperature < 0.0 or temperature > 1.0:
                raise ValidationError("Temperature must be between 0.0 and 1.0")
        
        if max_tokens is not None:
            if not isinstance(max_tokens, int):
                raise ValidationError("Max tokens must be an integer")
            if max_tokens < 1 or max_tokens > 4000:
                raise ValidationError("Max tokens must be between 1 and 4000")
    
    @classmethod
    def validate_cortex_search_params(cls, service_name: str, query: str, 
                                    limit: int = 10, filter_expr: Optional[str] = None) -> None:
        """
        Validate Cortex Search parameters.
        
        Args:
            service_name: Name of the search service
            query: Search query
            limit: Maximum number of results
            filter_expr: Optional filter expression
            
        Raises:
            ValidationError: If parameters are invalid
        """
        if not service_name or not service_name.strip():
            raise ValidationError("Service name cannot be empty")
        
        QueryValidator.validate_identifier(service_name, "service name")
        
        if not query or not query.strip():
            raise ValidationError("Search query cannot be empty")
        
        if len(query) > 1000:  # Reasonable limit
            raise ValidationError("Search query too long (max 1000 characters)")
        
        if not isinstance(limit, int):
            raise ValidationError("Limit must be an integer")
        
        if limit < 1 or limit > 100:
            raise ValidationError("Limit must be between 1 and 100")
        
        if filter_expr and len(filter_expr) > 500:
            raise ValidationError("Filter expression too long (max 500 characters)")
    
    @classmethod
    def validate_cortex_analyst_params(cls, service_name: str, question: str, 
                                     include_sql: bool = True, include_data: bool = True) -> None:
        """
        Validate Cortex Analyst parameters.
        
        Args:
            service_name: Name of the analyst service
            question: Natural language question
            include_sql: Whether to include SQL in response
            include_data: Whether to include data in response
            
        Raises:
            ValidationError: If parameters are invalid
        """
        if not service_name or not service_name.strip():
            raise ValidationError("Service name cannot be empty")
        
        QueryValidator.validate_identifier(service_name, "service name")
        
        if not question or not question.strip():
            raise ValidationError("Question cannot be empty")
        
        if len(question) > 2000:  # Reasonable limit
            raise ValidationError("Question too long (max 2000 characters)")
        
        if not isinstance(include_sql, bool):
            raise ValidationError("Include SQL must be a boolean")
        
        if not isinstance(include_data, bool):
            raise ValidationError("Include data must be a boolean")
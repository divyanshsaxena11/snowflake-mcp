"""
Snowflake client for MCP integration.
Handles database connections and query execution.
"""

import os
import logging
import yaml
from typing import Dict, List, Any, Optional, Tuple
from contextlib import asynccontextmanager
import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.connector.errors import DatabaseError, ProgrammingError, OperationalError
from dotenv import load_dotenv

from .errors import (
    SnowflakeMCPError, ConnectionError, AuthenticationError, 
    QueryError, ConfigurationError, ValidationError,
    CortexCompleteError, CortexSearchError, CortexAnalystError,
    CortexServiceNotFoundError, CortexModelNotSupportedError
)
from .validators import QueryValidator, ParameterValidator, ConnectionValidator

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


class SnowflakeClient:
    """Client for Snowflake database operations including Cortex AI services."""
    
    def __init__(self, service_config_path: Optional[str] = None):
        """Initialize Snowflake client with connection parameters and Cortex services."""
        self.connection_params = {
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'role': os.getenv('SNOWFLAKE_ROLE'),
            'region': os.getenv('SNOWFLAKE_REGION'),
            'authenticator': os.getenv('SNOWFLAKE_AUTHENTICATOR', 'snowflake'),
            'client_session_keep_alive': os.getenv('SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE', 'true').lower() == 'true'
        }
        
        # Load Cortex services configuration
        self.cortex_config = self._load_cortex_config(service_config_path)
        
        # Validate connection parameters
        try:
            ConnectionValidator.validate_connection_params(self.connection_params)
        except ValidationError as e:
            raise ConfigurationError(f"Invalid connection configuration: {e}")
    
    def _load_cortex_config(self, config_path: Optional[str] = None) -> Dict[str, Any]:
        """Load Cortex services configuration from YAML file."""
        if not config_path:
            config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'service_config.yaml')
        
        try:
            if os.path.exists(config_path):
                with open(config_path, 'r') as file:
                    return yaml.safe_load(file) or {}
            else:
                logger.warning(f"Cortex configuration file not found at {config_path}")
                return {}
        except Exception as e:
            logger.error(f"Error loading Cortex configuration: {e}")
            return {}
    
    @asynccontextmanager
    async def get_connection(self):
        """Get a Snowflake database connection."""
        connection = None
        try:
            connection = snowflake.connector.connect(**self.connection_params)
            yield connection
        except (DatabaseError, ProgrammingError, OperationalError) as e:
            logger.error(f"Snowflake connection error: {e}")
            if "Authentication failed" in str(e) or "Invalid credentials" in str(e):
                raise AuthenticationError(f"Authentication failed: {e}")
            else:
                raise ConnectionError(f"Failed to connect to Snowflake: {e}")
        except Exception as e:
            logger.error(f"Unexpected error connecting to Snowflake: {e}")
            raise ConnectionError(f"Unexpected connection error: {e}")
        finally:
            if connection:
                try:
                    connection.close()
                except Exception as e:
                    logger.warning(f"Error closing connection: {e}")
    
    async def execute_query(self, query: str, params: Optional[Dict] = None, allow_ddl: bool = False) -> Tuple[List[Dict[str, Any]], List[str]]:
        """
        Execute a SQL query and return results.
        
        Args:
            query: SQL query to execute
            params: Optional query parameters
            allow_ddl: Whether to allow DDL operations
            
        Returns:
            Tuple of (results, column_names)
        """
        # Validate query and parameters
        try:
            QueryValidator.validate_query(query, allow_ddl)
            validated_params = ParameterValidator.validate_params(params)
        except ValidationError as e:
            raise QueryError(f"Query validation failed: {e}")
        
        async with self.get_connection() as conn:
            try:
                cursor = conn.cursor(DictCursor)
                if validated_params:
                    cursor.execute(query, validated_params)
                else:
                    cursor.execute(query)
                
                results = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description] if cursor.description else []
                
                return results, column_names
                
            except (DatabaseError, ProgrammingError, OperationalError) as e:
                logger.error(f"Query execution failed: {e}")
                raise QueryError(f"Query execution failed: {e}")
            except Exception as e:
                logger.error(f"Unexpected error during query execution: {e}")
                raise QueryError(f"Unexpected query error: {e}")
    
    async def get_databases(self) -> List[Dict[str, Any]]:
        """Get list of available databases."""
        query = "SHOW DATABASES"
        results, _ = await self.execute_query(query)
        return results
    
    async def get_schemas(self, database: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get list of schemas."""
        if database:
            query = f"SHOW SCHEMAS IN DATABASE {database}"
        else:
            query = "SHOW SCHEMAS"
        results, _ = await self.execute_query(query)
        return results
    
    async def get_tables(self, database: Optional[str] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get list of tables."""
        if database and schema:
            query = f"SHOW TABLES IN {database}.{schema}"
        elif database:
            query = f"SHOW TABLES IN DATABASE {database}"
        else:
            query = "SHOW TABLES"
        results, _ = await self.execute_query(query)
        return results
    
    async def get_columns(self, table: str, database: Optional[str] = None, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get column information for a table."""
        if database and schema:
            query = f"DESCRIBE TABLE {database}.{schema}.{table}"
        elif database:
            query = f"DESCRIBE TABLE {database}..{table}"
        else:
            query = f"DESCRIBE TABLE {table}"
        results, _ = await self.execute_query(query)
        return results
    
    async def get_warehouses(self) -> List[Dict[str, Any]]:
        """Get list of available warehouses."""
        query = "SHOW WAREHOUSES"
        results, _ = await self.execute_query(query)
        return results
    
    async def get_roles(self) -> List[Dict[str, Any]]:
        """Get list of available roles."""
        query = "SHOW ROLES"
        results, _ = await self.execute_query(query)
        return results
    
    async def test_connection(self) -> bool:
        """Test the database connection."""
        try:
            async with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result[0] == 1
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    # Cortex Complete methods
    async def cortex_complete(self, prompt: str, model: Optional[str] = None, **kwargs) -> str:
        """
        Use Cortex Complete for chat completion.
        
        Args:
            prompt: The input prompt for completion
            model: Optional model name (defaults to configured model)
            **kwargs: Additional parameters for the completion
            
        Returns:
            Completion response as string
        """
        if not model:
            model = self.cortex_config.get('cortex_complete', {}).get('default_model', 'snowflake-llama-3.3-70b')
        
        # Validate model
        valid_models = ["snowflake-llama-3.3-70b", "snowflake-llama-3.1-8b", "snowflake-llama-3.1-70b"]
        if model not in valid_models:
            raise CortexModelNotSupportedError(f"Model '{model}' is not supported. Valid models: {', '.join(valid_models)}")
        
        # Build the SQL query for Cortex Complete
        query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{prompt}'"
        
        # Add additional parameters if provided
        if kwargs:
            for key, value in kwargs.items():
                if isinstance(value, str):
                    query += f", '{key}' => '{value}'"
                else:
                    query += f", '{key}' => {value}"
        
        query += ") AS response"
        
        try:
            results, _ = await self.execute_query(query)
            if results and len(results) > 0:
                return results[0]['RESPONSE']
            else:
                return "No response generated"
        except QueryError as e:
            logger.error(f"Cortex Complete query error: {e}")
            raise CortexCompleteError(f"Cortex Complete query failed: {e}")
        except Exception as e:
            logger.error(f"Cortex Complete error: {e}")
            raise CortexCompleteError(f"Cortex Complete failed: {e}")
    
    # Cortex Search methods
    async def cortex_search(self, service_name: str, query: str, limit: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """
        Use Cortex Search Service for semantic search.
        
        Args:
            service_name: Name of the search service
            query: Search query
            limit: Maximum number of results to return
            **kwargs: Additional search parameters
            
        Returns:
            List of search results
        """
        # Find the search service configuration
        search_services = self.cortex_config.get('search_services', [])
        service_config = None
        for service in search_services:
            if service.get('service_name') == service_name:
                service_config = service
                break
        
        if not service_config:
            raise CortexServiceNotFoundError(f"Search service '{service_name}' not found in configuration")
        
        database = service_config.get('database_name')
        schema = service_config.get('schema_name')
        
        if not database or not schema:
            raise ConfigurationError(f"Search service '{service_name}' missing database or schema configuration")
        
        # Build the SQL query for Cortex Search
        search_query = f"SELECT SNOWFLAKE.CORTEX.SEARCH('{database}.{schema}.{service_name}', '{query}', {limit}"
        
        # Add additional parameters if provided
        if kwargs:
            for key, value in kwargs.items():
                if isinstance(value, str):
                    search_query += f", '{key}' => '{value}'"
                else:
                    search_query += f", '{key}' => {value}"
        
        search_query += ") AS search_results"
        
        try:
            results, _ = await self.execute_query(search_query)
            if results and len(results) > 0:
                # Parse the search results JSON
                import json
                search_results = json.loads(results[0]['SEARCH_RESULTS'])
                return search_results
            else:
                return []
        except QueryError as e:
            logger.error(f"Cortex Search query error: {e}")
            raise CortexSearchError(f"Cortex Search query failed: {e}")
        except Exception as e:
            logger.error(f"Cortex Search error: {e}")
            raise CortexSearchError(f"Cortex Search failed: {e}")
    
    # Cortex Analyst methods
    async def cortex_analyst(self, service_name: str, question: str, **kwargs) -> Dict[str, Any]:
        """
        Use Cortex Analyst for natural language querying over structured data.
        
        Args:
            service_name: Name of the analyst service
            question: Natural language question
            **kwargs: Additional parameters
            
        Returns:
            Analysis results including SQL and data
        """
        # Find the analyst service configuration
        analyst_services = self.cortex_config.get('analyst_services', [])
        service_config = None
        for service in analyst_services:
            if service.get('service_name') == service_name:
                service_config = service
                break
        
        if not service_config:
            raise CortexServiceNotFoundError(f"Analyst service '{service_name}' not found in configuration")
        
        semantic_model = service_config.get('semantic_model')
        if not semantic_model:
            raise ConfigurationError(f"Analyst service '{service_name}' missing semantic model configuration")
        
        # Build the SQL query for Cortex Analyst
        query = f"SELECT SNOWFLAKE.CORTEX.ANALYST('{semantic_model}', '{question}'"
        
        # Add additional parameters if provided
        if kwargs:
            for key, value in kwargs.items():
                if isinstance(value, str):
                    query += f", '{key}' => '{value}'"
                else:
                    query += f", '{key}' => {value}"
        
        query += ") AS analysis_result"
        
        try:
            results, _ = await self.execute_query(query)
            if results and len(results) > 0:
                # Parse the analysis results JSON
                import json
                analysis_result = json.loads(results[0]['ANALYSIS_RESULT'])
                return analysis_result
            else:
                return {"error": "No analysis result generated"}
        except QueryError as e:
            logger.error(f"Cortex Analyst query error: {e}")
            raise CortexAnalystError(f"Cortex Analyst query failed: {e}")
        except Exception as e:
            logger.error(f"Cortex Analyst error: {e}")
            raise CortexAnalystError(f"Cortex Analyst failed: {e}")
    
    # Helper methods to get available services
    def get_available_search_services(self) -> List[Dict[str, str]]:
        """Get list of available search services."""
        return self.cortex_config.get('search_services', [])
    
    def get_available_analyst_services(self) -> List[Dict[str, str]]:
        """Get list of available analyst services."""
        return self.cortex_config.get('analyst_services', [])
    
    def get_cortex_complete_config(self) -> Dict[str, Any]:
        """Get Cortex Complete configuration."""
        return self.cortex_config.get('cortex_complete', {})

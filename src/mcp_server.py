"""
MCP Server for Snowflake integration.
Provides tools and resources for database operations.
"""

import asyncio
import json
import logging
import os
from typing import Any, Dict, List, Optional
from mcp.server import Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.types import (
    Resource, Tool, TextContent, ImageContent, EmbeddedResource,
    CallToolRequest, ListResourcesRequest, ListToolsRequest,
    ReadResourceRequest
)
from .snowflake_client import SnowflakeClient
from .errors import (
    SnowflakeMCPError, ConnectionError, AuthenticationError, 
    QueryError, ConfigurationError, ValidationError,
    CortexCompleteError, CortexSearchError, CortexAnalystError,
    CortexServiceNotFoundError, CortexModelNotSupportedError
)
from .validators import QueryValidator, ParameterValidator, ConnectionValidator, CortexValidator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize MCP server
server = Server("snowflake-mcp")

# Initialize Snowflake client
snowflake_client = None

@server.list_tools()
async def handle_list_tools() -> List[Tool]:
    """List available tools for Snowflake operations."""
    return [
        Tool(
            name="execute_query",
            description="Execute a SQL query on Snowflake database",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "SQL query to execute"
                    },
                    "params": {
                        "type": "object",
                        "description": "Optional query parameters",
                        "additionalProperties": True
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="get_databases",
            description="Get list of available databases",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="get_schemas",
            description="Get list of schemas in a database",
            inputSchema={
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "Database name (optional)"
                    }
                }
            }
        ),
        Tool(
            name="get_tables",
            description="Get list of tables in a database/schema",
            inputSchema={
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "Database name (optional)"
                    },
                    "schema": {
                        "type": "string",
                        "description": "Schema name (optional)"
                    }
                }
            }
        ),
        Tool(
            name="get_columns",
            description="Get column information for a table",
            inputSchema={
                "type": "object",
                "properties": {
                    "table": {
                        "type": "string",
                        "description": "Table name"
                    },
                    "database": {
                        "type": "string",
                        "description": "Database name (optional)"
                    },
                    "schema": {
                        "type": "string",
                        "description": "Schema name (optional)"
                    }
                },
                "required": ["table"]
            }
        ),
        Tool(
            name="get_warehouses",
            description="Get list of available warehouses",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="get_roles",
            description="Get list of available roles",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="test_connection",
            description="Test the Snowflake database connection",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        # Cortex Complete tool
        Tool(
            name="cortex_complete",
            description="Use Cortex Complete for chat completion with large language models",
            inputSchema={
                "type": "object",
                "properties": {
                    "prompt": {
                        "type": "string",
                        "description": "The input prompt for completion"
                    },
                    "model": {
                        "type": "string",
                        "description": "Optional model name (defaults to configured model)",
                        "enum": ["snowflake-llama-3.3-70b", "snowflake-llama-3.1-8b", "snowflake-llama-3.1-70b"]
                    },
                    "temperature": {
                        "type": "number",
                        "description": "Temperature for response generation (0.0 to 1.0)",
                        "minimum": 0.0,
                        "maximum": 1.0
                    },
                    "max_tokens": {
                        "type": "integer",
                        "description": "Maximum number of tokens to generate",
                        "minimum": 1
                    }
                },
                "required": ["prompt"]
            }
        ),
        # Cortex Search tool
        Tool(
            name="cortex_search",
            description="Use Cortex Search Service for semantic search over text data",
            inputSchema={
                "type": "object",
                "properties": {
                    "service_name": {
                        "type": "string",
                        "description": "Name of the search service to use"
                    },
                    "query": {
                        "type": "string",
                        "description": "Search query"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results to return",
                        "minimum": 1,
                        "maximum": 100,
                        "default": 10
                    },
                    "filter": {
                        "type": "string",
                        "description": "Optional filter expression for search results"
                    }
                },
                "required": ["service_name", "query"]
            }
        ),
        # Cortex Analyst tool
        Tool(
            name="cortex_analyst",
            description="Use Cortex Analyst for natural language querying over structured data",
            inputSchema={
                "type": "object",
                "properties": {
                    "service_name": {
                        "type": "string",
                        "description": "Name of the analyst service to use"
                    },
                    "question": {
                        "type": "string",
                        "description": "Natural language question about the data"
                    },
                    "include_sql": {
                        "type": "boolean",
                        "description": "Whether to include the generated SQL in the response",
                        "default": True
                    },
                    "include_data": {
                        "type": "boolean",
                        "description": "Whether to include the query results in the response",
                        "default": True
                    }
                },
                "required": ["service_name", "question"]
            }
        ),
        # List available Cortex services
        Tool(
            name="list_cortex_services",
            description="List available Cortex services (Search and Analyst)",
            inputSchema={
                "type": "object",
                "properties": {
                    "service_type": {
                        "type": "string",
                        "description": "Type of services to list",
                        "enum": ["search", "analyst", "complete", "all"],
                        "default": "all"
                    }
                }
            }
        )
    ]

@server.list_resources()
async def handle_list_resources() -> List[Resource]:
    """List available resources."""
    return [
        Resource(
            uri="snowflake://databases",
            name="Databases",
            description="List of available databases",
            mimeType="application/json"
        ),
        Resource(
            uri="snowflake://schemas",
            name="Schemas",
            description="List of available schemas",
            mimeType="application/json"
        ),
        Resource(
            uri="snowflake://tables",
            name="Tables",
            description="List of available tables",
            mimeType="application/json"
        ),
        Resource(
            uri="snowflake://warehouses",
            name="Warehouses",
            description="List of available warehouses",
            mimeType="application/json"
        ),
        Resource(
            uri="snowflake://roles",
            name="Roles",
            description="List of available roles",
            mimeType="application/json"
        ),
        Resource(
            uri="snowflake://cortex/search_services",
            name="Cortex Search Services",
            description="List of available Cortex Search services",
            mimeType="application/json"
        ),
        Resource(
            uri="snowflake://cortex/analyst_services",
            name="Cortex Analyst Services",
            description="List of available Cortex Analyst services",
            mimeType="application/json"
        ),
        Resource(
            uri="snowflake://cortex/complete_config",
            name="Cortex Complete Configuration",
            description="Cortex Complete configuration and available models",
            mimeType="application/json"
        )
    ]

@server.read_resource()
async def handle_read_resource(uri: str) -> str:
    """Read a specific resource."""
    global snowflake_client
    
    if not snowflake_client:
        snowflake_client = SnowflakeClient()
    
    try:
        if uri == "snowflake://databases":
            databases = await snowflake_client.get_databases()
            return json.dumps(databases, indent=2)
        elif uri == "snowflake://schemas":
            schemas = await snowflake_client.get_schemas()
            return json.dumps(schemas, indent=2)
        elif uri == "snowflake://tables":
            tables = await snowflake_client.get_tables()
            return json.dumps(tables, indent=2)
        elif uri == "snowflake://warehouses":
            warehouses = await snowflake_client.get_warehouses()
            return json.dumps(warehouses, indent=2)
        elif uri == "snowflake://roles":
            roles = await snowflake_client.get_roles()
            return json.dumps(roles, indent=2)
        elif uri == "snowflake://cortex/search_services":
            search_services = snowflake_client.get_available_search_services()
            return json.dumps(search_services, indent=2)
        elif uri == "snowflake://cortex/analyst_services":
            analyst_services = snowflake_client.get_available_analyst_services()
            return json.dumps(analyst_services, indent=2)
        elif uri == "snowflake://cortex/complete_config":
            complete_config = snowflake_client.get_cortex_complete_config()
            return json.dumps(complete_config, indent=2)
        else:
            raise ValueError(f"Unknown resource URI: {uri}")
    except Exception as e:
        logger.error(f"Error reading resource {uri}: {e}")
        return json.dumps({"error": str(e)})

@server.call_tool()
async def handle_call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle tool calls."""
    global snowflake_client
    
    if not snowflake_client:
        snowflake_client = SnowflakeClient()
    
    try:
        if name == "execute_query":
            query = arguments.get("query")
            params = arguments.get("params")
            
            if not query:
                return [TextContent(type="text", text="Error: Query is required")]
            
            results, columns = await snowflake_client.execute_query(query, params)
            
            # Format results
            if results:
                # Create a table-like output
                output = "Query Results:\n\n"
                output += "Columns: " + ", ".join(columns) + "\n\n"
                output += f"Rows: {len(results)}\n\n"
                
                # Show first few rows
                for i, row in enumerate(results[:10]):  # Limit to first 10 rows
                    output += f"Row {i+1}: {dict(row)}\n"
                
                if len(results) > 10:
                    output += f"\n... and {len(results) - 10} more rows"
            else:
                output = "Query executed successfully. No results returned."
            
            return [TextContent(type="text", text=output)]
        
        elif name == "get_databases":
            databases = await snowflake_client.get_databases()
            output = "Available Databases:\n\n"
            for db in databases:
                output += f"- {db.get('name', 'Unknown')}\n"
            return [TextContent(type="text", text=output)]
        
        elif name == "get_schemas":
            database = arguments.get("database")
            schemas = await snowflake_client.get_schemas(database)
            output = f"Available Schemas{' in ' + database if database else ''}:\n\n"
            for schema in schemas:
                output += f"- {schema.get('name', 'Unknown')}\n"
            return [TextContent(type="text", text=output)]
        
        elif name == "get_tables":
            database = arguments.get("database")
            schema = arguments.get("schema")
            tables = await snowflake_client.get_tables(database, schema)
            output = f"Available Tables{' in ' + (database + '.' + schema if database and schema else database or schema or '')}:\n\n"
            for table in tables:
                output += f"- {table.get('name', 'Unknown')}\n"
            return [TextContent(type="text", text=output)]
        
        elif name == "get_columns":
            table = arguments.get("table")
            database = arguments.get("database")
            schema = arguments.get("schema")
            
            if not table:
                return [TextContent(type="text", text="Error: Table name is required")]
            
            columns = await snowflake_client.get_columns(table, database, schema)
            output = f"Columns in {database + '.' + schema + '.' if database and schema else table}:\n\n"
            for col in columns:
                output += f"- {col.get('name', 'Unknown')} ({col.get('type', 'Unknown')})\n"
            return [TextContent(type="text", text=output)]
        
        elif name == "get_warehouses":
            warehouses = await snowflake_client.get_warehouses()
            output = "Available Warehouses:\n\n"
            for warehouse in warehouses:
                output += f"- {warehouse.get('name', 'Unknown')}\n"
            return [TextContent(type="text", text=output)]
        
        elif name == "get_roles":
            roles = await snowflake_client.get_roles()
            output = "Available Roles:\n\n"
            for role in roles:
                output += f"- {role.get('name', 'Unknown')}\n"
            return [TextContent(type="text", text=output)]
        
        elif name == "test_connection":
            success = await snowflake_client.test_connection()
            if success:
                return [TextContent(type="text", text="✅ Connection test successful!")]
            else:
                return [TextContent(type="text", text="❌ Connection test failed!")]
        
        # Cortex Complete handler
        elif name == "cortex_complete":
            prompt = arguments.get("prompt")
            model = arguments.get("model")
            temperature = arguments.get("temperature")
            max_tokens = arguments.get("max_tokens")
            
            if not prompt:
                return [TextContent(type="text", text="Error: Prompt is required")]
            
            # Validate parameters
            try:
                CortexValidator.validate_cortex_complete_params(prompt, model, temperature, max_tokens)
            except ValidationError as e:
                return [TextContent(type="text", text=f"❌ Validation Error: {str(e)}")]
            
            # Prepare parameters
            params = {}
            if temperature is not None:
                params["temperature"] = temperature
            if max_tokens is not None:
                params["max_tokens"] = max_tokens
            
            try:
                response = await snowflake_client.cortex_complete(prompt, model, **params)
                output = f"🤖 Cortex Complete Response:\n\n{response}"
                return [TextContent(type="text", text=output)]
            except Exception as e:
                return [TextContent(type="text", text=f"❌ Cortex Complete Error: {str(e)}")]
        
        # Cortex Search handler
        elif name == "cortex_search":
            service_name = arguments.get("service_name")
            query = arguments.get("query")
            limit = arguments.get("limit", 10)
            filter_expr = arguments.get("filter")
            
            if not service_name or not query:
                return [TextContent(type="text", text="Error: Service name and query are required")]
            
            # Validate parameters
            try:
                CortexValidator.validate_cortex_search_params(service_name, query, limit, filter_expr)
            except ValidationError as e:
                return [TextContent(type="text", text=f"❌ Validation Error: {str(e)}")]
            
            # Prepare parameters
            params = {}
            if filter_expr:
                params["filter"] = filter_expr
            
            try:
                results = await snowflake_client.cortex_search(service_name, query, limit, **params)
                output = f"🔍 Cortex Search Results for '{query}':\n\n"
                
                if results:
                    for i, result in enumerate(results, 1):
                        output += f"Result {i}:\n"
                        for key, value in result.items():
                            output += f"  {key}: {value}\n"
                        output += "\n"
                else:
                    output += "No results found."
                
                return [TextContent(type="text", text=output)]
            except Exception as e:
                return [TextContent(type="text", text=f"❌ Cortex Search Error: {str(e)}")]
        
        # Cortex Analyst handler
        elif name == "cortex_analyst":
            service_name = arguments.get("service_name")
            question = arguments.get("question")
            include_sql = arguments.get("include_sql", True)
            include_data = arguments.get("include_data", True)
            
            if not service_name or not question:
                return [TextContent(type="text", text="Error: Service name and question are required")]
            
            # Validate parameters
            try:
                CortexValidator.validate_cortex_analyst_params(service_name, question, include_sql, include_data)
            except ValidationError as e:
                return [TextContent(type="text", text=f"❌ Validation Error: {str(e)}")]
            
            # Prepare parameters
            params = {}
            if not include_sql:
                params["include_sql"] = False
            if not include_data:
                params["include_data"] = False
            
            try:
                result = await snowflake_client.cortex_analyst(service_name, question, **params)
                output = f"📊 Cortex Analyst Response for '{question}':\n\n"
                
                if "error" in result:
                    output += f"❌ Error: {result['error']}"
                else:
                    # Format the analysis result
                    if "sql" in result and include_sql:
                        output += f"Generated SQL:\n{result['sql']}\n\n"
                    
                    if "data" in result and include_data:
                        output += f"Query Results:\n{result['data']}\n\n"
                    
                    if "explanation" in result:
                        output += f"Explanation:\n{result['explanation']}\n\n"
                    
                    # Include any other fields
                    for key, value in result.items():
                        if key not in ["sql", "data", "explanation"]:
                            output += f"{key}: {value}\n"
                
                return [TextContent(type="text", text=output)]
            except Exception as e:
                return [TextContent(type="text", text=f"❌ Cortex Analyst Error: {str(e)}")]
        
        # List Cortex services handler
        elif name == "list_cortex_services":
            service_type = arguments.get("service_type", "all")
            
            try:
                output = "🧠 Available Cortex Services:\n\n"
                
                if service_type in ["search", "all"]:
                    search_services = snowflake_client.get_available_search_services()
                    output += "🔍 Search Services:\n"
                    if search_services:
                        for service in search_services:
                            output += f"  - {service.get('service_name', 'Unknown')}: {service.get('description', 'No description')}\n"
                    else:
                        output += "  No search services configured\n"
                    output += "\n"
                
                if service_type in ["analyst", "all"]:
                    analyst_services = snowflake_client.get_available_analyst_services()
                    output += "📊 Analyst Services:\n"
                    if analyst_services:
                        for service in analyst_services:
                            output += f"  - {service.get('service_name', 'Unknown')}: {service.get('description', 'No description')}\n"
                    else:
                        output += "  No analyst services configured\n"
                    output += "\n"
                
                if service_type in ["complete", "all"]:
                    complete_config = snowflake_client.get_cortex_complete_config()
                    output += "🤖 Complete Configuration:\n"
                    if complete_config:
                        default_model = complete_config.get('default_model', 'Not configured')
                        output += f"  - Default Model: {default_model}\n"
                    else:
                        output += "  No complete configuration found\n"
                
                return [TextContent(type="text", text=output)]
            except Exception as e:
                return [TextContent(type="text", text=f"❌ Error listing Cortex services: {str(e)}")]
        
        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]
    
    except ValidationError as e:
        logger.error(f"Validation error in tool {name}: {e}")
        return [TextContent(type="text", text=f"❌ Validation Error: {str(e)}")]
    except CortexCompleteError as e:
        logger.error(f"Cortex Complete error in tool {name}: {e}")
        return [TextContent(type="text", text=f"❌ Cortex Complete Error: {str(e)}")]
    except CortexSearchError as e:
        logger.error(f"Cortex Search error in tool {name}: {e}")
        return [TextContent(type="text", text=f"❌ Cortex Search Error: {str(e)}")]
    except CortexAnalystError as e:
        logger.error(f"Cortex Analyst error in tool {name}: {e}")
        return [TextContent(type="text", text=f"❌ Cortex Analyst Error: {str(e)}")]
    except CortexServiceNotFoundError as e:
        logger.error(f"Cortex service not found error in tool {name}: {e}")
        return [TextContent(type="text", text=f"❌ Cortex Service Not Found: {str(e)}")]
    except CortexModelNotSupportedError as e:
        logger.error(f"Cortex model not supported error in tool {name}: {e}")
        return [TextContent(type="text", text=f"❌ Cortex Model Not Supported: {str(e)}")]
    except QueryError as e:
        logger.error(f"Query error in tool {name}: {e}")
        return [TextContent(type="text", text=f"❌ Query Error: {str(e)}")]
    except ConnectionError as e:
        logger.error(f"Connection error in tool {name}: {e}")
        return [TextContent(type="text", text=f"❌ Connection Error: {str(e)}")]
    except AuthenticationError as e:
        logger.error(f"Authentication error in tool {name}: {e}")
        return [TextContent(type="text", text=f"❌ Authentication Error: {str(e)}")]
    except ConfigurationError as e:
        logger.error(f"Configuration error in tool {name}: {e}")
        return [TextContent(type="text", text=f"❌ Configuration Error: {str(e)}")]
    except SnowflakeMCPError as e:
        logger.error(f"Snowflake MCP error in tool {name}: {e}")
        return [TextContent(type="text", text=f"❌ Snowflake Error: {str(e)}")]
    except Exception as e:
        logger.error(f"Unexpected error executing tool {name}: {e}")
        return [TextContent(type="text", text=f"❌ Unexpected Error: {str(e)}")]

async def main():
    """Main function to run the MCP server."""
    logger.info("Starting Snowflake MCP Server...")
    
    # Test connection on startup
    try:
        global snowflake_client
        # Try to load service config from current directory
        service_config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'service_config.yaml')
        snowflake_client = SnowflakeClient(service_config_path)
        if await snowflake_client.test_connection():
            logger.info("✅ Snowflake connection successful")
        else:
            logger.warning("❌ Snowflake connection failed")
    except Exception as e:
        logger.error(f"Failed to initialize Snowflake client: {e}")
    
    # Run the server
    async with stdio_server() as (read_stream, write_stream):
        initialization_options = server.create_initialization_options()
        await server.run(
            read_stream,
            write_stream,
            initialization_options
#            InitializationOptions(
#                server_name="snowflake-mcp",
#                server_version="1.0.0",
#                capabilities=server.get_capabilities(
#                    notification_options=None,
#                    experimental_capabilities=None
#                )
#            )
        )

if __name__ == "__main__":
    asyncio.run(main())

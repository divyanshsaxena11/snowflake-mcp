# Snowflake Cortex AI Integration

This document describes how to integrate and use Snowflake's Cortex AI services (Analyst, Search Service, and Complete) with your MCP server.

## Overview

The Snowflake MCP server now supports three Cortex AI services:

1. **Cortex Complete** - Chat completion using large language models
2. **Cortex Search Service** - Semantic search over text data
3. **Cortex Analyst** - Natural language querying over structured data

## Prerequisites

### 1. Snowflake Setup

Before using Cortex services, ensure you have:

- A Snowflake account with Cortex AI enabled
- Appropriate roles and permissions for Cortex services
- Created the necessary Cortex services in your Snowflake environment

### 2. Required Permissions

Your Snowflake user/role needs:

- `SNOWFLAKE.CORTEX_USER` role for basic Cortex access
- `SNOWFLAKE.CORTEX_ANALYST_USER` role for Cortex Analyst
- `USAGE` privileges on Cortex Search services
- `READ` or `WRITE` privileges on stages containing semantic models
- `SELECT` privileges on tables referenced by semantic models

### 3. Environment Variables

Ensure your `.env` file contains the necessary Snowflake connection parameters:

```env
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_REGION=your_region
```

## Configuration

### Service Configuration File

Create a `service_config.yaml` file in your project root to define your Cortex services:

```yaml
# Cortex Complete configuration
cortex_complete:
  default_model: "snowflake-llama-3.3-70b"
  # Available models:
  # - snowflake-llama-3.3-70b
  # - snowflake-llama-3.1-8b
  # - snowflake-llama-3.1-70b

# Cortex Search services
search_services:
  - service_name: "customer_support_search"
    description: "Search service that indexes customer support transcripts and documentation"
    database_name: "SUPPORT_DB"
    schema_name: "SERVICES"
  
  - service_name: "product_catalog_search"
    description: "Search service for product catalog and specifications"
    database_name: "PRODUCT_DB"
    schema_name: "CATALOG"

# Cortex Analyst services
analyst_services:
  - service_name: "sales_analyst"
    semantic_model: "@SALES_DB.ANALYTICS.SEMANTIC_MODELS/sales_semantic_model.yaml"
    description: "Analyst service that provides natural language querying over sales data"
  
  - service_name: "customer_analyst"
    semantic_model: "@CUSTOMER_DB.ANALYTICS.SEMANTIC_MODELS/customer_semantic_model.yaml"
    description: "Analyst service for customer data analysis and insights"
```

## Available Tools

### 1. Cortex Complete

**Tool Name:** `cortex_complete`

**Description:** Use Cortex Complete for chat completion with large language models.

**Parameters:**
- `prompt` (required): The input prompt for completion
- `model` (optional): Model name (defaults to configured model)
- `temperature` (optional): Temperature for response generation (0.0 to 1.0)
- `max_tokens` (optional): Maximum number of tokens to generate (1 to 4000)

**Example Usage:**
```json
{
  "prompt": "Explain the benefits of using Snowflake for data warehousing",
  "model": "snowflake-llama-3.3-70b",
  "temperature": 0.7,
  "max_tokens": 500
}
```

### 2. Cortex Search

**Tool Name:** `cortex_search`

**Description:** Use Cortex Search Service for semantic search over text data.

**Parameters:**
- `service_name` (required): Name of the search service to use
- `query` (required): Search query
- `limit` (optional): Maximum number of results to return (1 to 100, default: 10)
- `filter` (optional): Filter expression for search results

**Example Usage:**
```json
{
  "service_name": "customer_support_search",
  "query": "login issues password reset",
  "limit": 5,
  "filter": "category = 'technical_support'"
}
```

### 3. Cortex Analyst

**Tool Name:** `cortex_analyst`

**Description:** Use Cortex Analyst for natural language querying over structured data.

**Parameters:**
- `service_name` (required): Name of the analyst service to use
- `question` (required): Natural language question about the data
- `include_sql` (optional): Whether to include the generated SQL in the response (default: true)
- `include_data` (optional): Whether to include the query results in the response (default: true)

**Example Usage:**
```json
{
  "service_name": "sales_analyst",
  "question": "What were the top 5 products by revenue last quarter?",
  "include_sql": true,
  "include_data": true
}
```

### 4. List Cortex Services

**Tool Name:** `list_cortex_services`

**Description:** List available Cortex services (Search and Analyst).

**Parameters:**
- `service_type` (optional): Type of services to list ("search", "analyst", "complete", "all", default: "all")

**Example Usage:**
```json
{
  "service_type": "all"
}
```

## Available Resources

The MCP server provides the following Cortex-related resources:

- `snowflake://cortex/search_services` - List of available search services
- `snowflake://cortex/analyst_services` - List of available analyst services
- `snowflake://cortex/complete_config` - Cortex Complete configuration

## Error Handling

The integration includes comprehensive error handling for Cortex-specific issues:

- **CortexCompleteError**: Errors in Cortex Complete operations
- **CortexSearchError**: Errors in Cortex Search operations
- **CortexAnalystError**: Errors in Cortex Analyst operations
- **CortexServiceNotFoundError**: When a requested service is not found in configuration
- **CortexModelNotSupportedError**: When an unsupported model is requested

## Validation

All Cortex service parameters are validated before execution:

- Prompt length limits (10,000 characters for Complete, 2,000 for Analyst)
- Query length limits (1,000 characters for Search)
- Model validation against supported models
- Parameter type and range validation
- Service name validation

## Setup Instructions

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Services

1. Create your `service_config.yaml` file with your Cortex services
2. Ensure your Snowflake environment has the necessary Cortex services created
3. Verify your Snowflake user has the required permissions

### 3. Start the MCP Server

```bash
python server.py
```

The server will automatically load the Cortex configuration and make the services available through the MCP protocol.

## Troubleshooting

### Common Issues

1. **Service Not Found Error**
   - Verify the service name in your configuration matches exactly
   - Check that the service exists in your Snowflake environment

2. **Permission Denied**
   - Ensure your Snowflake user has the `SNOWFLAKE.CORTEX_USER` role
   - Verify access to the specific databases and schemas

3. **Model Not Supported**
   - Check that the model name is exactly as specified in the valid models list
   - Ensure the model is available in your Snowflake region

4. **Configuration Not Found**
   - Verify the `service_config.yaml` file exists in the project root
   - Check the YAML syntax is valid

### Debugging

Enable debug logging by setting the log level:

```python
logging.basicConfig(level=logging.DEBUG)
```

This will provide detailed information about Cortex service calls and any errors that occur.

## Best Practices

1. **Service Naming**: Use descriptive, unique names for your Cortex services
2. **Error Handling**: Always check for errors in the response before processing results
3. **Parameter Validation**: Validate user inputs before calling Cortex services
4. **Resource Management**: Monitor usage to avoid hitting rate limits
5. **Security**: Ensure proper access controls are in place for your Cortex services

## Examples

### Complete Example: Customer Support Analysis

```json
{
  "tool": "cortex_analyst",
  "parameters": {
    "service_name": "customer_analyst",
    "question": "What are the most common customer complaints this month?",
    "include_sql": true,
    "include_data": true
  }
}
```

### Search Example: Finding Documentation

```json
{
  "tool": "cortex_search",
  "parameters": {
    "service_name": "product_catalog_search",
    "query": "API authentication setup",
    "limit": 3
  }
}
```

### Complete Example: Generating Documentation

```json
{
  "tool": "cortex_complete",
  "parameters": {
    "prompt": "Write a brief explanation of how to use Snowflake Cortex AI services",
    "model": "snowflake-llama-3.3-70b",
    "temperature": 0.5,
    "max_tokens": 300
  }
}
```

This integration provides a powerful way to leverage Snowflake's Cortex AI capabilities through the MCP protocol, enabling natural language interactions with your data and AI-powered analysis.

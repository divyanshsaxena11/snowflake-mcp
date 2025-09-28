# Snowflake MCP Server

A Model Context Protocol (MCP) server that enables Cursor to interact with Snowflake databases through natural language queries and database operations.

## Features

- 🔐 **Secure Connection**: Supports Snowflake authentication with environment-based configuration
- 🛡️ **Query Validation**: Built-in SQL injection protection and query validation
- 📊 **Database Operations**: Execute queries, explore schemas, tables, and columns
- 🔧 **Comprehensive Tools**: Get databases, schemas, tables, columns, warehouses, and roles
- 🧠 **Cortex AI Integration**: Full support for Snowflake Cortex AI services
  - **Cortex Complete**: Chat completion with large language models
  - **Cortex Search**: Semantic search over text data
  - **Cortex Analyst**: Natural language querying over structured data
- ⚡ **Async Support**: Full async/await support for better performance
- 🚨 **Error Handling**: Detailed error messages and proper exception handling
- 📝 **Logging**: Comprehensive logging for debugging and monitoring

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd snowflake-mcp
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment variables**:
   ```bash
   cp env.example .env
   # Edit .env with your Snowflake credentials
   ```

## Configuration

### Environment Variables

Create a `.env` file with your Snowflake connection details:

```env
# Required Snowflake Connection Parameters
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_locator
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_WAREHOUSE=your_warehouse

# Optional Parameters
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_REGION=us-east-1
SNOWFLAKE_AUTHENTICATOR=snowflake
SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE=true
```

### Cortex AI Configuration

Create a `service_config.yaml` file to configure your Cortex AI services:

```yaml
# Cortex Complete configuration
cortex_complete:
  default_model: "snowflake-llama-3.3-70b"

# Cortex Search services
search_services:
  - service_name: "customer_support_search"
    description: "Search service for customer support data"
    database_name: "SUPPORT_DB"
    schema_name: "SERVICES"

# Cortex Analyst services
analyst_services:
  - service_name: "sales_analyst"
    semantic_model: "@SALES_DB.ANALYTICS.SEMANTIC_MODELS/sales_semantic_model.yaml"
    description: "Analyst service for sales data"
```

For detailed Cortex configuration instructions, see [CORTEX_INTEGRATION.md](CORTEX_INTEGRATION.md).

### Cursor Configuration

Add the following to your Cursor MCP configuration file (`mcp.json`):

```json
{
  "mcpServers": {
    "snowflake": {
      "command": "python",
      "args": ["/path/to/snowflake-mcp/server.py"],
      "env": {
        "SNOWFLAKE_USER": "your_username",
        "SNOWFLAKE_PASSWORD": "your_password",
        "SNOWFLAKE_ACCOUNT": "your_account_locator",
        "SNOWFLAKE_DATABASE": "your_database",
        "SNOWFLAKE_WAREHOUSE": "your_warehouse"
      }
    }
  }
}
```

## Usage

### Available Tools

The MCP server provides the following tools:

#### Database Operations

#### 1. `execute_query`
Execute SQL queries on your Snowflake database.

**Parameters:**
- `query` (required): SQL query to execute
- `params` (optional): Query parameters as a dictionary

**Example:**
```python
# Simple query
execute_query(query="SELECT * FROM users LIMIT 10")

# Parameterized query
execute_query(
    query="SELECT * FROM users WHERE age > :min_age",
    params={"min_age": 18}
)
```

#### 2. `get_databases`
Get a list of all available databases.

#### 3. `get_schemas`
Get a list of schemas in a database.

**Parameters:**
- `database` (optional): Database name to filter schemas

#### 4. `get_tables`
Get a list of tables in a database/schema.

**Parameters:**
- `database` (optional): Database name
- `schema` (optional): Schema name

#### 5. `get_columns`
Get column information for a specific table.

**Parameters:**
- `table` (required): Table name
- `database` (optional): Database name
- `schema` (optional): Schema name

#### 6. `get_warehouses`
Get a list of available warehouses.

#### 7. `get_roles`
Get a list of available roles.

#### 8. `test_connection`
Test the Snowflake database connection.

#### Cortex AI Tools

#### 9. `cortex_complete`
Use Cortex Complete for chat completion with large language models.

**Parameters:**
- `prompt` (required): The input prompt for completion
- `model` (optional): Model name (defaults to configured model)
- `temperature` (optional): Temperature for response generation (0.0 to 1.0)
- `max_tokens` (optional): Maximum number of tokens to generate

**Example:**
```json
{
  "prompt": "Explain the benefits of using Snowflake for data warehousing",
  "model": "snowflake-llama-3.3-70b",
  "temperature": 0.7,
  "max_tokens": 500
}
```

#### 10. `cortex_search`
Use Cortex Search Service for semantic search over text data.

**Parameters:**
- `service_name` (required): Name of the search service to use
- `query` (required): Search query
- `limit` (optional): Maximum number of results to return (1 to 100, default: 10)
- `filter` (optional): Filter expression for search results

**Example:**
```json
{
  "service_name": "customer_support_search",
  "query": "login issues password reset",
  "limit": 5
}
```

#### 11. `cortex_analyst`
Use Cortex Analyst for natural language querying over structured data.

**Parameters:**
- `service_name` (required): Name of the analyst service to use
- `question` (required): Natural language question about the data
- `include_sql` (optional): Whether to include the generated SQL in the response
- `include_data` (optional): Whether to include the query results in the response

**Example:**
```json
{
  "service_name": "sales_analyst",
  "question": "What were the top 5 products by revenue last quarter?",
  "include_sql": true,
  "include_data": true
}
```

#### 12. `list_cortex_services`
List available Cortex services (Search and Analyst).

**Parameters:**
- `service_type` (optional): Type of services to list ("search", "analyst", "complete", "all")

### Available Resources

The server also provides these resources for exploration:

#### Database Resources
- `snowflake://databases` - List of databases
- `snowflake://schemas` - List of schemas
- `snowflake://tables` - List of tables
- `snowflake://warehouses` - List of warehouses
- `snowflake://roles` - List of roles

#### Cortex AI Resources
- `snowflake://cortex/search_services` - List of available search services
- `snowflake://cortex/analyst_services` - List of available analyst services
- `snowflake://cortex/complete_config` - Cortex Complete configuration

## Security Features

### Query Validation
- **SQL Injection Protection**: Validates queries for dangerous patterns
- **DDL Protection**: By default, prevents DDL operations (DROP, CREATE, ALTER, etc.)
- **Parameter Validation**: Ensures query parameters are properly formatted

### Connection Security
- **Environment Variables**: Sensitive credentials stored in environment variables
- **Connection Validation**: Validates connection parameters before attempting connection
- **Error Handling**: Proper error handling without exposing sensitive information

## Error Handling

The server provides detailed error messages for different types of errors:

- **ValidationError**: Input validation failures
- **QueryError**: SQL query execution errors
- **ConnectionError**: Database connection issues
- **AuthenticationError**: Authentication failures
- **ConfigurationError**: Configuration problems

## Development

### Project Structure

```
snowflake-mcp/
├── src/
│   ├── __init__.py
│   ├── snowflake_client.py    # Snowflake database client with Cortex support
│   ├── mcp_server.py          # MCP server implementation
│   ├── errors.py              # Custom exceptions including Cortex errors
│   └── validators.py          # Input validation utilities including Cortex validation
├── server.py                  # Entry point
├── service_config.yaml        # Cortex AI services configuration
├── requirements.txt           # Python dependencies
├── env.example               # Environment variables template
├── README.md                 # This file
└── CORTEX_INTEGRATION.md     # Detailed Cortex AI integration guide
```

### Running the Server

```bash
# Run directly
python server.py

# Or run the MCP server module
python -m src.mcp_server
```

### Testing

Test the connection:
```python
from src.snowflake_client import SnowflakeClient

client = SnowflakeClient()
success = await client.test_connection()
print(f"Connection test: {'✅ Success' if success else '❌ Failed'}")
```

## Troubleshooting

### Common Issues

1. **Authentication Failed**
   - Verify your Snowflake credentials
   - Check if your account is active
   - Ensure the user has proper permissions

2. **Connection Timeout**
   - Check your network connection
   - Verify the account locator format
   - Ensure the warehouse is running

3. **Query Validation Errors**
   - Check for SQL injection patterns in your query
   - Ensure the query follows proper SQL syntax
   - Use parameterized queries for dynamic values

4. **Configuration Errors**
   - Verify all required environment variables are set
   - Check the format of connection parameters
   - Ensure the .env file is in the correct location

### Logging

The server provides detailed logging. Check the console output for:
- Connection status
- Query execution details
- Error messages
- Validation warnings

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review the error logs
3. Create an issue in the repository
4. Contact the maintainers

---

**Note**: This MCP server is designed for read-only operations by default. Modify the validation settings if you need to perform DDL operations.

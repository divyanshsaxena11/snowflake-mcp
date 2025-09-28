#!/usr/bin/env python3
"""
Test script for Snowflake MCP connection.
Run this to verify your configuration before using with Cursor.
"""

import asyncio
import os
import sys
from dotenv import load_dotenv

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.snowflake_client import SnowflakeClient
from src.errors import SnowflakeMCPError

async def test_connection():
    """Test the Snowflake connection and basic operations."""
    print("🔧 Testing Snowflake MCP Connection...")
    print("=" * 50)
    
    try:
        # Load environment variables
        load_dotenv()
        
        # Initialize client
        print("📡 Initializing Snowflake client...")
        client = SnowflakeClient()
        print("✅ Client initialized successfully")
        
        # Test connection
        print("\n🔌 Testing database connection...")
        if await client.test_connection():
            print("✅ Connection test successful!")
        else:
            print("❌ Connection test failed!")
            return False
        
        # Test basic operations
        print("\n📊 Testing basic operations...")
        
        # Get databases
        print("  - Getting databases...")
        databases = await client.get_databases()
        print(f"    Found {len(databases)} databases")
        
        # Get schemas
        print("  - Getting schemas...")
        schemas = await client.get_schemas()
        print(f"    Found {len(schemas)} schemas")
        
        # Get tables
        print("  - Getting tables...")
        tables = await client.get_tables()
        print(f"    Found {len(tables)} tables")
        
        # Get warehouses
        print("  - Getting warehouses...")
        warehouses = await client.get_warehouses()
        print(f"    Found {len(warehouses)} warehouses")
        
        # Get roles
        print("  - Getting roles...")
        roles = await client.get_roles()
        print(f"    Found {len(roles)} roles")
        
        print("\n🎉 All tests passed! Your Snowflake MCP is ready to use.")
        return True
        
    except SnowflakeMCPError as e:
        print(f"❌ Snowflake MCP Error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        return False

async def test_query():
    """Test query execution."""
    print("\n🔍 Testing query execution...")
    print("=" * 30)
    
    try:
        client = SnowflakeClient()
        
        # Test simple query
        print("  - Testing simple query...")
        results, columns = await client.execute_query("SELECT CURRENT_TIMESTAMP() as current_time")
        if results:
            print(f"    ✅ Query successful: {results[0]}")
        else:
            print("    ❌ Query returned no results")
        
        # Test parameterized query
        print("  - Testing parameterized query...")
        results, columns = await client.execute_query(
            "SELECT :param as test_value", 
            {"param": "Hello Snowflake MCP!"}
        )
        if results:
            print(f"    ✅ Parameterized query successful: {results[0]}")
        else:
            print("    ❌ Parameterized query returned no results")
            
        print("✅ Query tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Query test failed: {e}")
        return False

async def main():
    """Main test function."""
    print("🚀 Snowflake MCP Test Suite")
    print("=" * 50)
    
    # Check environment variables
    required_vars = [
        'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD', 'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_DATABASE', 'SNOWFLAKE_WAREHOUSE'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        print("Please set these in your .env file or environment.")
        return False
    
    # Run tests
    connection_ok = await test_connection()
    if not connection_ok:
        return False
    
    query_ok = await test_query()
    if not query_ok:
        return False
    
    print("\n🎉 All tests completed successfully!")
    print("Your Snowflake MCP server is ready to use with Cursor.")
    return True

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)

#!/usr/bin/env python3
"""
Quick script to get available Snowflake databases.
"""

import asyncio
import os
import sys
from dotenv import load_dotenv

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.snowflake_client import SnowflakeClient

async def get_databases():
    """Get and display available databases."""
    try:
        # Load environment variables
        load_dotenv()
        
        # Initialize client
        client = SnowflakeClient()
        
        # Get databases
        databases = await client.get_databases()
        
        print("Available Snowflake Databases:")
        print("=" * 40)
        
        if databases:
            for db in databases:
                print(f"- {db.get('name', 'Unknown')}")
        else:
            print("No databases found or connection failed.")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(get_databases())

#!/bin/bash

# Snowflake MCP Installation Script

echo "🚀 Installing Snowflake MCP Server..."
echo "=================================="

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not installed."
    echo "Please install Python 3.8 or higher and try again."
    exit 1
fi

# Check Python version
python_version=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
required_version="3.8"

if [ "$(printf '%s\n' "$required_version" "$python_version" | sort -V | head -n1)" != "$required_version" ]; then
    echo "❌ Python $python_version is installed, but Python $required_version or higher is required."
    exit 1
fi

echo "✅ Python $python_version detected"

# Create virtual environment
echo "📦 Creating virtual environment..."
python3 -m venv venv

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "⬆️  Upgrading pip..."
pip install --upgrade pip

# Install requirements
echo "📚 Installing dependencies..."
pip install -r requirements.txt

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "📝 Creating .env file from template..."
    cp env.example .env
    echo "⚠️  Please edit .env file with your Snowflake credentials"
fi

# Make scripts executable
chmod +x server.py
chmod +x test_connection.py

echo ""
echo "✅ Installation completed successfully!"
echo ""
echo "Next steps:"
echo "1. Edit .env file with your Snowflake credentials"
echo "2. Run: python test_connection.py (to test your connection)"
echo "3. Configure Cursor with the provided mcp.json configuration"
echo "4. Run: python server.py (to start the MCP server)"
echo ""
echo "For detailed instructions, see README.md"

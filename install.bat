@echo off
REM Snowflake MCP Installation Script for Windows

echo 🚀 Installing Snowflake MCP Server...
echo ==================================

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python is required but not installed.
    echo Please install Python 3.8 or higher and try again.
    pause
    exit /b 1
)

echo ✅ Python detected

REM Create virtual environment
echo 📦 Creating virtual environment...
python -m venv venv

REM Activate virtual environment
echo 🔧 Activating virtual environment...
call venv\Scripts\activate.bat

REM Upgrade pip
echo ⬆️  Upgrading pip...
python -m pip install --upgrade pip

REM Install requirements
echo 📚 Installing dependencies...
pip install -r requirements.txt

REM Create .env file if it doesn't exist
if not exist .env (
    echo 📝 Creating .env file from template...
    copy env.example .env
    echo ⚠️  Please edit .env file with your Snowflake credentials
)

echo.
echo ✅ Installation completed successfully!
echo.
echo Next steps:
echo 1. Edit .env file with your Snowflake credentials
echo 2. Run: python test_connection.py (to test your connection)
echo 3. Configure Cursor with the provided mcp.json configuration
echo 4. Run: python server.py (to start the MCP server)
echo.
echo For detailed instructions, see README.md
pause

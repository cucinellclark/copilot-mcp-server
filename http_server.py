#!/usr/bin/env python3
"""
Copilot MCP Server - HTTP Mode

This server provides MCP tools for python_code and rag_database functionality.
"""

from fastmcp import FastMCP
from tools.python_code_tools import register_python_code_tools
import json
import sys
import os

# Load configuration (optional - adjust path as needed)
config_path = os.path.join(os.path.dirname(__file__), "config", "config.json")
try:
    with open(config_path, "r") as f:
        config = json.load(f)
except FileNotFoundError:
    print("Warning: config/config.json not found, using defaults", file=sys.stderr)
    config = {}

# Get configuration values
port = int(os.environ.get("PORT", config.get("port", 12011)))
mcp_url = config.get("mcp_url", "127.0.0.1")
python_code_config = config.get("python_code", {})
rag_database_config = config.get("rag_database", {})

# Create FastMCP server
mcp = FastMCP("Copilot MCP Server")

# Register all tools with configuration
print("Registering python_code tools...", file=sys.stderr)
register_python_code_tools(mcp, python_code_config)

# Add health check tool
@mcp.tool()
def health_check() -> str:
    """Health check endpoint"""
    return '{"status": "healthy", "service": "copilot-mcp", "mode": "http"}'

def main() -> int:
    """Main entry point for the Copilot MCP Server in HTTP mode."""
    print(f"Starting Copilot MCP Server on port {port}...", file=sys.stderr)
    print(f"  - Server URL: http://{mcp_url}:{port}", file=sys.stderr)
    
    try:
        # Run in HTTP mode
        mcp.run(transport="http", host=mcp_url, port=port)
    except KeyboardInterrupt:
        print("Server stopped.", file=sys.stderr)
    except Exception as e:
        print(f"Server error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())


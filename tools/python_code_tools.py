#!/usr/bin/env python3
"""
Python Code Tools

This module contains MCP tools for executing and managing Python code.
"""

import json
import sys
from typing import Optional, Dict, Any
from fastmcp import FastMCP

from functions.python_code_functions import (
    execute_python_code,
    validate_python_code,
    get_python_environment_info
)


def register_python_code_tools(mcp: FastMCP, config: dict = None, token_provider=None):
    """
    Register all Python code-related MCP tools with the FastMCP server.
    
    Args:
        mcp: FastMCP server instance
        config: Configuration dictionary for Python code settings
        token_provider: TokenProvider instance for handling authentication tokens
    """
    if config is None:
        config = {}
    
    default_timeout = config.get("default_timeout", 30)
    max_timeout = config.get("max_timeout", 300)
    capture_output_default = config.get("capture_output", True)
    
    @mcp.tool()
    def run_python_code(
        code: str
    ) -> Dict[str, Any]:
        """
        Execute Python code and return the result.
        Validates syntax before execution. If syntax errors are found,
        they are returned in the response without executing the code.
        
        Args:
            code: The Python code to execute
        
        Returns:
            JSON string with execution results including:
            - success: bool indicating if execution succeeded
            - output: stdout output
            - error: stderr output or syntax error (if any)
            - result: return value (if any)
            - execution_time: time taken in seconds
        """
        try:
            # Extract token using TokenProvider (if available)
            # Token is extracted from Authorization header, not passed as parameter
            auth_token = None
            if token_provider:
                auth_token = token_provider.get_token()
                # Token is available but not required for Python code execution
                # It can be used by the code if needed (e.g., for API calls)
            
            # Validate syntax first
            validation_result = validate_python_code(code)
            if not validation_result["valid"]:
                # Return syntax error in the same format as execution errors
                error_msg = f"Syntax error"
                if validation_result.get("line"):
                    error_msg += f" on line {validation_result['line']}"
                if validation_result.get("error"):
                    error_msg += f": {validation_result['error']}"
                
                return {
                    "success": False,
                    "output": "",
                    "error": error_msg,
                    "errorType": "SYNTAX_ERROR",
                    "result": None,
                    "execution_time": 0.0,
                    "source": "bvbrc-python-execution"
                }
            
            # If validation passes, execute the code
            result = execute_python_code(
                code=code,
                config=config,
                token=auth_token  # Pass token for workspace uploads
            )
            return result
        except Exception as e:
            return {
                "success": False,
                "error": f"Error executing Python code: {str(e)}",
                "errorType": "API_ERROR",
                "source": "bvbrc-python-execution"
            }
    
    @mcp.tool()
    def get_python_info() -> Dict[str, Any]:
        """
        Get information about the Python environment.
        
        Returns:
            JSON string with Python environment information:
            - version: Python version
            - platform: platform information
            - installed_packages: list of installed packages (optional)
        """
        print("Fetching Python environment information...", file=sys.stderr)
        try:
            result = get_python_environment_info(config=config)
            return result
        except Exception as e:
            return {
                "error": f"Error getting Python info: {str(e)}",
                "errorType": "API_ERROR",
                "source": "bvbrc-python-execution"
            }


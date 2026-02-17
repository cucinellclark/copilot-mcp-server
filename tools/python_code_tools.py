#!/usr/bin/env python3
"""
Python Code Tools with Advanced File Handling

This module provides MCP tools for executing and managing Python code in isolated
containers with comprehensive file tracking and workspace integration.

Features:
- Secure execution in Singularity containers (no network access)
- Automatic detection and tracking of generated files
- File metadata extraction (type, size, MIME type, content preview)
- Automatic upload of all outputs to user's workspace
- Syntax validation before execution
- Detailed execution results and error reporting
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
        Execute Python code for processing local files.
        
        This tool is specifically designed for processing local files in your workspace.
        It provides secure Python code execution with comprehensive file management:
        - Automatically detects and tracks all files created during execution
        - Returns metadata for generated files (name, size, type, MIME type)
        - Includes content preview for small text files
        - Uploads all generated files (script + outputs) to user's workspace
        - Validates syntax before execution to catch errors early
        
        All code runs in an isolated environment with no network access. Files are saved
        to a unique timestamped directory and automatically uploaded to the workspace at:
        /<user_id>/home/CopilotCodeDev/python_run_<timestamp>_<uuid>/
        
        Args:
            code: The Python code to execute for processing local files
        
        Returns:
            Execution results including:
            - success: bool indicating if execution succeeded
            - output: stdout from the script
            - error: stderr output or syntax/execution errors
            - execution_time: time taken in seconds
            - output_files: array of file metadata for all generated files
            - workspace_upload: details about workspace upload (path, file count, status)
        
        Example use cases:
        - Data processing: load CSV, transform, save results
        - File generation: create plots, reports, or data exports
        - Batch operations: process multiple files and save outputs
        - Complex analysis: multi-step workflows with intermediate file outputs
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
        Get detailed information about the Python execution environment.
        
        Returns comprehensive details about the Python runtime that will be used
        for code execution, including version, platform, and system architecture.
        
        Returns:
            Python environment details:
            - version: Full Python version string
            - version_info: Major, minor, and micro version numbers
            - platform: Platform information (OS and version)
            - architecture: System architecture details
            - machine: Machine type
            - processor: Processor information
            - python_path: Path to the Python executable
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


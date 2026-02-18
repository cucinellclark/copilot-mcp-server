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
from fastmcp.utilities.logging import get_logger

from functions.python_code_functions import (
    execute_python_code,
    validate_python_code,
    get_python_environment_info
)

logger = get_logger(__name__)


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
        code: str,
        session_id: str
    ) -> Dict[str, Any]:
        """
        Execute Python code for processing local files.
        
        This tool is specifically designed for processing local files on the local filesystem.
        It provides secure Python code execution with comprehensive file management:
        - Automatically detects and tracks all files created during execution
        - Returns metadata for generated files (name, size, type, MIME type)
        - Includes content preview for small text files
        - Uploads all generated files (script + outputs) to user's workspace
        - Validates syntax before execution to catch errors early
        
        All code runs in an isolated environment with no network access. Files are saved
        to a unique timestamped local directory under /tmp (or configured temp_directory)
        and may also be uploaded to workspace for convenience.
        
         CRITICAL PATH RULE:
         - When writing Python code like open(...), always use local filesystem paths from
           output_files[*].path (for example: /tmp/.../file.json).
         - Do NOT use workspace_upload.workspace_path with open(...). Workspace paths are
           remote workspace identifiers, not local OS file paths.
         - If session_id is provided, session files are available at:
           /tmp/copilot/sessions/{session_id}/downloads/
        
        Args:
            code: The Python code to execute for processing local files
            session_id: Session ID for accessing session-specific files in /tmp/copilot/sessions/{session_id}/ (REQUIRED)
        
        Returns:
            Execution results including:
            - success: bool indicating if execution succeeded
            - output: stdout from the script
            - error: stderr output or syntax/execution errors
            - execution_time: time taken in seconds
            - output_files: array of file metadata for all generated files
            - workspace_upload: details about workspace upload (workspace identifier, file count, status)
        
        Example use cases:
        - Data processing: load CSV, transform, save results
        - File generation: create plots, reports, or data exports
        - Batch operations: process multiple files and save outputs
        - Complex analysis: multi-step workflows with intermediate file outputs
        """
        logger.info(f"[run_python_code] Starting Python code execution (session_id: {session_id})")
        logger.debug(f"[run_python_code] Code length: {len(code)} characters")
        
        # Validate session_id is provided
        if not session_id:
            error_msg = "session_id is required for run_python_code but was not provided"
            logger.error(f"[run_python_code] {error_msg}")
            return {
                "success": False,
                "output": "",
                "error": error_msg,
                "errorType": "MISSING_PARAMETER",
                "result": None,
                "execution_time": 0.0,
                "source": "bvbrc-python-execution"
            }
        
        try:
            # Extract token using TokenProvider (if available)
            # Token is extracted from Authorization header, not passed as parameter
            auth_token = None
            if token_provider:
                auth_token = token_provider.get_token()
                if auth_token:
                    logger.debug("[run_python_code] Authentication token retrieved successfully")
                else:
                    logger.warning("[run_python_code] Token provider available but no token retrieved")
                # Token is available but not required for Python code execution
                # It can be used by the code if needed (e.g., for API calls)
            else:
                logger.debug("[run_python_code] No token provider available")
            
            # Validate syntax first
            logger.debug("[run_python_code] Validating Python code syntax")
            validation_result = validate_python_code(code)
            if not validation_result["valid"]:
                # Return syntax error in the same format as execution errors
                error_msg = f"Syntax error"
                if validation_result.get("line"):
                    error_msg += f" on line {validation_result['line']}"
                if validation_result.get("error"):
                    error_msg += f": {validation_result['error']}"
                
                logger.warning(f"[run_python_code] Syntax validation failed: {error_msg}")
                return {
                    "success": False,
                    "output": "",
                    "error": error_msg,
                    "errorType": "SYNTAX_ERROR",
                    "result": None,
                    "execution_time": 0.0,
                    "source": "bvbrc-python-execution"
                }
            
            logger.info("[run_python_code] Syntax validation passed, executing code")
            # If validation passes, execute the code
            result = execute_python_code(
                code=code,
                config=config,
                token=auth_token,  # Pass token for workspace uploads
                session_id=session_id  # Pass session_id for binding session directory
            )
            
            logger.info(f"[run_python_code] Execution completed - success: {result.get('success', False)}, execution_time: {result.get('execution_time', 0):.2f}s")
            if result.get("output_files"):
                logger.info(f"[run_python_code] Generated {len(result['output_files'])} output files")
            if result.get("workspace_upload"):
                upload_info = result["workspace_upload"]
                if upload_info.get("success"):
                    logger.info(f"[run_python_code] Workspace upload successful: {upload_info.get('successful', 0)}/{upload_info.get('total_files', 0)} files")
                else:
                    error_detail = upload_info.get('error', 'Unknown error')
                    # If there are file-specific errors, include them
                    if upload_info.get('files'):
                        failed_files = [f for f in upload_info['files'] if not f.get('success')]
                        if failed_files:
                            error_detail = f"{len(failed_files)} file(s) failed: " + "; ".join([f"{f.get('file', 'unknown')}: {f.get('error', 'unknown')}" for f in failed_files[:3]])
                    logger.warning(f"[run_python_code] Workspace upload failed: {error_detail}")
            
            return result
        except Exception as e:
            logger.error(f"[run_python_code] Unexpected error: {str(e)}", exc_info=True)
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
        logger.info("[get_python_info] Fetching Python environment information")
        try:
            result = get_python_environment_info(config=config)
            logger.debug(f"[get_python_info] Retrieved Python info: version={result.get('version_info', {}).get('major', '?')}.{result.get('version_info', {}).get('minor', '?')}")
            return result
        except Exception as e:
            logger.error(f"[get_python_info] Error getting Python info: {str(e)}", exc_info=True)
            return {
                "error": f"Error getting Python info: {str(e)}",
                "errorType": "API_ERROR",
                "source": "bvbrc-python-execution"
            }


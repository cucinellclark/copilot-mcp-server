"""
Python Code Functions

This module provides functions for executing and managing Python code.
"""

import sys
import subprocess
import ast
import platform
from typing import Dict, Any, Optional, List, Set
import time
import os
import uuid
import mimetypes
from datetime import datetime
from fastmcp.utilities.logging import get_logger
from .workspace_functions import (
    get_user_id_from_token,
    upload_files_to_workspace
)

logger = get_logger(__name__)


def _get_files_in_directory(directory: str) -> Set[str]:
    """Get set of all file paths in a directory (recursively)."""
    files = set()
    if not os.path.exists(directory):
        return files
    try:
        for root, dirs, filenames in os.walk(directory):
            for filename in filenames:
                full_path = os.path.join(root, filename)
                # Normalize path to handle symlinks
                full_path = os.path.normpath(full_path)
                files.add(full_path)
    except Exception:
        # If we can't walk the directory, return empty set
        pass
    return files


def _get_file_info(file_path: str, include_contents: bool = True) -> Dict[str, Any]:
    """Get metadata about a file.
    
    Args:
        file_path: Path to the file
        include_contents: Whether to include file contents (text for small files only)
    
    Returns:
        Dictionary with file metadata
    """
    try:
        stat = os.stat(file_path)
        file_info = {
            "path": file_path,
            "local_path": file_path,
            "name": os.path.basename(file_path),
            "size": stat.st_size,
            "modified_time": stat.st_mtime,
            "is_file": os.path.isfile(file_path),
        }
        
        # Try to determine MIME type
        mime_type, _ = mimetypes.guess_type(file_path)
        if mime_type:
            file_info["mime_type"] = mime_type
            file_info["type"] = mime_type.split('/')[0]  # 'image', 'text', etc.
        else:
            # Fallback: check extension
            ext = os.path.splitext(file_path)[1].lower()
            if ext in ['.png', '.jpg', '.jpeg', '.gif', '.bmp', '.svg', '.webp']:
                file_info["type"] = "image"
                file_info["mime_type"] = f"image/{ext[1:]}" if ext else "image/unknown"
            elif ext in ['.csv', '.txt', '.json', '.xml', '.yaml', '.yml']:
                file_info["type"] = "text"
            elif ext in ['.pdf']:
                file_info["type"] = "document"
                file_info["mime_type"] = "application/pdf"
            else:
                file_info["type"] = "unknown"
        
        if not include_contents:
            return file_info
        
        # For small text files, include content preview
        if file_info.get("type") == "text" and stat.st_size < 10000:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    file_info["content"] = content
            except (UnicodeDecodeError, IOError):
                # File might be binary or not readable
                pass
        
        # Note: Images are not included as base64 to keep response size manageable
        
        return file_info
    except Exception as e:
        return {
            "path": file_path,
            "error": f"Could not read file info: {str(e)}"
        }


def execute_python_code(
    code: str,
    timeout: Optional[int] = 30,
    capture_output: bool = True,
    config: Optional[dict] = None,
    token: Optional[str] = None,
    session_id: str = None
) -> Dict[str, Any]:
    """
    Execute Python code by writing it to a temporary script and running it
    through a Singularity container. Generated files and the script are uploaded
    to the user's workspace.
    
    Args:
        code: The Python code to execute
        timeout: Maximum execution time in seconds
        capture_output: Whether to capture stdout/stderr
        config: Configuration dictionary with settings
        token: Authentication token for workspace uploads
        session_id: Session ID to bind /tmp/copilot/sessions/{session_id}/ directory (REQUIRED)
    
    Returns:
        Dictionary with execution results including output_files and workspace_upload_results
    """
    logger.info(f"[execute_python_code] Starting Python code execution (session_id: {session_id})")
    if config is None:
        config = {}
    start_time = time.time()
    result = {
        "success": False,
        "output": "",
        "error": "",
        "result": None,
        "execution_time": 0.0,
        "output_files": [],  # Files created by the script
        "workspace_upload": None,  # Workspace upload results
        "source": "bvbrc-python-execution"
    }
    
    # Validate session_id is provided
    if not session_id:
        error_msg = "session_id is required but was not provided"
        logger.error(f"[execute_python_code] {error_msg}")
        result["error"] = error_msg
        result["errorType"] = "MISSING_PARAMETER"
        result["execution_time"] = time.time() - start_time
        return result
    
    # Get configuration values
    singularity_container = config.get("singularity_container")
    effective_timeout = timeout or config.get("default_timeout", 30)
    include_file_contents = config.get("include_file_contents", True)  # Config option
    workspace_output = config.get("workspace_output", "CopilotCodeDev")
    workspace_url = config.get("workspace_url", "https://p3.theseed.org/services/Workspace")
    copilot_sessions_base = config.get("copilot_sessions_base", "/tmp/copilot/sessions")
    
    # Use session directory as the base for execution
    session_dir = os.path.join(copilot_sessions_base, session_id)
    
    logger.debug(f"[execute_python_code] Configuration - timeout: {effective_timeout}s, session_dir: {session_dir}, workspace_output: {workspace_output}")
    logger.debug(f"[execute_python_code] Token provided: {token is not None}, session_id: {session_id}")
    
    # Validate required configuration
    if not singularity_container:
        error_msg = "singularity_container not specified in config"
        logger.error(f"[execute_python_code] {error_msg}")
        result["error"] = error_msg
        result["errorType"] = "CONFIGURATION_ERROR"
        result["execution_time"] = time.time() - start_time
        return result
    
    if not os.path.exists(singularity_container):
        error_msg = f"Singularity container not found: {singularity_container}"
        logger.error(f"[execute_python_code] {error_msg}")
        result["error"] = error_msg
        result["errorType"] = "CONFIGURATION_ERROR"
        result["execution_time"] = time.time() - start_time
        return result
    
    logger.debug(f"[execute_python_code] Singularity container found: {singularity_container}")
    
    # Verify session directory exists and is accessible
    if not os.path.exists(session_dir):
        error_msg = f"Session directory does not exist: {session_dir}"
        logger.error(f"[execute_python_code] {error_msg}")
        result["error"] = error_msg
        result["errorType"] = "CONFIGURATION_ERROR"
        result["execution_time"] = time.time() - start_time
        return result
    
    if not os.path.isdir(session_dir):
        error_msg = f"Session directory path exists but is not a directory: {session_dir}"
        logger.error(f"[execute_python_code] {error_msg}")
        result["error"] = error_msg
        result["errorType"] = "CONFIGURATION_ERROR"
        result["execution_time"] = time.time() - start_time
        return result
    
    if not os.access(session_dir, os.W_OK):
        error_msg = f"Session directory is not writable: {session_dir}"
        logger.error(f"[execute_python_code] {error_msg}")
        result["error"] = error_msg
        result["errorType"] = "CONFIGURATION_ERROR"
        result["execution_time"] = time.time() - start_time
        return result
    
    # Create unique folder for this code execution within the session directory
    # Format: python_run_YYYYMMDD_HHMMSS_UUID
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id = uuid.uuid4().hex[:8]
    run_folder_name = f"python_run_{timestamp}_{run_id}"
    run_folder_path = os.path.join(session_dir, run_folder_name)
    
    logger.info(f"[execute_python_code] Creating run folder: {run_folder_path}")
    
    # Create the run folder
    try:
        os.makedirs(run_folder_path, exist_ok=True)
        logger.debug(f"[execute_python_code] Run folder created successfully")
    except Exception as e:
        error_msg = f"Failed to create run folder: {str(e)}"
        logger.error(f"[execute_python_code] {error_msg}", exc_info=True)
        result["error"] = error_msg
        result["errorType"] = "EXECUTION_ERROR"
        result["execution_time"] = time.time() - start_time
        return result
    
    # Create script file in the run folder
    script_filename = "script.py"
    script_path = os.path.join(run_folder_path, script_filename)
    
    try:
        # Record files before execution to detect new output files
        logger.debug("[execute_python_code] Recording files before execution")
        files_before = _get_files_in_directory(run_folder_path)
        logger.debug(f"[execute_python_code] Found {len(files_before)} files before execution")
        
        # Write code to script file
        logger.debug(f"[execute_python_code] Writing code to script file: {script_path}")
        with open(script_path, 'w') as f:
            f.write(code)
        logger.debug(f"[execute_python_code] Script file written ({len(code)} characters)")
        
        # Prepare Singularity command
        # TODO: Need to work on network isolation: create loopback interface
        # Bind only the session directory - this gives access to both:
        #   - Downloaded files in {session_dir}/downloads/
        #   - Script and output files in {session_dir}/python_run_*/
        singularity_cmd = [
            "singularity", "exec",
            "--containall",
            "--cleanenv",
            "--no-home",
            "--no-mount", "cwd",
            "--bind", f"{session_dir}:{session_dir}",
            singularity_container,
            "python", script_path
        ]
        
        logger.info(f"[execute_python_code] Executing Python script in Singularity container (timeout: {effective_timeout}s)")
        logger.debug(f"[execute_python_code] Bound session directory: {session_dir}")
        logger.debug(f"[execute_python_code] Singularity command: {' '.join(singularity_cmd)}")
        
        # Execute the script through Singularity
        try:
            exec_start_time = time.time()
            process = subprocess.run(
                singularity_cmd,
                capture_output=capture_output,
                text=True,
                timeout=effective_timeout,
                cwd=run_folder_path  # Set working directory to run folder
            )
            exec_time = time.time() - exec_start_time
            
            result["success"] = (process.returncode == 0)
            if capture_output:
                result["output"] = process.stdout
                result["error"] = process.stderr
            else:
                result["output"] = ""
                result["error"] = ""
            
            logger.info(f"[execute_python_code] Execution completed in {exec_time:.2f}s - return code: {process.returncode}")
            if process.stdout:
                logger.debug(f"[execute_python_code] stdout length: {len(process.stdout)} characters")
            if process.stderr:
                logger.debug(f"[execute_python_code] stderr length: {len(process.stderr)} characters")
                if process.returncode != 0:
                    logger.warning(f"[execute_python_code] stderr content: {process.stderr[:500]}")
            
            if process.returncode != 0:
                result["error"] = result["error"] or f"Process exited with code {process.returncode}"
                result["errorType"] = "EXECUTION_ERROR"
                logger.warning(f"[execute_python_code] Execution failed with return code {process.returncode}")
                
        except subprocess.TimeoutExpired:
            error_msg = f"Execution timed out after {effective_timeout} seconds"
            logger.error(f"[execute_python_code] {error_msg}")
            result["error"] = error_msg
            result["errorType"] = "TIMEOUT_ERROR"
            result["success"] = False
        except FileNotFoundError:
            error_msg = "Singularity command not found. Is Singularity installed?"
            logger.error(f"[execute_python_code] {error_msg}")
            result["error"] = error_msg
            result["errorType"] = "CONFIGURATION_ERROR"
            result["success"] = False
        except Exception as e:
            error_msg = f"Error executing Singularity command: {str(e)}"
            logger.error(f"[execute_python_code] {error_msg}", exc_info=True)
            result["error"] = error_msg
            result["errorType"] = "EXECUTION_ERROR"
            result["success"] = False
        finally:
            # Always try to detect output files, even if execution failed
            # (files might have been created before the error/timeout)
            logger.debug("[execute_python_code] Detecting output files")
            try:
                files_after = _get_files_in_directory(run_folder_path)
                new_files = files_after - files_before
                logger.debug(f"[execute_python_code] Found {len(files_after)} total files, {len(new_files)} new files")
                
                # Filter out the script file itself (it was created before execution)
                script_path_normalized = os.path.normpath(script_path)
                output_files = [f for f in new_files if os.path.normpath(f) != script_path_normalized]
                logger.info(f"[execute_python_code] Detected {len(output_files)} output files (excluding script)")
                
                # Get file information for each output file
                result["output_files"] = []
                for file_path in sorted(output_files):
                    file_info = _get_file_info(file_path, include_contents=include_file_contents)
                    result["output_files"].append(file_info)
                    logger.debug(f"[execute_python_code] Output file: {file_info.get('name', 'unknown')} ({file_info.get('size', 0)} bytes, type: {file_info.get('type', 'unknown')})")
            except Exception as e:
                # If file detection fails, log but don't fail the whole execution
                logger.error(f"[execute_python_code] Error detecting output files: {str(e)}", exc_info=True)
                result["output_files"] = []
                if not result.get("error"):
                    result["error"] = f"Error detecting output files: {str(e)}"
            
            # Upload files to workspace if token is provided
            if token:
                logger.info("[execute_python_code] Starting workspace upload")
                try:
                    # Extract user ID from token
                    logger.debug("[execute_python_code] Extracting user ID from token")
                    user_id = get_user_id_from_token(token)
                    if not user_id:
                        error_msg = "Could not extract user ID from token"
                        logger.warning(f"[execute_python_code] {error_msg}")
                        result["workspace_upload"] = {
                            "success": False,
                            "error": error_msg
                        }
                    else:
                        logger.debug(f"[execute_python_code] User ID extracted: {user_id}")
                        # Build workspace path: /<user_id>/home/<workspace_output>/<run_folder_name>
                        workspace_dir = f"/{user_id}/home/{workspace_output}/{run_folder_name}"
                        logger.info(f"[execute_python_code] Workspace directory: {workspace_dir}")
                        
                        # Collect all files to upload (script + output files)
                        files_to_upload = [script_path]
                        for file_info in result["output_files"]:
                            if "path" in file_info:
                                files_to_upload.append(file_info["path"])
                        
                        logger.info(f"[execute_python_code] Uploading {len(files_to_upload)} files to workspace")
                        logger.debug(f"[execute_python_code] Files to upload: {[os.path.basename(f) for f in files_to_upload]}")
                        
                        upload_start_time = time.time()
                        upload_result = upload_files_to_workspace(
                            files_to_upload,
                            workspace_dir,
                            token,
                            workspace_url
                        )
                        upload_time = time.time() - upload_start_time
                        
                        result["workspace_upload"] = {
                            "success": upload_result.get("success", False),
                            "workspace_path": workspace_dir,
                            "total_files": upload_result.get("total_files", 0),
                            "successful": upload_result.get("successful", 0),
                            "failed": upload_result.get("failed", 0),
                            "files": upload_result.get("files", [])
                        }
                        
                        if upload_result.get("success", False):
                            logger.info(f"[execute_python_code] Workspace upload completed in {upload_time:.2f}s: {upload_result.get('successful', 0)}/{upload_result.get('total_files', 0)} files uploaded successfully")
                        else:
                            logger.warning(f"[execute_python_code] Workspace upload completed with errors: {upload_result.get('successful', 0)}/{upload_result.get('total_files', 0)} files uploaded, {upload_result.get('failed', 0)} failed")
                            # Log individual file errors
                            for file_result in upload_result.get("files", []):
                                if not file_result.get("success"):
                                    logger.error(f"[execute_python_code] Failed to upload {file_result.get('file', 'unknown')}: {file_result.get('error', 'Unknown error')}")
                        
                except Exception as e:
                    error_msg = f"Error uploading to workspace: {str(e)}"
                    logger.error(f"[execute_python_code] {error_msg}", exc_info=True)
                    result["workspace_upload"] = {
                        "success": False,
                        "error": error_msg
                    }
            else:
                logger.debug("[execute_python_code] No token provided, skipping workspace upload")
                result["workspace_upload"] = {
                    "success": False,
                    "message": "No token provided, skipping workspace upload"
                }
        
    except IOError as e:
        error_msg = f"Failed to write script file: {str(e)}"
        logger.error(f"[execute_python_code] {error_msg}", exc_info=True)
        result["error"] = error_msg
        result["errorType"] = "EXECUTION_ERROR"
        result["success"] = False
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(f"[execute_python_code] {error_msg}", exc_info=True)
        result["error"] = error_msg
        result["errorType"] = "EXECUTION_ERROR"
        result["success"] = False
    finally:
        result["execution_time"] = time.time() - start_time
        logger.info(f"[execute_python_code] Total execution time: {result['execution_time']:.2f}s, success: {result.get('success', False)}")
    
    return result


def validate_python_code(code: str) -> Dict[str, Any]:
    """
    Validate Python code syntax without executing it.
    
    Args:
        code: The Python code to validate
    
    Returns:
        Dictionary with validation results
    """
    logger.debug("[validate_python_code] Validating Python code syntax")
    result = {
        "valid": False,
        "error": None,
        "line": None
    }
    
    try:
        # Parse the code to check syntax
        ast.parse(code)
        result["valid"] = True
        logger.debug("[validate_python_code] Syntax validation passed")
    except SyntaxError as e:
        result["valid"] = False
        result["error"] = str(e.msg)
        result["line"] = e.lineno
        logger.debug(f"[validate_python_code] Syntax error on line {e.lineno}: {e.msg}")
    except Exception as e:
        result["valid"] = False
        result["error"] = f"Validation error: {str(e)}"
        logger.warning(f"[validate_python_code] Validation error: {str(e)}", exc_info=True)
    
    return result


def get_python_environment_info(config: Optional[dict] = None) -> Dict[str, Any]:
    """
    Get information about the Python environment.
    
    Args:
        config: Configuration dictionary with settings
    
    Returns:
        Dictionary with Python environment information
    """
    if config is None:
        config = {}
    info = {
        "version": sys.version,
        "version_info": {
            "major": sys.version_info.major,
            "minor": sys.version_info.minor,
            "micro": sys.version_info.micro
        },
        "platform": platform.platform(),
        "architecture": platform.architecture(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "python_path": sys.executable,
        "source": "bvbrc-python-execution"
    }
    
    # TODO: Optionally include installed packages
    # This can be done by running: pip list or checking sys.modules
    # info["installed_packages"] = get_installed_packages()
    
    return info


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
import base64
import mimetypes
from datetime import datetime
from .workspace_functions import (
    get_user_id_from_token,
    upload_files_to_workspace
)


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
        include_contents: Whether to include file contents (base64 for images, text for small files)
    
    Returns:
        Dictionary with file metadata
    """
    try:
        stat = os.stat(file_path)
        file_info = {
            "path": file_path,
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
        
        # For images, optionally include base64 (if size is reasonable)
        if file_info.get("type") == "image" and stat.st_size < 5 * 1024 * 1024:  # < 5MB
            try:
                with open(file_path, 'rb') as f:
                    file_info["base64"] = base64.b64encode(f.read()).decode('utf-8')
            except IOError:
                pass
        
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
    token: Optional[str] = None
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
    
    Returns:
        Dictionary with execution results including output_files and workspace_upload_results
    """
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
        "workspace_upload": None  # Workspace upload results
    }
    
    # Get configuration values
    temp_directory = config.get("temp_directory", "/tmp/copilot_python")
    singularity_container = config.get("singularity_container")
    effective_timeout = timeout or config.get("default_timeout", 30)
    include_file_contents = config.get("include_file_contents", True)  # Config option
    workspace_output = config.get("workspace_output", "CopilotCodeDev")
    workspace_url = config.get("workspace_url", "https://p3.theseed.org/services/Workspace")
    
    # Validate required configuration
    if not singularity_container:
        result["error"] = "singularity_container not specified in config"
        result["execution_time"] = time.time() - start_time
        return result
    
    if not os.path.exists(singularity_container):
        result["error"] = f"Singularity container not found: {singularity_container}"
        result["execution_time"] = time.time() - start_time
        return result
    
    # Verify temp directory exists and is accessible
    if not os.path.exists(temp_directory):
        result["error"] = f"Temp directory does not exist: {temp_directory}"
        result["execution_time"] = time.time() - start_time
        return result
    
    if not os.path.isdir(temp_directory):
        result["error"] = f"Temp directory path exists but is not a directory: {temp_directory}"
        result["execution_time"] = time.time() - start_time
        return result
    
    if not os.access(temp_directory, os.W_OK):
        result["error"] = f"Temp directory is not writable: {temp_directory}"
        result["execution_time"] = time.time() - start_time
        return result
    
    # Create unique folder for this code execution
    # Format: python_run_YYYYMMDD_HHMMSS_UUID
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id = uuid.uuid4().hex[:8]
    run_folder_name = f"python_run_{timestamp}_{run_id}"
    run_folder_path = os.path.join(temp_directory, run_folder_name)
    
    # Create the run folder
    try:
        os.makedirs(run_folder_path, exist_ok=True)
    except Exception as e:
        result["error"] = f"Failed to create run folder: {str(e)}"
        result["execution_time"] = time.time() - start_time
        return result
    
    # Create script file in the run folder
    script_filename = "script.py"
    script_path = os.path.join(run_folder_path, script_filename)
    
    try:
        # Record files before execution to detect new output files
        files_before = _get_files_in_directory(run_folder_path)
        
        # Write code to script file
        with open(script_path, 'w') as f:
            f.write(code)
        
        # Prepare Singularity command
        # Mount both temp_directory (parent) and run_folder so the container can access everything
        singularity_cmd = [
            "singularity",
            "exec",
            "--bind", f"{temp_directory}:{temp_directory}",
            singularity_container,
            "python",
            script_path
        ]
        
        # Execute the script through Singularity
        try:
            process = subprocess.run(
                singularity_cmd,
                capture_output=capture_output,
                text=True,
                timeout=effective_timeout,
                cwd=run_folder_path  # Set working directory to run folder
            )
            
            result["success"] = (process.returncode == 0)
            if capture_output:
                result["output"] = process.stdout
                result["error"] = process.stderr
            else:
                result["output"] = ""
                result["error"] = ""
            
            if process.returncode != 0:
                result["error"] = result["error"] or f"Process exited with code {process.returncode}"
                
        except subprocess.TimeoutExpired:
            result["error"] = f"Execution timed out after {effective_timeout} seconds"
            result["success"] = False
        except FileNotFoundError:
            result["error"] = "Singularity command not found. Is Singularity installed?"
            result["success"] = False
        except Exception as e:
            result["error"] = f"Error executing Singularity command: {str(e)}"
            result["success"] = False
        finally:
            # Always try to detect output files, even if execution failed
            # (files might have been created before the error/timeout)
            try:
                files_after = _get_files_in_directory(run_folder_path)
                new_files = files_after - files_before
                
                # Filter out the script file itself (it was created before execution)
                script_path_normalized = os.path.normpath(script_path)
                output_files = [f for f in new_files if os.path.normpath(f) != script_path_normalized]
                
                # Get file information for each output file
                result["output_files"] = []
                for file_path in sorted(output_files):
                    file_info = _get_file_info(file_path, include_contents=include_file_contents)
                    result["output_files"].append(file_info)
            except Exception as e:
                # If file detection fails, log but don't fail the whole execution
                result["output_files"] = []
                if not result.get("error"):
                    result["error"] = f"Error detecting output files: {str(e)}"
            
            # Upload files to workspace if token is provided
            if token:
                try:
                    # Extract user ID from token
                    user_id = get_user_id_from_token(token)
                    if not user_id:
                        result["workspace_upload"] = {
                            "success": False,
                            "error": "Could not extract user ID from token"
                        }
                    else:
                        # Build workspace path: /<user_id>/home/<workspace_output>/<run_folder_name>
                        workspace_dir = f"/{user_id}/home/{workspace_output}/{run_folder_name}"
                        
                        # Collect all files to upload (script + output files)
                        files_to_upload = [script_path]
                        for file_info in result["output_files"]:
                            if "path" in file_info:
                                files_to_upload.append(file_info["path"])
                        
                        # Upload files to workspace
                        print(f"Uploading {len(files_to_upload)} files to workspace: {workspace_dir}", file=sys.stderr)
                        upload_result = upload_files_to_workspace(
                            files_to_upload,
                            workspace_dir,
                            token,
                            workspace_url
                        )
                        
                        result["workspace_upload"] = {
                            "success": upload_result.get("success", False),
                            "workspace_path": workspace_dir,
                            "total_files": upload_result.get("total_files", 0),
                            "successful": upload_result.get("successful", 0),
                            "failed": upload_result.get("failed", 0),
                            "files": upload_result.get("files", [])
                        }
                        
                        print(f"Workspace upload complete: {upload_result.get('successful', 0)}/{upload_result.get('total_files', 0)} files uploaded", file=sys.stderr)
                        
                except Exception as e:
                    result["workspace_upload"] = {
                        "success": False,
                        "error": f"Error uploading to workspace: {str(e)}"
                    }
                    print(f"Error uploading to workspace: {str(e)}", file=sys.stderr)
            else:
                result["workspace_upload"] = {
                    "success": False,
                    "message": "No token provided, skipping workspace upload"
                }
        
    except IOError as e:
        result["error"] = f"Failed to write script file: {str(e)}"
        result["success"] = False
    except Exception as e:
        result["error"] = f"Unexpected error: {str(e)}"
        result["success"] = False
    finally:
        result["execution_time"] = time.time() - start_time
    
    return result


def validate_python_code(code: str) -> Dict[str, Any]:
    """
    Validate Python code syntax without executing it.
    
    Args:
        code: The Python code to validate
    
    Returns:
        Dictionary with validation results
    """
    result = {
        "valid": False,
        "error": None,
        "line": None
    }
    
    try:
        # Parse the code to check syntax
        ast.parse(code)
        result["valid"] = True
    except SyntaxError as e:
        result["valid"] = False
        result["error"] = str(e.msg)
        result["line"] = e.lineno
    except Exception as e:
        result["valid"] = False
        result["error"] = f"Validation error: {str(e)}"
    
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
        "python_path": sys.executable
    }
    
    # TODO: Optionally include installed packages
    # This can be done by running: pip list or checking sys.modules
    # info["installed_packages"] = get_installed_packages()
    
    return info


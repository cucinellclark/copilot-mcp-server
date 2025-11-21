"""
Workspace Functions for CopilotMCP

This module provides functions for uploading generated code and output files to the BV-BRC workspace.
Adapted from bvbrc-mcp-server/functions/workspace_functions.py
"""

import os
import sys
import requests
import json
from typing import Optional, Dict, Any, List


class JsonRpcCaller:
    """A minimal, generic JSON-RPC caller class for workspace operations."""
    
    def __init__(self, service_url: str):
        """
        Initialize the JSON-RPC caller with service URL.
        
        Args:
            service_url: The base URL for the workspace service API
        """
        self.service_url = service_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/jsonrpc+json'
        })
    
    def call(self, method: str, params: Optional[Any] = None, request_id: int = 1, token: str = None) -> Any:
        """
        Make a JSON-RPC call to the workspace API.
        
        Args:
            method: The RPC method name to call
            params: Optional parameters for the method
            request_id: Request ID for the JSON-RPC call
            token: Authentication token for API calls
        Returns:
            The response from the API call
        """
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "id": request_id,
            "params": params or {},
        }

        if token:
            self.session.headers.update({
                'Authorization': f'{token}'
            })

        try:
            response = self.session.post(
                self.service_url,
                data=json.dumps(payload),
                timeout=30
            )
            response.raise_for_status()
            
            result = response.json()
            
            # Check for JSON-RPC errors
            if isinstance(result, dict) and "error" in result:
                raise ValueError(f"JSON-RPC error: {result['error']}")
            
            # Return the result field
            if isinstance(result, dict):
                return result.get("result", {})
            return result
        
        except Exception as e:
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                print(f"Workspace API error: {e.response.text}", file=sys.stderr)
            else:
                print(f"Workspace API error: {str(e)}", file=sys.stderr)
            raise
    
    def close(self):
        """Close the HTTP session."""
        self.session.close()


def get_user_id_from_token(token: str) -> Optional[str]:
    """
    Extract user ID from a BV-BRC/KBase style auth token.
    
    Token format example: "un=username|tokenid=...|expiry=..."
    
    Args:
        token: Authentication token
    
    Returns:
        User ID extracted from token, or None if invalid
    """
    if not token:
        return None
    try:
        # Token format: "un=username|..."; take first segment and strip prefix
        return token.split('|')[0].replace('un=', '')
    except Exception as e:
        print(f"Error extracting user ID from token: {e}", file=sys.stderr)
        return None


def workspace_create_upload_node(api: JsonRpcCaller, workspace_path: str, token: str) -> Dict[str, Any]:
    """
    Create an upload node in the workspace and get the upload URL.
    
    Args:
        api: JsonRpcCaller instance configured with workspace URL
        workspace_path: Full path in workspace where file should be created
        token: Authentication token for API calls
    
    Returns:
        Dictionary with upload URL and metadata
    """
    try:
        # Call Workspace.create to create upload node
        # Format: [[path, type, metadata, content]]
        result = api.call(
            "Workspace.create",
            {
                "objects": [[workspace_path, 'unspecified', {}, '']],
                "createUploadNodes": True,
                "overwrite": None
            },
            1,
            token
        )
        
        # Parse the result
        if result and len(result) > 0 and len(result[0]) > 0:
            # Extract the metadata array from result[0][0]
            meta_list = result[0][0]
            
            # Convert the array to a structured object
            meta_obj = {
                "id": meta_list[4],
                "path": meta_list[2] + meta_list[0],
                "name": meta_list[0],
                "type": meta_list[1],
                "creation_time": meta_list[3],
                "link_reference": meta_list[11],  # This is the upload URL
                "owner_id": meta_list[5],
                "size": meta_list[6],
                "user_meta": meta_list[7],
                "auto_meta": meta_list[8],
                "user_permission": meta_list[9],
                "global_permission": meta_list[10],
            }
            
            return {
                "success": True,
                "upload_url": meta_obj["link_reference"],
                "metadata": meta_obj
            }
        else:
            return {
                "success": False,
                "error": "No valid result returned from workspace API"
            }
            
    except Exception as e:
        return {
            "success": False,
            "error": f"Error creating upload node: {str(e)}"
        }


def upload_file_to_workspace_url(file_path: str, upload_url: str, token: str) -> Dict[str, Any]:
    """
    Upload a file to the specified workspace upload URL (Shock API).
    
    Args:
        file_path: Path to the local file to upload
        upload_url: The upload URL from workspace_create_upload_node
        token: Authentication token for API calls
    
    Returns:
        Dictionary with upload result status and message
    """
    try:
        # Check if file exists
        if not os.path.exists(file_path):
            return {"success": False, "error": f"File {file_path} does not exist"}
        
        # Set up headers for the Shock API request
        headers = {
            'Authorization': 'OAuth ' + token
        }
        
        # Prepare the file for multipart form data upload
        with open(file_path, 'rb') as file:
            files = {
                'upload': (os.path.basename(file_path), file, 'application/octet-stream')
            }
            
            # Make the PUT request with multipart form data
            response = requests.put(upload_url, files=files, headers=headers, timeout=30)
        
        if response.status_code == 200:
            return {
                "success": True, 
                "message": f"File {os.path.basename(file_path)} uploaded successfully",
                "status_code": response.status_code
            }
        else:
            return {
                "success": False, 
                "error": f"Upload failed with status code {response.status_code}: {response.text}",
                "status_code": response.status_code
            }
            
    except Exception as e:
        return {"success": False, "error": f"Upload failed: {str(e)}"}


def upload_file_to_workspace(
    file_path: str,
    workspace_dir: str,
    token: str,
    workspace_url: str = "https://p3.theseed.org/services/Workspace"
) -> Dict[str, Any]:
    """
    Upload a file to the BV-BRC workspace.
    
    This is a high-level function that handles the complete upload process:
    1. Creates an upload node in the workspace
    2. Uploads the file to the upload URL
    
    Args:
        file_path: Path to the local file to upload
        workspace_dir: Directory in workspace where file should be uploaded
        token: Authentication token
        workspace_url: URL of the workspace service
    
    Returns:
        Dictionary with upload status and information
    """
    try:
        # Create API client
        api = JsonRpcCaller(workspace_url)
        
        # Get the filename
        filename = os.path.basename(file_path)
        
        # Build full workspace path
        workspace_path = os.path.join(workspace_dir, filename)
        
        # Create upload node
        print(f"Creating upload node for {workspace_path}", file=sys.stderr)
        create_result = workspace_create_upload_node(api, workspace_path, token)
        
        if not create_result.get("success"):
            return {
                "success": False,
                "file": filename,
                "error": create_result.get("error", "Failed to create upload node")
            }
        
        upload_url = create_result["upload_url"]
        
        # Upload the file
        print(f"Uploading file to {upload_url}", file=sys.stderr)
        upload_result = upload_file_to_workspace_url(file_path, upload_url, token)
        
        if upload_result.get("success"):
            return {
                "success": True,
                "file": filename,
                "workspace_path": workspace_path,
                "message": upload_result.get("message", "File uploaded successfully")
            }
        else:
            return {
                "success": False,
                "file": filename,
                "error": upload_result.get("error", "Upload failed")
            }
            
    except Exception as e:
        return {
            "success": False,
            "file": os.path.basename(file_path),
            "error": f"Error uploading file: {str(e)}"
        }
    finally:
        try:
            api.close()
        except:
            pass


def upload_files_to_workspace(
    file_paths: List[str],
    workspace_dir: str,
    token: str,
    workspace_url: str = "https://p3.theseed.org/services/Workspace"
) -> Dict[str, Any]:
    """
    Upload multiple files to the BV-BRC workspace.
    
    Args:
        file_paths: List of local file paths to upload
        workspace_dir: Directory in workspace where files should be uploaded
        token: Authentication token
        workspace_url: URL of the workspace service
    
    Returns:
        Dictionary with upload results for all files
    """
    results = {
        "total_files": len(file_paths),
        "successful": 0,
        "failed": 0,
        "files": []
    }
    
    for file_path in file_paths:
        result = upload_file_to_workspace(file_path, workspace_dir, token, workspace_url)
        results["files"].append(result)
        
        if result.get("success"):
            results["successful"] += 1
        else:
            results["failed"] += 1
    
    results["success"] = results["failed"] == 0
    
    return results


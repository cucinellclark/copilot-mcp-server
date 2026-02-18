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
from urllib.parse import urljoin
from fastmcp.utilities.logging import get_logger

logger = get_logger(__name__)


def _extract_create_metadata(result: Any) -> Optional[Any]:
    """
    Extract the workspace metadata array/object from Workspace.create response.
    Handles minor shape variations in JSON-RPC result payloads.
    """
    if result is None:
        return None

    # Common shape: [[meta_array]]
    if isinstance(result, list) and result:
        first = result[0]
        if isinstance(first, list) and first:
            second = first[0]
            # Sometimes wrapped as [[[meta_array], ...]]
            if isinstance(second, list) and second and isinstance(second[0], list):
                return second[0]
            return second

    # Fallback if service returns object-like metadata directly
    if isinstance(result, dict):
        return result

    return None


def _extract_upload_url(meta: Any) -> str:
    """
    Extract upload URL from metadata array/object.
    Returns empty string if no usable URL is present.
    """
    if isinstance(meta, dict):
        # Some responses nest URL fields under metadata-like containers.
        for nested_key in ("metadata", "meta", "data"):
            nested = meta.get(nested_key)
            if isinstance(nested, (dict, list)):
                nested_url = _extract_upload_url(nested)
                if nested_url:
                    return nested_url

        for key in ("link_reference", "linkReference", "upload_url", "uploadUrl", "url"):
            value = meta.get(key)
            if isinstance(value, str) and value.strip():
                return value
        return ""

    if isinstance(meta, list):
        # Historical location for Workspace metadata array link reference.
        if len(meta) > 11 and isinstance(meta[11], str) and meta[11].startswith(("http://", "https://")):
            return meta[11]

        # Fallback: locate first URL-looking string.
        for value in meta:
            if isinstance(value, str) and value.startswith(("http://", "https://")):
                return value

    return ""


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
    logger.debug(f"[workspace_create_upload_node] Creating upload node for: {workspace_path}")
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
        
        logger.debug(f"[workspace_create_upload_node] API call result type: {type(result)}")

        meta = _extract_create_metadata(result)
        if meta is None:
            error_msg = "No valid metadata returned from Workspace.create"
            logger.error(f"[workspace_create_upload_node] {error_msg}, result={result}")
            return {
                "success": False,
                "error": error_msg
            }

        if isinstance(meta, dict):
            meta_obj = {
                "id": meta.get("id", ""),
                "path": meta.get("path", workspace_path),
                "name": meta.get("name", os.path.basename(workspace_path)),
                "type": meta.get("type", "unspecified"),
                "creation_time": meta.get("creation_time", ""),
                "link_reference": meta.get("link_reference", ""),
                "owner_id": meta.get("owner_id", ""),
                "size": meta.get("size", 0),
                "user_meta": meta.get("user_meta", {}),
                "auto_meta": meta.get("auto_meta", {}),
                "user_permission": meta.get("user_permission", ""),
                "global_permission": meta.get("global_permission", ""),
            }
        else:
            # Metadata array shape from Workspace service
            meta_obj = {
                "id": meta[4] if len(meta) > 4 else "",
                "path": (meta[2] + meta[0]) if len(meta) > 2 else workspace_path,
                "name": meta[0] if len(meta) > 0 else os.path.basename(workspace_path),
                "type": meta[1] if len(meta) > 1 else "unspecified",
                "creation_time": meta[3] if len(meta) > 3 else "",
                "link_reference": meta[11] if len(meta) > 11 else "",
                "owner_id": meta[5] if len(meta) > 5 else "",
                "size": meta[6] if len(meta) > 6 else 0,
                "user_meta": meta[7] if len(meta) > 7 else {},
                "auto_meta": meta[8] if len(meta) > 8 else {},
                "user_permission": meta[9] if len(meta) > 9 else "",
                "global_permission": meta[10] if len(meta) > 10 else "",
            }

        upload_url = _extract_upload_url(meta)
        if isinstance(upload_url, str):
            upload_url = upload_url.strip()

        # Some Workspace deployments return relative upload links.
        if upload_url and upload_url.startswith("/"):
            upload_url = urljoin(f"{api.service_url}/", upload_url.lstrip("/"))

        if not upload_url:
            logger.error(
                "[workspace_create_upload_node] Workspace.create returned metadata without upload URL",
                extra={
                    "workspace_path": workspace_path,
                    "meta_preview": str(meta)[:1000],
                    "result_preview": str(result)[:1000],
                },
            )
            return {
                "success": False,
                "error": "Workspace.create did not return a valid upload URL",
                "metadata": meta_obj,
            }

        meta_obj["link_reference"] = upload_url
        logger.info(
            f"[workspace_create_upload_node] Upload node created successfully, upload_url: {upload_url[:80]}..."
        )
        return {
            "success": True,
            "upload_url": upload_url,
            "metadata": meta_obj
        }
            
    except Exception as e:
        error_msg = f"Error creating upload node: {str(e)}"
        logger.error(f"[workspace_create_upload_node] {error_msg}", exc_info=True)
        return {
            "success": False,
            "error": error_msg
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
    logger.debug(f"[upload_file_to_workspace_url] Uploading {os.path.basename(file_path)} to Shock API")
    try:
        # Check if file exists
        if not os.path.exists(file_path):
            error_msg = f"File {file_path} does not exist"
            logger.error(f"[upload_file_to_workspace_url] {error_msg}")
            return {"success": False, "error": error_msg}
        
        file_size = os.path.getsize(file_path)
        logger.debug(f"[upload_file_to_workspace_url] File size: {file_size} bytes")
        
        if not upload_url or not isinstance(upload_url, str):
            error_msg = "Upload URL is missing or not a string"
            logger.error(f"[upload_file_to_workspace_url] {error_msg}")
            return {"success": False, "error": error_msg}

        if not upload_url.startswith(("http://", "https://")):
            error_msg = f"Upload URL is invalid (must start with http/https): {upload_url}"
            logger.error(f"[upload_file_to_workspace_url] {error_msg}")
            return {"success": False, "error": error_msg}

        # Set up headers for the Shock API request
        headers = {
            'Authorization': 'OAuth ' + token
        }
        
        # Prepare the file for multipart form data upload
        logger.debug(f"[upload_file_to_workspace_url] Making PUT request to: {upload_url[:80]}...")
        with open(file_path, 'rb') as file:
            files = {
                'upload': (os.path.basename(file_path), file, 'application/octet-stream')
            }
            
            # Make the PUT request with multipart form data
            response = requests.put(upload_url, files=files, headers=headers, timeout=30)
        
        logger.debug(f"[upload_file_to_workspace_url] Response status code: {response.status_code}")
        
        if response.status_code == 200:
            logger.info(f"[upload_file_to_workspace_url] File {os.path.basename(file_path)} uploaded successfully")
            return {
                "success": True, 
                "message": f"File {os.path.basename(file_path)} uploaded successfully",
                "status_code": response.status_code
            }
        else:
            error_msg = f"Upload failed with status code {response.status_code}: {response.text[:200]}"
            logger.error(f"[upload_file_to_workspace_url] {error_msg}")
            return {
                "success": False, 
                "error": error_msg,
                "status_code": response.status_code
            }
            
    except Exception as e:
        error_msg = f"Upload failed: {str(e)}"
        logger.error(f"[upload_file_to_workspace_url] {error_msg}", exc_info=True)
        return {"success": False, "error": error_msg}


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
    filename = os.path.basename(file_path)
    logger.info(f"[upload_file_to_workspace] Uploading file: {filename}")
    logger.debug(f"[upload_file_to_workspace] Local path: {file_path}, workspace_dir: {workspace_dir}")
    
    try:
        # Create API client
        api = JsonRpcCaller(workspace_url)

        # Ensure destination directory exists before creating upload nodes.
        ensure_result = ensure_workspace_directory_exists(api, workspace_dir, token)
        if not ensure_result.get("success"):
            error_msg = ensure_result.get("error", "Failed to ensure workspace directory exists")
            logger.error(f"[upload_file_to_workspace] {error_msg}")
            return {
                "success": False,
                "file": filename,
                "error": error_msg
            }
        
        # Build full workspace path
        workspace_path = os.path.join(workspace_dir, filename)
        
        # Create upload node
        logger.debug(f"[upload_file_to_workspace] Creating upload node for: {workspace_path}")
        create_result = workspace_create_upload_node(api, workspace_path, token)
        
        if not create_result.get("success"):
            error_msg = create_result.get("error", "Failed to create upload node")
            logger.error(f"[upload_file_to_workspace] Failed to create upload node: {error_msg}")
            return {
                "success": False,
                "file": filename,
                "error": error_msg
            }
        
        upload_url = create_result["upload_url"]
        logger.debug(f"[upload_file_to_workspace] Got upload URL, proceeding with file upload")
        
        # Upload the file
        upload_result = upload_file_to_workspace_url(file_path, upload_url, token)
        
        if upload_result.get("success"):
            logger.info(f"[upload_file_to_workspace] File {filename} uploaded successfully to {workspace_path}")
            return {
                "success": True,
                "file": filename,
                "workspace_path": workspace_path,
                "message": upload_result.get("message", "File uploaded successfully")
            }
        else:
            error_msg = upload_result.get("error", "Upload failed")
            logger.error(f"[upload_file_to_workspace] File upload failed: {error_msg}")
            return {
                "success": False,
                "file": filename,
                "error": error_msg
            }
            
    except Exception as e:
        error_msg = f"Error uploading file: {str(e)}"
        logger.error(f"[upload_file_to_workspace] {error_msg}", exc_info=True)
        return {
            "success": False,
            "file": filename,
            "error": error_msg
        }
    finally:
        try:
            api.close()
        except:
            pass


def ensure_workspace_directory_exists(api: JsonRpcCaller, workspace_dir: str, token: str) -> Dict[str, Any]:
    """
    Ensure the destination workspace directory exists.
    """
    logger.debug(f"[ensure_workspace_directory_exists] Ensuring directory exists: {workspace_dir}")
    try:
        result = api.call(
            "Workspace.create",
            {
                "objects": [[workspace_dir, "folder", {}, ""]],
                "createUploadNodes": False,
                "overwrite": False
            },
            1,
            token
        )
        logger.debug(
            "[ensure_workspace_directory_exists] Directory ensure call completed",
            extra={"workspace_dir": workspace_dir, "result_preview": str(result)[:1000]}
        )
        return {
            "success": True,
            "workspace_dir": workspace_dir
        }
    except Exception as e:
        msg = str(e).lower()
        # Existing directories commonly surface as "exists/already exists".
        if "already exists" in msg or "exists" in msg:
            logger.debug(
                f"[ensure_workspace_directory_exists] Directory already exists: {workspace_dir}"
            )
            return {
                "success": True,
                "workspace_dir": workspace_dir
            }

        error_msg = f"Failed to ensure workspace directory {workspace_dir}: {str(e)}"
        logger.error(f"[ensure_workspace_directory_exists] {error_msg}", exc_info=True)
        return {
            "success": False,
            "error": error_msg
        }


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
    logger.info(f"[upload_files_to_workspace] Starting batch upload of {len(file_paths)} files")
    results = {
        "total_files": len(file_paths),
        "successful": 0,
        "failed": 0,
        "files": []
    }
    
    for i, file_path in enumerate(file_paths, 1):
        logger.debug(f"[upload_files_to_workspace] Uploading file {i}/{len(file_paths)}: {os.path.basename(file_path)}")
        result = upload_file_to_workspace(file_path, workspace_dir, token, workspace_url)
        results["files"].append(result)
        
        if result.get("success"):
            results["successful"] += 1
        else:
            results["failed"] += 1
            logger.warning(f"[upload_files_to_workspace] File {i} failed: {result.get('error', 'Unknown error')}")
    
    results["success"] = results["failed"] == 0
    logger.info(f"[upload_files_to_workspace] Batch upload complete: {results['successful']}/{results['total_files']} successful, {results['failed']} failed")
    
    return results


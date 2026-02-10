"""
File Info Functions

This module provides functions for getting file information from user sessions.
Files are indexed in MongoDB (session_files) and stored on disk under the
session downloads directory.
"""

import os
import json
from typing import Dict, Any

from common.config import get_config
from functions.file_registry import get_file_record


def get_file_path(session_id: str, file_id: str) -> str:
    """
    Get the absolute file path for a given session and file ID.
    
    Args:
        session_id: Session identifier
        file_id: File identifier
    
    Returns:
        Absolute path to the file
    """
    # Prefer DB-backed file registry (Option A)
    record = get_file_record(session_id, file_id)
    if record and record.get("filePath"):
        return record["filePath"]

    # Fallback to direct filesystem lookup
    config = get_config().file_utilities or {}
    base_path = config.get("session_base_path", "/tmp/copilot/sessions")
    downloads_path = os.path.join(base_path, session_id, "downloads")
    
    # Try different extensions
    extensions = ['', '.json', '.csv', '.tsv', '.txt']
    for ext in extensions:
        file_path = os.path.join(downloads_path, f"{file_id}{ext}")
        if os.path.exists(file_path):
            return file_path
    
    # Return base path without extension if not found
    return os.path.join(downloads_path, file_id)


def detect_file_type(file_path: str) -> str:
    """
    Detect the file type based on extension and content.
    
    Returns: 'json_array', 'json_object', 'csv', 'tsv', or 'text'
    """
    ext = os.path.splitext(file_path)[1].lower()
    
    if ext == '.json':
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    return 'json_array'
                elif isinstance(data, dict):
                    return 'json_object'
        except:
            pass
    
    if ext == '.csv':
        return 'csv'
    
    if ext == '.tsv':
        return 'tsv'
    
    return 'text'


def get_file_info_func(session_id: str, file_id: str) -> Dict[str, Any]:
    """
    Get detailed information about a saved file.
    
    Args:
        session_id: Session identifier
        file_id: File identifier
    
    Returns:
        Dictionary with file metadata including:
        - fileId: The file identifier
        - fileName: Name of the file
        - filePath: Absolute path to the file
        - dataType: Type of data (json_array, json_object, csv, tsv, text)
        - size: File size in bytes
        - availableActions: List of available tools for this file type
        - note: Usage instructions
    """
    # Validate parameters
    if not session_id or not file_id:
        return {
            "error": True,
            "errorType": "INVALID_PARAMETERS",
            "message": "Missing required parameters: session_id and file_id",
            "source": "bvbrc-file-utilities"
        }
    
    # Prefer DB-backed metadata when available
    record = get_file_record(session_id, file_id)
    if record and record.get("filePath") and os.path.exists(record["filePath"]):
        file_info = {
            "fileId": file_id,
            "fileName": record.get("fileName") or os.path.basename(record["filePath"]),
            "filePath": record.get("filePath"),
            "dataType": record.get("dataType") or detect_file_type(record["filePath"]),
            "size": record.get("size") or os.path.getsize(record["filePath"]),
            "modified": os.path.getmtime(record["filePath"]),
            "availableActions": _get_available_actions_for_data_type(record.get("dataType") or detect_file_type(record["filePath"])),
            "note": "Use internal_server file tools to query, search, or extract data from this file"
        }
        if record.get("recordCount") is not None:
            file_info["recordCount"] = record.get("recordCount")
        return file_info

    # Fallback to direct file lookup
    file_path = get_file_path(session_id, file_id)

    if not os.path.exists(file_path):
        return {
            "error": True,
            "errorType": "FILE_NOT_FOUND",
            "message": f"File {file_id} not found in session {session_id}",
            "details": {"fileId": file_id, "session_id": session_id},
            "source": "bvbrc-file-utilities"
        }
    
    try:
        # Get file metadata
        stat = os.stat(file_path)
        file_name = os.path.basename(file_path)
        data_type = detect_file_type(file_path)
        
        # Build file info response
        file_info = {
            "fileId": file_id,
            "fileName": file_name,
            "filePath": file_path,
            "dataType": data_type,
            "size": stat.st_size,
            "modified": stat.st_mtime,
            "availableActions": _get_available_actions_for_data_type(data_type),
            "note": "Use internal_server file tools to query, search, or extract data from this file"
        }
        
        return file_info
        
    except Exception as e:
        return {
            "error": True,
            "errorType": "INVALID_PARAMETERS",
            "message": f"Error getting file info: {str(e)}",
            "source": "bvbrc-file-utilities"
        }


def _get_available_actions_for_data_type(data_type: str) -> list:
    """
    Get available internal_server actions based on data type.
    
    Args:
        data_type: The data type from file metadata
    
    Returns:
        List of available action tools
    """
    base_actions = [
        'internal_server.read_file_lines',
        'internal_server.search_file'
    ]
    
    if data_type in ['json_array', 'json_object']:
        return [
            *base_actions,
            'internal_server.query_json',
            'internal_server.get_file_statistics'
        ]
    elif data_type in ['csv', 'tsv']:
        return [
            *base_actions,
            'internal_server.extract_csv_columns',
            'internal_server.get_file_statistics'
        ]
    elif data_type == 'text':
        return base_actions
    else:
        return base_actions


def _truncate_json_value(value: Any, max_str_len: int = 150, max_depth: int = 0) -> Any:
    """
    Truncate or summarize JSON values for preview purposes.
    
    Args:
        value: The value to truncate
        max_str_len: Maximum string length before truncation
        max_depth: Current nesting depth (0 = truncate nested structures)
    
    Returns:
        Truncated/summarized version of the value
    """
    # Strings: always truncate
    if isinstance(value, str) and len(value) > max_str_len:
        return value[:max_str_len] + "..."

    # If we've exhausted depth, summarize containers
    if max_depth <= 0:
        if isinstance(value, dict):
            return f"{{... {len(value)} keys}}"
        if isinstance(value, list):
            return f"[... {len(value)} items]"
        return value

    # Otherwise expand containers up to max_depth, but keep it bounded
    if isinstance(value, dict):
        if len(value) > 50:
            return f"{{... {len(value)} keys}}"
        return {k: _truncate_json_value(v, max_str_len=max_str_len, max_depth=max_depth - 1) for k, v in value.items()}

    if isinstance(value, list):
        items = value if len(value) <= 10 else value[:10]
        expanded = [_truncate_json_value(v, max_str_len=max_str_len, max_depth=max_depth - 1) for v in items]
        if len(value) > len(items):
            expanded.append(f"... {len(value) - len(items)} more items")
        return expanded

    return value


def head_file_func(session_id: str, file_id: str, max_lines: int = 20, 
                    max_chars: int = 1000) -> Dict[str, Any]:
    """
    Get a preview of the beginning of a file with smart formatting based on file type.
    
    Args:
        session_id: Session identifier
        file_id: File identifier
        max_lines: Maximum number of lines for text files (default: 20)
        max_chars: Maximum characters for text files (default: 1000)
    
    Returns:
        Dictionary with preview data appropriate for the file type:
        - For text files: first N lines based on heuristic
        - For CSV/TSV: header + first 3 data rows
        - For JSON arrays: first item with summarized nested values
        - For JSON objects: all keys with summarized nested values
    """
    # Validate parameters
    if not session_id or not file_id:
        return {
            "error": True,
            "errorType": "INVALID_PARAMETERS",
            "message": "Missing required parameters: session_id and file_id",
            "source": "bvbrc-file-utilities"
        }
    
    # Get file path
    file_path = get_file_path(session_id, file_id)
    
    if not os.path.exists(file_path):
        return {
            "error": True,
            "errorType": "FILE_NOT_FOUND",
            "message": f"File {file_id} not found in session {session_id}",
            "details": {"fileId": file_id, "session_id": session_id},
            "source": "bvbrc-file-utilities"
        }
    
    try:
        data_type = detect_file_type(file_path)
        
        if data_type == 'json_array':
            # Read JSON array and return first item with truncated values
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            if not isinstance(data, list) or len(data) == 0:
                return {
                    "fileId": file_id,
                    "dataType": data_type,
                    "preview": [],
                    "note": "Empty JSON array"
                }
            
            first_item = data[0]
            
            # If first item is a dict, show all keys with truncated values
            if isinstance(first_item, dict):
                preview = {
                    key: _truncate_json_value(val, max_depth=2)
                    for key, val in first_item.items()
                }
            else:
                # If it's not a dict, just show it (truncate if needed)
                preview = _truncate_json_value(first_item, max_depth=2)
            
            return {
                "fileId": file_id,
                "dataType": data_type,
                "preview": preview,
                "totalItems": len(data),
                "note": f"Showing first item of {len(data)} total items"
            }
        
        elif data_type == 'json_object':
            # Read JSON object and return all keys with truncated values
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not isinstance(data, dict):
                return {
                    "fileId": file_id,
                    "dataType": data_type,
                    "preview": data,
                    "note": "JSON value (not an object)"
                }
            
            preview = {key: _truncate_json_value(val, max_depth=2) for key, val in data.items()}
            
            return {
                "fileId": file_id,
                "dataType": data_type,
                "preview": preview,
                "totalKeys": len(data),
                "note": f"Showing all {len(data)} keys with expanded values (depth=2)"
            }
        
        elif data_type in ['csv', 'tsv']:
            # Read CSV/TSV and return header + first 3 rows
            delimiter = ',' if data_type == 'csv' else '\t'
            
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = []
                for i, line in enumerate(f):
                    if i < 4:  # Header + 3 data rows
                        lines.append(line.rstrip('\n\r'))
                    else:
                        break
            
            # Count total lines
            with open(file_path, 'r', encoding='utf-8') as f:
                total_lines = sum(1 for _ in f)
            
            return {
                "fileId": file_id,
                "dataType": data_type,
                "preview": {
                    "delimiter": delimiter,
                    "lines": lines
                },
                "totalLines": total_lines,
                "note": f"Showing header + first 3 data rows of {total_lines} total lines"
            }
        
        else:  # text file
            # Use heuristic: up to max_lines or max_chars, whichever comes first
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = []
                char_count = 0
                line_count = 0
                
                for line in f:
                    line_stripped = line.rstrip('\n\r')
                    line_len = len(line_stripped)
                    
                    # Check if adding this line would exceed limits
                    if line_count >= max_lines or (char_count + line_len > max_chars and line_count > 0):
                        break
                    
                    lines.append(line_stripped)
                    char_count += line_len
                    line_count += 1
            
            # Count total lines
            with open(file_path, 'r', encoding='utf-8') as f:
                total_lines = sum(1 for _ in f)
            
            return {
                "fileId": file_id,
                "dataType": data_type,
                "preview": {
                    "lines": lines
                },
                "linesShown": len(lines),
                "totalLines": total_lines,
                "note": f"Showing first {len(lines)} lines of {total_lines} total lines"
            }
    
    except Exception as e:
        return {
            "error": True,
            "errorType": "PROCESSING_ERROR",
            "message": f"Error reading file preview: {str(e)}",
            "source": "bvbrc-file-utilities"
        }


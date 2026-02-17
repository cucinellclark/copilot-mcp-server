#!/usr/bin/env python3
"""
File Tools (minimal)

Single consolidated module for working with files saved under a Copilot session.

Tool surface area is intentionally small to support the core workflow:
  1) tool produces a file_reference (saved on disk)
  2) agent pulls bounded slices of that file back into context to summarize/answer questions
"""

import sys
from typing import Optional, List, Dict, Any

from fastmcp import FastMCP

from functions.file_utilities_functions import (
    search_file_func,
    query_json_func,
)

# Kept here intentionally so file_utilities config remains documented in one place
# after preview_file/read_file_lines moved to the consolidated workspace server.
FILE_UTILITIES_CONFIG_KEYS = {
    "session_base_path": "/tmp/copilot/sessions",
    "max_file_size_mb": 100,
    "default_limit": 1000,
    "max_limit": 10000,
    "mongo_url": "mongodb://127.0.0.1:27017/copilot",
    "mongo_database": "copilot",
    "mongo_collection": "session_files",
}


def register_file_tools(mcp: FastMCP, config: dict = None):
    """
    Register minimal file tools with the FastMCP server.

    Args:
        mcp: FastMCP server instance
        config: file_utilities settings. Supported keys are documented in
            FILE_UTILITIES_CONFIG_KEYS for compatibility with shared config.
    """
    if config is None:
        config = {}

    @mcp.tool()
    def search_file(
        session_id: str,
        file_id: str,
        pattern: str,
        is_regex: Optional[bool] = False,
        case_sensitive: Optional[bool] = False,
        fields: Optional[List[str]] = None,
        limit: Optional[int] = 100
    ) -> Dict[str, Any]:
        """
        Search for a pattern in a saved file (grep-like).
        """
        print(f"Searching file {file_id} for pattern '{pattern}'...", file=sys.stderr)
        try:
            return search_file_func(
                session_id=session_id,
                file_id=file_id,
                pattern=pattern,
                is_regex=is_regex or False,
                case_sensitive=case_sensitive or False,
                fields=fields,
                limit=limit or 100
            )
        except Exception as e:
            return {
                "error": True,
                "errorType": "INVALID_PARAMETERS",
                "message": f"Error searching file: {str(e)}",
                "source": "bvbrc-file-utilities"
            }

    @mcp.tool()
    def query_json(
        session_id: str,
        file_id: str,
        filters: Optional[List[Dict[str, Any]]] = None,
        fields: Optional[List[str]] = None,
        sort: Optional[Dict[str, str]] = None,
        limit: Optional[int] = 1000,
        offset: Optional[int] = 0
    ) -> Dict[str, Any]:
        """
        Query a JSON array file with simple filters/projection/sort/pagination.
        """
        print(f"Querying JSON file {file_id}...", file=sys.stderr)
        try:
            return query_json_func(
                session_id=session_id,
                file_id=file_id,
                filters=filters,
                fields=fields,
                sort=sort,
                limit=limit or 1000,
                offset=offset or 0
            )
        except Exception as e:
            return {
                "error": True,
                "errorType": "INVALID_PARAMETERS",
                "message": f"Error querying JSON: {str(e)}",
                "source": "bvbrc-file-utilities"
            }



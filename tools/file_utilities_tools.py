#!/usr/bin/env python3
"""
File Utilities Tools

This module contains MCP tools for reading and analyzing files from user sessions.
"""

import json
import sys
from typing import Optional, List, Dict, Any
from fastmcp import FastMCP

from functions.file_utilities_functions import (
    read_file_lines_func,
    search_file_func,
    query_json_func,
    extract_csv_columns_func,
    get_file_statistics_func
)


def register_file_utilities_tools(mcp: FastMCP, config: dict = None):
    """
    Register all file utilities-related MCP tools with the FastMCP server.
    
    Args:
        mcp: FastMCP server instance
        config: Configuration dictionary for file utilities settings
    """
    if config is None:
        config = {}
    
    @mcp.tool()
    def read_file_lines(
        session_id: str,
        file_id: str,
        start: Optional[int] = 1,
        end: Optional[int] = None,
        limit: Optional[int] = 1000
    ) -> str:
        """
        Read specific line ranges from a file. Supports JSON arrays, CSV/TSV, and text files.
        For JSON arrays, returns records as objects. For CSV/TSV, parses rows as objects with 
        headers as keys. For text files, returns array of line strings.
        
        Args:
            session_id: Session identifier (required)
            file_id: File identifier (required)
            start: Line number to start reading from (1-indexed, default: 1)
            end: Line number to end at (inclusive, optional)
            limit: Maximum number of lines to return (default: 1000, max: 10000)
        
        Returns:
            JSON string with:
            - lines: Array of line contents or parsed records
            - startLine: Actual start line returned
            - endLine: Actual end line returned
            - totalLines: Total lines/records in file
            - hasMore: Whether there are more lines after this range
        """
        print(f"Reading lines from file {file_id} in session {session_id}...", file=sys.stderr)
        try:
            result = read_file_lines_func(
                session_id=session_id,
                file_id=file_id,
                start=start or 1,
                end=end,
                limit=limit or 1000
            )
            return json.dumps(result, indent=2)
        except Exception as e:
            return json.dumps({
                "error": True,
                "errorType": "INVALID_PARAMETERS",
                "message": f"Error reading file lines: {str(e)}"
            }, indent=2)
    
    @mcp.tool()
    def search_file(
        session_id: str,
        file_id: str,
        pattern: str,
        is_regex: Optional[bool] = False,
        case_sensitive: Optional[bool] = False,
        fields: Optional[List[str]] = None,
        limit: Optional[int] = 100
    ) -> str:
        """
        Search for a pattern in a file (grep-like functionality). Supports literal string 
        matching and regex patterns. For JSON/CSV files, can search specific fields only.
        
        Args:
            session_id: Session identifier (required)
            file_id: File identifier (required)
            pattern: Search pattern - literal string or regex (required)
            is_regex: Whether pattern is a regex (default: False - literal search)
            case_sensitive: Whether search is case-sensitive (default: False)
            fields: For JSON/CSV files, search only these fields (optional)
            limit: Maximum number of matches to return (default: 100, max: 1000)
        
        Returns:
            JSON string with:
            - matches: Array of match objects with lineNumber, record, and matchedField
            - totalMatches: Total number of matches found
            - truncated: Whether results were limited by the limit parameter
        """
        print(f"Searching file {file_id} for pattern '{pattern}'...", file=sys.stderr)
        try:
            result = search_file_func(
                session_id=session_id,
                file_id=file_id,
                pattern=pattern,
                is_regex=is_regex or False,
                case_sensitive=case_sensitive or False,
                fields=fields,
                limit=limit or 100
            )
            return json.dumps(result, indent=2)
        except Exception as e:
            return json.dumps({
                "error": True,
                "errorType": "INVALID_PARAMETERS",
                "message": f"Error searching file: {str(e)}"
            }, indent=2)
    
    @mcp.tool()
    def query_json(
        session_id: str,
        file_id: str,
        filters: Optional[List[Dict[str, Any]]] = None,
        fields: Optional[List[str]] = None,
        sort: Optional[Dict[str, str]] = None,
        limit: Optional[int] = 1000,
        offset: Optional[int] = 0
    ) -> str:
        """
        Query JSON array files with SQL-like filtering, projection, and sorting.
        Apply multiple filters (AND logic), select specific fields, sort results, and paginate.
        Only works on JSON array files.
        
        Args:
            session_id: Session identifier (required)
            file_id: File identifier (required)
            filters: Array of filter conditions with AND logic. Each filter has:
                     - field: Field path (supports nested with dot notation, e.g., "metadata.length")
                     - operator: One of "==", "!=", ">", "<", ">=", "<=", "contains", "startsWith", "endsWith"
                     - value: Value to compare against
            fields: Which fields to return (projection), e.g., ["genome_id", "name", "length"]
            sort: Sort configuration with:
                  - field: Field to sort by
                  - order: "asc" or "desc"
            limit: Maximum records to return (default: 1000, max: 10000)
            offset: Number of records to skip for pagination (default: 0)
        
        Returns:
            JSON string with:
            - records: Matching records (projected if fields specified)
            - totalMatching: Total records matching filters (before limit/offset)
            - returned: Number of records in this response
        """
        print(f"Querying JSON file {file_id}...", file=sys.stderr)
        try:
            result = query_json_func(
                session_id=session_id,
                file_id=file_id,
                filters=filters,
                fields=fields,
                sort=sort,
                limit=limit or 1000,
                offset=offset or 0
            )
            return json.dumps(result, indent=2)
        except Exception as e:
            return json.dumps({
                "error": True,
                "errorType": "INVALID_PARAMETERS",
                "message": f"Error querying JSON: {str(e)}"
            }, indent=2)
    
    @mcp.tool()
    def extract_csv_columns(
        session_id: str,
        file_id: str,
        columns: List[str],
        limit: Optional[int] = 1000,
        skip_rows: Optional[int] = 0
    ) -> str:
        """
        Extract specific columns from CSV/TSV files. Returns only the requested columns
        as objects, handling missing columns gracefully. Only works on CSV/TSV files.
        
        Args:
            session_id: Session identifier (required)
            file_id: File identifier (required)
            columns: List of column names to extract (required)
            limit: Maximum number of rows to return (default: 1000, max: 10000)
            skip_rows: Number of rows to skip after header (default: 0)
        
        Returns:
            JSON string with:
            - data: Array of objects with only requested columns
            - columns: List of columns that were extracted
            - rowCount: Number of rows returned
            - totalRows: Total rows in file (excluding header)
        """
        print(f"Extracting columns {columns} from CSV file {file_id}...", file=sys.stderr)
        try:
            result = extract_csv_columns_func(
                session_id=session_id,
                file_id=file_id,
                columns=columns,
                limit=limit or 1000,
                skip_rows=skip_rows or 0
            )
            return json.dumps(result, indent=2)
        except Exception as e:
            return json.dumps({
                "error": True,
                "errorType": "INVALID_PARAMETERS",
                "message": f"Error extracting CSV columns: {str(e)}"
            }, indent=2)
    
    @mcp.tool()
    def get_file_statistics(
        session_id: str,
        file_id: str,
        fields: Optional[List[str]] = None,
        sample_size: Optional[int] = 10
    ) -> str:
        """
        Get statistical summaries for file fields. Automatically detects field types 
        (numeric, string, boolean) and calculates appropriate statistics. For numeric fields,
        calculates min, max, mean, median, and standard deviation. For string fields, provides
        unique count and top values. For boolean fields, shows distribution.
        
        Args:
            session_id: Session identifier (required)
            file_id: File identifier (required)
            fields: Fields to analyze (if not provided, analyzes all fields)
            sample_size: Number of sample values to return per field (default: 10)
        
        Returns:
            JSON string with:
            - statistics: Object with per-field statistics including:
                         * type: "numeric", "string", or "boolean"
                         * count: Non-null values
                         * nullCount: Null values
                         * For numeric: min, max, mean, median, stdDev, uniqueCount, topValues, sample
                         * For string: uniqueCount, topValues, sample
                         * For boolean: distribution (true/false counts)
            - totalRecords: Total number of records analyzed
        """
        print(f"Calculating statistics for file {file_id}...", file=sys.stderr)
        try:
            result = get_file_statistics_func(
                session_id=session_id,
                file_id=file_id,
                fields=fields,
                sample_size=sample_size or 10
            )
            return json.dumps(result, indent=2)
        except Exception as e:
            return json.dumps({
                "error": True,
                "errorType": "INVALID_PARAMETERS",
                "message": f"Error calculating statistics: {str(e)}"
            }, indent=2)

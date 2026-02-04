"""
File Utilities Functions

This module provides functions for reading and analyzing files from user sessions.
Files are stored in /tmp/copilot/sessions/{session_id}/downloads/ directory.
"""

import os
import json
import csv
import re
import statistics
from typing import Dict, Any, List, Optional, Union
import random


def get_file_path(session_id: str, file_id: str) -> str:
    """
    Get the absolute file path for a given session and file ID.
    
    Args:
        session_id: Session identifier
        file_id: File identifier
    
    Returns:
        Absolute path to the file
    """
    base_path = f"/tmp/copilot/sessions/{session_id}/downloads"
    
    # Try different extensions
    extensions = ['', '.json', '.csv', '.tsv', '.txt']
    for ext in extensions:
        file_path = os.path.join(base_path, f"{file_id}{ext}")
        if os.path.exists(file_path):
            return file_path
    
    # Return base path without extension if not found
    return os.path.join(base_path, file_id)


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


def read_file_lines_func(
    session_id: str,
    file_id: str,
    start: int = 1,
    end: Optional[int] = None,
    limit: int = 1000
) -> Dict[str, Any]:
    """
    Read specific line ranges from a file.
    
    Args:
        session_id: Session identifier
        file_id: File identifier
        start: Line number to start (1-indexed)
        end: Line number to end (inclusive)
        limit: Maximum lines to return (max: 10000)
    
    Returns:
        Dictionary with lines, startLine, endLine, totalLines, and hasMore
    """
    # Validate parameters
    if not session_id or not file_id:
        return {
            "error": True,
            "errorType": "INVALID_PARAMETERS",
            "message": "Missing required parameters: session_id and file_id",
            "source": "bvbrc-file-utilities"
        }
    
    # Enforce limit maximum
    limit = min(limit, 10000)
    
    # Get file path
    file_path = get_file_path(session_id, file_id)
    
    if not os.path.exists(file_path):
        return {
            "error": True,
            "errorType": "FILE_NOT_FOUND",
            "message": f"File {file_id} not found",
            "details": {"fileId": file_id, "session_id": session_id},
            "source": "bvbrc-file-utilities"
        }
    
    file_type = detect_file_type(file_path)
    
    try:
        if file_type in ['json_array', 'json_object']:
            return _read_json_lines(file_path, start, end, limit, file_type)
        elif file_type in ['csv', 'tsv']:
            return _read_csv_lines(file_path, start, end, limit, file_type)
        else:
            return _read_text_lines(file_path, start, end, limit)
    except Exception as e:
        return {
            "error": True,
            "errorType": "INVALID_PARAMETERS",
            "message": f"Error reading file: {str(e)}",
            "source": "bvbrc-file-utilities"
        }


def _read_json_lines(file_path: str, start: int, end: Optional[int], limit: int, file_type: str) -> Dict[str, Any]:
    """Read lines from JSON file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if file_type == 'json_object':
        # Convert object to array of key-value pairs
        data = [{"key": k, "value": v} for k, v in data.items()]
    
    total_lines = len(data)
    
    # Adjust indices (1-indexed to 0-indexed)
    start_idx = max(0, start - 1)
    end_idx = min(end if end else total_lines, total_lines)
    end_idx = min(start_idx + limit, end_idx)
    
    lines = data[start_idx:end_idx]
    
    return {
        "lines": lines,
        "startLine": start_idx + 1,
        "endLine": end_idx,
        "totalLines": total_lines,
        "hasMore": end_idx < total_lines
    }


def _read_csv_lines(file_path: str, start: int, end: Optional[int], limit: int, file_type: str) -> Dict[str, Any]:
    """Read lines from CSV/TSV file."""
    delimiter = '\t' if file_type == 'tsv' else ','
    
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        rows = list(reader)
    
    total_lines = len(rows)
    
    # Adjust indices
    start_idx = max(0, start - 1)
    end_idx = min(end if end else total_lines, total_lines)
    end_idx = min(start_idx + limit, end_idx)
    
    lines = rows[start_idx:end_idx]
    
    return {
        "lines": lines,
        "startLine": start_idx + 1,
        "endLine": end_idx,
        "totalLines": total_lines,
        "hasMore": end_idx < total_lines,
        "source": "bvbrc-file-utilities"
    }


def _read_text_lines(file_path: str, start: int, end: Optional[int], limit: int) -> Dict[str, Any]:
    """Read lines from text file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    total_lines = len(lines)
    
    # Adjust indices
    start_idx = max(0, start - 1)
    end_idx = min(end if end else total_lines, total_lines)
    end_idx = min(start_idx + limit, end_idx)
    
    result_lines = [line.rstrip('\n') for line in lines[start_idx:end_idx]]
    
    return {
        "lines": result_lines,
        "startLine": start_idx + 1,
        "endLine": end_idx,
        "totalLines": total_lines,
        "hasMore": end_idx < total_lines,
        "source": "bvbrc-file-utilities"
    }


def search_file_func(
    session_id: str,
    file_id: str,
    pattern: str,
    is_regex: bool = False,
    case_sensitive: bool = False,
    fields: Optional[List[str]] = None,
    limit: int = 100
) -> Dict[str, Any]:
    """
    Search for pattern in a file (grep-like functionality).
    
    Args:
        session_id: Session identifier
        file_id: File identifier
        pattern: Search pattern (literal or regex)
        is_regex: Whether pattern is a regex
        case_sensitive: Whether search is case-sensitive
        fields: For JSON/CSV, search only these fields
        limit: Maximum matches to return (max: 1000)
    
    Returns:
        Dictionary with matches, totalMatches, and truncated flag
    """
    # Validate parameters
    if not session_id or not file_id or not pattern:
        return {
            "error": True,
            "errorType": "INVALID_PARAMETERS",
            "message": "Missing required parameters",
            "source": "bvbrc-file-utilities"
        }
    
    # Enforce limit
    limit = min(limit, 1000)
    
    # Get file path
    file_path = get_file_path(session_id, file_id)
    
    if not os.path.exists(file_path):
        return {
            "error": True,
            "errorType": "FILE_NOT_FOUND",
            "message": f"File {file_id} not found"
        }
    
    file_type = detect_file_type(file_path)
    
    # Compile pattern
    try:
        if is_regex:
            flags = 0 if case_sensitive else re.IGNORECASE
            regex = re.compile(pattern, flags)
        else:
            # Escape special regex characters for literal search
            escaped_pattern = re.escape(pattern)
            flags = 0 if case_sensitive else re.IGNORECASE
            regex = re.compile(escaped_pattern, flags)
    except Exception as e:
        return {
            "error": True,
            "errorType": "INVALID_PARAMETERS",
            "message": f"Invalid regex pattern: {str(e)}",
            "source": "bvbrc-file-utilities"
        }
    
    try:
        if file_type in ['json_array', 'json_object']:
            return _search_json(file_path, regex, fields, limit, file_type)
        elif file_type in ['csv', 'tsv']:
            return _search_csv(file_path, regex, fields, limit, file_type)
        else:
            return _search_text(file_path, regex, limit)
    except Exception as e:
        return {
            "error": True,
            "errorType": "INVALID_PARAMETERS",
            "message": f"Error searching file: {str(e)}",
            "source": "bvbrc-file-utilities"
        }


def _search_json(file_path: str, regex: re.Pattern, fields: Optional[List[str]], limit: int, file_type: str) -> Dict[str, Any]:
    """Search in JSON file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if file_type == 'json_object':
        data = [{"key": k, "value": v} for k, v in data.items()]
    
    matches = []
    total_matches = 0
    
    for line_num, record in enumerate(data, start=1):
        if len(matches) >= limit:
            total_matches += 1
            continue
        
        matched_field = None
        
        if fields:
            # Search specific fields
            for field in fields:
                value = _get_nested_value(record, field)
                if value is not None and regex.search(str(value)):
                    matched_field = field
                    break
        else:
            # Search entire record
            if regex.search(json.dumps(record)):
                matched_field = None
        
        if matched_field is not None or (matched_field is None and not fields and regex.search(json.dumps(record))):
            matches.append({
                "lineNumber": line_num,
                "record": record,
                "matchedField": matched_field
            })
            total_matches += 1
    
    return {
        "matches": matches,
        "totalMatches": total_matches,
        "truncated": total_matches > len(matches)
    }


def _search_csv(file_path: str, regex: re.Pattern, fields: Optional[List[str]], limit: int, file_type: str) -> Dict[str, Any]:
    """Search in CSV/TSV file."""
    delimiter = '\t' if file_type == 'tsv' else ','
    
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        rows = list(reader)
    
    matches = []
    total_matches = 0
    
    for line_num, row in enumerate(rows, start=1):
        if len(matches) >= limit:
            total_matches += 1
            continue
        
        matched_field = None
        
        if fields:
            # Search specific columns
            for field in fields:
                if field in row and regex.search(str(row[field])):
                    matched_field = field
                    break
        else:
            # Search all columns
            for field, value in row.items():
                if regex.search(str(value)):
                    matched_field = field
                    break
        
        if matched_field:
            matches.append({
                "lineNumber": line_num,
                "record": row,
                "matchedField": matched_field
            })
            total_matches += 1
    
    return {
        "matches": matches,
        "totalMatches": total_matches,
        "truncated": total_matches > len(matches),
        "source": "bvbrc-file-utilities"
    }


def _search_text(file_path: str, regex: re.Pattern, limit: int) -> Dict[str, Any]:
    """Search in text file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    matches = []
    total_matches = 0
    
    for line_num, line in enumerate(lines, start=1):
        if len(matches) >= limit:
            total_matches += 1
            continue
        
        if regex.search(line):
            matches.append({
                "lineNumber": line_num,
                "record": line.rstrip('\n'),
                "matchedField": None
            })
            total_matches += 1
    
    return {
        "matches": matches,
        "totalMatches": total_matches,
        "truncated": total_matches > len(matches),
        "source": "bvbrc-file-utilities"
    }


def query_json_func(
    session_id: str,
    file_id: str,
    filters: Optional[List[Dict[str, Any]]] = None,
    fields: Optional[List[str]] = None,
    sort: Optional[Dict[str, str]] = None,
    limit: int = 1000,
    offset: int = 0
) -> Dict[str, Any]:
    """
    Query JSON file with SQL-like filtering.
    
    Args:
        session_id: Session identifier
        file_id: File identifier
        filters: Array of filter conditions (AND logic)
        fields: Fields to return (projection)
        sort: Sort configuration {field, order}
        limit: Maximum records (max: 10000)
        offset: Skip N records (pagination)
    
    Returns:
        Dictionary with records, totalMatching, and returned count
    """
    # Validate parameters
    if not session_id or not file_id:
        return {
            "error": True,
            "errorType": "INVALID_PARAMETERS",
            "message": "Missing required parameters"
        }
    
    # Enforce limit
    limit = min(limit, 10000)
    
    # Get file path
    file_path = get_file_path(session_id, file_id)
    
    if not os.path.exists(file_path):
        return {
            "error": True,
            "errorType": "FILE_NOT_FOUND",
            "message": f"File {file_id} not found",
            "source": "bvbrc-file-utilities"
        }
    
    file_type = detect_file_type(file_path)
    
    if file_type not in ['json_array', 'json_object']:
        return {
            "error": True,
            "errorType": "UNSUPPORTED_OPERATION",
            "message": "query_json only works on JSON arrays",
            "details": {
                "fileId": file_id,
                "expectedType": "json_array",
                "actualType": file_type
            },
            "source": "bvbrc-file-utilities"
        }
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not isinstance(data, list):
            return {
                "error": True,
                "errorType": "UNSUPPORTED_OPERATION",
                "message": "query_json only works on JSON arrays",
                "source": "bvbrc-file-utilities"
            }
        
        # Apply filters
        filtered = data
        if filters:
            for filter_def in filters:
                filtered = [
                    record for record in filtered
                    if _apply_filter(record, filter_def)
                ]
        
        # Project fields if specified
        if fields:
            filtered = [
                {field: _get_nested_value(record, field) for field in fields}
                for record in filtered
            ]
        
        # Sort if specified
        if sort:
            field = sort.get('field')
            order = sort.get('order', 'asc')
            reverse = (order == 'desc')
            
            try:
                filtered = sorted(
                    filtered,
                    key=lambda x: _get_nested_value(x, field) or 0,
                    reverse=reverse
                )
            except Exception:
                # If sorting fails, continue without sorting
                pass
        
        # Paginate
        total_matching = len(filtered)
        records = filtered[offset:offset + limit]
        
        return {
            "records": records,
            "totalMatching": total_matching,
            "returned": len(records),
            "source": "bvbrc-file-utilities"
        }
        
    except Exception as e:
        return {
            "error": True,
            "errorType": "INVALID_PARAMETERS",
            "message": f"Error querying JSON: {str(e)}",
            "source": "bvbrc-file-utilities"
        }


def _get_nested_value(obj: Dict, field_path: str) -> Any:
    """Get nested value from object using dot notation."""
    parts = field_path.split('.')
    value = obj
    
    for part in parts:
        if isinstance(value, dict) and part in value:
            value = value[part]
        else:
            return None
    
    return value


def _apply_filter(record: Dict, filter_def: Dict[str, Any]) -> bool:
    """Apply a single filter to a record."""
    field = filter_def.get('field')
    operator = filter_def.get('operator')
    value = filter_def.get('value')
    
    field_value = _get_nested_value(record, field)
    
    if field_value is None:
        return False
    
    try:
        if operator == '==':
            return field_value == value
        elif operator == '!=':
            return field_value != value
        elif operator == '>':
            return field_value > value
        elif operator == '<':
            return field_value < value
        elif operator == '>=':
            return field_value >= value
        elif operator == '<=':
            return field_value <= value
        elif operator == 'contains':
            return str(value).lower() in str(field_value).lower()
        elif operator == 'startsWith':
            return str(field_value).startswith(str(value))
        elif operator == 'endsWith':
            return str(field_value).endswith(str(value))
        else:
            return False
    except Exception:
        return False


def extract_csv_columns_func(
    session_id: str,
    file_id: str,
    columns: List[str],
    limit: int = 1000,
    skip_rows: int = 0
) -> Dict[str, Any]:
    """
    Extract specific columns from CSV/TSV file.
    
    Args:
        session_id: Session identifier
        file_id: File identifier
        columns: List of column names to extract
        limit: Maximum rows (max: 10000)
        skip_rows: Rows to skip after header
    
    Returns:
        Dictionary with data, columns, rowCount, and totalRows
    """
    # Validate parameters
    if not session_id or not file_id or not columns:
        return {
            "error": True,
            "errorType": "INVALID_PARAMETERS",
            "message": "Missing required parameters",
            "source": "bvbrc-file-utilities"
        }
    
    # Enforce limit
    limit = min(limit, 10000)
    
    # Get file path
    file_path = get_file_path(session_id, file_id)
    
    if not os.path.exists(file_path):
        return {
            "error": True,
            "errorType": "FILE_NOT_FOUND",
            "message": f"File {file_id} not found",
            "source": "bvbrc-file-utilities"
        }
    
    file_type = detect_file_type(file_path)
    
    if file_type not in ['csv', 'tsv']:
        return {
            "error": True,
            "errorType": "UNSUPPORTED_OPERATION",
            "message": "extract_csv_columns only works on CSV/TSV files",
            "details": {
                "fileId": file_id,
                "expectedType": "csv or tsv",
                "actualType": file_type
            },
            "source": "bvbrc-file-utilities"
        }
    
    delimiter = '\t' if file_type == 'tsv' else ','
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            all_rows = list(reader)
        
        total_rows = len(all_rows)
        
        # Skip rows and apply limit
        rows = all_rows[skip_rows:skip_rows + limit]
        
        # Extract only requested columns
        data = []
        for row in rows:
            extracted = {}
            for col in columns:
                extracted[col] = row.get(col, None)
            data.append(extracted)
        
        return {
            "data": data,
            "columns": columns,
            "rowCount": len(data),
            "totalRows": total_rows,
            "source": "bvbrc-file-utilities"
        }
        
    except Exception as e:
        return {
            "error": True,
            "errorType": "INVALID_PARAMETERS",
            "message": f"Error extracting CSV columns: {str(e)}",
            "source": "bvbrc-file-utilities"
        }


def get_file_statistics_func(
    session_id: str,
    file_id: str,
    fields: Optional[List[str]] = None,
    sample_size: int = 10
) -> Dict[str, Any]:
    """
    Get statistical summaries for file fields.
    
    Args:
        session_id: Session identifier
        file_id: File identifier
        fields: Fields to analyze (if empty, analyze all numeric fields)
        sample_size: Number of sample values to return
    
    Returns:
        Dictionary with statistics per field and totalRecords
    """
    # Validate parameters
    if not session_id or not file_id:
        return {
            "error": True,
            "errorType": "INVALID_PARAMETERS",
            "message": "Missing required parameters"
        }
    
    # Get file path
    file_path = get_file_path(session_id, file_id)
    
    if not os.path.exists(file_path):
        return {
            "error": True,
            "errorType": "FILE_NOT_FOUND",
            "message": f"File {file_id} not found"
        }
    
    file_type = detect_file_type(file_path)
    
    try:
        if file_type in ['json_array']:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            records = data if isinstance(data, list) else []
        elif file_type in ['csv', 'tsv']:
            delimiter = '\t' if file_type == 'tsv' else ','
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter=delimiter)
                records = list(reader)
        else:
            return {
                "error": True,
                "errorType": "UNSUPPORTED_OPERATION",
                "message": "Statistics only work on JSON arrays or CSV/TSV files",
                "source": "bvbrc-file-utilities"
            }
        
        if not records:
            return {
                "statistics": {},
                "totalRecords": 0,
                "source": "bvbrc-file-utilities"
            }
        
        # Determine fields to analyze
        if not fields:
            # Analyze all fields from first record
            fields = list(records[0].keys()) if records else []
        
        statistics_result = {}
        
        for field in fields:
            field_stats = _calculate_field_statistics(records, field, sample_size)
            if field_stats:
                statistics_result[field] = field_stats
        
        return {
            "statistics": statistics_result,
            "totalRecords": len(records),
            "source": "bvbrc-file-utilities"
        }
        
    except Exception as e:
        return {
            "error": True,
            "errorType": "INVALID_PARAMETERS",
            "message": f"Error calculating statistics: {str(e)}",
            "source": "bvbrc-file-utilities"
        }


def _calculate_field_statistics(records: List[Dict], field: str, sample_size: int) -> Optional[Dict[str, Any]]:
    """Calculate statistics for a single field."""
    values = []
    null_count = 0
    
    for record in records:
        value = _get_nested_value(record, field)
        if value is None:
            null_count += 1
        else:
            values.append(value)
    
    if not values:
        return None
    
    # Detect field type
    sample_val = values[0]
    
    if isinstance(sample_val, bool):
        # Boolean field
        true_count = sum(1 for v in values if v is True)
        false_count = len(values) - true_count
        
        return {
            "type": "boolean",
            "count": len(values),
            "nullCount": null_count,
            "distribution": {
                "true": true_count,
                "false": false_count
            }
        }
    
    elif isinstance(sample_val, (int, float)):
        # Numeric field
        numeric_values = [float(v) for v in values if isinstance(v, (int, float))]
        
        if not numeric_values:
            return None
        
        # Calculate statistics
        sorted_values = sorted(numeric_values)
        n = len(sorted_values)
        
        median = sorted_values[n // 2] if n % 2 == 1 else (sorted_values[n // 2 - 1] + sorted_values[n // 2]) / 2
        
        mean_val = statistics.mean(numeric_values)
        std_dev = statistics.stdev(numeric_values) if len(numeric_values) > 1 else 0
        
        # Top values
        value_counts = {}
        for v in numeric_values:
            value_counts[v] = value_counts.get(v, 0) + 1
        
        top_values = [
            {"value": v, "count": c}
            for v, c in sorted(value_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        ]
        
        # Sample
        sample = random.sample(numeric_values, min(sample_size, len(numeric_values)))
        
        return {
            "type": "numeric",
            "count": len(numeric_values),
            "nullCount": null_count,
            "uniqueCount": len(set(numeric_values)),
            "min": min(numeric_values),
            "max": max(numeric_values),
            "mean": mean_val,
            "median": median,
            "stdDev": std_dev,
            "topValues": top_values,
            "sample": sample
        }
    
    else:
        # String field
        string_values = [str(v) for v in values]
        
        # Value counts
        value_counts = {}
        for v in string_values:
            value_counts[v] = value_counts.get(v, 0) + 1
        
        top_values = [
            {"value": v, "count": c}
            for v, c in sorted(value_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        ]
        
        # Sample
        sample = random.sample(string_values, min(sample_size, len(string_values)))
        
        return {
            "type": "string",
            "count": len(string_values),
            "nullCount": null_count,
            "uniqueCount": len(set(string_values)),
            "topValues": top_values,
            "sample": sample
        }

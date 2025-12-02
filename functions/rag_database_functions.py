"""
RAG Database Functions

This module provides functions for querying and managing RAG (Retrieval-Augmented Generation) databases.
"""

from typing import Dict, Any, Optional
import sys
import time
from pathlib import Path

# Add rag_api to Python path to enable imports
_rag_api_path = Path(__file__).parent.parent / "rag_api"
if str(_rag_api_path) not in sys.path:
    sys.path.insert(0, str(_rag_api_path))

from app.services.rag_service import get_rag_service


def query_rag_helpdesk(
    query: str,
    top_k: Optional[int] = 5,
    config: Optional[dict] = None,
) -> Dict[str, Any]:
    """
    Query the RAG helpdesk index to retrieve relevant support and helpdesk documents.

    This function is intended to back the `query_rag_helpdesk` MCP tool.
    It wraps the RAG_API service to query the helpdesk database.

    Args:
        query: The search query string, typically a support or troubleshooting question.
        top_k: Number of top results to return.
        config: Configuration dictionary with RAG / index settings.
                Can include 'database_name' (default: 'helpdesk') and 'score_threshold' (default: 0.0).

    Returns:
        Dictionary with query results containing:
        - results: List of retrieved documents with content, score, and metadata
        - count: Number of results returned
        - query: The original query string
        - index: The database name queried
    """
    if config is None:
        config = {}

    # Get database name from config, default to 'helpdesk'
    database_name = config.get("database_name", "bvbrc_helpdesk")
    score_threshold = config.get("score_threshold", 0.0)

    try:
        # Get the RAG service and perform the search
        rag_service = get_rag_service()
        search_result = rag_service.search(
            db_name=database_name,
            query=query,
            top_k=top_k,
            score_threshold=score_threshold,
        )

        # Transform the RAG service response to match expected format
        documents = search_result.get("documents", [])
        results = [
            {
                "content": doc.get("content", ""),
                "score": doc.get("score", 0.0),
                "metadata": doc.get("metadata", {}),
            }
            for doc in documents
        ]

        result: Dict[str, Any] = {
            "results": results,
            "count": len(results),
            "query": query,
            "index": database_name,
        }

        return result

    except Exception as e:
        # Return error result in expected format
        return {
            "results": [],
            "count": 0,
            "query": query,
            "index": database_name,
            "error": str(e),
        }


def list_publication_datasets(
    query: str,
    top_k: Optional[int] = 5,
    config: Optional[dict] = None,
) -> Dict[str, Any]:
    """
    List publication datasets relevant to a query using a RAG-backed dataset index.

    This function is intended to back the `list_publication_datasets` MCP tool.

    Args:
        query: The search query string describing the desired data or analysis.
        top_k: Maximum number of datasets to return.
        config: Configuration dictionary with dataset index / RAG settings.

    Returns:
        Dictionary with dataset listing results.
    """
    if config is None:
        config = {}

    # TODO: Implement publication dataset lookup logic.
    # Example steps:
    # 1. Encode the query.
    # 2. Search a publication / dataset index (e.g., vector DB + metadata filters).
    # 3. Return a structured list of datasets that match.

    _ = time.time()  # Placeholder for potential timing / metrics later.

    result: Dict[str, Any] = {
        "results": [],
        "count": 0,
        "query": query,
        "index": "publication_datasets",
    }

    return result


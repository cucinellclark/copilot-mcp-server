#!/usr/bin/env python3
"""
RAG Database Tools

This module contains MCP tools for querying and managing RAG (Retrieval-Augmented Generation) databases.
"""

import json
import sys
from typing import Optional
from fastmcp import FastMCP

from functions.rag_database_functions import (
    query_rag_helpdesk_func,
    list_publication_datasets_func,
)

def register_rag_database_tools(mcp: FastMCP, config: dict = None):
    """
    Register all RAG database-related MCP tools with the FastMCP server.
    
    Args:
        mcp: FastMCP server instance
        config: Configuration dictionary for RAG database settings
    """
    if config is None:
        config = {}
    
    top_k_default = config.get("top_k_default", 5)

    @mcp.tool(name="helpdesk_service_usage")
    def helpdesk_service_usage(
        query: str,
        top_k: Optional[int] = None,
    ) -> str:
        """
        Primary tool for BV-BRC usage/how-to/support questions. Use this FIRST for anything like “How do I use a service application?”, workflows, troubleshooting, parameters explained, example runs, BV-BRC pages/features, or documentation needs. Prefer this over service metadata tools; it returns authoritative helpdesk/doc content from the BV-BRC helpdesk RAG.

        Args:
            query: The search query string, typically a user help or troubleshooting question.
            top_k: Number of top results to return (uses config default if not provided).

        Returns:
            JSON string with query results:
            - results: list of retrieved documents with scores and metadata
            - count: number of results returned
            - query: the original query
        """
        exec_top_k = top_k if top_k is not None else top_k_default

        print(
            f"Querying BV-BRC helpdesk: {query} (top_k={exec_top_k})...",
            file=sys.stderr,
        )
        try:
            result = query_rag_helpdesk_func(
                query=query,
                top_k=exec_top_k,
                config=config,
            )
            return json.dumps(result, indent=2)
        except Exception as e:
            return json.dumps(
                {
                    "error": f"Error querying RAG helpdesk: {str(e)}",
                    "results": [],
                    "count": 0,
                },
                indent=2,
            )

    @mcp.tool()
    def list_publication_datasets(
        query: str,
        top_k: Optional[int] = None,
    ) -> str:
        """
        List publication datasets relevant to a query using the RAG index.

        Args:
            query: The search query string, typically describing an analysis or data need.
            top_k: Optional maximum number of datasets to return (uses config default if not provided).

        Returns:
            JSON string with dataset results:
            - results: list of datasets with identifiers, titles, and metadata
            - count: number of results returned
            - query: the original query
        """
        exec_top_k = top_k if top_k is not None else top_k_default

        print(
            f"Listing publication datasets for query: {query} (top_k={exec_top_k})...",
            file=sys.stderr,
        )
        try:
            result = list_publication_datasets_func(
                query=query,
                top_k=exec_top_k,
                config=config,
            )
            return json.dumps(result, indent=2)
        except Exception as e:
            return json.dumps(
                {
                    "error": f"Error listing publication datasets: {str(e)}",
                    "results": [],
                    "count": 0,
                },
                indent=2,
            )


#!/usr/bin/env python3
"""
RAG Database Tools

This module contains MCP tools for querying and managing RAG (Retrieval-Augmented Generation) databases.
"""

import json
import sys
from typing import Optional, List
from fastmcp import FastMCP

from functions.rag_database_functions import (
    query_rag_database,
    add_to_rag_database,
    search_rag_database,
    get_rag_database_info
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
    
    @mcp.tool()
    def query_rag(
        query: str,
        top_k: Optional[int] = None,
        collection: Optional[str] = None
    ) -> str:
        """
        Query the RAG database to retrieve relevant documents.
        
        Args:
            query: The search query string
            top_k: Number of top results to return (uses config default if not provided)
            collection: Optional collection/database name to query
        
        Returns:
            JSON string with query results:
            - results: list of retrieved documents with scores
            - count: number of results returned
            - query: the original query
        """
        exec_top_k = top_k if top_k is not None else top_k_default
        exec_collection = collection if collection is not None else config.get("collection_name")
        
        print(f"Querying RAG database: {query} (top_k={exec_top_k}, collection={exec_collection})...", file=sys.stderr)
        try:
            result = query_rag_database(
                query=query,
                top_k=exec_top_k,
                collection=exec_collection,
                config=config
            )
            return json.dumps(result, indent=2)
        except Exception as e:
            return json.dumps({
                "error": f"Error querying RAG database: {str(e)}",
                "results": [],
                "count": 0
            }, indent=2)
    
    @mcp.tool()
    def add_to_rag(
        content: str,
        metadata: Optional[dict] = None,
        collection: Optional[str] = None
    ) -> str:
        """
        Add content to the RAG database.
        
        Args:
            content: The text content to add
            metadata: Optional metadata dictionary (e.g., {"title": "...", "source": "..."})
            collection: Optional collection/database name
        
        Returns:
            JSON string with operation result:
            - success: bool indicating if operation succeeded
            - id: unique identifier for the added document
            - message: status message
        """
        exec_collection = collection if collection is not None else config.get("collection_name")
        print(f"Adding content to RAG database (collection: {exec_collection})...", file=sys.stderr)
        try:
            result = add_to_rag_database(
                content=content,
                metadata=metadata,
                collection=exec_collection,
                config=config
            )
            return json.dumps(result, indent=2)
        except Exception as e:
            return json.dumps({
                "success": False,
                "error": f"Error adding to RAG database: {str(e)}"
            }, indent=2)
    
    @mcp.tool()
    def search_rag(
        query: str,
        filters: Optional[dict] = None,
        top_k: Optional[int] = 10,
        collection: Optional[str] = None
    ) -> str:
        """
        Search the RAG database with optional filters.
        
        Args:
            query: The search query string
            filters: Optional filters to apply (e.g., {"source": "...", "date": "..."})
            top_k: Number of top results to return (default: 10)
            collection: Optional collection/database name to search
        
        Returns:
            JSON string with search results:
            - results: list of matching documents with scores and metadata
            - count: number of results returned
        """
        exec_top_k = top_k if top_k is not None else top_k_default
        exec_collection = collection if collection is not None else config.get("collection_name")
        print(f"Searching RAG database: {query} (filters: {filters}, top_k={exec_top_k}, collection={exec_collection})...", file=sys.stderr)
        try:
            result = search_rag_database(
                query=query,
                filters=filters,
                top_k=exec_top_k,
                collection=exec_collection,
                config=config
            )
            return json.dumps(result, indent=2)
        except Exception as e:
            return json.dumps({
                "error": f"Error searching RAG database: {str(e)}",
                "results": [],
                "count": 0
            }, indent=2)
    
    @mcp.tool()
    def get_rag_info(collection: Optional[str] = None) -> str:
        """
        Get information about the RAG database.
        
        Args:
            collection: Optional collection/database name
        
        Returns:
            JSON string with database information:
            - total_documents: total number of documents
            - collections: list of available collections
            - database_type: type of database backend
        """
        exec_collection = collection if collection is not None else config.get("collection_name")
        print(f"Fetching RAG database information (collection: {exec_collection})...", file=sys.stderr)
        try:
            result = get_rag_database_info(collection=exec_collection, config=config)
            return json.dumps(result, indent=2)
        except Exception as e:
            return json.dumps({
                "error": f"Error getting RAG database info: {str(e)}"
            }, indent=2)


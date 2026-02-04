#!/usr/bin/env python3
"""Simple script to test the RAG database functionality."""

import json
import sys
import re
from pathlib import Path

# Add the project root to the path so we can import functions
REPO_ROOT = Path(__file__).resolve().parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from functions.rag_database_functions import query_rag_helpdesk_func


def normalize_line_terminators(obj):
    """Recursively normalize unusual line terminators in strings."""
    if isinstance(obj, dict):
        return {k: normalize_line_terminators(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [normalize_line_terminators(item) for item in obj]
    elif isinstance(obj, str):
        # Replace Line Separator (U+2028) and Paragraph Separator (U+2029) with regular newlines
        obj = obj.replace('\u2028', '\n').replace('\u2029', '\n')
        return obj
    else:
        return obj

def test_rag_database():
    """Test the RAG database by querying the helpdesk."""
    query = "how do I use the metagenomic binning service"
    
    print(f"Testing RAG Database")
    print(f"Query: {query}\n")
    print("=" * 80)
    
    # Load config from config/config.json
    config_path = REPO_ROOT / "config" / "config.json"
    with open(config_path, "r", encoding="utf-8") as f:
        root_config = json.load(f)
    
    # Extract RAG database configuration
    rag_config = root_config.get("rag_database", {})
    
    try:
        # Call the RAG helpdesk function (same as the MCP tool uses)
        result = query_rag_helpdesk_func(
            query=query,
            top_k=5,
            config=rag_config
        )
        
        # Print results
        print(f"Database: {result.get('index', 'N/A')}")
        print(f"Total Results: {result.get('count', 0)}")
        print(f"Source: {result.get('source', 'N/A')}\n")
        
        if result.get('error'):
            print(f"ERROR: {result['error']}")
            if result.get('errorType'):
                print(f"Error Type: {result['errorType']}")
        
        if result.get('summary'):
            print("SUMMARY:")
            print("-" * 80)
            print(result['summary'])
            print("-" * 80)
            print()
        
        if result.get('summary_error'):
            print(f"Summary Error: {result['summary_error']}\n")
        
        print("\nRETRIEVED DOCUMENTS:")
        print("=" * 80)
        for i, doc in enumerate(result.get('results', []), 1):
            print(f"\n[{i}] Score: {doc.get('score', 0):.2f}")
            content = doc.get('content', '')
            # Show first 300 chars
            if len(content) > 300:
                print(f"Content: {content[:300]}...")
            else:
                print(f"Content: {content}")
            
            if doc.get('metadata'):
                print(f"Metadata: {json.dumps(doc['metadata'], indent=2)}")
            print("-" * 80)
        
        # Save full result to file (normalize unusual line terminators)
        output_file = REPO_ROOT / "test_rag_output.json"
        normalized_result = normalize_line_terminators(result)
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(normalized_result, f, indent=2, ensure_ascii=False)
        
        print(f"\n✓ Full results saved to: {output_file}")
        return result
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    test_rag_database()


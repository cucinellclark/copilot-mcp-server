#!/usr/bin/env python3
"""Quick helper to call query_rag_helpdesk_func and print the result."""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Make sure the repo root is on the Python path so `functions` can be imported.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from functions.rag_database_functions import query_rag_helpdesk_func


def main() -> None:
    # Set your query here.
    query = "How do I use the Metagenomic Binning service?"

    # Load summarization model settings from config/config.json (rag_database section).
    root_config_path = REPO_ROOT / "config" / "config.json"
    with open(root_config_path, "r", encoding="utf-8") as f:
        root_config = json.load(f)

    summarization_model = root_config.get("rag_database", {}).get("summarization_model", {})

    # Use only the established config (no overrides).
    config = {"summarization_model": summarization_model}

    result = query_rag_helpdesk_func(query=query, top_k=5, config=config)
    outfile = 'test.json'
    with open(outfile, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    print(f"Results written to {outfile}")


if __name__ == "__main__":
    main()


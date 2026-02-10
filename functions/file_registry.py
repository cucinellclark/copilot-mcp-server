"""
File registry helper for CopilotMCP.

Resolves file metadata from the Copilot MongoDB index (session_files collection).
Falls back gracefully when DB config is missing.
"""

import os
import sys
from datetime import datetime
from typing import Optional, Dict, Any

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from common.config import get_config

_client: Optional[MongoClient] = None


def _get_db_settings() -> Dict[str, str]:
    config = get_config().file_utilities or {}
    return {
        "mongo_url": os.environ.get("COPILOT_MONGO_URL") or config.get("mongo_url", ""),
        "mongo_database": os.environ.get("COPILOT_MONGO_DB") or config.get("mongo_database", "copilot"),
        "mongo_collection": os.environ.get("COPILOT_MONGO_FILE_COLLECTION") or config.get("mongo_collection", "session_files")
    }


def _get_client() -> Optional[MongoClient]:
    global _client
    if _client is not None:
        return _client

    settings = _get_db_settings()
    mongo_url = settings.get("mongo_url")
    if not mongo_url:
        return None

    # Use a conservative server selection timeout to avoid blocking tool calls
    _client = MongoClient(mongo_url, serverSelectionTimeoutMS=5000)
    return _client


def get_file_record(session_id: str, file_id: str) -> Optional[Dict[str, Any]]:
    if not session_id or not file_id:
        return None

    client = _get_client()
    if client is None:
        return None

    settings = _get_db_settings()
    db = client[settings["mongo_database"]]
    collection = db[settings["mongo_collection"]]
    try:
        record = collection.find_one({"session_id": session_id, "fileId": file_id})
        if record:
            # Touch lastAccessed for parity with API behavior
            collection.update_one({"_id": record["_id"]}, {"$set": {"lastAccessed": datetime.utcnow()}})
        return record
    except PyMongoError as exc:
        print(f"[file_registry] MongoDB lookup failed: {exc}", file=sys.stderr)
        return None



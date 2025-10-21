# cluster/intents.py
from __future__ import annotations
from typing import Dict, List

# Canonical cluster intent ids (must match ops/formatters handlers)
INTENTS: List[str] = [
    "rename_cluster",
    "rename_group",
    "move_group",
    "move_procedure",
    "delete_procedure",
    "delete_table",
    "add_cluster",
    "delete_cluster",
    "restore_procedure",
    "restore_table",
    "list_trash",
    "empty_trash",
    "get_cluster_summary",
    "get_cluster_detail",
]

# Human-friendly labels
INTENT_LABELS: Dict[str, str] = {
    "rename_cluster": "Rename a cluster",
    "rename_group": "Rename a procedure group",
    "move_group": "Move a group to another cluster",
    "move_procedure": "Move a procedure to another cluster",
    "delete_procedure": "Delete a procedure (move to trash)",
    "delete_table": "Delete a table from catalog",
    "add_cluster": "Create a new cluster",
    "delete_cluster": "Delete an empty cluster",
    "restore_procedure": "Restore a procedure from trash",
    "restore_table": "Restore a table from trash",
    "list_trash": "List items in trash",
    "empty_trash": "Permanently delete all trash items",
    "get_cluster_summary": "Get cluster summary overview",
    "get_cluster_detail": "Get detailed view of a cluster",
}

def list_intents() -> List[str]:
    """Return list of all cluster intents"""
    return list(INTENTS)

def label_of(intent: str) -> str:
    """Return human-friendly label for intent"""
    return INTENT_LABELS.get(intent, intent)

def normalize_name(name: str) -> str:
    """
    Normalize names:
      - Remove brackets, backticks, quotes
      - Trim whitespace
    """
    if not name:
        return name
    s = name.strip().strip("`").strip('"').strip("'")
    # Remove brackets
    s = s.replace("[", "").replace("]", "")
    return s.strip()

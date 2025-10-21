# cluster/ops.py
"""
Deterministic cluster operations.
All functions are pure (or nearly pure) - they operate on ClusterState and return results.
No LLM calls, no I/O, just state manipulation.
"""
from __future__ import annotations
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime

# These will be called with state parameter
# Format: function(state: ClusterState, ...params) -> result

def rename_cluster(state, cluster_identifier: str, new_name: str) -> Dict[str, Any]:
    """Rename a cluster"""
    cluster_id = state.find_cluster_id(cluster_identifier)
    cluster = state.clusters[cluster_id]
    old_name = cluster.display_name or cluster_id
    cluster.display_name = new_name
    return {
        "ok": True,
        "message": f"Renamed cluster '{cluster_id}' from '{old_name}' to '{new_name}'"
    }


def rename_group(state, group_identifier: str, new_name: str) -> Dict[str, Any]:
    """Rename a procedure group"""
    group_id = state.find_group_id(group_identifier)
    for cluster in state.clusters.values():
        for group in cluster.groups:
            if group.group_id == group_id:
                old_name = group.display_name or group_id
                group.display_name = new_name
                return {
                    "ok": True,
                    "message": f"Renamed group '{group_id}' from '{old_name}' to '{new_name}'"
                }
    return {"ok": False, "message": f"Group '{group_identifier}' not found"}


def move_group(state, group_identifier: str, target_cluster_identifier: str) -> Dict[str, Any]:
    """Move a procedure group to another cluster"""
    group_id = state.find_group_id(group_identifier)
    target_cluster_id = state.find_cluster_id(target_cluster_identifier)

    # Find and remove group from source cluster
    source_cluster = None
    group_to_move = None
    for cluster in state.clusters.values():
        for group in cluster.groups:
            if group.group_id == group_id:
                source_cluster = cluster
                group_to_move = group
                cluster.groups.remove(group)
                break
        if group_to_move:
            break

    if not group_to_move:
        return {"ok": False, "message": f"Group '{group_identifier}' not found"}

    # Add to target cluster
    target_cluster = state.clusters[target_cluster_id]
    target_cluster.groups.append(group_to_move)

    # Rebuild indexes and edges
    state.rebuild_indexes()

    return {
        "ok": True,
        "message": f"Moved group '{group_id}' from '{source_cluster.cluster_id}' to '{target_cluster_id}'"
    }


def move_procedure(state, procedure_name: str, target_cluster_identifier: str) -> Dict[str, Any]:
    """Move a procedure to another cluster"""
    target_cluster_id = state.find_cluster_id(target_cluster_identifier)
    result = state.move_procedure(procedure_name, target_cluster_id)
    state.rebuild_indexes()

    source_group_id, new_group_id = result
    return {
        "ok": True,
        "message": f"Moved procedure '{procedure_name}' from group '{source_group_id}' to cluster '{target_cluster_id}' (new group: '{new_group_id}')",
        "source_group_id": source_group_id,
        "new_group_id": new_group_id
    }


def delete_procedure(state, procedure_name: str) -> Dict[str, Any]:
    """Delete a procedure (moves to trash)"""
    result = state.delete_procedure(procedure_name)
    if not result.get("ok"):
        return result

    state.rebuild_indexes()
    return result


def delete_table(state, table_name: str) -> Dict[str, Any]:
    """Delete a table (moves to trash)"""
    result = state.delete_table(table_name)
    if not result.get("ok"):
        return result

    state.rebuild_indexes()
    return result


def add_cluster(state, cluster_id: str, display_name: Optional[str] = None) -> Dict[str, Any]:
    """Add a new cluster"""
    result = state.add_cluster(cluster_id, display_name)
    if result.get("ok"):
        state.rebuild_indexes()
    return result


def delete_cluster(state, cluster_identifier: str) -> Dict[str, Any]:
    """Delete a cluster if it's empty"""
    result = state.delete_cluster_if_empty(cluster_identifier)
    if result.get("ok"):
        state.rebuild_indexes()
    return result


def restore_procedure(
    state,
    procedure_name: str,
    target_cluster_id: Optional[str] = None,
    force_new_group: bool = False
) -> Dict[str, Any]:
    """Restore a procedure from trash"""
    result = state.restore_procedure(procedure_name, target_cluster_id, force_new_group)
    if result.get("ok"):
        state.rebuild_indexes()
    return result


def restore_table(state, trash_index: int) -> Dict[str, Any]:
    """Restore a table from trash"""
    result = state.restore_table(trash_index)
    if result.get("ok"):
        state.rebuild_indexes()
    return result


def list_trash(state) -> Dict[str, Any]:
    """List all items in trash"""
    return state.list_trash()


def empty_trash(state) -> Dict[str, Any]:
    """Permanently delete all items in trash"""
    result = state.empty_trash()
    if result.get("ok"):
        state.rebuild_indexes()
    return result


def get_cluster_summary(state) -> Dict[str, Any]:
    """Get cluster summary payload"""
    return state.summary_payload()


def get_cluster_detail(state, cluster_identifier: str) -> Dict[str, Any]:
    """Get cluster detail payload"""
    return state.cluster_payload(cluster_identifier)

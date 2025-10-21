# cluster/formatters.py
"""
Formatters for cluster operations - convert operation results to markdown.
Follows the same pattern as qcat/formatters.py (deterministic, no LLM).
"""
from __future__ import annotations
from typing import Dict, Any, List

def render_rename_cluster(result: Dict[str, Any]) -> str:
    """Format rename cluster result"""
    if result.get("status") == "ok":
        return f"✓ {result.get('message', 'Cluster renamed successfully')}"
    return f"✗ Error: {result.get('error', 'Unknown error')}"

def render_rename_group(result: Dict[str, Any]) -> str:
    """Format rename group result"""
    if result.get("status") == "ok":
        return f"✓ {result.get('message', 'Group renamed successfully')}"
    return f"✗ Error: {result.get('error', 'Unknown error')}"

def render_move_group(result: Dict[str, Any]) -> str:
    """Format move group result"""
    if result.get("status") == "ok":
        return f"✓ {result.get('message', 'Group moved successfully')}"
    return f"✗ Error: {result.get('error', 'Unknown error')}"

def render_move_procedure(result: Dict[str, Any]) -> str:
    """Format move procedure result"""
    if result.get("status") == "ok":
        return f"✓ {result.get('message', 'Procedure moved successfully')}"
    return f"✗ Error: {result.get('error', 'Unknown error')}"

def render_delete_procedure(result: Dict[str, Any]) -> str:
    """Format delete procedure result"""
    if result.get("status") == "ok":
        lines = [f"✓ {result.get('message', 'Procedure deleted')}"]

        if "result" in result:
            r = result["result"]
            if r.get("tables_now_orphaned"):
                lines.append(f"\n**Tables now orphaned:** {', '.join(r['tables_now_orphaned'])}")
            if r.get("tables_auto_removed"):
                lines.append(f"**Virtual tables removed:** {', '.join(r['tables_auto_removed'])}")
            if r.get("empty_group_deleted"):
                lines.append(f"**Empty group auto-deleted:** `{r.get('original_group')}`")

        return "\n".join(lines)
    return f"✗ Error: {result.get('error', 'Unknown error')}"

def render_delete_table(result: Dict[str, Any]) -> str:
    """Format delete table result"""
    if result.get("status") == "ok":
        lines = [f"✓ {result.get('message', 'Table deleted')}"]

        if "result" in result:
            r = result["result"]
            if r.get("became_missing"):
                lines.append(f"\n**Note:** Table is still referenced by procedures - marked as missing")
            if r.get("referencing_groups"):
                lines.append(f"**Referenced by groups:** {', '.join(r['referencing_groups'])}")

        return "\n".join(lines)
    return f"✗ Error: {result.get('error', 'Unknown error')}"

def render_add_cluster(result: Dict[str, Any]) -> str:
    """Format add cluster result"""
    if result.get("status") == "ok":
        return f"✓ {result.get('message', 'Cluster created successfully')}"
    return f"✗ Error: {result.get('error', 'Unknown error')}"

def render_delete_cluster(result: Dict[str, Any]) -> str:
    """Format delete cluster result"""
    if result.get("status") == "ok":
        return f"✓ {result.get('message', 'Cluster deleted successfully')}"
    return f"✗ Error: {result.get('error', 'Unknown error')}"

def render_restore_procedure(result: Dict[str, Any]) -> str:
    """Format restore procedure result"""
    if result.get("status") == "ok":
        lines = [f"✓ {result.get('message', 'Procedure restored')}"]

        if "result" in result:
            r = result["result"]
            action = r.get("action", "unknown")
            if action == "joined_existing_group":
                lines.append(f"\n**Action:** Joined existing group `{r.get('target_group')}` (100% similarity)")
            elif action == "created_new_group":
                lines.append(f"\n**Action:** Created new singleton group `{r.get('target_group')}`")

            if r.get("tables_reinserted"):
                lines.append(f"**Tables reinserted:** {', '.join(r['tables_reinserted'])}")
            if r.get("tables_unorphaned"):
                lines.append(f"**Tables un-orphaned:** {', '.join(r['tables_unorphaned'])}")

        return "\n".join(lines)
    return f"✗ Error: {result.get('error', 'Unknown error')}"

def render_restore_table(result: Dict[str, Any]) -> str:
    """Format restore table result"""
    if result.get("status") == "ok":
        return f"✓ {result.get('message', 'Table restored successfully')}"
    return f"✗ Error: {result.get('error', 'Unknown error')}"

def render_list_trash(result: Dict[str, Any]) -> str:
    """Format trash list"""
    lines = ["# Trash Contents\n"]

    procedures = result.get("procedures", [])
    tables = result.get("tables", [])
    total = result.get("total_count", 0)

    if total == 0:
        return "Trash is empty."

    if procedures:
        lines.append(f"## Procedures ({len(procedures)})\n")
        for proc in procedures:
            proc_name = proc.get("procedure_name", "Unknown")
            table_count = proc.get("table_count", 0)
            deleted_at = proc.get("deleted_at", "Unknown")
            original_cluster = proc.get("original_cluster", "Unknown")
            lines.append(f"- **`{proc_name}`** (from cluster `{original_cluster}`, {table_count} tables)")
            lines.append(f"  - Deleted: {deleted_at[:19] if deleted_at else 'Unknown'}")

    if tables:
        lines.append(f"\n## Tables ({len(tables)})\n")
        for item in tables:
            idx = item.get("index", "?")
            data = item.get("data", {})
            table_name = data.get("table_name", "Unknown")
            was_global = data.get("was_global", False)
            was_orphaned = data.get("was_orphaned", False)
            deleted_at = item.get("deleted_at", "Unknown")

            status = []
            if was_global:
                status.append("global")
            if was_orphaned:
                status.append("orphaned")
            status_str = f" ({', '.join(status)})" if status else ""

            lines.append(f"- [{idx}] **`{table_name}`**{status_str}")
            lines.append(f"  - Deleted: {deleted_at[:19] if deleted_at else 'Unknown'}")

    lines.append(f"\n**Total:** {total} items")
    return "\n".join(lines)

def render_empty_trash(result: Dict[str, Any]) -> str:
    """Format empty trash result"""
    if result.get("status") == "ok":
        lines = [f"✓ {result.get('message', 'Trash emptied')}"]

        if "result" in result:
            r = result["result"]
            proc_count = r.get("deleted_procedures", 0)
            table_count = r.get("deleted_tables", 0)
            lines.append(f"\n**Permanently deleted:** {proc_count} procedures, {table_count} tables")

        return "\n".join(lines)
    return f"✗ Error: {result.get('error', 'Unknown error')}"

def render_cluster_summary(summary: Dict[str, Any]) -> str:
    """Format cluster summary overview"""
    lines = ["# Cluster Summary\n"]

    clusters = summary.get("clusters", [])
    global_tables = summary.get("global_tables", [])

    lines.append(f"**Total Clusters:** {len(clusters)}")
    lines.append(f"**Global Tables:** {len(global_tables)}\n")

    if clusters:
        lines.append("## Clusters\n")
        for cluster in clusters:
            cluster_id = cluster.get("cluster_id", "Unknown")
            display_name = cluster.get("display_name", cluster_id)
            proc_count = cluster.get("procedure_count", 0)
            table_count = len(cluster.get("tables", []))
            group_count = len(cluster.get("group_ids", []))

            lines.append(f"### `{cluster_id}` - {display_name}")
            lines.append(f"- Procedures: {proc_count}")
            lines.append(f"- Groups: {group_count}")
            lines.append(f"- Tables: {table_count}\n")

    return "\n".join(lines)

def render_cluster_detail(detail: Dict[str, Any]) -> str:
    """Format cluster detail view"""
    cluster = detail.get("cluster", {})
    groups = detail.get("groups", [])
    global_tables = detail.get("global_tables", [])

    cluster_id = cluster.get("cluster_id", "Unknown")
    display_name = cluster.get("display_name", cluster_id)

    lines = [f"# Cluster: `{cluster_id}` - {display_name}\n"]

    # Summary stats
    proc_count = cluster.get("procedure_count", 0)
    table_count = len(cluster.get("tables", []))
    lines.append(f"**Procedures:** {proc_count}")
    lines.append(f"**Tables:** {table_count}")
    lines.append(f"**Groups:** {len(groups)}\n")

    # Groups
    if groups:
        lines.append("## Groups\n")
        for group in groups:
            group_id = group.get("group_id", "Unknown")
            display = group.get("display_name", group_id)
            procedures = group.get("procedures", [])
            tables = group.get("tables", [])
            is_singleton = group.get("is_singleton", False)

            group_type = "Singleton" if is_singleton else "Multi-procedure"
            lines.append(f"### `{group_id}` - {display} ({group_type})")
            lines.append(f"**Procedures:** {', '.join(f'`{p}`' for p in procedures)}")
            lines.append(f"**Tables:** {', '.join(f'`{t}`' for t in tables)}\n")

    return "\n".join(lines)

"""FastAPI prototype for interactive cluster editing backed by clusters.json.

This service keeps cluster data in memory, exposes read APIs for summaries and
detail diagrams, and routes all mutations through backend commands so UI and
semantic agents share the same logic.
"""

from __future__ import annotations

import json
import re
import threading
from collections import Counter, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
import subprocess

from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class ProcedureGroup:
    group_id: str
    cluster_id: str
    procedures: List[str]
    tables: List[str]
    is_singleton: bool
    display_name: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "group_id": self.group_id,
            "cluster_id": self.cluster_id,
            "procedures": list(self.procedures),
            "tables": list(self.tables),
            "is_singleton": self.is_singleton,
            "display_name": self.display_name,
        }


@dataclass
class ClusterInfo:
    cluster_id: str
    group_ids: List[str]
    display_name: Optional[str] = None
    procedures: List[str] = field(default_factory=list)
    tables: List[str] = field(default_factory=list)
    procedure_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "cluster_id": self.cluster_id,
            "display_name": self.display_name,
            "group_ids": list(self.group_ids),
            "procedures": list(self.procedures),
            "procedure_count": self.procedure_count,
            "tables": list(self.tables),
        }


@dataclass
class SimilarityEdge:
    source: str
    target: str
    similarity: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "target": self.target,
            "similarity": self.similarity,
        }


@dataclass
class ProcedureTableEdge:
    group_id: str
    table: str
    is_global_table: bool

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class TrashItem:
    """Represents a deleted entity (procedure or table)."""
    item_type: str  # 'procedure' or 'table'
    item_id: str
    data: Dict[str, Any]
    deleted_at: str  # ISO timestamp

    def to_dict(self) -> Dict[str, Any]:
        return {
            "item_type": self.item_type,
            "item_id": self.item_id,
            "data": self.data,
            "deleted_at": self.deleted_at,
        }


# ---------------------------------------------------------------------------
# Cluster state & helpers
# ---------------------------------------------------------------------------


class ClusterState:
    """In-memory representation of the cluster model."""

    def __init__(
        self,
        *,
        clusters: Dict[str, ClusterInfo],
        groups: Dict[str, ProcedureGroup],
        cluster_order: List[str],
        group_order: List[str],
        global_tables: Set[str],
        missing_tables: Set[str],
        orphaned_tables: Set[str],
        similarity_edges: List[SimilarityEdge],
        parameters: Dict[str, Any],
        catalog_path: Optional[str],
    ) -> None:
        self.clusters = clusters
        self.groups = groups
        self.cluster_order = cluster_order
        self.group_order = group_order
        self.global_tables = set(global_tables)
        self.missing_tables = set(missing_tables)
        self.orphaned_tables = set(orphaned_tables)
        self.similarity_edges = similarity_edges
        self.parameters = parameters or {}
        self.catalog_path = catalog_path

        self.table_usage: Counter[str] = Counter()
        self.table_nodes: List[Dict[str, Any]] = []
        self.procedure_table_edges: List[ProcedureTableEdge] = []
        self.last_updated: datetime = datetime.now(timezone.utc)
        self.trash: List[TrashItem] = []

        self._ensure_trash_cluster()
        self.rebuild_indexes()

    def _ensure_trash_cluster(self):
        """Ensure a special 'Trash' cluster exists for deleted procedures."""
        if "trash" not in self.clusters:
            self.clusters["trash"] = ClusterInfo(
                cluster_id="trash",
                group_ids=[],
                display_name="Trash",
            )
            if "trash" not in self.cluster_order:
                self.cluster_order.append("trash")

    # ------------------------------------------------------------------ #
    # Loading & serialization
    # ------------------------------------------------------------------ #

    @classmethod
    def from_json(cls, payload: Dict[str, Any]) -> "ClusterState":
        clusters = {}
        cluster_order: List[str] = []
        for item in payload.get("clusters", []):
            cluster_id = item["cluster_id"]
            cluster = ClusterInfo(
                cluster_id=cluster_id,
                group_ids=list(item.get("group_ids", [])),
                display_name=item.get("display_name"),
                procedures=list(item.get("procedures", [])),
                tables=list(item.get("tables", [])),
                procedure_count=item.get("procedure_count", len(item.get("procedures", []))),
            )
            clusters[cluster_id] = cluster
            cluster_order.append(cluster_id)

        groups = {}
        group_order: List[str] = []
        for item in payload.get("procedure_groups", []):
            group_id = item["group_id"]
            group = ProcedureGroup(
                group_id=group_id,
                cluster_id=item["cluster_id"],
                procedures=list(item.get("procedures", [])),
                tables=list(item.get("tables", [])),
                is_singleton=bool(item.get("is_singleton", False)),
                display_name=item.get("display_name"),
            )
            groups[group_id] = group
            group_order.append(group_id)

        similarity_edges = [
            SimilarityEdge(
                source=item["source"],
                target=item["target"],
                similarity=float(item["similarity"]),
            )
            for item in payload.get("similarity_edges", [])
        ]

        global_tables = set(payload.get("global_tables", []))
        parameters = payload.get("parameters", {})
        catalog_path = payload.get("catalog_path")

        # Load missing tables from table_nodes if available
        missing_tables: Set[str] = set()
        orphaned_tables: Set[str] = set()
        for table_node in payload.get("table_nodes", []):
            if table_node.get("is_missing", False):
                missing_tables.add(table_node["table"])
            if table_node.get("is_orphaned", False):
                orphaned_tables.add(table_node["table"])

        # Load trash
        trash_items = []
        for item_data in payload.get("trash", []):
            trash_items.append(TrashItem(
                item_type=item_data["item_type"],
                item_id=item_data["item_id"],
                data=item_data["data"],
                deleted_at=item_data["deleted_at"],
            ))

        state = cls(
            clusters=clusters,
            groups=groups,
            cluster_order=cluster_order,
            group_order=group_order,
            global_tables=global_tables,
            missing_tables=missing_tables,
            orphaned_tables=orphaned_tables,
            similarity_edges=similarity_edges,
            parameters=parameters,
            catalog_path=catalog_path,
        )
        state.trash = trash_items
        return state

    def snapshot(self) -> Dict[str, Any]:
        """Serialize current state in the same shape as the JSON snapshot."""
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "catalog_path": self.catalog_path,
            "parameters": self.parameters,
            "global_tables": sorted(self.global_tables),
            "clusters": [self.clusters[cid].to_dict() for cid in self.cluster_order if cid in self.clusters],
            "procedure_groups": [self.groups[gid].to_dict() for gid in self.group_order if gid in self.groups],
            "similarity_edges": [edge.to_dict() for edge in self.similarity_edges],
            "table_nodes": list(self.table_nodes),
            "procedure_table_edges": [edge.to_dict() for edge in self.procedure_table_edges],
            "trash": [item.to_dict() for item in self.trash],
        }

    # ------------------------------------------------------------------ #
    # Lookup helpers
    # ------------------------------------------------------------------ #

    def find_cluster_id(self, identifier: str) -> str:
        """Resolve a cluster identifier by ID or display name (case-insensitive)."""
        identifier_lower = identifier.lower()
        if identifier in self.clusters:
            return identifier

        matches = [
            cid
            for cid, cluster in self.clusters.items()
            if cluster.display_name and cluster.display_name.lower() == identifier_lower
        ]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise ValueError(f"Ambiguous cluster name '{identifier}'. Matches: {matches}")
        raise KeyError(f"Cluster '{identifier}' not found")

    def find_group_id(self, identifier: str) -> str:
        """Resolve a group identifier by ID or display name (case-insensitive)."""
        if identifier in self.groups:
            return identifier

        identifier_lower = identifier.lower()
        matches = [
            gid
            for gid, group in self.groups.items()
            if group.display_name and group.display_name.lower() == identifier_lower
        ]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise ValueError(f"Ambiguous group name '{identifier}'. Matches: {matches}")
        raise KeyError(f"Group '{identifier}' not found")

    def find_group_by_procedure(self, procedure_name: str) -> ProcedureGroup:
        for group in self.groups.values():
            if procedure_name in group.procedures:
                return group
        raise KeyError(f"Procedure '{procedure_name}' not found in any group")

    # ------------------------------------------------------------------ #
    # Mutation operations
    # ------------------------------------------------------------------ #

    def rename_cluster(self, cluster_identifier: str, new_name: str) -> None:
        cluster_id = self.find_cluster_id(cluster_identifier)
        cluster = self.clusters[cluster_id]
        cluster.display_name = new_name.strip()
        self.rebuild_indexes()

    def rename_group(self, group_identifier: str, new_name: str) -> None:
        group_id = self.find_group_id(group_identifier)
        group = self.groups[group_id]
        group.display_name = new_name.strip()
        self.rebuild_indexes()

    def move_group(self, group_identifier: str, target_cluster_identifier: str) -> None:
        group_id = self.find_group_id(group_identifier)
        target_cluster_id = self.find_cluster_id(target_cluster_identifier)

        group = self.groups[group_id]
        if group.cluster_id == target_cluster_id:
            return  # No-op

        # Remove from current cluster listing
        if group.cluster_id in self.clusters:
            source_cluster = self.clusters[group.cluster_id]
            source_cluster.group_ids = [gid for gid in source_cluster.group_ids if gid != group_id]

        # Add to target cluster
        target_cluster = self.clusters[target_cluster_id]
        if group_id not in target_cluster.group_ids:
            target_cluster.group_ids.append(group_id)

        group.cluster_id = target_cluster_id
        self.rebuild_indexes()

    def move_procedure(self, procedure_name: str, target_cluster_identifier: str) -> Tuple[str, str]:
        target_cluster_id = self.find_cluster_id(target_cluster_identifier)
        group = self.find_group_by_procedure(procedure_name)

        if group.cluster_id == target_cluster_id and group.is_singleton:
            return group.group_id, target_cluster_id

        if group.is_singleton:
            # Just move the existing group
            self.move_group(group.group_id, target_cluster_id)
            return group.group_id, target_cluster_id

        # Remove procedure from current group
        if procedure_name not in group.procedures:
            raise KeyError(f"Procedure '{procedure_name}' not found in expected group '{group.group_id}'")

        group.procedures = [proc for proc in group.procedures if proc != procedure_name]
        if len(group.procedures) <= 1:
            group.is_singleton = len(group.procedures) == 1
            if group.is_singleton and len(group.procedures) == 1 and not group.display_name:
                # Default singleton display name to the remaining procedure
                group.display_name = group.procedures[0]

        # Create a new singleton group for the procedure
        candidate_id = procedure_name
        suffix = 1
        while candidate_id in self.groups:
            suffix += 1
            candidate_id = f"{procedure_name}__{suffix}"

        new_group = ProcedureGroup(
            group_id=candidate_id,
            cluster_id=target_cluster_id,
            procedures=[procedure_name],
            tables=list(group.tables),
            is_singleton=True,
            display_name=procedure_name,
        )

        self.groups[new_group.group_id] = new_group
        self.group_order.append(new_group.group_id)

        target_cluster = self.clusters[target_cluster_id]
        target_cluster.group_ids.append(new_group.group_id)

        self.rebuild_indexes()
        return new_group.group_id, target_cluster_id

    # ------------------------------------------------------------------ #
    # Trash operations (delete/restore real entities)
    # ------------------------------------------------------------------ #

    def delete_procedure(self, procedure_name: str) -> Dict[str, Any]:
        """Delete a procedure (real entity) by moving it to Trash cluster.

        Side effects:
        - Procedure moved to Trash cluster as singleton group
        - Original group loses this procedure
        - If original group becomes empty → auto-deleted (virtual entity cleanup)
        - Tables only accessed by this procedure become orphaned
        - Tables that are orphaned AND missing → auto-removed from tracking
        """
        # Find the group containing this procedure
        group = self.find_group_by_procedure(procedure_name)
        original_group_id = group.group_id
        original_cluster_id = group.cluster_id

        # Don't allow deleting if already in trash
        if group.cluster_id == "trash":
            raise ValueError(f"Procedure '{procedure_name}' is already in trash")

        # Get procedure's table dependencies
        procedure_tables = list(group.tables)

        # Calculate which tables will become orphaned
        tables_to_orphan = []
        for table in procedure_tables:
            # Count how many OTHER procedures/groups use this table
            usage_count = 0
            for g in self.groups.values():
                if g.cluster_id == "trash":
                    continue  # Don't count trash procedures
                if g.group_id == original_group_id:
                    # Count other procedures in this group
                    other_procs = [p for p in g.procedures if p != procedure_name]
                    if other_procs:
                        usage_count += 1
                elif table in g.tables:
                    usage_count += 1

            if usage_count == 0:
                tables_to_orphan.append(table)

        # Create trash metadata
        trash_metadata = {
            "procedure_name": procedure_name,
            "original_group_id": original_group_id,
            "original_cluster_id": original_cluster_id,
            "tables": procedure_tables,
        }

        # Create trash item
        trash_item = TrashItem(
            item_type="procedure",
            item_id=procedure_name,
            data=trash_metadata,
            deleted_at=datetime.now(timezone.utc).isoformat(),
        )
        self.trash.append(trash_item)

        # Remove procedure from original group
        group.procedures = [p for p in group.procedures if p != procedure_name]

        # Check if original group is now empty → auto-delete (virtual entity)
        empty_group_deleted = False
        if len(group.procedures) == 0:
            # Remove empty group
            if original_cluster_id in self.clusters:
                cluster = self.clusters[original_cluster_id]
                cluster.group_ids = [gid for gid in cluster.group_ids if gid != original_group_id]

            self.group_order.remove(original_group_id)
            del self.groups[original_group_id]
            empty_group_deleted = True
        else:
            # Update group metadata
            group.is_singleton = len(group.procedures) == 1
            if group.is_singleton and not group.display_name:
                group.display_name = group.procedures[0]

        # Create singleton group in Trash cluster
        trash_group_id = f"trash_{procedure_name}"
        suffix = 1
        while trash_group_id in self.groups:
            suffix += 1
            trash_group_id = f"trash_{procedure_name}_{suffix}"

        trash_group = ProcedureGroup(
            group_id=trash_group_id,
            cluster_id="trash",
            procedures=[procedure_name],
            tables=[],  # Trash procedures have no table connections (disconnected nodes)
            is_singleton=True,
            display_name=procedure_name,
        )

        self.groups[trash_group_id] = trash_group
        self.group_order.append(trash_group_id)
        self.clusters["trash"].group_ids.append(trash_group_id)

        # Mark tables as orphaned
        for table in tables_to_orphan:
            self.orphaned_tables.add(table)

        # Auto-remove virtual entities: tables that are BOTH missing AND orphaned
        tables_auto_removed = []
        for table in list(self.missing_tables):
            if table in self.orphaned_tables:
                # This is a virtual entity (doesn't exist in catalog, not used by any proc)
                self.missing_tables.discard(table)
                self.orphaned_tables.discard(table)
                tables_auto_removed.append(table)

        # Rebuild indexes
        self.rebuild_indexes()

        return {
            "deleted_procedure": procedure_name,
            "original_group": original_group_id,
            "original_cluster": original_cluster_id,
            "empty_group_deleted": empty_group_deleted,
            "moved_to_trash_group": trash_group_id,
            "tables_now_orphaned": tables_to_orphan,
            "tables_auto_removed": tables_auto_removed,
        }

    def delete_table(self, table_name: str) -> Dict[str, Any]:
        """Delete a table (real entity from catalog).

        Protection: Cannot delete missing tables (virtual entities)

        Side effects:
        - Table removed from catalog tracking
        - If table is still referenced by procedures → becomes missing
        - Groups that reference it keep the reference (table becomes missing)
        - If table becomes both missing AND orphaned → auto-removed from tracking
        """
        # Protection: Cannot delete missing tables (they're virtual entities)
        if table_name in self.missing_tables:
            raise ValueError(
                f"Cannot delete missing table '{table_name}'. "
                f"Missing tables are virtual entities (don't exist in catalog). "
                f"They can only be removed by deleting procedures that reference them."
            )

        # Check if table exists in system
        is_global = table_name in self.global_tables
        is_orphaned = table_name in self.orphaned_tables

        # Find groups that reference this table (excluding trash)
        referencing_groups = [
            g.group_id for g in self.groups.values()
            if table_name in g.tables and g.cluster_id != "trash"
        ]

        if not is_global and not is_orphaned and not referencing_groups:
            raise KeyError(f"Table '{table_name}' not found in system")

        # Table is now deleted from catalog
        # If it's still referenced by procedures, it becomes missing
        will_become_missing = len(referencing_groups) > 0

        # Create trash item for table
        trash_item = TrashItem(
            item_type="table",
            item_id=table_name,
            data={
                "table_name": table_name,
                "was_global": is_global,
                "was_orphaned": is_orphaned,
                "referencing_groups": referencing_groups,
            },
            deleted_at=datetime.now(timezone.utc).isoformat(),
        )
        self.trash.append(trash_item)

        # Remove from tracking sets
        self.global_tables.discard(table_name)
        self.orphaned_tables.discard(table_name)

        # If table is referenced, mark as missing
        if will_become_missing:
            self.missing_tables.add(table_name)

        # Check if it becomes both missing and orphaned (shouldn't happen, but handle it)
        auto_removed = False
        if table_name in self.missing_tables and table_name in self.orphaned_tables:
            self.missing_tables.discard(table_name)
            self.orphaned_tables.discard(table_name)
            auto_removed = True

        # Rebuild indexes
        self.rebuild_indexes()

        return {
            "deleted_table": table_name,
            "was_global": is_global,
            "was_orphaned": is_orphaned,
            "became_missing": will_become_missing,
            "referencing_groups": referencing_groups,
            "auto_removed": auto_removed,
        }

    def restore_procedure(
        self,
        procedure_name: str,
        target_cluster_id: str,
        force_new_group: bool = False
    ) -> Dict[str, Any]:
        """Restore a procedure from Trash cluster with strict auto-grouping (100% similarity).

        Auto-grouping logic:
        1. Find the trash group containing this procedure
        2. Find existing groups in target cluster with EXACT same table set
        3. If exact match found and not force_new_group → join that group
        4. Otherwise → create new singleton group
        5. Auto-reinsertion: If procedure needs missing tables, reinsert them
        """
        # Find procedure in trash
        trash_group = self.find_group_by_procedure(procedure_name)

        if trash_group.cluster_id != "trash":
            raise ValueError(
                f"Procedure '{procedure_name}' is not in trash "
                f"(currently in '{trash_group.cluster_id}')"
            )

        # Validate target cluster
        if target_cluster_id not in self.clusters:
            raise KeyError(f"Target cluster '{target_cluster_id}' not found")

        if target_cluster_id == "trash":
            raise ValueError("Cannot restore to Trash cluster")

        # Get procedure's tables
        procedure_tables = set(trash_group.tables)

        # Auto-reinsertion: Check for missing tables
        tables_reinserted = []
        for table in procedure_tables:
            if table not in self.missing_tables:
                continue

            # Table is missing - ensure it's tracked as missing
            # Since it's about to be used by this procedure, it won't be orphaned anymore
            self.missing_tables.add(table)
            tables_reinserted.append(table)

        # Auto-grouping: Find group with EXACT same table set (100% similarity)
        exact_match_group = None

        if not force_new_group:
            target_cluster = self.clusters[target_cluster_id]

            for group_id in target_cluster.group_ids:
                group = self.groups.get(group_id)
                if not group:
                    continue

                # Check for EXACT table match (100% similarity)
                group_tables = set(group.tables)

                if procedure_tables == group_tables:
                    # Perfect match!
                    exact_match_group = group
                    break

        # Decision: Join existing group or create new one?
        if exact_match_group:
            # Join existing group (100% similarity)
            target_group = exact_match_group

            # Add procedure to group
            target_group.procedures.append(procedure_name)
            target_group.is_singleton = False  # Now has multiple procedures

            # Tables are already identical, no need to merge

            # Remove procedure from trash group
            trash_group.procedures.remove(procedure_name)

            # If trash group is now empty, delete it (auto-cleanup virtual entity)
            if not trash_group.procedures:
                self.clusters["trash"].group_ids.remove(trash_group.group_id)
                self.group_order.remove(trash_group.group_id)
                del self.groups[trash_group.group_id]

            action = "joined_existing_group"
            target_group_id = target_group.group_id
            similarity = 1.0  # 100%

        else:
            # Create new singleton group in target cluster
            new_group_id = procedure_name
            suffix = 1
            while new_group_id in self.groups:
                suffix += 1
                new_group_id = f"{procedure_name}_{suffix}"

            new_group = ProcedureGroup(
                group_id=new_group_id,
                cluster_id=target_cluster_id,
                procedures=[procedure_name],
                tables=list(procedure_tables),
                is_singleton=True,
                display_name=procedure_name,
            )

            self.groups[new_group_id] = new_group
            self.group_order.append(new_group_id)
            self.clusters[target_cluster_id].group_ids.append(new_group_id)

            # Remove procedure from trash group
            trash_group.procedures.remove(procedure_name)

            # If trash group is now empty, delete it (auto-cleanup virtual entity)
            if not trash_group.procedures:
                self.clusters["trash"].group_ids.remove(trash_group.group_id)
                self.group_order.remove(trash_group.group_id)
                del self.groups[trash_group.group_id]

            action = "created_new_group"
            target_group_id = new_group_id
            similarity = 0.0

        # Un-orphan tables that are now used
        tables_unorphaned = []
        for table in procedure_tables:
            if table in self.orphaned_tables:
                self.orphaned_tables.discard(table)
                tables_unorphaned.append(table)

        # Rebuild indexes
        self.rebuild_indexes()

        return {
            "restored_procedure": procedure_name,
            "target_cluster": target_cluster_id,
            "target_group": target_group_id,
            "action": action,
            "similarity": similarity,
            "exact_match": action == "joined_existing_group",
            "tables_reinserted": tables_reinserted,
            "tables_unorphaned": tables_unorphaned,
        }

    def restore_table(self, trash_index: int) -> Dict[str, Any]:
        """Restore a deleted table from trash."""
        if trash_index < 0 or trash_index >= len(self.trash):
            raise ValueError(f"Invalid trash index: {trash_index}")

        trash_item = self.trash[trash_index]

        if trash_item.item_type != "table":
            raise ValueError(f"Expected table, got {trash_item.item_type}")

        table_name = trash_item.data["table_name"]
        was_global = trash_item.data["was_global"]
        was_orphaned = trash_item.data["was_orphaned"]

        # Remove from missing tables (it's back in catalog)
        self.missing_tables.discard(table_name)

        # Restore table status flags
        if was_orphaned:
            self.orphaned_tables.add(table_name)

        # Remove from trash
        self.trash.pop(trash_index)

        # Rebuild indexes to recalculate global tables
        self.rebuild_indexes()

        return {
            "restored_table": table_name,
            "was_global": was_global,
            "was_orphaned": was_orphaned,
        }

    def list_trash(self) -> Dict[str, Any]:
        """List all items in trash (tables + procedures in Trash cluster)."""
        # Tables in trash list
        tables = [
            {
                "index": idx,
                **item.to_dict(),
            }
            for idx, item in enumerate(self.trash)
            if item.item_type == "table"
        ]

        # Procedures in trash cluster
        trash_cluster = self.clusters.get("trash")
        procedures = []

        if trash_cluster:
            for group_id in trash_cluster.group_ids:
                if group_id in self.groups:
                    group = self.groups[group_id]

                    for proc_name in group.procedures:
                        # Find trash metadata if available
                        trash_meta = None
                        for item in self.trash:
                            if item.item_type == "procedure" and item.item_id == proc_name:
                                trash_meta = item
                                break

                        procedures.append({
                            "procedure_name": proc_name,
                            "group_id": group_id,
                            "tables": group.tables,
                            "table_count": len(group.tables),
                            "deleted_at": trash_meta.deleted_at if trash_meta else None,
                            "original_cluster": (
                                trash_meta.data.get("original_cluster_id")
                                if trash_meta else None
                            ),
                        })

        return {
            "tables": tables,
            "procedures": procedures,
            "total_count": len(tables) + len(procedures),
        }

    def empty_trash(self) -> Dict[str, Any]:
        """Permanently delete all items in trash (both tables and procedures)."""
        # Count items
        table_count = sum(1 for item in self.trash if item.item_type == "table")

        # Count procedures in trash cluster
        trash_cluster = self.clusters.get("trash")
        procedure_count = 0
        procedures_deleted = []

        if trash_cluster:
            for group_id in list(trash_cluster.group_ids):
                if group_id in self.groups:
                    group = self.groups[group_id]
                    procedures_deleted.extend(group.procedures)
                    procedure_count += len(group.procedures)

                    self.group_order.remove(group_id)
                    del self.groups[group_id]

            trash_cluster.group_ids.clear()

        # Clear trash list
        self.trash.clear()

        # Rebuild indexes
        self.rebuild_indexes()

        return {
            "deleted_tables": table_count,
            "deleted_procedures": procedure_count,
            "procedures": procedures_deleted,
            "total": table_count + procedure_count,
        }

    def add_cluster(self, cluster_id: str, display_name: Optional[str] = None) -> Dict[str, Any]:
        """Create a new cluster."""
        if cluster_id in self.clusters:
            raise ValueError(f"Cluster '{cluster_id}' already exists")

        cluster = ClusterInfo(
            cluster_id=cluster_id,
            group_ids=[],
            display_name=display_name or cluster_id,
        )

        self.clusters[cluster_id] = cluster
        self.cluster_order.append(cluster_id)

        self.rebuild_indexes()

        return {
            "created_cluster": cluster_id,
            "display_name": cluster.display_name,
        }

    def delete_cluster_if_empty(self, cluster_identifier: str) -> Dict[str, Any]:
        """Delete a cluster ONLY if it's empty (administrative cleanup)."""
        cluster_id = self.find_cluster_id(cluster_identifier)
        cluster = self.clusters[cluster_id]

        # Don't allow deleting special clusters
        if cluster_id == "trash":
            raise ValueError("Cannot delete the Trash cluster")

        # Check if cluster is empty
        if cluster.group_ids:
            raise ValueError(
                f"Cannot delete cluster '{cluster_id}' - it has {len(cluster.group_ids)} groups. "
                f"Delete or move all procedures first."
            )

        # Remove empty cluster
        self.cluster_order.remove(cluster_id)
        del self.clusters[cluster_id]

        return {
            "deleted_cluster": cluster_id,
        }

    # ------------------------------------------------------------------ #
    @staticmethod
    def _escape_label(text: str) -> str:
        return (
            text.replace("\\", "\\\\")
            .replace("\n", "\\n")
            .replace('"', '\\"')
        )

    # ------------------------------------------------------------------ #
    # Derived data recomputation
    # ------------------------------------------------------------------ #

    def rebuild_indexes(self) -> None:
        """Refresh computed metadata after any mutation."""
        # Ensure cluster memberships are in sync with group assignments
        for cluster in self.clusters.values():
            cluster.group_ids = [
                gid
                for gid in cluster.group_ids
                if gid in self.groups and self.groups[gid].cluster_id == cluster.cluster_id
            ]

        for group_id, group in self.groups.items():
            cluster = self.clusters.get(group.cluster_id)
            if cluster and group_id not in cluster.group_ids:
                cluster.group_ids.append(group_id)

        # Recompute cluster summaries
        for cluster in self.clusters.values():
            procedure_set = {
                proc
                for gid in cluster.group_ids
                for proc in self.groups[gid].procedures
            }
            table_set = {
                table
                for gid in cluster.group_ids
                for table in self.groups[gid].tables
            }
            cluster.procedures = sorted(procedure_set)
            cluster.tables = sorted(table_set)
            cluster.procedure_count = len(cluster.procedures)

        # Recompute table usage & global tables
        table_usage: Counter[str] = Counter()
        table_cluster_map: Dict[str, Set[str]] = defaultdict(set)
        for group in self.groups.values():
            for table in group.tables:
                table_usage[table] += 1
                table_cluster_map[table].add(group.cluster_id)

        self.table_usage = table_usage

        min_global_clusters = int(self.parameters.get("min_global_clusters", 2) or 2)
        self.global_tables = {
            table for table, clusters in table_cluster_map.items() if len(clusters) >= min_global_clusters
        }

        # Build table_nodes including missing and orphaned flags
        self.table_nodes = [
            {
                "table": table,
                "usage_count": self.table_usage[table],
                "is_global": table in self.global_tables,
                "is_missing": table in self.missing_tables,
                "is_orphaned": False,  # Used tables can't be orphaned
            }
            for table in sorted(self.table_usage.keys())
        ] + [
            {
                "table": table,
                "usage_count": 0,
                "is_global": False,
                "is_missing": False,
                "is_orphaned": True,
            }
            for table in sorted(self.orphaned_tables)
        ]

        self.procedure_table_edges = [
            ProcedureTableEdge(
                group_id=group_id,
                table=table,
                is_global_table=table in self.global_tables,
            )
            for group_id, group in self.groups.items()
            for table in group.tables
        ]

        self._recompute_similarity_edges()
        self.last_updated = datetime.now(timezone.utc)

    def _recompute_similarity_edges(self) -> None:
        min_group_size = int(self.parameters.get("min_group_size", 1) or 1)
        threshold = float(self.parameters.get("similarity_threshold", 0.5) or 0.5)

        groups_sorted = sorted(self.groups.values(), key=lambda g: g.group_id)
        edges: List[SimilarityEdge] = []

        core_tables_map: Dict[str, Set[str]] = {
            group.group_id: {table for table in group.tables if table not in self.global_tables}
            for group in groups_sorted
        }

        for i, group_a in enumerate(groups_sorted):
            core_a = core_tables_map[group_a.group_id]
            if len(core_a) < min_group_size:
                continue
            for group_b in groups_sorted[i + 1 :]:
                core_b = core_tables_map[group_b.group_id]
                if len(core_b) < min_group_size:
                    continue

                intersection = core_a & core_b
                if not intersection:
                    continue

                union = core_a | core_b
                similarity = len(intersection) / len(union) if union else 0.0
                if similarity >= threshold:
                    edges.append(
                        SimilarityEdge(
                            source=group_a.group_id,
                            target=group_b.group_id,
                            similarity=similarity,
                        )
                    )

        self.similarity_edges = edges

    # ------------------------------------------------------------------ #
    # DOT generation
    # ------------------------------------------------------------------ #

    def generate_summary_dot(self) -> str:
        lines: List[str] = ["graph cluster_overview {"]
        lines.append("  graph [layout=neato, overlap=false, splines=true];")
        lines.append('  node [fontname="Helvetica"];')
        lines.append("")

        if self.global_tables:
            lines.append("  // Global tables referenced by multiple clusters")
            for table in sorted(self.global_tables):
                label = self._escape_label(table)
                # Check if table is missing
                if table in self.missing_tables:
                    prefix = "tableX::"
                    fillcolor = "#9E9E9E"  # Gray color for missing tables
                    missing_label = self._escape_label(f"{table}\n(missing)")
                    lines.append(
                        f'  "{table}" [shape=box,style="filled,bold",fillcolor="{fillcolor}",penwidth=2,'
                        f'id="{prefix}{table}",label="{missing_label}"];'
                    )
                else:
                    lines.append(
                        f'  "{table}" [shape=box,style="filled,bold",fillcolor="#FFF2CC",penwidth=2,'
                        f'id="table::{table}",label="{label}"];'
                    )
            lines.append("")

        lines.append("  // Cluster nodes")
        for cluster_id in self.cluster_order:
            cluster = self.clusters.get(cluster_id)
            if not cluster:
                continue
            display = cluster.display_name or cluster.cluster_id
            # Count only non-singleton groups (singletons are just standalone procedures)
            non_singleton_count = sum(
                1 for gid in cluster.group_ids
                if gid in self.groups and not self.groups[gid].is_singleton
            )
            # Use text labels instead of icons for Graphviz compatibility:
            # P for procedures, G for groups, T for tables
            # Escape each part individually, then join with literal \n for DOT format
            label_lines = [
                self._escape_label(display),
                self._escape_label(f"({cluster.cluster_id})"),
                self._escape_label(f"P:{cluster.procedure_count} G:{non_singleton_count} T:{len(cluster.tables)}"),
            ]
            safe_label = "\\n".join(label_lines)
            # Add tooltip attribute to ensure Graphviz generates <title> element in SVG
            # Use cluster:: prefix for consistent entity type detection
            lines.append(
                f'  "{cluster.cluster_id}" [shape=box,style="rounded,filled",fillcolor="#E1BEE7",'
                f'id="cluster::{cluster.cluster_id}",URL="cluster://{cluster.cluster_id}",tooltip="{cluster.cluster_id}",label="{safe_label}"];'
            )

        # Add missing tables that are not global (non-global missing tables)
        non_global_missing = self.missing_tables - self.global_tables
        if non_global_missing:
            lines.append("")
            lines.append("  // Missing tables (not global)")
            for table in sorted(non_global_missing):
                label = self._escape_label(table)
                missing_label = self._escape_label(f"{table}\n(missing)")
                lines.append(
                    f'  "{table}" [shape=box,style="filled,bold",fillcolor="#9E9E9E",penwidth=2,'
                    f'id="tableX::{table}",label="{missing_label}"];'
                )

        # Add orphaned tables if any
        if self.orphaned_tables:
            lines.append("")
            lines.append("  // Orphaned tables (unused)")
            for table in sorted(self.orphaned_tables):
                label = self._escape_label(table)
                orphaned_label = self._escape_label(f"{table}\n(orphaned)")
                lines.append(
                    f'  "{table}" [shape=box,style="filled,dashed",fillcolor="#FF9800",penwidth=1,'
                    f'id="tableO::{table}",label="{orphaned_label}"];'
                )

        lines.append("")
        lines.append("  // Cluster-to-table edges")
        for cluster in self.clusters.values():
            for table in cluster.tables:
                # Connect clusters to global tables
                if table in self.global_tables:
                    lines.append(f'  "{cluster.cluster_id}" -- "{table}";')
                # Also connect clusters to non-global missing tables
                elif table in non_global_missing:
                    lines.append(f'  "{cluster.cluster_id}" -- "{table}";')

        lines.append("}")
        return "\n".join(lines) + "\n"

    def generate_cluster_dot(self, cluster_identifier: str) -> str:
        cluster_id = self.find_cluster_id(cluster_identifier)
        cluster = self.clusters[cluster_id]

        lines: List[str] = [f"graph {cluster.cluster_id}_detail {{"]
        lines.append("  graph [layout=neato, overlap=false, splines=true];")
        lines.append('  node [fontname="Helvetica"];')
        lines.append("")

        lines.append("  // Table nodes")
        for table in cluster.tables:
            label = self._escape_label(table)
            # Check if table is missing
            if table in self.missing_tables:
                prefix = "tableX::"
                fillcolor = "#9E9E9E"  # Gray color for missing tables
                missing_label = self._escape_label(f"{table}\n(missing)")
                lines.append(
                    f'  "{table}" [shape=box,style="filled,bold",fillcolor="{fillcolor}",penwidth=2,'
                    f'id="{prefix}{table}",label="{missing_label}"];'
                )
            elif table in self.global_tables:
                global_label = self._escape_label(f"{table}\n(global)")
                lines.append(
                    f'  "{table}" [shape=box,style="filled,bold",fillcolor="#FFF2CC",penwidth=2,'
                    f'id="table::{table}",label="{global_label}"];'
                )
            else:
                lines.append(
                    f'  "{table}" [shape=box,style=filled,fillcolor="#E0ECF8",id="table::{table}",label="{label}"];'
                )

        lines.append("")
        lines.append("  // Procedure / group nodes")
        for group_id in cluster.group_ids:
            group = self.groups.get(group_id)
            if not group:
                continue
            display = group.display_name or group.group_id
            safe_display = self._escape_label(display)
            if group.is_singleton:
                lines.append(
                    f'  "{group.group_id}" [shape=box,style="rounded,filled",fillcolor="#E8F5E9",'
                    f'id="pg::{group.group_id}",label="{safe_display}"];'
                )
            else:
                procedures_label = "\n".join(group.procedures)
                label = f"{display}\n({group.group_id})\n---\n{procedures_label}"
                safe_label = self._escape_label(label)
                lines.append(
                    f'  "{group.group_id}" [shape=box,style="rounded,filled",fillcolor="#F9E2E7",'
                    f'id="pg::{group.group_id}",label="{safe_label}"];'
                )

        lines.append("")
        lines.append("  // Access edges")
        # Skip edges for Trash cluster - show disconnected nodes only
        if cluster_id != "trash":
            for group_id in cluster.group_ids:
                group = self.groups.get(group_id)
                if not group:
                    continue
                for table in group.tables:
                    lines.append(f'  "{group.group_id}" -- "{table}";')

        lines.append("}")
        return "\n".join(lines) + "\n"

    # ------------------------------------------------------------------ #
    # Convenience serializers
    # ------------------------------------------------------------------ #

    def summary_payload(self) -> Dict[str, Any]:
        return {
            "clusters": [self.clusters[cid].to_dict() for cid in self.cluster_order if cid in self.clusters],
            "global_tables": sorted(self.global_tables),
            "table_nodes": list(self.table_nodes),
            "parameters": self.parameters,
            "last_updated": self.last_updated.isoformat(),
        }

    def cluster_payload(self, cluster_identifier: str) -> Dict[str, Any]:
        cluster_id = self.find_cluster_id(cluster_identifier)
        cluster = self.clusters[cluster_id]
        groups = [self.groups[gid].to_dict() for gid in cluster.group_ids if gid in self.groups]
        return {
            "cluster": cluster.to_dict(),
            "groups": groups,
            "global_tables": sorted(self.global_tables),
            "last_updated": self.last_updated.isoformat(),
        }


# ---------------------------------------------------------------------------
# Command parsing
# ---------------------------------------------------------------------------


class CommandParser:
    """Parse natural-language style commands into structured operations."""

    _re_rename_cluster = re.compile(r"^rename\s+cluster\s+(\S+)\s+to\s+(.+)$", re.IGNORECASE)
    _re_rename_group = re.compile(r"^rename\s+(?:group|procedure\s+group)\s+(\S+)\s+to\s+(.+)$", re.IGNORECASE)
    _re_move_group = re.compile(
        r"^move\s+(?:group|procedure\s+group)\s+(\S+)\s+to\s+cluster\s+(\S+)$", re.IGNORECASE
    )
    _re_move_procedure = re.compile(
        r"^move\s+procedure\s+(\S+)\s+to\s+cluster\s+(\S+)$", re.IGNORECASE
    )
    _re_delete_procedure = re.compile(r"^delete\s+procedure\s+(.+)$", re.IGNORECASE)
    _re_delete_table = re.compile(r"^delete\s+table\s+(.+)$", re.IGNORECASE)
    _re_add_cluster = re.compile(r"^add\s+cluster\s+(\S+)(?:\s+(.+))?$", re.IGNORECASE)
    _re_delete_cluster = re.compile(r"^delete\s+cluster\s+(\S+)$", re.IGNORECASE)

    def parse(self, text: str) -> Dict[str, Any]:
        text = text.strip()
        if not text:
            raise ValueError("Empty command")

        if match := self._re_rename_cluster.match(text):
            cluster_id, new_name = match.groups()
            return {"type": "rename_cluster", "cluster_id": cluster_id, "new_name": new_name.strip('" ').strip()}

        if match := self._re_rename_group.match(text):
            group_id, new_name = match.groups()
            return {"type": "rename_group", "group_id": group_id, "new_name": new_name.strip('" ').strip()}

        if match := self._re_move_group.match(text):
            group_id, cluster_id = match.groups()
            return {"type": "move_group", "group_id": group_id, "cluster_id": cluster_id}

        if match := self._re_move_procedure.match(text):
            procedure_name, cluster_id = match.groups()
            return {"type": "move_procedure", "procedure": procedure_name, "cluster_id": cluster_id}

        if match := self._re_delete_procedure.match(text):
            procedure_name = match.group(1).strip('`').strip()
            return {"type": "delete_procedure", "procedure_name": procedure_name}

        if match := self._re_delete_table.match(text):
            table_name = match.group(1).strip('`').strip()
            return {"type": "delete_table", "table_name": table_name}

        if match := self._re_add_cluster.match(text):
            cluster_id, display_name = match.groups()
            result = {"type": "add_cluster", "cluster_id": cluster_id}
            if display_name:
                result["display_name"] = display_name.strip('" ').strip()
            return result

        if match := self._re_delete_cluster.match(text):
            cluster_id = match.group(1)
            return {"type": "delete_cluster", "cluster_id": cluster_id}

        raise ValueError(f"Unrecognized command: '{text}'")


# ---------------------------------------------------------------------------
# Cluster service (state + lock)
# ---------------------------------------------------------------------------


class ClusterService:
    def __init__(self, snapshot_path: Path) -> None:
        if not snapshot_path.exists():
            raise FileNotFoundError(f"clusters.json not found at {snapshot_path}")
        self.snapshot_path = snapshot_path
        self._lock = threading.RLock()
        self._parser = CommandParser()
        self._state = ClusterState.from_json(self._load_snapshot())

    def _load_snapshot(self) -> Dict[str, Any]:
        with self.snapshot_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def _save_snapshot(self) -> None:
        """Save current state to clusters.json."""
        snapshot = self._state.snapshot()
        with self.snapshot_path.open("w", encoding="utf-8") as handle:
            json.dump(snapshot, handle, indent=2, ensure_ascii=False)

    @property
    def state(self) -> ClusterState:
        return self._state

    # ---------------------------- Read endpoints ---------------------------- #

    def summary(self) -> Dict[str, Any]:
        with self._lock:
            return self._state.summary_payload()

    def cluster_detail(self, cluster_identifier: str) -> Dict[str, Any]:
        with self._lock:
            try:
                return self._state.cluster_payload(cluster_identifier)
            except (KeyError, ValueError) as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc

    def summary_dot(self) -> str:
        with self._lock:
            return self._state.generate_summary_dot()

    def cluster_dot(self, cluster_identifier: str) -> str:
        with self._lock:
            try:
                return self._state.generate_cluster_dot(cluster_identifier)
            except (KeyError, ValueError) as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return self._state.snapshot()

    # --------------------------- Mutation endpoints ------------------------ #

    def _dot_to_svg(self, dot_source: str) -> str:
        try:
            proc = subprocess.run(
                ["dot", "-Tsvg"],
                input=dot_source,
                text=True,
                capture_output=True,
                check=True,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=500, detail="Graphviz 'dot' command not found.") from exc
        except subprocess.CalledProcessError as exc:
            raise HTTPException(status_code=500, detail=exc.stderr or "Graphviz rendering failed.") from exc
        return proc.stdout

    def summary_svg(self) -> str:
        with self._lock:
            dot = self._state.generate_summary_dot()
            return self._dot_to_svg(dot)

    def cluster_svg(self, cluster_identifier: str) -> str:
        with self._lock:
            try:
                dot = self._state.generate_cluster_dot(cluster_identifier)
            except (KeyError, ValueError) as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            return self._dot_to_svg(dot)

    def reload(self) -> Dict[str, Any]:
        with self._lock:
            payload = self._load_snapshot()
            self._state = ClusterState.from_json(payload)
            return self._state.summary_payload()

    def rebuild_from_catalog(self) -> Dict[str, Any]:
        """Rebuild clusters.json from catalog.json (DESTRUCTIVE - creates fresh clusters).

        This operation:
        1. Reads catalog.json path from current state
        2. Runs clustering algorithm to generate new clusters.json
        3. Reloads the newly generated state
        4. Discards ALL current clusters, groups, trash, and custom names

        Returns:
            Dict with status and clustering statistics
        """
        with self._lock:
            from cluster.clustering import rebuild_clusters_from_catalog

            # Get catalog path from current state
            catalog_path_str = self._state.catalog_path
            if not catalog_path_str:
                raise ValueError("Catalog path not set in current state - cannot rebuild")

            catalog_path = Path(catalog_path_str)
            if not catalog_path.exists():
                raise FileNotFoundError(f"Catalog file not found: {catalog_path}")

            # Use existing clustering parameters
            parameters = dict(self._state.parameters) if self._state.parameters else {}

            # Rebuild clusters (this overwrites clusters.json)
            stats = rebuild_clusters_from_catalog(
                catalog_path=catalog_path,
                output_path=self.snapshot_path,
                parameters=parameters,
            )

            # Reload the newly generated snapshot
            payload = self._load_snapshot()
            self._state = ClusterState.from_json(payload)

            return {
                "status": "ok",
                "message": "Clusters rebuilt from catalog",
                "statistics": stats,
                "summary": self._state.summary_payload(),
            }

    def execute(self, command: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            cmd_type = command.get("type")
            if cmd_type == "rename_cluster":
                cluster_id = command["cluster_id"]
                new_name = command["new_name"]
                self._state.rename_cluster(cluster_id, new_name)
                message = f"Cluster '{cluster_id}' renamed to '{new_name}'."
            elif cmd_type == "rename_group":
                group_id = command["group_id"]
                new_name = command["new_name"]
                self._state.rename_group(group_id, new_name)
                message = f"Group '{group_id}' renamed to '{new_name}'."
            elif cmd_type == "move_group":
                group_id = command["group_id"]
                cluster_id = command["cluster_id"]
                self._state.move_group(group_id, cluster_id)
                message = f"Group '{group_id}' moved to cluster '{cluster_id}'."
            elif cmd_type == "move_procedure":
                procedure = command["procedure"]
                cluster_id = command["cluster_id"]
                new_group_id, _ = self._state.move_procedure(procedure, cluster_id)
                message = f"Procedure '{procedure}' moved to cluster '{cluster_id}' (group '{new_group_id}')."
            elif cmd_type == "delete_procedure":
                procedure_name = command["procedure_name"]
                result = self._state.delete_procedure(procedure_name)
                message = f"Procedure '{procedure_name}' deleted and moved to trash."
            elif cmd_type == "delete_table":
                table_name = command["table_name"]
                result = self._state.delete_table(table_name)
                message = f"Table '{table_name}' deleted from catalog."
            elif cmd_type == "add_cluster":
                cluster_id = command["cluster_id"]
                display_name = command.get("display_name")
                result = self._state.add_cluster(cluster_id, display_name)
                message = f"Cluster '{cluster_id}' created."
            elif cmd_type == "delete_cluster":
                cluster_id = command["cluster_id"]
                result = self._state.delete_cluster_if_empty(cluster_id)
                message = f"Cluster '{cluster_id}' deleted."
            else:
                raise HTTPException(status_code=400, detail=f"Unsupported command type '{cmd_type}'.")

            # Save changes to disk
            self._save_snapshot()

            return {
                "status": "ok",
                "message": message,
                "summary": self._state.summary_payload(),
            }

    def execute_text(self, text: str) -> Dict[str, Any]:
        try:
            command = self._parser.parse(text)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return self.execute(command)

    # --------------------------- Trash operations -------------------------- #

    def delete_procedure(self, procedure_name: str) -> Dict[str, Any]:
        """Delete a procedure and move to trash."""
        with self._lock:
            try:
                result = self._state.delete_procedure(procedure_name)
                self._save_snapshot()
                return {
                    "status": "ok",
                    "message": f"Procedure '{procedure_name}' deleted and moved to trash.",
                    "result": result,
                }
            except (KeyError, ValueError) as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

    def delete_table(self, table_name: str) -> Dict[str, Any]:
        """Delete a table from catalog."""
        with self._lock:
            try:
                result = self._state.delete_table(table_name)
                self._save_snapshot()
                return {
                    "status": "ok",
                    "message": f"Table '{table_name}' deleted.",
                    "result": result,
                }
            except (KeyError, ValueError) as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

    def add_cluster(self, cluster_id: str, display_name: Optional[str] = None) -> Dict[str, Any]:
        """Create a new cluster."""
        with self._lock:
            try:
                result = self._state.add_cluster(cluster_id, display_name)
                self._save_snapshot()
                return {
                    "status": "ok",
                    "message": f"Cluster '{cluster_id}' created.",
                    "result": result,
                }
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

    def delete_cluster(self, cluster_identifier: str) -> Dict[str, Any]:
        """Delete an empty cluster."""
        with self._lock:
            try:
                result = self._state.delete_cluster_if_empty(cluster_identifier)
                self._save_snapshot()
                return {
                    "status": "ok",
                    "message": f"Cluster '{cluster_identifier}' deleted.",
                    "result": result,
                }
            except (KeyError, ValueError) as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

    def list_trash(self) -> Dict[str, Any]:
        """List all items in trash."""
        with self._lock:
            return self._state.list_trash()

    def restore_procedure(
        self, procedure_name: str, target_cluster_id: str, force_new_group: bool = False
    ) -> Dict[str, Any]:
        """Restore a procedure from trash."""
        with self._lock:
            try:
                result = self._state.restore_procedure(procedure_name, target_cluster_id, force_new_group)
                self._save_snapshot()
                return {
                    "status": "ok",
                    "message": f"Procedure '{procedure_name}' restored to cluster '{target_cluster_id}'.",
                    "result": result,
                }
            except (KeyError, ValueError) as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

    def restore_table(self, trash_index: int) -> Dict[str, Any]:
        """Restore a table from trash."""
        with self._lock:
            try:
                result = self._state.restore_table(trash_index)
                self._save_snapshot()
                return {
                    "status": "ok",
                    "message": f"Table '{result['restored_table']}' restored.",
                    "result": result,
                }
            except (KeyError, ValueError) as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

    def empty_trash(self) -> Dict[str, Any]:
        """Permanently delete all trash items."""
        with self._lock:
            result = self._state.empty_trash()
            self._save_snapshot()
            return {
                "status": "ok",
                "message": f"Trash emptied: {result['total']} items permanently deleted.",
                "result": result,
            }


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

# Serve from VectorizeCatalog/static/cluster (standalone mode)

def create_app(snapshot_path: Optional[Path] = None) -> FastAPI:
    base_dir = Path(__file__).resolve().parent.parent
    root_above = base_dir.parent
    default_snapshot = root_above / "output" / "cluster" / "clusters.json"
    service = ClusterService(snapshot_path or default_snapshot)

    app = FastAPI(title="Cluster Editing Prototype")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    print("[create_app] Initializing Cluster Service API")

    static_dir = base_dir / "static" / "cluster"
    if static_dir.exists():
        print(f"[create_app] Serving Cluster UI from {static_dir}")
        app.mount("/cluster-ui", StaticFiles(directory=str(static_dir)), name="cluster-ui")

        @app.get("/", include_in_schema=False)
        def index() -> FileResponse:
            return FileResponse(str(static_dir / "index.html"))

        @app.get("/index.html", include_in_schema=False)
        def index_html() -> FileResponse:
            return FileResponse(str(static_dir / "index.html"))

        @app.get("/favicon.ico", include_in_schema=False)
        def favicon() -> FileResponse:
            return FileResponse(str(static_dir / "favicon.ico"))
    
    @app.get("/api/cluster/summary")
    def get_summary() -> Dict[str, Any]:
        return service.summary()

    @app.get("/api/cluster/{cluster_id}")
    def get_cluster(cluster_id: str) -> Dict[str, Any]:
        return service.cluster_detail(cluster_id)

    @app.get("/api/cluster/dot/summary", response_class=PlainTextResponse)
    def get_summary_dot() -> str:
        return service.summary_dot()

    @app.get("/api/cluster/dot/{cluster_id}", response_class=PlainTextResponse)
    def get_cluster_dot(cluster_id: str) -> str:
        return service.cluster_dot(cluster_id)

    @app.get("/api/cluster/svg/summary")
    def get_summary_svg() -> Response:
        svg = service.summary_svg()
        return Response(content=svg, media_type="image/svg+xml")

    @app.get("/api/cluster/svg/{cluster_id}")
    def get_cluster_svg(cluster_id: str) -> Response:
        svg = service.cluster_svg(cluster_id)
        return Response(content=svg, media_type="image/svg+xml")

    @app.get("/api/cluster/snapshot")
    def get_snapshot() -> Dict[str, Any]:
        return service.snapshot()

    @app.post("/api/cluster/reload")
    def post_reload() -> Dict[str, Any]:
        return {
            "status": "ok",
            "summary": service.reload(),
        }

    @app.post("/api/cluster/command")
    def post_command(payload: Dict[str, Any]) -> Dict[str, Any]:
        if "command" in payload:
            return service.execute_text(payload["command"])
        return service.execute(payload)

    # Trash operation endpoints
    @app.post("/api/cluster/delete/procedure")
    def post_delete_procedure(payload: Dict[str, str]) -> Dict[str, Any]:
        procedure_name = payload.get("procedure_name")
        if not procedure_name:
            raise HTTPException(status_code=400, detail="Missing 'procedure_name' in payload")
        return service.delete_procedure(procedure_name)

    @app.post("/api/cluster/delete/table")
    def post_delete_table(payload: Dict[str, str]) -> Dict[str, Any]:
        table_name = payload.get("table_name")
        if not table_name:
            raise HTTPException(status_code=400, detail="Missing 'table_name' in payload")
        return service.delete_table(table_name)

    @app.post("/api/cluster/add")
    def post_add_cluster(payload: Dict[str, str]) -> Dict[str, Any]:
        cluster_id = payload.get("cluster_id")
        if not cluster_id:
            raise HTTPException(status_code=400, detail="Missing 'cluster_id' in payload")
        display_name = payload.get("display_name")
        return service.add_cluster(cluster_id, display_name)

    @app.post("/api/cluster/delete/cluster")
    def post_delete_cluster(payload: Dict[str, str]) -> Dict[str, Any]:
        cluster_id = payload.get("cluster_id")
        if not cluster_id:
            raise HTTPException(status_code=400, detail="Missing 'cluster_id' in payload")
        return service.delete_cluster(cluster_id)

    @app.get("/api/cluster/trash")
    def get_trash() -> Dict[str, Any]:
        return service.list_trash()

    @app.post("/api/cluster/trash/restore")
    def post_restore(payload: Dict[str, Any]) -> Dict[str, Any]:
        item_type = payload.get("item_type")
        if item_type == "procedure":
            procedure_name = payload.get("procedure_name")
            target_cluster_id = payload.get("target_cluster_id")
            force_new_group = payload.get("force_new_group", False)
            if not procedure_name or not target_cluster_id:
                raise HTTPException(
                    status_code=400,
                    detail="Missing 'procedure_name' or 'target_cluster_id' in payload"
                )
            return service.restore_procedure(procedure_name, target_cluster_id, force_new_group)
        elif item_type == "table":
            trash_index = payload.get("trash_index")
            if trash_index is None:
                raise HTTPException(status_code=400, detail="Missing 'trash_index' in payload")
            return service.restore_table(trash_index)
        else:
            raise HTTPException(status_code=400, detail=f"Unknown item_type: '{item_type}'")

    @app.post("/api/cluster/trash/empty")
    def post_empty_trash() -> Dict[str, Any]:
        return service.empty_trash()

    # ---------------------------------------------------------------------------
    # Semantic Agent Endpoint
    # ---------------------------------------------------------------------------

    from pydantic import BaseModel as PydanticBaseModel

    class AgentQuery(PydanticBaseModel):
        query: str
        intent_override: Optional[Dict[str, Any]] = None
        accept_proposal: bool = False

    @app.post("/api/cluster/ask")
    def cluster_ask(body: AgentQuery) -> Dict[str, Any]:
        """Semantic agent endpoint - uses LLM to classify intent"""
        from cluster.agent import agent_answer

        return agent_answer(
            query=body.query,
            cluster_service=service,
            intent_override=body.intent_override,
            accept_proposal=body.accept_proposal,
        )

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8010)

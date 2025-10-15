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

        self.rebuild_indexes()

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

        return cls(
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
                    fillcolor = "#FFCDD2"  # Light red color for missing tables
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
            # Use icons instead of text labels:
            # ⚙ for procedures, ◉ for groups, ▭ for tables
            # Escape each part individually, then join with literal \n for DOT format
            label_lines = [
                self._escape_label(display),
                self._escape_label(f"({cluster.cluster_id})"),
                self._escape_label(f"⚙ {cluster.procedure_count}  ◉ {non_singleton_count}  ▭ {len(cluster.tables)}"),
            ]
            safe_label = "\\n".join(label_lines)
            # Add tooltip attribute to ensure Graphviz generates <title> element in SVG
            # Use cluster:: prefix for consistent entity type detection
            lines.append(
                f'  "{cluster.cluster_id}" [shape=box,style="rounded,filled",fillcolor="#E1BEE7",'
                f'id="cluster::{cluster.cluster_id}",URL="cluster://{cluster.cluster_id}",tooltip="{cluster.cluster_id}",label="{safe_label}"];'
            )

        # Add orphaned tables if any
        if self.orphaned_tables:
            lines.append("")
            lines.append("  // Orphaned tables (unused)")
            for table in sorted(self.orphaned_tables):
                label = self._escape_label(table)
                orphaned_label = self._escape_label(f"{table}\n(orphaned)")
                lines.append(
                    f'  "{table}" [shape=box,style="filled,dashed",fillcolor="#E0E0E0",penwidth=1,'
                    f'id="tableO::{table}",label="{orphaned_label}"];'
                )

        lines.append("")
        lines.append("  // Cluster-to-global-table edges")
        for cluster in self.clusters.values():
            for table in cluster.tables:
                if table in self.global_tables:
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
                fillcolor = "#FFCDD2"  # Light red color for missing tables
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


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------


def create_app(snapshot_path: Optional[Path] = None) -> FastAPI:
    base_dir = Path(__file__).resolve().parent.parent
    default_snapshot = base_dir / "output" / "cluster" / "clusters.json"
    service = ClusterService(snapshot_path or default_snapshot)

    app = FastAPI(title="Cluster Editing Prototype")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    static_dir = Path(__file__).resolve().parent / "static" / "cluster"
    if static_dir.exists():
        app.mount("/cluster-ui", StaticFiles(directory=str(static_dir)), name="cluster-ui")

        @app.get("/", include_in_schema=False)
        def index() -> FileResponse:
            return FileResponse(str(static_dir / "index.html"))

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

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8010)

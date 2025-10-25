"""Clustering algorithm for grouping procedures based on shared table access patterns.

This module provides the core clustering logic for analyzing SQL catalog data
and generating procedure groups and clusters.

Refactored from ../output/cluster/cluster_catalog.py to be module-callable.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple


def is_likely_alias(safe_name: str) -> bool:
    """Check if a table name is likely an alias rather than a real table.

    Heuristics:
    - Single character names (a, b, c, etc.)
    - Names with no schema prefix that are very short (1-2 chars)
    """
    if not safe_name:
        return True

    # Check if it's just a single character (very likely an alias)
    if len(safe_name) == 1:
        return True

    # Check if it has a schema prefix
    if '.' in safe_name:
        # Has schema, check the table part
        parts = safe_name.split('.')
        table_part = parts[-1]
        # Single char table name even with schema is suspicious
        if len(table_part) <= 1:
            return True
    else:
        # No schema and very short name (2 chars or less) is suspicious
        if len(safe_name) <= 2:
            return True

    return False


def is_system_table(safe_name: str, exclude_system_tables: bool = True) -> bool:
    """Check if a table is a SQL Server system table/view.

    System tables/views include:
    - sys.* (modern catalog views like sys.objects, sys.tables)
    - sysobjects, syscolumns, etc. (legacy system tables)
    - INFORMATION_SCHEMA.* (ANSI standard views)
    - MSreplication_* (replication tables)
    - dtproperties (legacy extended properties)

    Args:
        safe_name: Table name (may be schema-qualified like "sys.objects" or "dbo·sysobjects")
        exclude_system_tables: If False, always returns False (no filtering)

    Returns:
        True if table should be excluded from clustering, False otherwise
    """
    if not exclude_system_tables:
        return False

    if not safe_name:
        return False

    # Normalize: handle both · and . as separators
    normalized = safe_name.lower().replace('·', '.')

    # Pattern 1: sys.* (modern catalog views)
    if normalized.startswith('sys.'):
        return True

    # Pattern 2: INFORMATION_SCHEMA.* (ANSI standard views)
    if normalized.startswith('information_schema.'):
        return True

    # Pattern 3: Legacy system tables (any schema)
    legacy_system_tables = {
        'sysobjects', 'syscolumns', 'sysindexes', 'systypes', 'sysdepends',
        'sysreferences', 'sysusers', 'syspermissions', 'sysconstraints',
        'sysfiles', 'sysfilegroups', 'sysforeignkeys', 'sysfulltextcatalogs',
        'sysindexkeys', 'sysmembers', 'sysprotects', 'sysallocunits',
        'syscacheobjects', 'syscharsets', 'sysconfigures', 'syscurconfigs',
        'sysdatabases', 'syslanguages', 'syslockinfo', 'syslogins',
        'sysmessages', 'sysoledbusers', 'sysperfinfo', 'sysprocesses',
        'sysremotelogins', 'sysservers',
    }

    # Extract table name (after schema separator if present)
    if '.' in normalized:
        table_name = normalized.split('.')[-1]
    else:
        table_name = normalized

    if table_name in legacy_system_tables:
        return True

    # Pattern 4: Replication tables
    if table_name.startswith('msreplication_'):
        return True

    # Pattern 5: Legacy extended properties table
    if table_name == 'dtproperties':
        return True

    # Pattern 6: Trace tables
    if table_name.startswith('trace_xe_'):
        return True

    return False


def gather_procedure_groups(
    catalog: Dict,
    exclude_system_tables: bool = True,
    exclude_patterns: Optional[List[str]] = None,
) -> Tuple[List[Dict], Counter, Dict[str, str], Set[str], Set[str]]:
    """
    Group procedures by table access patterns.

    Args:
        catalog: Catalog dictionary with Procedures, Tables, Views
        exclude_system_tables: If True, exclude SQL Server system tables (default: True)
        exclude_patterns: Optional list of additional patterns to exclude (e.g., ["temp_", "archive_"])

    Returns:
        - groups: List of procedure groups
        - table_usage: Counter of table usage (using normalized names)
        - table_display_names: Mapping from normalized name to original display name
        - missing_tables: Set of table names that don't exist in catalog
        - orphaned_tables: Set of existing table names that are never accessed by procedures
    """
    raw_groups: Dict[Tuple[str, ...], Dict[str, List[str]]] = {}
    procedures = catalog.get("Procedures", {})
    table_display_names: Dict[str, str] = {}  # normalized -> original for display
    exclude_patterns = exclude_patterns or []

    # Build set of existing tables (normalized, case-insensitive)
    existing_tables: Set[str] = set()
    existing_tables_original: Dict[str, str] = {}  # normalized -> original name
    for table_name in catalog.get("Tables", {}).keys():
        normalized = table_name.lower()
        existing_tables.add(normalized)
        existing_tables_original[normalized] = table_name
    for view_name in catalog.get("Views", {}).keys():
        normalized = view_name.lower()
        existing_tables.add(normalized)
        existing_tables_original[normalized] = view_name

    missing_tables: Set[str] = set()
    excluded_tables_count = 0  # Track how many system tables were filtered

    for proc_name, proc_body in procedures.items():
        table_refs: Set[str] = set()
        for access_key in ("Reads", "Writes"):
            for item in proc_body.get(access_key, []) or []:
                safe_name = item.get("Safe_Name")
                if not safe_name:
                    continue

                # Skip aliases
                if is_likely_alias(safe_name):
                    continue

                # Skip system tables (if enabled)
                if is_system_table(safe_name, exclude_system_tables):
                    excluded_tables_count += 1
                    continue

                # Skip custom exclude patterns
                normalized_lower = safe_name.lower().replace('·', '.')
                skip = False
                for pattern in exclude_patterns:
                    if pattern.lower() in normalized_lower:
                        skip = True
                        break
                if skip:
                    excluded_tables_count += 1
                    continue

                # Normalize to lowercase for grouping (SQL Server is case-insensitive)
                normalized = safe_name.lower()
                table_refs.add(normalized)
                # Keep first occurrence of original name for display
                if normalized not in table_display_names:
                    table_display_names[normalized] = safe_name
                # Check if table exists in catalog
                if normalized not in existing_tables:
                    missing_tables.add(normalized)

        if not table_refs:
            continue
        key = tuple(sorted(table_refs))
        entry = raw_groups.setdefault(key, {"tables": key, "procedures": []})
        entry["procedures"].append(proc_name)

    # Log filtering statistics
    if excluded_tables_count > 0:
        print(f"[gather_procedure_groups] Excluded {excluded_tables_count} system/pattern table references")

    groups: List[Dict] = []
    table_usage = Counter()
    pg_counter = 0

    for table_key, entry in sorted(raw_groups.items()):
        procedures_for_group = sorted(entry["procedures"])
        tables = list(table_key)
        is_singleton = len(procedures_for_group) == 1
        if is_singleton:
            group_id = procedures_for_group[0]
        else:
            group_id = f"PG{pg_counter}"
            pg_counter += 1

        groups.append(
            {
                "group_id": group_id,
                "procedures": procedures_for_group,
                "tables": tables,
                "core_tables": [],
                "is_singleton": is_singleton,
            }
        )
        for table in table_key:
            table_usage[table] += 1

    # Identify orphaned tables: existing tables that are never referenced by any procedure
    accessed_tables = set(table_usage.keys())
    orphaned_tables: Set[str] = existing_tables - accessed_tables

    # Add orphaned tables to table_display_names for consistent display
    for orphaned in orphaned_tables:
        if orphaned not in table_display_names:
            table_display_names[orphaned] = existing_tables_original.get(orphaned, orphaned)

    return groups, table_usage, table_display_names, missing_tables, orphaned_tables


def identify_global_tables(
    clusters: Sequence[Dict],
    min_clusters: int = 2,
) -> Set[str]:
    """Identify global tables: tables accessed by multiple clusters.

    Args:
        clusters: List of cluster summaries
        min_clusters: Minimum number of clusters to qualify as global (default: 2)

    Returns:
        Set of table names that are accessed by >= min_clusters clusters
    """
    if len(clusters) < min_clusters:
        return set()

    table_to_clusters: Dict[str, Set[str]] = defaultdict(set)

    for cluster in clusters:
        cluster_id = cluster["cluster_id"]
        for table in cluster["tables"]:
            table_to_clusters[table].add(cluster_id)

    global_tables = {
        table
        for table, cluster_ids in table_to_clusters.items()
        if len(cluster_ids) >= min_clusters
    }

    return global_tables


def build_similarity_edges(
    groups: Sequence[Dict],
    min_group_size: int,
    threshold: float,
) -> List[Tuple[int, int, float]]:
    """Build similarity edges between procedure groups based on shared tables.

    Args:
        groups: List of procedure groups
        min_group_size: Minimum number of tables in a group to consider
        threshold: Minimum Jaccard similarity to create an edge

    Returns:
        List of (group_idx1, group_idx2, similarity) tuples
    """
    index_by_table: Dict[str, List[int]] = defaultdict(list)
    group_sizes: Dict[int, int] = {}

    for idx, group in enumerate(groups):
        table_set = set(group["tables"])
        group_sizes[idx] = len(table_set)
        for table in table_set:
            index_by_table[table].append(idx)

    relevant = {idx for idx, size in group_sizes.items() if size >= max(min_group_size, 0)}
    pair_intersections: Dict[Tuple[int, int], int] = defaultdict(int)

    for table, group_indices in index_by_table.items():
        candidates = sorted(idx for idx in group_indices if idx in relevant)
        if len(candidates) < 2:
            continue
        for left, right in combinations(candidates, 2):
            pair_intersections[(left, right)] += 1

    edges: List[Tuple[int, int, float]] = []
    for (left, right), intersection in pair_intersections.items():
        left_size = group_sizes.get(left, 0)
        right_size = group_sizes.get(right, 0)
        union = left_size + right_size - intersection
        if union <= 0:
            continue
        similarity = intersection / union
        if similarity >= threshold:
            edges.append((left, right, similarity))

    return edges


def build_clusters_two_phase(
    groups: Sequence[Dict],
    edges: Sequence[Tuple[int, int, float]],
    min_assignment_similarity: float = 0.0,
) -> List[List[int]]:
    """Build clusters using 2-phase greedy assignment algorithm.

    Phase 1: Isolated groups (zero similarity with all others) get singleton clusters
    Phase 2: Remaining groups assigned to cluster with highest similarity

    This approach avoids transitive closure problems of union-find while
    maintaining better cluster balance.

    Args:
        groups: List of procedure groups with tables
        edges: List of (group_idx1, group_idx2, similarity) edges
        min_assignment_similarity: Minimum similarity to assign to cluster (default: 0.0)

    Returns:
        List of clusters, each containing group indices
    """
    group_count = len(groups)

    # Build adjacency map from edges
    adjacency: Dict[int, Set[int]] = defaultdict(set)
    similarity_map: Dict[Tuple[int, int], float] = {}

    for left, right, sim in edges:
        adjacency[left].add(right)
        adjacency[right].add(left)
        similarity_map[(min(left, right), max(left, right))] = sim

    # Phase 1: Find isolated groups (zero similarity with all others)
    isolated_groups: List[int] = []
    connected_groups: List[int] = []

    for idx in range(group_count):
        if idx not in adjacency or len(adjacency[idx]) == 0:
            isolated_groups.append(idx)
        else:
            connected_groups.append(idx)

    # Create singleton clusters for isolated groups
    clusters: List[List[int]] = [[idx] for idx in isolated_groups]

    # Phase 2: Assign connected groups to best-fit clusters
    # Sort by table count (descending) - process complex groups first
    connected_groups.sort(key=lambda idx: len(groups[idx]["tables"]), reverse=True)

    for group_idx in connected_groups:
        group_tables = set(groups[group_idx]["tables"])

        if not clusters:
            # No clusters yet, create first one
            clusters.append([group_idx])
            continue

        # Calculate similarity with each existing cluster
        best_cluster_idx = -1
        best_similarity = -1.0

        for cluster_idx, cluster_members in enumerate(clusters):
            # Compute cluster-level similarity (Jaccard with union of all cluster tables)
            cluster_tables: Set[str] = set()
            for member_idx in cluster_members:
                cluster_tables.update(groups[member_idx]["tables"])

            # Jaccard similarity: intersection / union
            intersection = len(group_tables & cluster_tables)
            union = len(group_tables | cluster_tables)

            if union > 0:
                similarity = intersection / union
            else:
                similarity = 0.0

            # Track best cluster (with tie-breaking by smallest cluster for balance)
            if similarity > best_similarity:
                best_similarity = similarity
                best_cluster_idx = cluster_idx
            elif similarity == best_similarity and best_cluster_idx >= 0:
                # Tie-break: prefer smaller cluster for better balance
                if len(clusters[cluster_idx]) < len(clusters[best_cluster_idx]):
                    best_cluster_idx = cluster_idx

        # Assign to best cluster or create new one
        # Note: best_similarity will be > 0 for connected groups (they have edges)
        # but we still respect min_assignment_similarity threshold
        if best_similarity > 0 and best_similarity >= min_assignment_similarity:
            clusters[best_cluster_idx].append(group_idx)
        else:
            # Similarity too low (or zero), create new cluster
            clusters.append([group_idx])

    # Sort clusters by size (descending) then by smallest member (for determinism)
    sorted_clusters = sorted(
        clusters, key=lambda members: (-len(members), min(members))
    )

    return [sorted(members) for members in sorted_clusters]


def build_clusters(group_count: int, edges: Sequence[Tuple[int, int, float]]) -> List[List[int]]:
    """Build clusters using union-find algorithm based on similarity edges.

    DEPRECATED: This function uses union-find which creates transitive closures
    and can result in unbalanced clusters. Use build_clusters_two_phase() instead.

    Args:
        group_count: Total number of procedure groups
        edges: List of (group_idx1, group_idx2, similarity) edges

    Returns:
        List of clusters, each containing group indices
    """
    parent = list(range(group_count))

    def find(node: int) -> int:
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    def union(left: int, right: int) -> None:
        root_left = find(left)
        root_right = find(right)
        if root_left == root_right:
            return
        parent[root_right] = root_left

    for left, right, _ in edges:
        union(left, right)

    clusters: Dict[int, List[int]] = defaultdict(list)
    for idx in range(group_count):
        clusters[find(idx)].append(idx)

    sorted_clusters = sorted(
        clusters.values(), key=lambda members: (-len(members), min(members))
    )
    return [sorted(members) for members in sorted_clusters]


def summarize_clusters(
    groups: Sequence[Dict],
    clusters: Sequence[Sequence[int]],
) -> List[Dict]:
    """Summarize each cluster with its procedures and tables.

    Args:
        groups: List of procedure groups
        clusters: List of clusters (each cluster is a list of group indices)

    Returns:
        List of cluster summaries
    """
    summaries: List[Dict] = []
    for cluster_index, members in enumerate(clusters):
        cluster_id = f"C{cluster_index}"
        group_ids = [groups[idx]["group_id"] for idx in members]
        procedures = sorted(
            {
                procedure
                for idx in members
                for procedure in groups[idx]["procedures"]
            }
        )
        tables = sorted(
            {
                table
                for idx in members
                for table in groups[idx]["tables"]
            }
        )
        summaries.append(
            {
                "cluster_id": cluster_id,
                "group_ids": group_ids,
                "procedure_count": len(procedures),
                "procedures": procedures,
                "tables": tables,
            }
        )
        for idx in members:
            groups[idx]["cluster_id"] = cluster_id
    return summaries


def rebuild_clusters_from_catalog(
    catalog_path: Path,
    output_path: Path,
    parameters: Optional[Dict[str, any]] = None,
) -> Dict[str, any]:
    """
    Rebuild clusters.json from catalog.json using 2-phase clustering algorithm.

    Args:
        catalog_path: Path to catalog.json
        output_path: Path to save clusters.json
        parameters: Optional clustering parameters:
            - similarity_threshold: float (default: 0.5) - for edge creation
            - min_group_size: int (default: 1) - minimum tables in group for similarity
            - min_global_clusters: int (default: 2) - min clusters to mark table as global
            - min_assignment_similarity: float (default: 0.0) - min similarity to assign to cluster
            - use_two_phase: bool (default: True) - use new 2-phase algorithm
            - exclude_system_tables: bool (default: True) - exclude SQL Server system tables
            - exclude_patterns: List[str] (default: []) - additional table name patterns to exclude

    Returns:
        Dict with clustering results and statistics
    """
    # Load parameters with defaults
    params = parameters or {}
    similarity_threshold = params.get("similarity_threshold", 0.5)
    min_group_size = params.get("min_group_size", 1)
    min_global_clusters = params.get("min_global_clusters", 2)
    min_assignment_similarity = params.get("min_assignment_similarity", 0.0)
    use_two_phase = params.get("use_two_phase", True)
    exclude_system_tables = params.get("exclude_system_tables", True)
    exclude_patterns = params.get("exclude_patterns", [])

    # Load catalog
    if not catalog_path.exists():
        raise FileNotFoundError(f"Catalog file not found: {catalog_path}")

    with catalog_path.open("r", encoding="utf-8") as handle:
        catalog = json.load(handle)

    # Step 1: Group procedures by table access patterns (with filtering)
    groups, table_usage, table_display_names, missing_tables, orphaned_tables = gather_procedure_groups(
        catalog,
        exclude_system_tables=exclude_system_tables,
        exclude_patterns=exclude_patterns,
    )

    if not groups:
        raise ValueError("No procedures with table access were found in the catalog snapshot.")

    # Step 2: Build similarity edges between procedure groups
    edges = build_similarity_edges(
        groups,
        min_group_size=min_group_size,
        threshold=similarity_threshold,
    )

    # Step 3: Cluster procedure groups based on similarity
    if use_two_phase:
        # Use new 2-phase algorithm (avoids transitive closure)
        clusters = build_clusters_two_phase(
            groups=groups,
            edges=edges,
            min_assignment_similarity=min_assignment_similarity,
        )
    else:
        # Use legacy union-find algorithm (can create unbalanced clusters)
        clusters = build_clusters(len(groups), edges)

    cluster_summaries = summarize_clusters(groups, clusters)

    # Step 4: Identify global tables (accessed by multiple clusters)
    global_tables = identify_global_tables(
        cluster_summaries,
        min_clusters=min_global_clusters,
    )

    # Build edge payload for JSON
    edge_payload = [
        {
            "source": groups[left]["group_id"],
            "target": groups[right]["group_id"],
            "similarity": weight,
        }
        for left, right, weight in edges
    ]

    # Build output JSON
    output_data = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "catalog_path": str(catalog_path),
        "parameters": {
            "similarity_threshold": similarity_threshold,
            "min_group_size": min_group_size,
            "min_global_clusters": min_global_clusters,
            "min_assignment_similarity": min_assignment_similarity,
            "use_two_phase": use_two_phase,
            "exclude_system_tables": exclude_system_tables,
            "exclude_patterns": exclude_patterns,
        },
        "global_tables": sorted(global_tables),
        "procedure_groups": [
            {
                "group_id": group["group_id"],
                "cluster_id": group.get("cluster_id"),
                "is_singleton": group["is_singleton"],
                "procedures": group["procedures"],
                "tables": group["tables"],
            }
            for group in groups
        ],
        "clusters": cluster_summaries,
        "similarity_edges": edge_payload,
        "table_nodes": [
            {
                "table": table,
                "usage_count": table_usage[table],
                "is_global": table in global_tables,
                "is_missing": table in missing_tables,
                "is_orphaned": False,  # Used tables can't be orphaned
            }
            for table in sorted(table_usage.keys())
        ] + [
            {
                "table": table,
                "usage_count": 0,
                "is_global": False,
                "is_missing": False,
                "is_orphaned": True,
            }
            for table in sorted(orphaned_tables)
        ],
        "procedure_table_edges": [
            {
                "group_id": group["group_id"],
                "table": table,
                "is_global_table": table in global_tables,
            }
            for group in groups
            for table in group["tables"]
        ],
    }

    # Write to output file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, sort_keys=True)

    # Return statistics
    return {
        "procedure_groups": len(groups),
        "clusters": len(cluster_summaries),
        "similarity_edges": len(edges),
        "global_tables": len(global_tables),
        "missing_tables": len(missing_tables),
        "orphaned_tables": len(orphaned_tables),
        "catalog_path": str(catalog_path),
        "output_path": str(output_path),
    }

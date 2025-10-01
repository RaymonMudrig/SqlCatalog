# VectorizeCatalog/qcat/graph.py
from __future__ import annotations
import re
from functools import lru_cache
from typing import Dict, List, Set, Any, Optional, Tuple

from qcat.loader import load_items
from qcli.printers import read_sql_from_item

_ID = r"(?:\[[^\]]+\]|[A-Za-z0-9_]+)"

FROM_JOIN = re.compile(rf"(?is)\b(?:from|join)\s+({_ID}(?:\s*\.\s*{_ID})?)")
INS_INTO  = re.compile(rf"(?is)\binsert\s+into\s+({_ID}(?:\s*\.\s*{_ID})?)")
UPD_TBL   = re.compile(rf"(?is)\bupdate\s+({_ID}(?:\s*\.\s*{_ID})?)")
DEL_FROM  = re.compile(rf"(?is)\bdelete\s+from\s+({_ID}(?:\s*\.\s*{_ID})?)")
EXEC_PROC = re.compile(rf"(?is)\bexec(?:ute)?\s+({_ID}(?:\s*\.\s*{_ID})?)")

def _unbr(x: str) -> str:
    x = x.strip()
    if x.startswith("[") and x.endswith("]"): return x[1:-1]
    return x

def _split_qname(q: str) -> Tuple[Optional[str], str]:
    q = q.strip()
    parts = [p.strip() for p in re.split(r"\s*\.\s*", q)]
    if len(parts) == 1: return None, _unbr(parts[0])
    return _unbr(parts[0]), _unbr(parts[1])

def _safe(schema: Optional[str], name: str) -> str:
    return f"{schema}·{name}" if schema else name

class CatalogGraph:
    def __init__(self, items: List[Dict[str, Any]]):
        self.items = items
        self.by_safe: Dict[str, Dict[str, Any]] = {}
        self.kind_index: Dict[str, Set[str]] = {"table": set(), "view": set(), "procedure": set(), "function": set()}

        # table_safe -> {proc_or_view_safes}
        self.table_readers: Dict[str, Set[str]] = {}
        # table_safe -> {proc_safes}
        self.table_writers: Dict[str, Set[str]] = {}

        # object (table OR view) -> {proc_or_view_safes} that read it
        self.object_readers: Dict[str, Set[str]] = {}

        # proc_safe -> {callee_proc_safes}
        self.calls: Dict[str, Set[str]] = {}
        # proc_safe -> {caller_proc_safes}
        self.calls_rev: Dict[str, Set[str]] = {}

        self._index()

    def _index(self):
        # index items & kinds
        for it in self.items:
            k = (it.get("kind") or "").lower()
            s = it.get("safe_name")
            if not s:
                schema = it.get("schema") or ""
                name = it.get("name") or ""
                s = _safe(schema, name)
                it["safe_name"] = s
            self.by_safe[s] = it
            if k in self.kind_index:
                self.kind_index[k].add(s)

        # catalog-provided references (often includes views/procs referencing tables)
        for t_safe in list(self.kind_index.get("table", set())):
            t = self.by_safe.get(t_safe, {})
            self.table_readers.setdefault(t_safe, set())
            self.table_writers.setdefault(t_safe, set())
            self.object_readers.setdefault(t_safe, set())
            refs = t.get("referenced_by") or t.get("Referenced_By") or []
            for r in refs:
                safe = r.get("Safe_Name") or r.get("safe_name")
                if safe:
                    self.table_readers[t_safe].add(safe)
                    self.object_readers[t_safe].add(safe)

        # augment with SQL parsing of routines (for reads/writes/calls)
        scan_set = list(self.kind_index.get("procedure", set())) + list(self.kind_index.get("view", set()))
        for s in scan_set:
            it = self.by_safe[s]
            sql, _ = read_sql_from_item(it)
            if not sql:
                continue

            # readers: FROM/JOIN to tables OR views
            for m in FROM_JOIN.finditer(sql):
                sc, nm = _split_qname(m.group(1))
                tgt = _safe(sc, nm)
                if tgt in self.by_safe:
                    self.object_readers.setdefault(tgt, set()).add(s)
                    if tgt in self.kind_index["table"]:
                        self.table_readers.setdefault(tgt, set()).add(s)

            # writers: INSERT/UPDATE/DELETE only for tables
            for rx in (INS_INTO, UPD_TBL, DEL_FROM):
                for m in rx.finditer(sql):
                    sc, nm = _split_qname(m.group(1))
                    tgt = _safe(sc, nm)
                    if tgt in self.kind_index["table"]:
                        self.table_writers.setdefault(tgt, set()).add(s)

            # calls
            for m in EXEC_PROC.finditer(sql):
                sc, nm = _split_qname(m.group(1))
                callee = _safe(sc, nm)
                if callee in self.kind_index["procedure"]:
                    self.calls.setdefault(s, set()).add(callee)
                    self.calls_rev.setdefault(callee, set()).add(s)

    # ---- helpers ----
    def _bfs_callers(self, seeds: Set[str]) -> Set[str]:
        """Return seeds plus all transitive callers up the call graph."""
        out = set(seeds)
        frontier = list(seeds)
        while frontier:
            cur = frontier.pop()
            for caller in self.calls_rev.get(cur, set()):
                if caller not in out:
                    out.add(caller)
                    frontier.append(caller)
        return out

    def get_procs_reading_table(self, table_safe: str, include_via_views: bool = True, include_indirect: bool = True) -> Set[str]:
        readers = set(self.table_readers.get(table_safe, set()))
        procs = {s for s in readers if (self.by_safe.get(s, {}).get("kind") or "").lower() == "procedure"}
        if include_via_views:
            views = {s for s in readers if (self.by_safe.get(s, {}).get("kind") or "").lower() == "view"}
            for v in views:
                for r in self.object_readers.get(v, set()):
                    if (self.by_safe.get(r, {}).get("kind") or "").lower() == "procedure":
                        procs.add(r)
        if include_indirect and procs:
            procs = self._bfs_callers(procs)
        return procs

    def get_procs_writing_table(self, table_safe: str, include_indirect: bool = True) -> Set[str]:
        writers = set(self.table_writers.get(table_safe, set()))
        writers = {s for s in writers if (self.by_safe.get(s, {}).get("kind") or "").lower() == "procedure"}
        if include_indirect and writers:
            writers = self._bfs_callers(writers)
        return writers

# cached default graph (no args only)
@lru_cache(None)
def _default_graph() -> CatalogGraph:
    return CatalogGraph(load_items())

def ensure_graph(items: Optional[List[Dict[str, Any]]] = None) -> CatalogGraph:
    """
    If items is None, return a cached singleton graph built from load_items().
    If a list is provided, build a fresh graph (no caching; lists are unhashable).
    """
    if items is None:
        return _default_graph()
    return CatalogGraph(items)

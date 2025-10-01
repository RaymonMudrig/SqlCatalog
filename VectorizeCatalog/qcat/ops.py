# VectorizeCatalog/qcat/ops.py
from __future__ import annotations
from typing import List, Dict, Any, Optional, Tuple, Set
import re

from qcli.printers import read_sql_from_item
from qcat.name_match import split_safe
from qcat.graph import ensure_graph

# -------------------- utilities --------------------

def _as_display(it: Dict[str, Any]) -> str:
    return f"{(it.get('schema') or '') + '.' if it.get('schema') else ''}{it.get('name') or it.get('safe_name')}"

def _strip_brackets(x: str) -> str:
    x = x.strip()
    if x.startswith("[") and x.endswith("]"): return x[1:-1]
    if x.startswith("`") and x.endswith("`"): return x[1:-1]
    if x.startswith('"') and x.endswith('"'): return x[1:-1]
    return x

def _split_qualified(name: str) -> Tuple[Optional[str], str]:
    """Accepts [dbo].[Order], dbo.Order, [Order], Order, dbo·Order → (schema?, base)"""
    s = name.strip()
    if "·" in s:
        parts = s.split("·", 1)
        return parts[0] or None, parts[1]
    parts = [p.strip() for p in re.split(r"\s*\.\s*", s)]
    if len(parts) == 1:
        return None, _strip_brackets(parts[0])
    return _strip_brackets(parts[0]), _strip_brackets(parts[1])

def _ci_get(d: Dict[str, Any], key: str, default=None):
    if key in d: return d[key]
    kl = key.lower()
    for k, v in d.items():
        if isinstance(k, str) and k.lower() == kl:
            return v
    return default

def _extract_columns_from_item(it: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Accepts either:
      - columns: [ { name, type, ... } ]
      - Columns: { "ColName": { Type, Nullable, Default, Doc } }
    """
    if not it: return []
    cols = it.get("columns")
    if isinstance(cols, list):
        out = []
        for c in cols:
            if not isinstance(c, dict): continue
            nm = c.get("name") or c.get("Name")
            if not nm: continue
            out.append({
                "name": nm,
                "type": c.get("type") or c.get("Type"),
                "nullable": c.get("nullable") if "nullable" in c else c.get("Nullable"),
                "default": c.get("default") if "default" in c else c.get("Default"),
                "doc": c.get("doc") if "doc" in c else c.get("Doc"),
            })
        if out: return out

    cols_obj = _ci_get(it, "Columns")
    if isinstance(cols_obj, dict):
        out = []
        for nm, meta in cols_obj.items():
            meta = meta or {}
            out.append({
                "name": nm,
                "type": _ci_get(meta, "Type"),
                "nullable": _ci_get(meta, "Nullable"),
                "default": _ci_get(meta, "Default"),
                "doc": _ci_get(meta, "Doc"),
            })
        return out
    return []

def _safe(schema: Optional[str], name: str) -> str:
    return f"{schema}·{name}" if schema else name

def _names_for_match(it: Dict[str, Any]) -> Tuple[Optional[str], List[str]]:
    """Return (schema, [candidate names]) for exact matching."""
    schema = it.get("schema") or _ci_get(it, "Schema") or ""
    cands = []
    for k in ("name", "Original_Name", "Safe_Name", "safe_name"):
        v = it.get(k) or _ci_get(it, k)
        if not v: continue
        if isinstance(v, str):
            # split safe_name variant
            if "·" in v:
                parts = v.split("·", 1)
                if len(parts) == 2:
                    if not schema: schema = parts[0]
                    cands.append(parts[1])
                else:
                    cands.append(v)
            else:
                cands.append(v)
    # remove duplicates, keep order
    seen = set(); out = []
    for n in cands:
        ln = n.lower()
        if ln in seen: continue
        seen.add(ln); out.append(n)
    return schema, out

# Replace the whole function in VectorizeCatalog/qcat/ops.py

def _find_item(items: List[Dict[str, Any]], kind: str, name: str, fuzzy: bool = False) -> Optional[Dict[str, Any]]:
    """
    Deterministic finder: only returns an item if it EXACTLY matches requested schema/name.
    Fuzzy=True allows a last-resort contains() search on base name, but never cross-kind.
    """
    want_schema, want_base = _split_qualified(name)
    wl_schema = (want_schema or "").lower()
    wl_base   = (want_base or "").lower()
    k_l = kind.lower()

    # 1) exact name match, honoring schema if provided
    for it in items:
        if (it.get("kind") or "").lower() != k_l:
            continue
        s, cands = _names_for_match(it)
        s_l = (s or "").lower()
        if wl_schema and s_l != wl_schema:
            continue
        for nm in cands:
            if (nm or "").lower() == wl_base:
                return it

    # 2) exact via safe_name equality (dbo·Name) when schema provided
    if wl_schema:
        safe = _safe(want_schema, want_base).lower()
        for it in items:
            if (it.get("kind") or "").lower() != k_l:
                continue
            sname = (it.get("safe_name") or _ci_get(it, "Safe_Name") or "")
            if isinstance(sname, str) and sname.lower() == safe:
                return it

    # 3) exact name-only (no schema supplied) – re-check in case step 1 missed a shape
    if not wl_schema:
        for it in items:
            if (it.get("kind") or "").lower() != k_l:
                continue
            _s, cands = _names_for_match(it)
            for nm in cands:
                if (nm or "").lower() == wl_base:
                    return it

    # 4) optional fuzzy (contains) within same kind
    if fuzzy:
        for it in items:
            if (it.get("kind") or "").lower() != k_l:
                continue
            _s, cands = _names_for_match(it)
            if any(wl_base in (nm or "").lower() for nm in cands):
                return it

    return None

# -------------------- exhaustive, graph-backed ops --------------------

def procs_access_table(items: List[Dict[str, Any]], table_name: str, fuzzy=False, include_via_views=True, include_indirect=True) -> List[Dict[str, Any]]:
    g = ensure_graph(items)
    t = _find_item(items, "table", table_name, fuzzy=fuzzy)
    if not t: return []
    s = t.get("safe_name")
    out: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for proc_safe in g.get_procs_reading_table(s, include_via_views=include_via_views, include_indirect=include_indirect):
        if proc_safe in seen: continue
        seen.add(proc_safe); out.append(g.by_safe[proc_safe])
    return out

def procs_update_table(items: List[Dict[str, Any]], table_name: str, fuzzy=False, include_indirect=True) -> List[Dict[str, Any]]:
    g = ensure_graph(items)
    t = _find_item(items, "table", table_name, fuzzy=fuzzy)
    if not t: return []
    s = t.get("safe_name")
    out: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for proc_safe in g.get_procs_writing_table(s, include_indirect=include_indirect):
        if proc_safe in seen: continue
        seen.add(proc_safe); out.append(g.by_safe[proc_safe])
    return out

def views_access_table(items: List[Dict[str, Any]], table_name: str, fuzzy=False, transitive=True) -> List[Dict[str, Any]]:
    g = ensure_graph(items)
    t = _find_item(items, "table", table_name, fuzzy=fuzzy)
    if not t: return []
    views_safes = set()
    s = t.get("safe_name")
    for proc_or_view in g.table_readers.get(s, set()):
        it = g.by_safe.get(proc_or_view)
        if it and (it.get("kind") or "").lower() == "view":
            views_safes.add(proc_or_view)
    return [g.by_safe[v] for v in sorted(views_safes)]

def tables_accessed_by_procedure(items: List[Dict[str, Any]], proc_name: str, fuzzy=False, include_indirect=True) -> Dict[str, List[str]]:
    g = ensure_graph(items)
    p = _find_item(items, "procedure", proc_name, fuzzy=fuzzy)
    if not p: return {"reads": [], "writes": []}
    ps = p.get("safe_name")
    reads: Set[str] = set(); writes: Set[str] = set()
    for t, readers in g.table_readers.items():
        if ps in readers: reads.add(t)
    for t, writers in g.table_writers.items():
        if ps in writers: writes.add(t)
    if include_indirect:
        for callee in g.calls.get(ps, set()):
            for t, readers in g.table_readers.items():
                if callee in readers: reads.add(t)
            for t, writers in g.table_writers.items():
                if callee in writers: writes.add(t)
    return {"reads": sorted(reads), "writes": sorted(writes)}

def tables_accessed_by_view(items: List[Dict[str, Any]], view_name: str, fuzzy=False) -> List[str]:
    g = ensure_graph(items)
    v = _find_item(items, "view", view_name, fuzzy=fuzzy)
    if not v: return []
    vs = v.get("safe_name")
    outs: Set[str] = set()
    for t, readers in g.table_readers.items():
        if vs in readers: outs.add(t)
    return sorted(outs)

def unaccessed_tables(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    g = ensure_graph(items)
    tables = [g.by_safe[s] for s in g.kind_index.get("table", [])]
    out = []
    for t in tables:
        s = t.get("safe_name")
        readers = g.table_readers.get(s, set())
        writers = g.table_writers.get(s, set())
        if not readers and not writers:
            out.append(t)
    return out

# -------------------- columns / returns / unused --------------------

def list_columns_of_table(items: List[Dict[str, Any]], table_name: str, fuzzy=False) -> Dict[str, Any]:
    """
    Deterministic: never returns a different table.
    {
      "found": bool,
      "match": <item or None>,
      "columns": [ { name, type, nullable, default, doc } ]
    }
    """
    it = _find_item(items, "table", table_name, fuzzy=fuzzy)
    if not it:
        return {"found": False, "match": None, "columns": []}
    cols = _extract_columns_from_item(it)
    return {"found": True, "match": it, "columns": cols}

def columns_returned_by_procedure(items: List[Dict[str, Any]], proc_name: str, fuzzy=True) -> List[str]:
    p = _find_item(items, "procedure", proc_name, fuzzy=fuzzy)
    if not p: return []
    cols = p.get("result_columns") or p.get("column_refs") or []
    if cols:
        out = []
        for c in cols:
            if isinstance(c, str): out.append(c)
            elif isinstance(c, dict) and c.get("name"): out.append(c["name"])
        seen=set(); uniq=[]
        for x in out:
            lx=x.lower()
            if lx in seen: continue
            seen.add(lx); uniq.append(x)
        return uniq

    # Fallback: naive SELECT parsing
    sql, _ = read_sql_from_item(p)
    if not sql: return []
    outs: List[str] = []
    for m in re.finditer(r"(?is)\bselect\b(.+?)\bfrom\b", sql):
        raw = m.group(1)
        for token in raw.split(","):
            token = token.strip()
            m1 = re.search(r"(?i)\bas\s+([A-Za-z0-9_]+)\b", token)
            if m1: outs.append(m1.group(1)); continue
            m2 = re.search(r"(?i)\b([A-Za-z0-9_]+)\s*=", token)
            if m2: outs.append(m2.group(1)); continue
            m3 = re.search(r"(?i)([A-Za-z0-9_]+)$", token)
            if m3: outs.append(m3.group(1))
    seen=set(); uniq=[]
    for x in outs:
        lx=x.lower()
        if lx in seen: continue
        seen.add(lx); uniq.append(x)
    return uniq

def unused_columns_of_table(items: List[Dict[str, Any]], table_name: str, fuzzy=False) -> List[Dict[str, Any]]:
    g = ensure_graph(items)
    t = _find_item(items, "table", table_name, fuzzy=fuzzy)
    if not t: return []
    schema = t.get("schema") or _ci_get(t, "Schema") or ""
    table = split_safe(t.get("safe_name") or _ci_get(t, "Safe_Name") or "")[1] or (t.get("name") or _ci_get(t, "Original_Name") or "")
    cols = [c.get("name") for c in _extract_columns_from_item(t) if c.get("name")]
    if not cols: return []
    needles = {c: re.compile(rf"(?i)\b(?:\[{re.escape(schema)}\]\s*\.\s*)?\[{re.escape(table)}\]\s*\.\s*\[{re.escape(c)}\]\b|(?i)\b{re.escape(table)}\s*\.\s*{re.escape(c)}\b") for c in cols}
    used: Set[str] = set()
    for it in g.items:
        if (it.get("kind") or "").lower() not in ("procedure","view"): continue
        sql, _ = read_sql_from_item(it)
        if not sql: continue
        for c, pat in needles.items():
            if c not in used and pat.search(sql):
                used.add(c)
        if len(used) == len(cols): break
    unused = [ {"name": c, "type": None} for c in cols if c not in used ]
    return unused

# -------------------- calls / trees --------------------

def procs_called_by_procedure(items: List[Dict[str, Any]], proc_name: str, fuzzy=False) -> List[str]:
    g = ensure_graph(items)
    p = _find_item(items, "procedure", proc_name, fuzzy=fuzzy)
    if not p: return []
    ps = p.get("safe_name")
    outs = [ _as_display(g.by_safe[c]) for c in g.calls.get(ps, set()) if c in g.by_safe ]
    return sorted(outs)

def call_tree(items: List[Dict[str, Any]], proc_name: str, max_depth: int = 3, fuzzy=False) -> Dict[str, Any]:
    g = ensure_graph(items)
    p = _find_item(items, "procedure", proc_name, fuzzy=fuzzy)
    root_name = proc_name
    root = None
    if p:
        root_name = _as_display(p)
        root = p.get("safe_name")
    visited: Set[str] = set()

    def build(safe: Optional[str], name: str, depth: int) -> Dict[str, Any]:
        if depth > max_depth or not safe: return {"name": name, "calls": []}
        if safe in visited: return {"name": name, "calls": []}
        visited.add(safe)
        calls = []
        for c in sorted(g.calls.get(safe, set())):
            it = g.by_safe.get(c)
            d = _as_display(it) if it else c
            calls.append(build(c, d, depth+1))
        return {"name": name, "calls": calls}

    return build(root, root_name, 1)

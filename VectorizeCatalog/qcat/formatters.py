# VectorizeCatalog/qcat/formatters.py
from __future__ import annotations
from typing import List, Dict, Any, Iterable, Optional, Set
import re

from qcat import ops as K
from qcat.graph import ensure_graph
from qcli.printers import read_sql_from_item

__all__ = [
    "render_procs_access_table",
    "render_procs_update_table",
    "render_views_access_table",
    "render_tables_accessed_by_procedure",
    "render_tables_accessed_by_view",
    "render_unaccessed_tables",
    "render_procs_called_by_procedure",
    "render_call_tree",
    "render_list_columns_of_table",
    "render_columns_returned_by_procedure",
    "render_unused_columns_of_table",
    "render_list_all_of_kind",
    "render_sql_of_entity",
]

# ---------- helpers ----------

def _disp(it: Dict[str, Any]) -> str:
    return f"{(it.get('schema') or '') + '.' if it.get('schema') else ''}{it.get('name') or it.get('safe_name')}"

def _sorted_unique(names: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for n in names:
        key = (n or "").lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(n)
    out.sort(key=lambda s: s.lower())
    return out

def _names(items: List[Dict[str, Any]]) -> List[str]:
    return _sorted_unique(_disp(it) for it in items)

def _by_safe(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {(it.get("safe_name") or _disp(it)): it for it in items}

def _disp_from_safes(items: List[Dict[str, Any]], safes: Iterable[str]) -> List[str]:
    by = _by_safe(items)
    return _sorted_unique(_disp(by.get(s, {"schema": "", "name": s})) for s in safes)

def _section(title: str, lines: List[str]) -> str:
    if not lines:
        return f"**{title}**\n- (none)"
    bullets = "\n".join(f"- {ln}" for ln in lines)
    return f"**{title}** ({len(lines)})\n{bullets}"

def _like_to_regex(like: str) -> re.Pattern:
    esc = re.escape(like)
    esc = esc.replace(r"\%", ".*").replace(r"\_", ".")
    return re.compile(f"^{esc}$", re.IGNORECASE)

# ---------- renderers (deterministic, use ops which read catalog.json) ----------

def render_procs_access_table(items: List[Dict[str, Any]], table_name: str) -> str:
    procs_all = K.procs_access_table(items, table_name, fuzzy=False, include_via_views=True, include_indirect=True)
    procs_wr  = K.procs_update_table(items, table_name, fuzzy=False, include_indirect=True)
    all_names = _names(procs_all)
    wr_names  = _names(procs_wr)
    wr_set = {n.lower() for n in wr_names}
    rd_names = [n for n in all_names if n.lower() not in wr_set]
    hdr = f"Procedures that access `{table_name}` — total {len(all_names)}"
    parts = [
        f"### {hdr}",
        _section("WRITE", wr_names),
        _section("READ", rd_names),
    ]
    return "\n\n".join(parts)

def render_procs_update_table(items: List[Dict[str, Any]], table_name: str) -> str:
    procs = K.procs_update_table(items, table_name, fuzzy=False, include_indirect=True)
    return _section(f"Procedures that UPDATE `{table_name}`", _names(procs))

def render_views_access_table(items: List[Dict[str, Any]], table_name: str) -> str:
    views = K.views_access_table(items, table_name, fuzzy=False, transitive=True)
    return _section(f"Views that access `{table_name}`", _names(views))

def render_tables_accessed_by_procedure(items: List[Dict[str, Any]], proc_name: str) -> str:
    rw = K.tables_accessed_by_procedure(items, proc_name, fuzzy=False, include_indirect=True)
    reads = _disp_from_safes(items, rw.get("reads", []))
    writes = _disp_from_safes(items, rw.get("writes", []))
    return "\n\n".join([
        _section(f"Tables READ by `{proc_name}`", reads),
        _section(f"Tables WRITTEN by `{proc_name}`", writes)
    ])

def render_tables_accessed_by_view(items: List[Dict[str, Any]], view_name: str) -> str:
    reads = K.tables_accessed_by_view(items, view_name, fuzzy=False)
    return _section(f"Tables READ by `{view_name}`", _disp_from_safes(items, reads))

def render_unaccessed_tables(items: List[Dict[str, Any]]) -> str:
    tabs = K.unaccessed_tables(items)
    return _section("Tables not accessed or updated by any procedures or views", _names(tabs))

def render_procs_called_by_procedure(items: List[Dict[str, Any]], proc_name: str) -> str:
    calls = K.procs_called_by_procedure(items, proc_name, fuzzy=False)
    return _section(f"Procedures called by `{proc_name}`", _sorted_unique(calls))

def render_call_tree(items: List[Dict[str, Any]], proc_name: str, depth: int = 3) -> str:
    tree = K.call_tree(items, proc_name, max_depth=depth, fuzzy=False)
    def fmt(n: Dict[str, Any], d: int = 0) -> List[str]:
        if not n: return []
        lines = [("  " * d) + f"- {n.get('name') or ''}"]
        for ch in (n.get("calls") or []):
            lines.extend(fmt(ch, d + 1))
        return lines
    lines = fmt(tree, 0)
    title = f"Call tree for `{proc_name}` (depth ≤ {depth})"
    return f"**{title}**\n" + ("\n".join(lines) if lines else "- (none)")

def render_list_columns_of_table(items: List[Dict[str, Any]], table_name: str) -> str:
    res = K.list_columns_of_table(items, table_name, fuzzy=False)
    if not res.get("found"):
        return f"**Table not found:** `{table_name}`.\nTry: `list all tables like 'Order%'` or include brackets: `[dbo].[Order]`."
    cols = res.get("columns") or []
    disp_schema = (res.get("match") or {}).get("schema") or (res.get("match") or {}).get("Schema") or ""
    disp_name   = (res.get("match") or {}).get("name")   or (res.get("match") or {}).get("Original_Name") or (res.get("match") or {}).get("Safe_Name") or table_name
    full = f"{disp_schema+'.' if disp_schema else ''}{disp_name}"
    if not cols:
        return f"**No columns recorded** for `{full}`."
    lines = []
    for c in cols:
        t = c.get("type") or ""
        nul = c.get("nullable")
        nul_str = " null" if nul is True else (" not null" if nul is False else "")
        lines.append(f"- `{c.get('name')}` {t}{nul_str}".rstrip())
    return f"**Columns of `{full}`** ({len(lines)})\n" + "\n".join(lines)

def render_columns_returned_by_procedure(items: List[Dict[str, Any]], proc_name: str) -> str:
    cols = K.columns_returned_by_procedure(items, proc_name, fuzzy=True)
    return _section(f"Columns returned by `{proc_name}`", [f"`{c}`" for c in cols])

def render_unused_columns_of_table(items: List[Dict[str, Any]], table_name: str) -> str:
    cols = K.unused_columns_of_table(items, table_name, fuzzy=False)
    if not cols:
        return f"No unused columns detected for `{table_name}`."
    lines = [f"- `{c.get('name')}` {c.get('type') or ''}".rstrip() for c in cols]
    return f"**Unused columns of `{table_name}`** ({len(lines)})\n" + "\n".join(lines)

def render_list_all_of_kind(items: List[Dict[str, Any]], kind: str,
                            schema: Optional[str] = None,
                            pattern: Optional[str] = None) -> str:
    k = (kind or "").lower()
    filtered = [it for it in items if (it.get("kind") or "").lower() == k]
    if schema:
        filtered = [it for it in filtered if (it.get("schema") or "").lower() == schema.lower()]
    if pattern:
        rx = _like_to_regex(pattern)
        filtered = [it for it in filtered if rx.match(it.get("name") or it.get("safe_name") or "")]
    return _section(
        f"All {k}s"
        + (f" in schema `{schema}`" if schema else "")
        + (f" matching `{pattern}`" if pattern else ""),
        _names(filtered),
    )

def render_sql_of_entity(items: List[Dict[str, Any]], kind: str, name: str, full: bool = True) -> str:
    """
    Print the create SQL of a table/procedure/view/function if available (from export or index).
    """
    # Reuse ops' deterministic finder via small wrapper
    if kind not in {"table", "procedure", "view", "function"}:
        return f"Unsupported kind: {kind}"
    # Build a tiny local finder to avoid duplicating ops internals
    from qcat.ops import _find_item as _find
    it = _find(items, kind, name, fuzzy=False)
    if not it:
        return f"No exact match for `{name}` (kind: {kind})."
    disp = _disp(it)
    sql, src = read_sql_from_item(it)
    if not sql:
        return f"**{kind.upper()} SQL — {disp}**\n(no SQL found on disk or in index)"
    if not full:
        head = sql.strip().splitlines()[:60]
        body = "\n".join(head)
        return f"**{kind.upper()} SQL — {disp}** (first {len(head)} lines)\n\n```\n{body}\n```"
    return f"**{kind.upper()} SQL — {disp}**\n\n```\n{sql.strip()}\n```"

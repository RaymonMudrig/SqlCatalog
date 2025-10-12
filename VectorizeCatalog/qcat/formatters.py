# qcat/formatters.py
from __future__ import annotations
from typing import List, Dict, Any, Optional
from qcat import ops as K

__all__ = [
    # counts / listings
    "render_count_of_kind",
    "render_list_all_of_kind",
    # legacy intents
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
    "render_sql_of_entity",
    # compare
    "render_compare_sql",
    "render_find_similar_sql",
]

def _display_name(it: Dict[str, Any]) -> str:
    schema = it.get("schema") or it.get("Schema") or ""
    nm = it.get("name") or it.get("Original_Name") or it.get("Safe_Name") or it.get("safe_name")
    return f"{schema}.{nm}" if schema and nm and "·" not in str(nm) else str(nm)

# ---- counts / lists ----

def render_count_of_kind(items: List[Dict[str, Any]], kind: Optional[str]) -> str:
    if not kind:
        return "Please specify kind (table/procedure/view/function)."
    names = K.list_all_of_kind(items, kind)
    title = kind.capitalize() + ("s" if not kind.endswith("s") else "")
    return f"There are **{len(names)} {title.lower()}**."

def render_list_all_of_kind(items: List[Dict[str, Any]], kind: Optional[str]) -> str:
    if not kind:
        return "Please specify kind (table/procedure/view/function)."
    rows = K.list_all_of_kind(items, kind)

    title = kind.capitalize() + ("s" if not kind.endswith("s") else "")
    if not rows:
        return f"No {title.lower()} found."
    lines = [f"**All {title.lower()} ({len(rows)})**"] + [f"- `{r}`" for r in rows]
    return "\n".join(lines)

# ---- legacy intents ----

def render_procs_access_table(items: List[Dict[str, Any]], table_name: str) -> str:
    procs = K.procs_access_table(items, table_name, fuzzy=False)
    if not procs:
        return f"No procedures found accessing `{table_name}`."
    lines = [f"**Procedures that access `{table_name}`** ({len(procs)})"]
    for it in procs:
        lines.append(f"- `{_display_name(it)}`")
    return "\n".join(lines)

def render_procs_update_table(items: List[Dict[str, Any]], table_name: str) -> str:
    procs = K.procs_update_table(items, table_name)
    if not procs:
        return f"No procedures found updating `{table_name}`."
    lines = [f"**Procedures that update `{table_name}`** ({len(procs)})"]
    for it in procs:
        lines.append(f"- `{_display_name(it)}`")
    return "\n".join(lines)

def render_views_access_table(items: List[Dict[str, Any]], table_name: str) -> str:
    views = K.views_access_table(items, table_name)
    if not views:
        return f"No views found accessing `{table_name}`."
    lines = [f"**Views that access `{table_name}`** ({len(views)})"]
    for it in views:
        lines.append(f"- `{_display_name(it)}`")
    return "\n".join(lines)

def render_tables_accessed_by_procedure(items: List[Dict[str, Any]], proc_name: str) -> str:
    reads, writes = K.tables_accessed_by_procedure(items, proc_name)
    if not reads and not writes:
        return f"No table accesses found for `{proc_name}`."
    out = [f"**Tables accessed by `{proc_name}`**"]
    if reads:
        out.append("**READS**")
        out.extend([f"- `{r}`" for r in reads])
    if writes:
        out.append("\n**WRITES**")
        out.extend([f"- `{w}`" for w in writes])
    return "\n".join(out)

def render_tables_accessed_by_view(items: List[Dict[str, Any]], view_name: str) -> str:
    reads = K.tables_accessed_by_view(items, view_name)
    if not reads:
        return f"No base tables found for `{view_name}`."
    lines = [f"**Tables accessed by view `{view_name}`** ({len(reads)})"] + [f"- `{r}`" for r in reads]
    return "\n".join(lines)

def render_unaccessed_tables(items: List[Dict[str, Any]]) -> str:
    tabs = K.unaccessed_tables(items)
    if not tabs:
        return "Every table appears to be accessed by at least one view or procedure."
    lines = [f"**Unaccessed / Unused tables ({len(tabs)})**"] + [f"- `{t}`" for t in tabs]
    return "\n".join(lines)

def render_procs_called_by_procedure(items: List[Dict[str, Any]], proc_name: str) -> str:
    procs = K.procs_called_by_procedure(items, proc_name)
    if not procs:
        return f"No called procedures found for `{proc_name}`."
    lines = [f"**Procedures called by `{proc_name}`** ({len(procs)})"] + [f"- `{p}`" for p in procs]
    return "\n".join(lines)

def render_call_tree(items: List[Dict[str, Any]], proc_name: str, depth: int = 6) -> str:
    tree_lines = K.call_tree(items, proc_name, max_depth=depth)
    if not tree_lines:
        return f"No call tree found for `{proc_name}`."
    return "**Call tree**\n\n```\n" + "\n".join(tree_lines) + "\n```"

def render_list_columns_of_table(items: List[Dict[str, Any]], table_name: str, schema_filter: Optional[str] = None) -> str:
    res = K.list_columns_of_table(items, table_name, fuzzy=False)
    if not res.get("found"):
        return f"No columns found for `{table_name}`."
    cols = res.get("columns") or []
    it = res.get("match") or {}
    head = f"**Columns of `{_display_name(it)}`** ({len(cols)})"
    body = [f"- `{c.get('name')}` {(c.get('type') or '').strip()} {'null' if c.get('nullable') else 'not null'}" for c in cols]
    return "\n".join([head] + body)

def render_columns_returned_by_procedure(items: List[Dict[str, Any]], proc_name: str) -> str:
    cols = K.columns_returned_by_procedure(items, proc_name)
    if not cols:
        return f"No returned columns metadata found for `{proc_name}`."
    lines = [f"**Columns returned by `{proc_name}`** ({len(cols)})"] + [f"- `{c}`" for c in cols]
    return "\n".join(lines)

def render_unused_columns_of_table(items: List[Dict[str, Any]], table_name: str) -> str:
    cols = K.unused_columns_of_table(items, table_name)
    if cols is None:
        return f"No table found for `{table_name}`."
    if not cols:
        return f"No unused/unaccessed columns in `{table_name}`."
    lines = [f"**Unused / Unaccessed columns of `{table_name}`** ({len(cols)})"] + [f"- `{c}`" for c in cols]
    return "\n".join(lines)

def render_sql_of_entity(items: List[Dict[str, Any]], kind: Optional[str], name: str) -> str:

    print(f"[formatters] render_sql_of_entity called with kind={kind}, name={name}")

    it_sql, src, disp = K.get_sql(items, kind, name)
    if not it_sql:
        return f"(no SQL found on disk or in index) — `{name}`"
    return f"### {disp}\n\n```sql\n{it_sql}\n```"

# ---- compare ----

def render_compare_sql(items: List[Dict[str, Any]],
                       left_kind: Optional[str], left_name: str,
                       right_kind: Optional[str], right_name: str) -> Dict[str, str]:

    print(f"[formatters] render_compare_sql called with left=({left_kind}, {left_name}) right=({right_kind}, {right_name})")

    return K.compare_sql(items, left_kind, left_name, right_kind, right_name)

def render_find_similar_sql(items: List[Dict[str, Any]],
                            kind: Optional[str], name: str,
                            threshold: float = 50.0) -> str:
    """
    Find and render entities with similar SQL to the given entity.

    Args:
        items: Catalog items
        kind: Entity kind (table, view, procedure, function) or None
        name: Entity name to compare against
        threshold: Minimum similarity percentage (default: 50.0)

    Returns:
        Formatted markdown string with results
    """
    print(f"[formatters] render_find_similar_sql called with kind={kind}, name={name}, threshold={threshold}")

    results = K.find_similar_sql(items, kind, name, threshold)

    if not results:
        return f"No similar entities found for `{name}` with similarity >= {threshold}%."

    # Build output
    lines = [f"**Similar entities to `{name}`** (threshold: {threshold}%)"]
    lines.append(f"Found {len(results)} similar entities:")
    lines.append("")

    for entity_name, similarity in results:
        lines.append(f"- `{entity_name}` — **{similarity}%** similarity")

    return "\n".join(lines)

# --- compat wrappers for legacy list_all_* intents -------------------------

def render_list_all_of_kind(items, kind: str, schema: str | None = None, name_pattern: str | None = None):
    """Generic renderer used by all list_all_* wrappers."""
    try:
        from qcat import ops as K
    except Exception:
        import qcat.ops as K  # fallback

    names = K.list_all_of_kind(items, kind, schema=schema, name_pattern=name_pattern)
    title_map = {"table": "Tables", "view": "Views", "procedure": "Procedures", "function": "Functions"}
    title = title_map.get(kind.lower(), f"{kind.title()}s")
    if not names:
        return f"**{title}**\n(none found)"
    lines = "\n".join(f"- `{n}`" for n in names)  # Added backticks for clickable entities
    return f"**{title}** ({len(names)})\n{lines}"


def render_list_all_tables(items, schema: str | None = None, name_pattern: str | None = None, pattern: str | None = None):
    """Back-compat alias → render_list_all_of_kind('table', ...)"""
    if name_pattern is None and pattern is not None:
        name_pattern = pattern
    return render_list_all_of_kind(items, "table", schema=schema, name_pattern=name_pattern)


def render_list_all_views(items, schema: str | None = None, name_pattern: str | None = None, pattern: str | None = None):
    """Back-compat alias → render_list_all_of_kind('view', ...)"""
    if name_pattern is None and pattern is not None:
        name_pattern = pattern
    return render_list_all_of_kind(items, "view", schema=schema, name_pattern=name_pattern)


def render_list_all_procedures(items, schema: str | None = None, name_pattern: str | None = None, pattern: str | None = None):
    """Back-compat alias → render_list_all_of_kind('procedure', ...)"""
    if name_pattern is None and pattern is not None:
        name_pattern = pattern
    return render_list_all_of_kind(items, "procedure", schema=schema, name_pattern=name_pattern)


def render_list_all_functions(items, schema: str | None = None, name_pattern: str | None = None, pattern: str | None = None):
    """Back-compat alias → render_list_all_of_kind('function', ...)"""
    if name_pattern is None and pattern is not None:
        name_pattern = pattern
    return render_list_all_of_kind(items, "function", schema=schema, name_pattern=name_pattern)

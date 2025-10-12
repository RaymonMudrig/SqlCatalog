# qcat/ops.py
from __future__ import annotations
from typing import List, Dict, Any, Optional, Tuple, Set, Iterable
import re, difflib, html

from qcli.printers import read_sql_from_item

# Map kind -> section key in catalog.json
_SECTION_BY_KIND = {
    "table": "Tables",
    "view": "Views",
    "procedure": "Procedures",
    "function": "Functions",
}

_bracket_re = re.compile(r'[\[\]`"]')
_spaces_re = re.compile(r'\s+')

def _norm_ident(s: Optional[str]) -> str:
    """Normalize SQL identifiers for matching: strip [ ], quotes, collapse spaces, lower, unify separator."""
    if not s:
        return ""
    s = s.replace("·", ".")
    s = _bracket_re.sub("", s)
    s = _spaces_re.sub(" ", s.strip())
    return s.lower()

def _split_schema_and_name(s: str) -> Tuple[Optional[str], str]:
    """Return (schema?, name) from e.g. '[dbo].[Order]' or 'Order'."""
    s = _norm_ident(s)
    if "." in s:
        sch, nm = s.split(".", 1)
        return (sch or None), nm
    return None, s

def _catalog_section(items: Dict[str, Any], kind: str) -> Dict[str, Any]:
    """Return catalog section dict (safe_name -> meta) for a kind."""
    catalog = (items or {}).get("catalog") or {}
    key = _SECTION_BY_KIND.get((kind or "").lower(), "")
    return catalog.get(key, {}) or {}

# ============================================================
# Normalization: accept either flat items list OR catalog.json
# ============================================================

def as_items_list(obj) -> List[Dict[str, Any]]:
    """
    Normalize input to a flat list of item dicts with at least:
      kind in {table, view, procedure, function}
      schema, name, safe_name (best-effort)

    Accepts:
      - items.json-like: List[dict]
      - wrapper dict: {"items":[...], "catalog":{...}}
      - catalog.json-like: {"Tables":{...}, "Views":{...}, "Procedures":{...}, "Functions":{...}}
      - dict keyed by id where values already look like items (contain 'kind')
    """
    # 1) Already a flat list
    if isinstance(obj, list):
        return obj

    if not isinstance(obj, dict):
        return []

    # 2) Wrapper dict with a ready-made list
    if isinstance(obj.get("items"), list):
        return obj["items"]

    # 3) If there's a nested 'catalog', use that as the source; otherwise use obj itself
    source = obj.get("catalog")
    if not isinstance(source, dict):
        source = obj

    # 3a) If values already look like items (contain 'kind'), just return those values
    vals = list(source.values())
    if vals and all(isinstance(v, dict) for v in vals) and any(("kind" in v or "Kind" in v) for v in vals):
        return vals  # dict-of-items form

    items: List[Dict[str, Any]] = []

    def _safe(schema: Optional[str], name: str) -> str:
        return f"{schema}·{name}" if schema else name

    # 4) Tables from catalog-like dict
    tables = source.get("Tables") or source.get("tables") or {}
    if isinstance(tables, dict):
        for key_name, meta in tables.items():
            meta = meta or {}
            schema = meta.get("Schema") or meta.get("schema") or ""
            name = meta.get("Original_Name") or meta.get("Name") or key_name
            sname = meta.get("Safe_Name") or meta.get("safe_name") or name
            # If Safe_Name already has schema separator, use it as-is; otherwise build it
            safe_name_final = sname if "·" in sname else _safe(schema, sname)
            item = {
                "kind": "table",
                "schema": schema,
                "name": name,
                "safe_name": safe_name_final,
                # carry over useful fields
                "Columns": meta.get("Columns"),
                "Primary_Key": meta.get("Primary_Key") or meta.get("PrimaryKey"),
                "Foreign_Keys": meta.get("Foreign_Keys"),
                "Indexes": meta.get("Indexes"),
                "Referenced_By": meta.get("Referenced_By"),
                "Doc": meta.get("Doc"),
            }
            items.append(item)

    # 5) Views / Procedures / Functions
    def _ingest(section: str, kind: str):
        src = source.get(section) or source.get(section.lower()) or {}
        if not isinstance(src, dict):
            return
        for key_name, meta in src.items():
            meta = meta or {}
            schema = meta.get("Schema") or meta.get("schema") or ""
            name = meta.get("Original_Name") or meta.get("Name") or key_name
            sname = meta.get("Safe_Name") or meta.get("safe_name") or name
            # If Safe_Name already has schema separator, use it as-is; otherwise build it
            safe_name_final = sname if "·" in sname else _safe(schema, sname)
            item = {
                "kind": kind,
                "schema": schema,
                "name": name,
                "safe_name": safe_name_final,
            }
            # carry references if present
            for k in ("Doc", "Reads", "Writes", "Calls", "Returns",
                      "Referenced_Tables", "References", "Column_Refs"):
                v = meta.get(k)
                if v is not None:
                    item[k] = v
            items.append(item)

    _ingest("Views", "view")
    _ingest("Procedures", "procedure")
    _ingest("Functions", "function")

    return items


# -------------------- Name utilities --------------------

def _strip_brackets(x: str) -> str:
    x = (x or "").strip()
    if x.startswith("[") and x.endswith("]"): return x[1:-1]
    if x.startswith("`") and x.endswith("`"): return x[1:-1]
    if x.startswith('"') and x.endswith('"'): return x[1:-1]
    return x

def _split_qualified(name: str) -> Tuple[Optional[str], str]:
    s = (name or "").strip()
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

def _as_display(it: Dict[str, Any]) -> str:
    schema = it.get("schema") or _ci_get(it, "Schema") or ""
    nm = it.get("name") or _ci_get(it, "Original_Name") or _ci_get(it, "Safe_Name") or it.get("safe_name")
    return f"{schema}.{nm}" if schema and nm and "·" not in str(nm) else str(nm)

def _safe(schema: Optional[str], name: str) -> str:
    return f"{schema}·{name}" if schema else name

def _names_for_match(it: Dict[str, Any]) -> Tuple[Optional[str], List[str]]:
    schema = it.get("schema") or _ci_get(it, "Schema") or ""
    cands = []
    for k in ("name", "Original_Name", "Safe_Name", "safe_name"):
        v = it.get(k) or _ci_get(it, k)
        if not v: continue
        if isinstance(v, str):
            if "·" in v:
                parts = v.split("·", 1)
                if len(parts) == 2:
                    if not schema: schema = parts[0]
                    cands.append(parts[1])
                else:
                    cands.append(v)
            else:
                cands.append(v)
    seen = set(); out = []
    for n in cands:
        ln = n.lower()
        if ln in seen: continue
        seen.add(ln); out.append(n)
    return schema, out

def _find_item(items: List[Dict[str, Any]], kind: str, name: str, fuzzy: bool = False) -> Optional[Dict[str, Any]]:
    items = as_items_list(items)
    want_schema, want_base = _split_qualified(name)
    wl_schema = (want_schema or "").lower()
    wl_base   = (want_base or "").lower()
    k_l = (kind or "").lower()

    print(f"[ops] _find_item called with kind={kind}, name={name}, fuzzy={fuzzy}, want_schema={want_schema}, want_base={want_base}")

    for it in items:
        if (it.get("kind") or "").lower() != k_l:
            continue
        s, cands = _names_for_match(it)
        if wl_schema and (s or "").lower() != wl_schema:
            continue
        for nm in cands:
            if (nm or "").lower() == wl_base:
                return it

    print(f"[ops] _find_item exact match failed, trying wl_schema={wl_schema}, fuzzy={fuzzy}")

    if wl_schema:
        safe = _safe(want_schema, want_base).lower()
        for it in items:
            if (it.get("kind") or "").lower() != k_l:
                continue
            sname = (it.get("safe_name") or _ci_get(it, "Safe_Name") or "")
            if isinstance(sname, str) and sname.lower() == safe:
                return it
            
    print(f"[ops] _find_item schema-restricted match failed, trying fuzzy={fuzzy}")

    if not wl_schema:
        for it in items:
            if (it.get("kind") or "").lower() != k_l:
                continue
            _s, cands = _names_for_match(it)
            for nm in cands:
                if (nm or "").lower() == wl_base:
                    return it

    print(f"[ops] _find_item no exact match found, fuzzy={fuzzy}")

    if fuzzy:
        for it in items:
            if (it.get("kind") or "").lower() != k_l:
                continue
            _s, cands = _names_for_match(it)
            if any(wl_base in (nm or "").lower() for nm in cands):
                return it
            
    print(f"[ops] _find_item no match found")

    return None

def find_item(
    items: Dict[str, Any],
    kind: str,
    name: str,
    schema: Optional[str] = None,
) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """
    Deterministic resolver for an entity in catalog.json.

    Tries, in order:
      1) exact match on normalized name (with/without schema) against Safe_Name, Original_Name, and key
      2) if schema filter present, restrict to that schema
      3) fallback: substring match on Safe_Name if (1) fails
    Returns (fully_qualified_display, meta) or (None, None).
    """
    if not name:
        return None, None

    sec = _catalog_section(items, kind)
    if not sec:
        return None, None

    in_schema, in_name = _split_schema_and_name(name)
    wl_schema = _norm_ident(schema) if schema else None
    if in_schema:
        wl_schema = in_schema

    # Build candidates with normalized fields once
    candidates: Iterable[Tuple[str, Dict[str, Any], str, str, str, str]] = []
    built = []
    for safe, meta in sec.items():
        sch = _norm_ident(meta.get("Schema") or "")
        if wl_schema and sch != wl_schema:
            continue

        safe_name = meta.get("Safe_Name") or safe
        display_schema = meta.get("Schema") or ""
        fq_display = f"{display_schema}.{safe_name}" if display_schema else safe_name

        nm_safe   = _norm_ident(safe_name)
        nm_orig   = _norm_ident(meta.get("Original_Name") or safe_name)
        nm_key    = _norm_ident(safe)
        nm_full_a = _norm_ident(f"{display_schema}.{safe_name}") if display_schema else nm_safe
        nm_full_b = _norm_ident(f"{display_schema}.{meta.get('Original_Name') or safe_name}") if display_schema else nm_orig

        built.append((fq_display, meta, sch, nm_safe, nm_orig, nm_key, nm_full_a, nm_full_b))

    # 1) Exact match against name part (no schema)
    for fq_display, meta, sch, nm_safe, nm_orig, nm_key, nm_full_a, nm_full_b in built:
        if in_name in (nm_safe, nm_orig, nm_key):
            return fq_display, meta

    # 1b) Exact match against full qualified forms
    nm_query_full = _norm_ident(name)
    for fq_display, meta, sch, nm_safe, nm_orig, nm_key, nm_full_a, nm_full_b in built:
        if nm_query_full in (nm_full_a, nm_full_b):
            return fq_display, meta

    # 2) Fallback: substring match on Safe_Name
    for fq_display, meta, sch, nm_safe, nm_orig, nm_key, nm_full_a, nm_full_b in built:
        if in_name and in_name in nm_safe:
            return fq_display, meta

    return None, None

def _build_by_safe(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    items = as_items_list(items)
    by = {}
    for it in items:
        s = it.get("safe_name") or _ci_get(it, "Safe_Name")
        if not s:
            schema = it.get("schema") or _ci_get(it, "Schema") or ""
            nm = it.get("name") or _ci_get(it, "Original_Name") or _ci_get(it, "Safe_Name")
            if nm:
                s = _safe(schema, nm) if schema else nm
        if isinstance(s, str):
            by[s] = it
    return by

# -------------------- reference helpers --------------------

READ_KEYS  = ("Reads","reads","tables_read","Tables_Read","Tables_Reads","tables_reads","Referenced_Tables","references","References")
WRITE_KEYS = ("Writes","writes","tables_written","Tables_Written","tables_writes","Updates","updates","Modified_Tables")
CALL_KEYS  = ("Calls","calls","Procedure_Calls","proc_calls","Referenced_Procedures")
RET_COL_KEYS = ("ReturnColumns","Return_Columns","OutputColumns","Output_Columns","Returns","returns","Columns_Returned")

def _parse_doc_lists(doc: Optional[str]) -> Dict[str, List[str]]:
    out = {"reads": [], "writes": [], "calls": [], "returns": []}
    if not doc or not isinstance(doc, str):
        return out
    # crude parse of "reads: a, b" / "writes: x, y" / "calls: p, q" / "returns: c1, c2"
    for kind in out.keys():
        m = re.search(rf"(?im)^{kind}\s*:\s*(.+)$", doc)
        if m:
            toks = [t.strip() for t in re.split(r"[,\s]+", m.group(1)) if t.strip()]
            out[kind] = toks
    return out

def _collect_list_from_keys(it: Dict[str, Any], keys: Tuple[str, ...]) -> List[str]:
    for k in keys:
        v = it.get(k) or _ci_get(it, k)
        if isinstance(v, list):
            # Handle both string entries and dict entries with Safe_Name
            result = []
            for x in v:
                if isinstance(x, dict):
                    # Extract Safe_Name from dict entry
                    name = x.get("Safe_Name") or x.get("safe_name")
                    if name:
                        result.append(str(name))
                elif isinstance(x, str):
                    result.append(str(x))
            return result
        if isinstance(v, str):
            # tolerate comma-separated
            return [t.strip() for t in v.split(",") if t.strip()]
    return []

def _get_reads(it: Dict[str, Any]) -> List[str]:
    reads = _collect_list_from_keys(it, READ_KEYS)
    if not reads:
        doc = _ci_get(it, "Doc") or it.get("doc")
        reads = _parse_doc_lists(doc).get("reads", [])
    return reads

def _get_writes(it: Dict[str, Any]) -> List[str]:
    writes = _collect_list_from_keys(it, WRITE_KEYS)
    if not writes:
        doc = _ci_get(it, "Doc") or it.get("doc")
        writes = _parse_doc_lists(doc).get("writes", [])
    return writes

def _get_calls(it: Dict[str, Any]) -> List[str]:
    calls = _collect_list_from_keys(it, CALL_KEYS)
    if not calls:
        doc = _ci_get(it, "Doc") or it.get("doc")
        calls = _parse_doc_lists(doc).get("calls", [])
    return calls

def _get_return_cols(it: Dict[str, Any]) -> List[str]:
    cols = _collect_list_from_keys(it, RET_COL_KEYS)
    if cols:
        return cols
    doc = _ci_get(it, "Doc") or it.get("doc")
    parsed = _parse_doc_lists(doc).get("returns", [])
    return parsed

def _normalize_ref_name(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("`", "").replace('"', "")
    if s.startswith("[") and s.endswith("]"):
        s = s[1:-1]
    return s

# -------------------- Deterministic ops used by formatters --------------------

def list_all_of_kind(items: List[Dict[str, Any]], kind: str) -> List[str]:

    print(f"[ops] list_all_of_kind called with kind={kind} items={items}")

    items = as_items_list(items)

    print(f"[ops] list_all_of_kind found {len(items)} total items")
    print(f"[ops] sample item: {items[0] if items else 'N/A'}")

    res = []
    k = (kind or "").lower()
    for it in items:
        if (it.get("kind") or "").lower() == k:
            res.append(_as_display(it))
    res.sort(key=lambda s: s.lower())
    return res

def procs_access_table(items: List[Dict[str, Any]], table_name: str, fuzzy=False) -> List[Dict[str, Any]]:
    """Find procedures that SELECT (read from) a table using Referenced_By. If no AccessType, accept all for backward compat."""
    items = as_items_list(items)
    t = _find_item(items, "table", table_name, fuzzy=fuzzy)
    if not t:
        return []
    by_safe = _build_by_safe(items)
    refs = _ci_get(t, "Referenced_By") or []
    results: List[Dict[str, Any]] = []
    seen: Set[str] = set()

    def _compose_safe(entry: Any) -> Optional[str]:
        if not isinstance(entry, dict):
            return None
        sname = entry.get("Safe_Name")
        if not sname:
            return None
        if "·" in sname:
            return sname
        schema = entry.get("Schema") or ""
        return _safe(schema, sname) if schema else sname

    for e in refs:
        # Accept if AccessType is 'read' OR not specified (backward compat)
        access_type = e.get("AccessType") if isinstance(e, dict) else None
        # Skip writes explicitly - allow reads and None (for old catalog.json that has no AccessType)
        if access_type == "write":
            continue
        s = _compose_safe(e)
        if not s or s in seen:
            continue
        it = by_safe.get(s)
        if it and (it.get("kind") or "").lower() == "procedure":
            seen.add(s)
            results.append(it)

    results.sort(key=lambda it: (_ci_get(it, "Schema") or it.get("schema") or "", it.get("name") or _ci_get(it, "Original_Name") or _ci_get(it, "Safe_Name") or it.get("safe_name") or ""))
    return results

def procs_update_table(items: List[Dict[str, Any]], table_name: str) -> List[Dict[str, Any]]:
    """Find procedures that UPDATE/INSERT/DELETE (write to) a table using Referenced_By with AccessType='write'."""
    items = as_items_list(items)
    t = _find_item(items, "table", table_name, fuzzy=False)
    if not t:
        return []
    by_safe = _build_by_safe(items)
    refs = _ci_get(t, "Referenced_By") or []
    results: List[Dict[str, Any]] = []
    seen: Set[str] = set()

    def _compose_safe(entry: Any) -> Optional[str]:
        if not isinstance(entry, dict):
            return None
        sname = entry.get("Safe_Name")
        if not sname:
            return None
        if "·" in sname:
            return sname
        schema = entry.get("Schema") or ""
        return _safe(schema, sname) if schema else sname

    for e in refs:
        # Filter for writes only
        access_type = e.get("AccessType") if isinstance(e, dict) else None
        if access_type != "write":
            continue
        s = _compose_safe(e)
        if not s or s in seen:
            continue
        it = by_safe.get(s)
        if it and (it.get("kind") or "").lower() == "procedure":
            seen.add(s)
            results.append(it)

    results.sort(key=lambda it: (_ci_get(it, "Schema") or it.get("schema") or "", it.get("name") or _ci_get(it, "Original_Name") or _ci_get(it, "Safe_Name") or it.get("safe_name") or ""))
    return results

def views_access_table(items: List[Dict[str, Any]], table_name: str) -> List[Dict[str, Any]]:
    items = as_items_list(items)
    t = _find_item(items, "table", table_name, fuzzy=False)
    if not t:
        return []
    by_safe = _build_by_safe(items)
    refs = _ci_get(t, "Referenced_By") or []
    out = []
    seen: Set[str] = set()
    for e in refs:
        if not isinstance(e, dict): continue
        sname = e.get("Safe_Name")
        if not sname:
            continue
        if "·" not in sname:
            sch = e.get("Schema") or ""
            sname = _safe(sch, sname) if sch else sname
        if sname in seen: 
            continue
        it = by_safe.get(sname)
        if it and (it.get("kind") or "").lower() == "view":
            out.append(it); seen.add(sname)
    out.sort(key=lambda it: (_ci_get(it, "Schema") or "", it.get("name") or _ci_get(it, "Original_Name") or _ci_get(it, "Safe_Name") or ""))
    return out

def tables_accessed_by_procedure(items: List[Dict[str, Any]], proc_name: str) -> Tuple[List[str], List[str]]:
    items = as_items_list(items)
    it = _find_item(items, "procedure", proc_name, fuzzy=False)
    if not it:
        return [], []
    reads = sorted({_normalize_ref_name(x) for x in _get_reads(it)})
    writes = sorted({_normalize_ref_name(x) for x in _get_writes(it)})
    return reads, writes

def tables_accessed_by_view(items: List[Dict[str, Any]], view_name: str) -> List[str]:
    items = as_items_list(items)
    view = _find_item(items, "view", view_name, fuzzy=False)
    if not view:
        return []
    reads = sorted({_normalize_ref_name(x) for x in _get_reads(view)})
    if reads:
        return reads
    this_safe = view.get("safe_name") or _ci_get(view, "Safe_Name")
    if not this_safe:
        schema = view.get("schema") or _ci_get(view, "Schema") or ""
        nm = view.get("name") or _ci_get(view, "Original_Name") or _ci_get(view, "Safe_Name")
        if nm: this_safe = _safe(schema, nm) if schema else nm
    out = []
    for t in items:
        if (t.get("kind") or "").lower() != "table":
            continue
        for e in _ci_get(t, "Referenced_By") or []:
            if not isinstance(e, dict): continue
            sname = e.get("Safe_Name")
            if not sname:
                continue
            if "·" not in sname:
                sch = e.get("Schema") or ""
                sname = _safe(sch, sname) if sch else sname
            if sname == this_safe:
                out.append(_as_display(t))
                break
    out.sort(key=lambda s: s.lower())
    return out

def unaccessed_tables(items: List[Dict[str, Any]]) -> List[str]:
    items = as_items_list(items)
    referenced: Set[str] = set()

    for it in items:
        kind = (it.get("kind") or "").lower()
        if kind in ("procedure","view"):
            for r in _get_reads(it):
                referenced.add(_normalize_ref_name(r).lower())
            for w in _get_writes(it):
                referenced.add(_normalize_ref_name(w).lower())

    unused = []
    for t in items:
        if (t.get("kind") or "").lower() != "table":
            continue
        name = _as_display(t)
        refs = _ci_get(t, "Referenced_By") or []
        _, base = _split_qualified(name)
        if refs:
            # it's referenced explicitly somewhere
            continue
        if base.lower() in referenced:
            continue
        unused.append(name)

    unused.sort(key=lambda s: s.lower())
    return unused

def procs_called_by_procedure(items: List[Dict[str, Any]], proc_name: str) -> List[str]:
    items = as_items_list(items)
    it = _find_item(items, "procedure", proc_name, fuzzy=False)
    if not it:
        return []
    calls = {_normalize_ref_name(x) for x in _get_calls(it)}
    if not calls:
        return []
    out = sorted(calls, key=lambda s: s.lower())
    return out

def call_tree(items: List[Dict[str, Any]], proc_name: str, max_depth: int = 6) -> List[str]:
    items = as_items_list(items)
    start = _find_item(items, "procedure", proc_name, fuzzy=False)
    if not start:
        return []

    lines: List[str] = []
    seen: Set[str] = set()

    def rec(it: Dict[str, Any], depth: int):
        prefix = "  " * depth + ("- " if depth > 0 else "")
        disp = _as_display(it)
        lines.append(f"{prefix}{disp}")
        if depth >= max_depth:
            lines.append("  " * (depth + 1) + "…")
            return
        key = disp.lower()
        if key in seen:
            lines.append("  " * (depth + 1) + "(cycle)")
            return
        seen.add(key)
        for callee in _get_calls(it):
            callee = _normalize_ref_name(callee)
            it2 = _find_item(items, "procedure", callee, fuzzy=True)
            if it2:
                rec(it2, depth + 1)
            else:
                lines.append("  " * (depth + 1) + f"- {callee} (?)")

    rec(start, 0)
    return lines

def _extract_columns_from_item(it: Dict[str, Any]) -> List[Dict[str, Any]]:
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
                "referenced_in": _ci_get(c, "Referenced_In") or [],
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
                "referenced_in": _ci_get(meta, "Referenced_In") or [],
            })
        return out
    return []

def list_columns_of_table(items: List[Dict[str, Any]], table_name: str, fuzzy=False) -> Dict[str, Any]:
    items = as_items_list(items)
    it = _find_item(items, "table", table_name, fuzzy=fuzzy)
    if not it:
        return {"found": False, "match": None, "columns": []}
    cols = _extract_columns_from_item(it)
    return {"found": True, "match": it, "columns": cols}

def columns_returned_by_procedure(items: List[Dict[str, Any]], proc_name: str) -> List[str]:
    """
    Return columns that a procedure might return.
    Format: schema.TableName.ColumnName for columns we can resolve to actual tables.
    """
    items = as_items_list(items)
    it = _find_item(items, "procedure", proc_name, fuzzy=False)
    if not it:
        return []

    # Try documented returns first
    cols = _get_return_cols(it)
    if not cols:
        ret = _ci_get(it, "Returns")
        if isinstance(ret, dict):
            arr = _ci_get(ret, "Columns") or _ci_get(ret, "columns")
            if isinstance(arr, list):
                cols = [str(x) for x in arr]

    # Fallback: extract columns from Column_Refs and try to resolve table names
    if not cols:
        col_refs = _ci_get(it, "Column_Refs")
        reads = _ci_get(it, "Reads") or []

        if isinstance(col_refs, dict) and reads:
            # Build a mapping of table names (with and without schema) to full safe names
            table_map = {}  # lowercase name -> full safe name
            for r in reads:
                if isinstance(r, dict):
                    safe = r.get("Safe_Name", "")
                    if safe:
                        # Add both the full name and just the base name
                        table_map[safe.lower()] = safe
                        if "·" in safe:
                            base = safe.split("·", 1)[1]
                            table_map[base.lower()] = safe
                        else:
                            table_map[safe.lower()] = safe

            # Collect all columns with resolved table names
            qualified_cols = []
            for table_ref, columns in col_refs.items():
                # Try to resolve the table reference
                resolved_table = table_map.get(table_ref.lower())

                if isinstance(columns, (list, set)):
                    for col in columns:
                        if resolved_table:
                            # Format: schema.TableName.ColumnName (convert · to .)
                            table_display = resolved_table.replace("·", ".")
                            qualified_cols.append(f"{table_display}.{col}")
                        else:
                            # Can't resolve - just use the column name
                            qualified_cols.append(str(col))

            cols = sorted(set(qualified_cols), key=lambda s: s.lower())

    # normalize
    return [_normalize_ref_name(c) for c in cols]

def unused_columns_of_table(items: List[Dict[str, Any]], table_name: str) -> Optional[List[str]]:
    items = as_items_list(items)
    it = _find_item(items, "table", table_name, fuzzy=False)
    if not it:
        return None
    cols = _extract_columns_from_item(it)
    out = []
    for c in cols:
        refs = c.get("referenced_in") or []
        if not refs:
            out.append(c.get("name"))
    return out

# -------------------- SQL fetch / normalize / diff / similarity --------------------

_SQL_COM_LINE = re.compile(r"(?m)--.*?$")
_SQL_COM_BLOCK = re.compile(r"/\*.*?\*/", re.S)
_WS = re.compile(r"[ \t]+")

def normalize_sql(sql: str) -> str:
    if not sql: return ""
    s = _SQL_COM_BLOCK.sub("", _SQL_COM_LINE.sub("", sql))
    s = s.replace("\r\n","\n").replace("\r","\n")
    s = "\n".join(_WS.sub(" ", ln).rstrip() for ln in s.split("\n"))
    s = "\n".join([ln for ln in s.split("\n") if ln.strip() != "" or ln == "\n"])
    return s.strip("\n")

# def _get_entity(items: List[Dict[str, Any]], kind: Optional[str], name: str) -> Optional[Dict[str, Any]]:

#     print(f"[ops] _get_entity called with kind={kind} name={name}")

#     items = as_items_list(items)
#     if kind:
#         it = _find_item(items, kind, name, fuzzy=False)
#         if it: return it
#     for k in ("table","view","procedure","function"):
#         if kind and k.lower()!=kind.lower(): continue
#         it = _find_item(items, k, name, fuzzy=False)
#         if it: return it
#     return None

def _get_entity(items: List[Dict[str, Any]], kind: Optional[str], name: str) -> Optional[Dict[str, Any]]:
    """
    Resolve an entity by name. If kind is one of {table, view, procedure, function},
    prefer that kind; otherwise (None / '' / 'any' / 'entity') treat as wildcard.
    """
    items = as_items_list(items)
    kind_l = (kind or "").lower()
    valid = {"table", "view", "procedure", "function"}
    wildcard = (kind_l == "" or kind_l is None or kind_l not in valid or kind_l in {"any", "entity"})

    print(f"[ops] _get_entity called with kind={kind} name={name}")

    # 1) If kind is valid, try exact in that kind first
    if not wildcard:
        it = _find_item(items, kind_l, name, fuzzy=False)
        if it:
            return it

    # 2) Try across all kinds (or restricted if kind is valid)
    for k in ("table", "view", "procedure", "function"):
        if not wildcard and k != kind_l:
            continue
        it = _find_item(items, k, name, fuzzy=False)
        if it:
            return it

    # 3) Fuzzy fallback across allowed kinds
    for k in ("table", "view", "procedure", "function"):
        if not wildcard and k != kind_l:
            continue
        it = _find_item(items, k, name, fuzzy=True)
        if it:
            return it

    return None

def get_sql(items: List[Dict[str, Any]], kind: Optional[str], name: str):
    it = _get_entity(items, kind, name)
    if not it: return None, None, None
    sql, src = read_sql_from_item(it)
    return sql, src, _as_display(it)

def unified_diff(left_name: str,
                 left_sql: str,
                 right_name: str,
                 right_sql: str,
                 context=3,
                 ensure_git_header: bool = True) -> str:
    """
    Robust unified diff:
      - context: int (lines of context) or "full" to include the entire file
      - normalizes CR/LF
      - ensures trailing newline
      - prepends git-style header for diff2html
    """
    ls = (left_sql or "").replace("\r\n", "\n").replace("\r", "\n")
    rs = (right_sql or "").replace("\r\n", "\n").replace("\r", "\n")
    a = ls.splitlines()
    b = rs.splitlines()

    if context == "full":
        n = max(len(a), len(b))
    elif isinstance(context, int):
        n = max(0, context)
    else:
        n = 3

    raw = list(difflib.unified_diff(
        a, b,
        fromfile=f"a/{left_name}",
        tofile=f"b/{right_name}",
        n=n,
    ))

    norm = [ln.rstrip("\r\n") for ln in raw]

    if ensure_git_header:
        if not norm or not norm[0].startswith("--- "):
            norm = [f"--- a/{left_name}", f"+++ b/{right_name}"] + norm

    body = "\n".join(norm)
    if not body.endswith("\n"):
        body += "\n"

    return f"diff --git a/{left_name} b/{right_name}\n{body}"


_ID = r"(?:\[[^\]]+\]|[A-Za-z_][A-Za-z0-9_]*)"
TOK = re.compile(rf"{_ID}")

def _token_set(s: str) -> Set[str]:
    return {t.lower() for t in TOK.findall(s or "")}

def _table_struct_from_item(it: Dict[str, Any]) -> Dict[str, Any]:
    cols = _extract_columns_from_item(it)
    colset = { (c.get("name") or "").lower() for c in cols if c.get("name") }
    types = { (c.get("name") or "").lower(): (c.get("type") or "").lower() for c in cols if c.get("name") }
    pk = set(map(str.lower, _ci_get(it, "Primary_Key") or []))
    idxs = set()
    idx_obj = _ci_get(it, "Indexes") or {}
    if isinstance(idx_obj, dict):
        for iname, ccols in idx_obj.items():
            if isinstance(ccols, list):
                for c in ccols: idxs.add((str(iname).lower(), (c or "").lower()))
    return {"columns": colset, "types": types, "pk": pk, "idx": idxs}

def similarity_sql(items: List[Dict[str, Any]],
                   left_it: Optional[Dict[str, Any]],
                   right_it: Optional[Dict[str, Any]],
                   left_norm: str, right_norm: str) -> Dict[str, Any]:
    edit = difflib.SequenceMatcher(None, left_norm, right_norm).ratio()
    ls, rs = _token_set(left_norm), _token_set(right_norm)
    inter = len(ls & rs); uni = max(1, len(ls | rs))
    token_sim = inter / uni
    structure_sim = None
    if left_it and right_it and (left_it.get("kind") or "").lower()=="table" and (right_it.get("kind") or "").lower()=="table":
        lt, rt = _table_struct_from_item(left_it), _table_struct_from_item(right_it)
        c_inter = len(lt["columns"] & rt["columns"]); c_uni = max(1, len(lt["columns"] | rt["columns"]))
        structure_sim = c_inter / c_uni
    if structure_sim is not None:
        overall = 0.45*edit + 0.35*token_sim + 0.20*structure_sim
    else:
        overall = 0.6*edit + 0.4*token_sim
    return {
        "overall": round(overall*100, 1),
        "edit": round(edit*100, 1),
        "token": round(token_sim*100, 1),
        "structure": (round(structure_sim*100,1) if structure_sim is not None else None),
    }

def compare_sql(items: List[Dict[str, Any]],
                left_kind: Optional[str], left_name: str,
                right_kind: Optional[str], right_name: str) -> Dict[str, Any]:
    
    print(f"[ops] compare_sql called with left=({left_kind}, {left_name}) right=({right_kind}, {right_name})")

    """
    Compare two entities' CREATE SQL.
    Uses get_sql() so it works whether SQL is on disk exports or in the index.
    Also computes a similarity score and a unified diff string (for diff2html).
    """
    items = as_items_list(items)

    # Resolve entities (for structure similarity and display names)
    l_it = _get_entity(items, left_kind, left_name)
    r_it = _get_entity(items, right_kind, right_name)

    print(f"[ops] compare_sql found left_it: {l_it is not None}, right_it: {r_it is not None}")

    if not l_it or not r_it:
        missing = []
        if not l_it: missing.append(f"`{left_name}`")
        if not r_it: missing.append(f"`{right_name}`")
        return {"answer": f"Cannot compare: not found {', '.join(missing)}."}

    # Fetch SQL via helper (returns sql, source, display)
    l_sql, _, l_disp = get_sql(items, left_kind, left_name)
    r_sql, _, r_disp = get_sql(items, right_kind, right_name)

    print(f"[ops] compare_sql left : \n{l_sql}")
    print(f"[ops] compare_sql right: \n{r_sql}")

    # If both missing, bail out early
    if not l_sql and not r_sql:
        return {"answer": f"No SQL found for `{l_disp}` and `{r_disp}`."}

    # Comparison-only pretty formatting, then compute similarity + unified diff
    l_fmt = format_sql_for_diff(l_sql or "")
    r_fmt = format_sql_for_diff(r_sql or "")
    
    sim   = similarity_sql(items, l_it, r_it, l_fmt, r_fmt)
    # udiff = unified_diff(l_disp, l_fmt, r_disp, r_fmt, context=3)
    udiff = unified_diff(l_disp, l_fmt, r_disp, r_fmt, context="full")

    # Optional structural summary if they are tables
    summary_lines = []
    if (l_it.get("kind") or "").lower() == "table" and (r_it.get("kind") or "").lower() == "table":
        lt, rt = _table_struct_from_item(l_it), _table_struct_from_item(r_it)
        added = sorted([c for c in rt["columns"] if c not in lt["columns"]])
        removed = sorted([c for c in lt["columns"] if c not in rt["columns"]])
        changed = []
        for c in sorted(lt["columns"] & rt["columns"]):
            if (lt["types"].get(c) or "") != (rt["types"].get(c) or ""):
                changed.append((c, lt["types"].get(c, "?"), rt["types"].get(c, "?")))
        if added:
            summary_lines.append("**Added columns:** " + ", ".join(f"`{c}`" for c in added))
        if removed:
            summary_lines.append("**Removed columns:** " + ", ".join(f"`{c}`" for c in removed))
        if changed:
            summary_lines.append("**Type changes:** " + ", ".join(f"`{c}` {a} → {b}" for c, a, b in changed))

    kL = (l_it.get("kind") or "").lower()
    kR = (r_it.get("kind") or "").lower()
    same_kind = kL == kR
    title = f"Compare {kL if same_kind else f'{kL} vs {kR}'}: `{l_disp}` ⇄ `{r_disp}`"

    md = [
        f"## Compare {kL if same_kind else f'{kL} vs {kR}'}: `{l_disp}` ⇄ `{r_disp}`",
        f"**Similarity:** {sim['overall']}%  _(edit: {sim['edit']}%, token: {sim['token']}%{', structure: ' + str(sim['structure']) + '%' if sim.get('structure') is not None else ''})_",
    ]
    if summary_lines:
        md.append("\n".join(summary_lines))

    # 👇 append a code-fenced diff so any renderer will show line breaks
    # md.append("\n**Unified diff (normalized for readability):**\n")

    # 1) Markdown fenced diff (works well in terminals / MD renderers)
    # md.append(_fenced_diff(udiff))

    # 2) HTML <pre> fallback (ensures browsers show it with real newlines even if MD not parsed)
    # md.append(_html_pre(udiff))

    print(f"[ops] compare_sql udiff : {udiff}")

    return {"answer": "\n\n".join(md), "unified_diff": udiff}

def find_similar_sql(items: List[Dict[str, Any]],
                     kind: Optional[str], name: str,
                     threshold: float = 50.0) -> List[Tuple[str, float]]:
    """
    Find entities with similar SQL to the given entity.

    Args:
        items: Catalog items
        kind: Entity kind (table, view, procedure, function) or None for any
        name: Entity name to compare against
        threshold: Minimum similarity percentage (default: 50.0)

    Returns:
        List of (entity_name, similarity_score) tuples, sorted by similarity descending
    """
    print(f"[ops] find_similar_sql called with kind={kind}, name={name}, threshold={threshold}")

    items = as_items_list(items)

    # Resolve the source entity
    source_it = _get_entity(items, kind, name)
    if not source_it:
        print(f"[ops] find_similar_sql: source entity not found")
        return []

    source_kind = (source_it.get("kind") or "").lower()
    source_sql, _, source_disp = get_sql(items, kind, name)

    if not source_sql:
        print(f"[ops] find_similar_sql: no SQL found for source entity")
        return []

    # Format SQL for comparison
    source_fmt = format_sql_for_diff(source_sql)

    # Find all entities of the same kind
    candidates = [it for it in items if (it.get("kind") or "").lower() == source_kind]

    print(f"[ops] find_similar_sql: found {len(candidates)} candidates of kind {source_kind}")

    # Compare each candidate with the source
    results = []
    for candidate_it in candidates:
        candidate_name = _as_display(candidate_it)

        # Skip self
        if candidate_name.lower() == source_disp.lower():
            continue

        # Get SQL for candidate
        candidate_sql, src = read_sql_from_item(candidate_it)
        if not candidate_sql:
            continue

        # Format and compute similarity
        candidate_fmt = format_sql_for_diff(candidate_sql)
        sim = similarity_sql(items, source_it, candidate_it, source_fmt, candidate_fmt)

        similarity_score = sim["overall"]

        # Only include if above threshold
        if similarity_score >= threshold:
            results.append((candidate_name, similarity_score))

    # Sort by similarity descending
    results.sort(key=lambda x: x[1], reverse=True)

    print(f"[ops] find_similar_sql: found {len(results)} entities above threshold {threshold}%")

    return results

# --- Pretty printer used only for comparison diffs ---

# Expand keyword set so major clauses always start new lines.
# (Order matters: longer patterns first.)
_KW_SEQ = [
    r'\bCREATE\s+PROCEDURE\b',
    r'\bCREATE\s+FUNCTION\b',
    r'\bCREATE\s+VIEW\b',
    r'\bCREATE\s+TABLE\b',
    r'\bINSERT\s+INTO\b',
    r'\bSELECT\b',
    r'\bUPDATE\b',
    r'\bDELETE\b',
    r'\bSET\b',
    r'\bVALUES\b',
    r'\bGROUP\s+BY\b',
    r'\bORDER\s+BY\b',
    r'\bHAVING\b',
    r'\bLEFT\s+OUTER\s+JOIN\b',
    r'\bRIGHT\s+OUTER\s+JOIN\b',
    r'\bFULL\s+OUTER\s+JOIN\b',
    r'\bINNER\s+JOIN\b',
    r'\bCROSS\s+JOIN\b',
    r'\bLEFT\s+JOIN\b',
    r'\bRIGHT\s+JOIN\b',
    r'\bOUTER\s+JOIN\b',
    r'\bJOIN\b',
    r'\bFROM\b',
    r'\bWHERE\b',
    r'\bON\b',
    r'\bBEGIN\b',
    r'\bEND\b',
    r'\bAS\b',
]

def _newline_around_keywords(s: str) -> str:
    """
    Put each keyword on its own line (case-insensitive).
    Ensures a line break BEFORE the keyword if not already at bol.
    """
    for pat in _KW_SEQ:
        s = re.sub(pat, lambda m: ("\n" if not s[:m.start()].endswith("\n") else "") + m.group(0), s, flags=re.IGNORECASE)
    return s

def _newline_after_commas_semicolons(s: str) -> str:
    """
    Helpful for long SET/SELECT lists: break after commas/semicolons unless already newline.
    """
    # comma followed by optional space that is not already newline -> comma + newline
    s = re.sub(r",(?!\s*\n)", ",\n", s)
    # semicolon ends a statement
    s = re.sub(r";(?!\s*\n)", ";\n", s)
    return s

def _indent_parentheses(s: str, indent: str = "  ") -> str:
    """
    Put every '(' and ')' on its own line and indent the content between them.
    This is a simple structural formatter (not a full SQL parser).
    """
    parts = re.split(r"([()])", s)
    out_lines = []
    level = 0

    def emit_text_block(block: str):
        nonlocal out_lines, level
        if not block:
            return
        for ln in block.splitlines():
            t = ln.strip()
            if t == "":
                continue
            out_lines.append(f"{indent*level}{t}")

    for p in parts:
        if p == "(":
            out_lines.append(f"{indent*level}(")
            level += 1
        elif p == ")":
            level = max(0, level - 1)
            out_lines.append(f"{indent*level})")
        else:
            emit_text_block(p)

    # collapse consecutive blank lines
    cleaned = []
    prev_blank = False
    for ln in out_lines:
        blank = (ln.strip() == "")
        if blank and prev_blank:
            continue
        cleaned.append(ln.rstrip())
        prev_blank = blank
    # ensure trailing newline for a nicer diff
    return ("\n".join(cleaned).strip() + "\n") if cleaned else ""

def format_sql_for_diff(sql: str) -> str:
    """
    Comparison-only pretty format:
      - strip comments and normalize line endings
      - keywords begin their own line
      - break after commas/semicolons
      - each '(' and ')' on its own line, nested content indented
      - BUT: keep numeric size specifiers like (18), (30), (18, 4) inline
    """
    if not sql:
        return ""
    # strip comments, normalize newlines
    s = _SQL_COM_BLOCK.sub("", _SQL_COM_LINE.sub("", sql))
    s = s.replace("\r\n", "\n").replace("\r", "\n")

    # 🔒 protect numeric-only parens (sizes) so we won't break them
    s, protected = _protect_numeric_parens(s)

    # normal clause/line shaping
    s = _newline_around_keywords(s)
    s = _newline_after_commas_semicolons(s)

    # break and indent remaining parentheses (the protected ones are placeholders now)
    s = _indent_parentheses(s, indent="  ")

    # 🔓 restore protected numeric parens exactly as they were
    s = _restore_numeric_parens(s, protected)

    return s

def _fenced_diff(udiff: str) -> str:
    return "```diff\n" + udiff.replace("```", "``\\`") + "\n```"

def _html_pre(udiff: str) -> str:
    return '<pre class="udiff" style="white-space:pre-wrap; margin:0">{}</pre>'.format(html.escape(udiff))

# Matches any simple, non-nested parenthesis group: "( ... )"
_NUM_PAREN_GLOB = re.compile(r"\(([^()]+)\)")
# Inside the parens: one or more integers (optionally negative), comma-separated, with optional spaces
_NUM_LIST = re.compile(r"^\s*-?\d+\s*(,\s*-?\d+)*\s*$")

def _protect_numeric_parens(s: str):
    """
    Replace (18), (30) or (18, 4) etc. with placeholders so we don't
    explode them into new lines/indentation during diff formatting.
    Returns (protected_string, mapping).
    """
    replacements = {}

    def repl(m: re.Match):
        inner = m.group(1)
        if _NUM_LIST.match(inner or ""):
            key = f"__PAREN_NUM_{len(replacements)}__"
            replacements[key] = f"({inner})"
            return key
        return m.group(0)

    protected = _NUM_PAREN_GLOB.sub(repl, s)
    return protected, replacements

def _restore_numeric_parens(s: str, replacements: dict):
    for k, v in replacements.items():
        s = s.replace(k, v)
    return s

# --- compat + generic list helpers -----------------------------------------

def _iter_names_from_items(items, kind: str):
    """Yield fully-qualified names for a kind from items['catalog']."""
    kind = (kind or "").lower()
    catalog = (items or {}).get("catalog") or {}
    section_map = {"table": "Tables", "view": "Views", "procedure": "Procedures", "function": "Functions"}
    section = catalog.get(section_map.get(kind, ""), {}) or {}
    for safe, meta in section.items():
        #schema = meta.get("Schema") or ""
        name = meta.get("Safe_Name") or safe
        # fq = f"{schema}.{name}" if schema else name
        fq = name
        yield fq

def list_all_of_kind(items, kind: str, schema: str | None = None, name_pattern: str | None = None):
    """Generic lister used by all list_all_* wrappers."""
    import re
    names = list(_iter_names_from_items(items, kind))
    if schema:
        wl_schema = schema.lower().strip("[]")
        names = [n for n in names if (n.split(".", 1)[0].lower().strip("[]") == wl_schema)]
    if name_pattern:
        rx = re.compile(name_pattern, re.I)
        names = [n for n in names if rx.search(n.split(".", 1)[-1])]
    return sorted(names, key=str.casefold)

def list_all_tables(items, schema: str | None = None, name_pattern: str | None = None, pattern: str | None = None):
    """Back-compat alias → list_all_of_kind('table', ...)"""
    if name_pattern is None and pattern is not None:
        name_pattern = pattern
    return list_all_of_kind(items, "table", schema=schema, name_pattern=name_pattern)

def list_all_views(items, schema: str | None = None, name_pattern: str | None = None, pattern: str | None = None):
    """Back-compat alias → list_all_of_kind('view', ...)"""
    if name_pattern is None and pattern is not None:
        name_pattern = pattern
    return list_all_of_kind(items, "view", schema=schema, name_pattern=name_pattern)

def list_all_procedures(items, schema: str | None = None, name_pattern: str | None = None, pattern: str | None = None):
    """Back-compat alias → list_all_of_kind('procedure', ...)"""
    if name_pattern is None and pattern is not None:
        name_pattern = pattern
    return list_all_of_kind(items, "procedure", schema=schema, name_pattern=name_pattern)

def list_all_functions(items, schema: str | None = None, name_pattern: str | None = None, pattern: str | None = None):
    """Back-compat alias → list_all_of_kind('function', ...)"""
    if name_pattern is None and pattern is not None:
        name_pattern = pattern
    return list_all_of_kind(items, "function", schema=schema, name_pattern=name_pattern)

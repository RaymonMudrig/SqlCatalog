from __future__ import annotations
import re
from typing import Optional, Dict, Any, List
from qcli.resolver import resolve_items_by_name
from qcat.intents import IntentId

_QUOTED = re.compile(r"""['"`\[]\s*([A-Za-z0-9_ .%]+?)\s*['"`\]]""")
_TOKEN   = re.compile(r"[A-Za-z0-9_]+")

def _norm(s: Optional[str]) -> str:
    return (s or "").lower().strip()

def _pick_quoted(text: str) -> Optional[str]:
    m = None
    for mm in _QUOTED.finditer(text):
        if (m is None) or (len(mm.group(1)) > len(m.group(1))):
            m = mm
    return m.group(1) if m else None

def _extract_after(text: str, keywords: List[str]) -> Optional[str]:
    t = " " + _norm(text) + " "
    for kw in keywords:
        kwl = " " + kw + " "
        i = t.find(kwl)
        if i >= 0:
            tail = t[i+len(kwl):]
            mt = _TOKEN.search(tail)
            if mt:
                return mt.group(0)
    return None

def _guess_sql_kind_from_phrase(q: str) -> Optional[str]:
    ql = _norm(q)
    if " table " in f" {ql} ": return "table"
    if " procedure " in f" {ql} " or " proc " in f" {ql} ": return "procedure"
    if " view " in f" {ql} ": return "view"
    if " function " in f" {ql} ": return "function"
    return None

def _has_any(q: str, needles: List[str]) -> bool:
    ql = _norm(q)
    return any(n in ql for n in needles)

def parse_prompt(prompt: str, items: List[Dict[str, Any]]) -> Dict[str, Any]:
    q = prompt or ""
    ql = _norm(q)

    include_via_views = _has_any(ql, ["via view", "through view", "including views", "include view"])
    fuzzy = _has_any(ql, ["similar", "fuzzy", "approx"])
    unused_only = _has_any(ql, ["unused", "unaccessed", "unreferenced", "not used"])

    # schema filter
    schema = None
    msch = re.search(r"(?:in|from)\s+schema\s+([A-Za-z0-9_]+)", ql)
    if msch: schema = msch.group(1)

    # name pattern (SQL LIKE)
    pattern = None
    if re.search(r"\b(like|matching|pattern)\b", ql):
        p = _pick_quoted(q)
        pattern = p if p else None

    # list-all detection
    if re.search(r"\b(list|show|print)\s+(all\s+)?tables?\b", ql) or ql in {"tables", "list tables", "show tables"}:
        return {"intent":"list_all_tables","name":None,"kind":"table","include_via_views":False,
                "fuzzy":False,"unused_only":False,"schema":schema,"pattern":pattern}
    if re.search(r"\b(list|show|print)\s+(all\s+)?views?\b", ql):
        return {"intent":"list_all_views","name":None,"kind":"view","include_via_views":False,
                "fuzzy":False,"unused_only":False,"schema":schema,"pattern":pattern}
    if re.search(r"\b(list|show|print)\s+(all\s+)?(procedures|procs|sprocs)\b", ql):
        return {"intent":"list_all_procedures","name":None,"kind":"procedure","include_via_views":False,
                "fuzzy":False,"unused_only":False,"schema":schema,"pattern":pattern}
    if re.search(r"\b(list|show|print)\s+(all\s+)?functions?\b", ql):
        return {"intent":"list_all_functions","name":None,"kind":"function","include_via_views":False,
                "fuzzy":False,"unused_only":False,"schema":schema,"pattern":pattern}

    name = _pick_quoted(q)
    if not name:
        for block in (["table", "on table", "from table", "table named"],
                      ["procedure", "proc", "stored procedure"],
                      ["view"],
                      ["function"]):
            n2 = _extract_after(q, block)
            if n2:
                name = n2
                break

    intent: Optional[IntentId] = None
    kind: Optional[str] = None

    if "call tree" in ql or "call graph" in ql:
        intent = "call_tree"; kind = "procedure"

    if intent is None and ("which procedure" in ql or "which procedures" in ql or "what procedure" in ql):
        if any(w in ql for w in ["update", "insert", "delete", "write", "writes", "modify", "updating", "writing"]):
            intent = "procs_update_table"; kind = "table"
        elif any(w in ql for w in ["access", "use", "uses", "read", "select", "reference", "references", "referencing"]):
            intent = "procs_access_table"; kind = "table"

    if intent is None and ("which view" in ql or "which views" in ql):
        if any(w in ql for w in ["access", "use", "uses", "read", "select", "reference", "references"]):
            intent = "views_access_table"; kind = "table"

    if intent is None and ("what table" in ql or "what tables" in ql or "which tables" in ql):
        if "procedure" in ql or "proc" in ql:
            intent = "tables_accessed_by_procedure"; kind = "procedure"
        elif "view" in ql:
            intent = "tables_accessed_by_view"; kind = "view"

    if intent is None and any(p in ql for p in ["unaccessed tables", "unused tables", "unreferenced tables", "not accessed tables"]):
        intent = "unaccessed_tables"; kind = "table"

    if intent is None and any(p in ql for p in ["procedures called by", "procs called by"]) and ("procedure" in ql or "proc" in ql):
        intent = "procs_called_by_procedure"; kind = "procedure"

    if intent is None and any(p in ql for p in [
        "list columns", "list column", "columns of", "column of",
        "describe table", "schema of", "explain what", "explain table"
    ]):
        kind = _guess_sql_kind_from_phrase(ql) or "table"
        intent = "list_columns_of_table" if kind == "table" else intent

    if intent is None and any(p in ql for p in ["columns returned by", "result set of", "output columns of"]):
        intent = "columns_returned_by_procedure"; kind = "procedure"

    if intent is None and any(p in ql for p in ["unused columns of", "unreferenced columns of", "not used columns of"]):
        intent = "unused_columns_of_table"; kind = "table"

    if intent is None and any(p in ql for p in ["print create", "show ddl", "create sql", "ddl of", "show create", "definition of"]):
        intent = "sql_of_entity"; kind = _guess_sql_kind_from_phrase(ql)

    if kind is None:
        kind = _guess_sql_kind_from_phrase(ql)

    if name and kind is None:
        for knd in ["table", "view", "procedure", "function"]:
            m = resolve_items_by_name(items, knd, name, strict=True)
            if m:
                kind = knd
                break

    if intent in ("procs_access_table", "procs_update_table"):
        include_via_views = True or include_via_views

    return {
        "intent": intent,
        "name": name,
        "kind": kind,
        "include_via_views": include_via_views,
        "fuzzy": False if intent and str(intent).startswith("list_all_") else fuzzy,
        "unused_only": unused_only,
        "schema": schema,
        "pattern": pattern,
    }

from __future__ import annotations
from typing import Dict, Any, List, Optional
from qcat.intents import parse_intent
from qcat import formatters as F

def agent_answer(query: str, items: List[Dict[str, Any]], emb=None,
                 schema_filter: Optional[str]=None, name_pattern: Optional[str]=None) -> Dict[str, Any]:
    """
    Returns a dict with at least {"answer": "..."} and optionally {"unified_diff": "..."} for diff2html.
    """
    p = parse_intent(query)
    itype = p.get("intent")

    print(f"[agent] parsed intent: {itype} with params {p}")

    # ---- new compare intent ----
    if itype == "compare_sql":
        L, R = p.get("left") or {}, p.get("right") or {}
        out = F.render_compare_sql(items, L.get("kind"), L.get("name"), R.get("kind"), R.get("name"))
        return out if isinstance(out, dict) else {"answer": str(out), "parsed": p}

    # ---- legacy intents preserved ----
    if itype == "procs_access_table":
        return {"answer": F.render_procs_access_table(items, p["name"]), "parsed": p}

    if itype == "procs_update_table":
        return {"answer": F.render_procs_update_table(items, p["name"]), "parsed": p}

    if itype == "views_access_table":
        return {"answer": F.render_views_access_table(items, p["name"]), "parsed": p}

    if itype == "tables_accessed_by_procedure":
        return {"answer": F.render_tables_accessed_by_procedure(items, p["name"]), "parsed": p}

    if itype == "tables_accessed_by_view":
        return {"answer": F.render_tables_accessed_by_view(items, p["name"]), "parsed": p}

    if itype == "unaccessed_tables":
        return {"answer": F.render_unaccessed_tables(items), "parsed": p}

    if itype == "procs_called_by_procedure":
        return {"answer": F.render_procs_called_by_procedure(items, p["name"]), "parsed": p}

    if itype == "call_tree":
        return {"answer": F.render_call_tree(items, p["name"], depth=6), "parsed": p}

    if itype == "list_columns_of_table":
        return {"answer": F.render_list_columns_of_table(items, p["name"]), "parsed": p}

    if itype == "columns_returned_by_procedure":
        return {"answer": F.render_columns_returned_by_procedure(items, p["name"]), "parsed": p}

    if itype == "unused_columns_of_table":
        return {"answer": F.render_unused_columns_of_table(items, p["name"]), "parsed": p}

    if itype == "sql_of_entity":
        return {"answer": F.render_sql_of_entity(items, p.get("kind"), p["name"]), "parsed": p}

    # explicit list-all intents
    if itype == "list_all_tables":
        return {"answer": F.render_list_all_of_kind(items, "table"), "parsed": p}
    if itype == "list_all_views":
        return {"answer": F.render_list_all_of_kind(items, "view"), "parsed": p}
    if itype == "list_all_procedures":
        return {"answer": F.render_list_all_of_kind(items, "procedure"), "parsed": p}
    if itype == "list_all_functions":
        return {"answer": F.render_list_all_of_kind(items, "function"), "parsed": p}

    # counts (not in your legacy list, but keeping it as it was working)
    if itype == "count_of_kind":
        return {"answer": F.render_count_of_kind(items, p.get("kind")), "parsed": p}

    # fallback
    return {"answer": f"Sorry, I couldn't understand. Parsed: {p}", "parsed": p}

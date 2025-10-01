from __future__ import annotations
from typing import List, Dict, Any, Optional

from qcat.loader import load_items, load_emb
from qcat.name_match import detect_kind
from qcat.search import semantic_search
from qcat.relations import procs_accessing_table as legacy_relations
from qcat.llm import llm_answer
from qcat.intents import IntentId
from qcat import formatters as F

LISTING_INTENTS = {
    "procs_access_table",
    "procs_update_table",
    "views_access_table",
    "tables_accessed_by_procedure",
    "tables_accessed_by_view",
    "unaccessed_tables",
    "procs_called_by_procedure",
    "call_tree",
    "list_columns_of_table",
    "columns_returned_by_procedure",
    "unused_columns_of_table",
    "list_all_tables",
    "list_all_views",
    "list_all_procedures",
    "list_all_functions",
}

def agent_answer(
    query: str,
    items: Optional[List[Dict[str, Any]]] = None,
    emb = None,
    k: int = 12,
    kind: str = "any",
    schema: Optional[str] = None,
    unused_only: bool = False,
    name_mode: str = "smart",
    forced_table: Optional[str] = None,
    include_via_views: bool = True,
    include_dynamic: bool = True,
    intent: Optional[IntentId] = None,
    name: Optional[str] = None,
    depth: int = 3,
    fuzzy: bool = False,
    # NEW filters
    schema_filter: Optional[str] = None,
    name_pattern: Optional[str] = None,
) -> Dict[str, Any]:
    items = items or load_items()
    emb = emb if emb is not None else load_emb()

    # Deterministic listing intents (no LLM)
    if intent in LISTING_INTENTS:
        if intent == "list_all_tables":
            ans = F.render_list_all_of_kind(items, "table", schema_filter, name_pattern)
        elif intent == "list_all_views":
            ans = F.render_list_all_of_kind(items, "view", schema_filter, name_pattern)
        elif intent == "list_all_procedures":
            ans = F.render_list_all_of_kind(items, "procedure", schema_filter, name_pattern)
        elif intent == "list_all_functions":
            ans = F.render_list_all_of_kind(items, "function", schema_filter, name_pattern)
        elif intent == "procs_access_table" and name:
            ans = F.render_procs_access_table(items, name)
        elif intent == "procs_update_table" and name:
            ans = F.render_procs_update_table(items, name)
        elif intent == "views_access_table" and name:
            ans = F.render_views_access_table(items, name)
        elif intent == "tables_accessed_by_procedure" and name:
            ans = F.render_tables_accessed_by_procedure(items, name)
        elif intent == "tables_accessed_by_view" and name:
            ans = F.render_tables_accessed_by_view(items, name)
        elif intent == "unaccessed_tables":
            ans = F.render_unaccessed_tables(items)
        elif intent == "procs_called_by_procedure" and name:
            ans = F.render_procs_called_by_procedure(items, name)
        elif intent == "call_tree" and name:
            ans = F.render_call_tree(items, name, depth=depth)
        elif intent == "list_columns_of_table" and name:
            ans = F.render_list_columns_of_table(items, name)
        elif intent == "columns_returned_by_procedure" and name:
            ans = F.render_columns_returned_by_procedure(items, name)
        elif intent == "unused_columns_of_table" and name:
            ans = F.render_unused_columns_of_table(items, name)
        else:
            ans = "Unsupported or missing object name."
        return {"answer": ans, "intent": intent, "picked": [], "sections": []}

    # Legacy relation fallback (if user asked but parser didn’t catch)
    handled, picked, sections = legacy_relations(
        query, items,
        name_mode=name_mode,
        forced_table=forced_table,
        include_via_views=include_via_views,
        include_dynamic=include_dynamic,
    )
    if handled:
        lines = [f"**Results for:** `{query}`"]
        for sec in sections:
            lines.append(f"\n**{sec.get('title')}**")
            if not sec.get("results"): lines.append("- (none)"); continue
            for row in sec["results"]:
                it = row["item"]; how = row["access"]
                disp = f"{(it.get('schema')+'.' if it.get('schema') else '')}{it.get('name') or it.get('safe_name')}"
                lines.append(f"- {it.get('kind')} `{disp}` — **{how}**")
        return {"answer": "\n".join(lines), "picked": picked, "sections": sections}

    # Open-ended explanatory → semantic + LLM summary
    from qcat.search import semantic_search
    from qcat.llm import llm_answer
    auto = detect_kind(query)
    use_kind = kind if kind != "any" else (auto or "any")
    picked = semantic_search(query, items, emb, k=k, kind=use_kind, schema=schema, unused_only=unused_only) or []
    ans = llm_answer(query, picked) or _fallback_semantic_answer(query, picked)
    return {"answer": ans, "picked": picked, "sections": []}

def _fallback_semantic_answer(query: str, picked: List[Dict[str, Any]]) -> str:
    if not picked:
        return f"**No results** for: `{query}`."
    lines = [f"**Top matches** for: `{query}`"]
    for it in picked:
        disp = f"{(it.get('schema')+'.' if it.get('schema') else '')}{it.get('name') or it.get('safe_name')}"
        status = it.get("status") or ""
        lines.append(f"- {it.get('kind')} `{disp}` {f'— {status}' if status else ''}")
    return "\n".join(lines)

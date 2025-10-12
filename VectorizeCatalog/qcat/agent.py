# qcat/agent.py
from __future__ import annotations
from typing import Any, Dict, Optional, Tuple
from qcat import ops as K
from qcat import formatters as F
from qcat.llm_intent import classify_intent
from qcat.intents import label_of

CONFIRM_THRESHOLD = 0.70  # propose if below this

def agent_answer(
    query: str,
    items: Dict[str, Any],
    emb: Optional[Any] = None,
    schema_filter: Optional[str] = None,
    name_pattern: Optional[str] = None,
    intent_override: Optional[Dict[str, Any]] = None,
    accept_proposal: bool = False,
) -> Dict[str, Any]:
    """
    Agentic dispatcher:
      - classify prompt -> intent (+ args) with confidence
      - if confidence >= threshold OR user accepted proposal -> execute
      - else return a proposal (needs_confirmation=True)
    """
    # 1) pick intent: override > classify
    if intent_override:
        L = dict(intent_override)
        L.setdefault("confidence", 1.0)
        L.setdefault("source", "override")
    else:
        L = classify_intent(query)

    print(f"[agent_answer] Classified intent: {L}")

    # If user accepted proposal, force execution
    if accept_proposal and L.get("intent") != "semantic":
        L["confidence"] = max(0.99, float(L.get("confidence", 0)))

    intent = L.get("intent")

    # 2) If not confident, propose to user
    if intent == "semantic" or float(L.get("confidence", 0)) < CONFIRM_THRESHOLD:
        # Build a short human message
        guess = L.get("intent", "semantic")
        guess_label = label_of(guess) if guess in F.INTENT_TO_RENDERER else "semantic"
        props = {k: v for k, v in L.items() if k not in ("intent", "confidence", "source")}
        return {
            "answer": f"I think you meant: **{guess_label}**.\n\n"
                      f"_Parsed:_ `{props}`\n\n"
                      f"Click **Accept** to run this, or refine your question.",
            "needs_confirmation": True,
            "proposal": L,
        }

    # 3) Confident: execute via formatters/ops
    # All your existing render_* functions are kept intact.
    try:
        if intent == "list_all_tables":
            return {"answer": F.render_list_all_tables(items, schema=L.get("schema"), name_pattern=L.get("pattern"))}
        if intent == "list_all_views":
            return {"answer": F.render_list_all_views(items, schema=L.get("schema"), name_pattern=L.get("pattern"))}
        if intent == "list_all_procedures":
            return {"answer": F.render_list_all_procedures(items, schema=L.get("schema"), name_pattern=L.get("pattern"))}
        if intent == "list_all_functions":
            return {"answer": F.render_list_all_functions(items, schema=L.get("schema"), name_pattern=L.get("pattern"))}

        if intent == "list_columns_of_table":
            return {"answer": F.render_list_columns_of_table(items, L.get("name"), schema_filter)}

        if intent == "procs_access_table":
            return {"answer": F.render_procs_access_table(items, L.get("name"))}
        if intent == "procs_update_table":
            return {"answer": F.render_procs_update_table(items, L.get("name"))}
        if intent == "views_access_table":
            return {"answer": F.render_views_access_table(items, L.get("name"))}
        if intent == "tables_accessed_by_procedure":
            return {"answer": F.render_tables_accessed_by_procedure(items, L.get("name"))}
        if intent == "tables_accessed_by_view":
            return {"answer": F.render_tables_accessed_by_view(items, L.get("name"))}
        if intent == "unaccessed_tables":
            return {"answer": F.render_unaccessed_tables(items)}
        if intent == "procs_called_by_procedure":
            return {"answer": F.render_procs_called_by_procedure(items, L.get("name"))}
        if intent == "call_tree":
            return {"answer": F.render_call_tree(items, L.get("name"))}
        if intent == "columns_returned_by_procedure":
            return {"answer": F.render_columns_returned_by_procedure(items, L.get("name"))}
        if intent == "unused_columns_of_table":
            return {"answer": F.render_unused_columns_of_table(items, L.get("name"))}

        if intent == "sql_of_entity":
            print(f"[agent_answer] Executing sql_of_entity with kind={L.get('kind')} name={L.get('name')}")
            return {"answer": F.render_sql_of_entity(items, L.get("kind") or "any", L.get("name"))}

        if intent == "compare_sql":
            left = L.get("left")
            right = L.get("right")
            kind = L.get("kind") or "any"
            return F.render_compare_sql(items, kind, left, kind, right)

        # fallback – shouldn’t happen
        return {"answer": "Sorry, I couldn't resolve your intent."}

    except Exception as e:
        return {"answer": f"Error while executing intent `{intent}`: {e!r}"}

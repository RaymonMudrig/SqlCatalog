# webapp/agent.py
"""
Unified webapp agent - routes to cluster or qcat backends based on intent.

Flow:
  webapp.agent.agent_answer()
    → webapp.llm_intent.classify_intent() [SINGLE LLM CALL with ALL intents]
    → Dispatch based on backend:
        - cluster → cluster.ops functions
        - qcat → qcat.ops functions
    → Format with respective formatters
"""
from __future__ import annotations
from typing import Any, Dict, Optional
from webapp.llm_intent import classify_intent, ALL_INTENTS
from cluster import ops as cluster_ops, formatters as cluster_fmt
from qcat import ops as qcat_ops, formatters as qcat_fmt

CONFIRM_THRESHOLD = 0.70  # propose if below this


def agent_answer(
    query: str,
    qcat_service: Any,  # QcatService instance
    cluster_service: Any,  # ClusterService instance
    intent_override: Optional[Dict[str, Any]] = None,
    accept_proposal: bool = False,
) -> Dict[str, Any]:
    """
    Unified agentic dispatcher:
      1. LLM classifies intent (cluster or qcat)
      2. Check confidence threshold
      3. Dispatch to appropriate backend ops
      4. Format results as markdown

    Args:
        query: Natural language query
        qcat_service: QcatService instance
        cluster_service: ClusterService instance
        intent_override: Optional intent dict to force
        accept_proposal: Whether to accept low-confidence proposals

    Returns:
        Dict with answer, entities, etc.
    """
    # 1) Pick intent: override > classify
    if intent_override:
        L = dict(intent_override)
        L.setdefault("confidence", 1.0)
        L.setdefault("source", "override")
    else:
        L = classify_intent(query)

    print(f"[webapp.agent] Classified intent: {L}")

    # If user accepted proposal, force execution
    if accept_proposal and L.get("intent") != "semantic":
        L["confidence"] = max(0.99, float(L.get("confidence", 0)))

    intent = L.get("intent")
    backend = L.get("backend")

    # 2) If not confident, show available commands
    if intent == "semantic" or float(L.get("confidence", 0)) < CONFIRM_THRESHOLD:
        from cluster.intents import INTENT_LABELS as cluster_labels
        from qcat.intents import INTENT_LABELS as qcat_labels

        cluster_cmds = "\n".join([f"- **{label}** (`{intent_id}`)" for intent_id, label in cluster_labels.items()])
        qcat_cmds = "\n".join([f"- **{label}** (`{intent_id}`)" for intent_id, label in qcat_labels.items()])

        return {
            "answer": f"""## Could not resolve your command

I tried to understand your request but couldn't confidently determine what you want to do.

### Cluster Management Commands
{cluster_cmds}

### Catalog Query Commands
{qcat_cmds}

**Tip**: Try rephrasing your request to match one of the above command patterns.
""",
            "needs_confirmation": True,
            "proposal": L,
        }

    # 3) Confident: execute via appropriate backend ops
    try:
        if backend == "cluster":
            return _execute_cluster_intent(intent, L, cluster_service)
        elif backend == "qcat":
            return _execute_qcat_intent(intent, L, qcat_service)
        else:
            return {"answer": f"Unknown backend: {backend}"}

    except Exception as e:
        return {"answer": f"Error while executing intent `{intent}` (backend: {backend}): {e!r}"}


def _execute_cluster_intent(intent: str, params: Dict[str, Any], service: Any) -> Dict[str, Any]:
    """Execute cluster intent via cluster.ops"""
    state = service.state

    if intent == "rename_cluster":
        result = cluster_ops.rename_cluster(state, params.get("cluster_id"), params.get("new_name"))
        service._save_snapshot()
        return {"answer": cluster_fmt.render_rename_cluster(result)}

    if intent == "rename_group":
        result = cluster_ops.rename_group(state, params.get("group_id"), params.get("new_name"))
        service._save_snapshot()
        return {"answer": cluster_fmt.render_rename_group(result)}

    if intent == "move_group":
        result = cluster_ops.move_group(state, params.get("group_id"), params.get("cluster_id"))
        service._save_snapshot()
        return {"answer": cluster_fmt.render_move_group(result)}

    if intent == "move_procedure":
        result = cluster_ops.move_procedure(state, params.get("procedure"), params.get("cluster_id"))
        service._save_snapshot()
        return {"answer": cluster_fmt.render_move_procedure(result)}

    if intent == "delete_procedure":
        procedure_name = params.get("procedure_name") or params.get("procedure")
        if not procedure_name:
            return {"answer": "Error: No procedure name provided for deletion."}
        result = cluster_ops.delete_procedure(state, procedure_name)
        service._save_snapshot()
        return {"answer": cluster_fmt.render_delete_procedure(result)}

    if intent == "delete_table":
        result = cluster_ops.delete_table(state, params.get("table_name"))
        service._save_snapshot()
        return {"answer": cluster_fmt.render_delete_table(result)}

    if intent == "add_cluster":
        result = cluster_ops.add_cluster(state, params.get("cluster_id"), params.get("display_name"))
        service._save_snapshot()
        return {"answer": cluster_fmt.render_add_cluster(result)}

    if intent == "delete_cluster":
        result = cluster_ops.delete_cluster(state, params.get("cluster_id"))
        service._save_snapshot()
        return {"answer": cluster_fmt.render_delete_cluster(result)}

    if intent == "restore_procedure":
        result = cluster_ops.restore_procedure(
            state,
            params.get("procedure_name"),
            params.get("target_cluster_id"),
            params.get("force_new_group", False)
        )
        service._save_snapshot()
        return {"answer": cluster_fmt.render_restore_procedure(result)}

    if intent == "restore_table":
        result = cluster_ops.restore_table(state, params.get("trash_index"))
        service._save_snapshot()
        return {"answer": cluster_fmt.render_restore_table(result)}

    if intent == "list_trash":
        result = cluster_ops.list_trash(state)
        return {"answer": cluster_fmt.render_list_trash(result)}

    if intent == "empty_trash":
        result = cluster_ops.empty_trash(state)
        service._save_snapshot()
        return {"answer": cluster_fmt.render_empty_trash(result)}

    if intent == "get_cluster_summary":
        summary = cluster_ops.get_cluster_summary(state)
        return {"answer": cluster_fmt.render_cluster_summary(summary)}

    if intent == "get_cluster_detail":
        detail = cluster_ops.get_cluster_detail(state, params.get("cluster_id"))
        return {"answer": cluster_fmt.render_cluster_detail(detail)}

    return {"answer": f"Sorry, I couldn't handle cluster intent `{intent}`."}


def _execute_qcat_intent(intent: str, params: Dict[str, Any], service: Any) -> Dict[str, Any]:
    """Execute qcat intent via qcat.ops"""
    items = service.items
    emb = service.emb

    # Extract common params
    name = params.get("name")
    kind = params.get("kind")
    name_a = params.get("name_a")
    name_b = params.get("name_b")
    k = params.get("k", 10)

    if intent == "procs_access_table":
        result = qcat_ops.procs_access_table(items, name)
        return {"answer": qcat_fmt.render_procs_access_table(items, name), "entities": result}

    if intent == "procs_update_table":
        result = qcat_ops.procs_update_table(items, name)
        return {"answer": qcat_fmt.render_procs_update_table(items, name), "entities": result}

    if intent == "views_access_table":
        result = qcat_ops.views_access_table(items, name)
        return {"answer": qcat_fmt.render_views_access_table(items, name), "entities": result}

    if intent == "tables_accessed_by_procedure":
        result = qcat_ops.tables_accessed_by_procedure(items, name)
        return {"answer": qcat_fmt.render_tables_accessed_by_procedure(items, name), "entities": result}

    if intent == "tables_accessed_by_view":
        result = qcat_ops.tables_accessed_by_view(items, name)
        return {"answer": qcat_fmt.render_tables_accessed_by_view(items, name), "entities": result}

    if intent == "unaccessed_tables":
        result = qcat_ops.unaccessed_tables(items)
        return {"answer": qcat_fmt.render_unaccessed_tables(items), "entities": result}

    if intent == "procs_called_by_procedure":
        result = qcat_ops.procs_called_by_procedure(items, name)
        return {"answer": qcat_fmt.render_procs_called_by_procedure(items, name), "entities": result}

    if intent == "call_tree":
        result = qcat_ops.call_tree(items, name)
        return {"answer": qcat_fmt.render_call_tree(items, name), "entities": result}

    if intent == "list_columns_of_table":
        result = qcat_ops.list_columns_of_table(items, name)
        return {"answer": qcat_fmt.render_list_columns_of_table(items, name), "entities": result}

    if intent == "columns_returned_by_procedure":
        result = qcat_ops.columns_returned_by_procedure(items, name)
        return {"answer": qcat_fmt.render_columns_returned_by_procedure(items, name), "entities": result}

    if intent == "unused_columns_of_table":
        result = qcat_ops.unused_columns_of_table(items, name)
        return {"answer": qcat_fmt.render_unused_columns_of_table(items, name), "entities": result}

    if intent == "sql_of_entity":
        # render_sql_of_entity handles everything internally via get_sql()
        answer = qcat_fmt.render_sql_of_entity(items, kind, name)
        # Create entity dict for memory
        entity = {"kind": kind or "any", "name": name}
        return {"answer": answer, "entities": [entity], "contains_sql": True}

    if intent == "list_all_tables":
        schema = params.get("schema")
        pattern = params.get("pattern") or params.get("name_pattern")
        result = qcat_ops.list_all_tables(items, schema=schema, name_pattern=pattern)
        answer = qcat_fmt.render_list_all_tables(items, schema=schema, name_pattern=pattern)
        # Convert list of names to list of entity dicts for compatibility
        entities = [{"kind": "table", "name": n, "safe_name": n} for n in result]
        return {"answer": answer, "entities": entities}

    if intent == "list_all_views":
        schema = params.get("schema")
        pattern = params.get("pattern") or params.get("name_pattern")
        result = qcat_ops.list_all_views(items, schema=schema, name_pattern=pattern)
        answer = qcat_fmt.render_list_all_views(items, schema=schema, name_pattern=pattern)
        entities = [{"kind": "view", "name": n, "safe_name": n} for n in result]
        return {"answer": answer, "entities": entities}

    if intent == "list_all_procedures":
        schema = params.get("schema")
        pattern = params.get("pattern") or params.get("name_pattern")
        result = qcat_ops.list_all_procedures(items, schema=schema, name_pattern=pattern)
        answer = qcat_fmt.render_list_all_procedures(items, schema=schema, name_pattern=pattern)
        entities = [{"kind": "procedure", "name": n, "safe_name": n} for n in result]
        return {"answer": answer, "entities": entities}

    if intent == "list_all_functions":
        schema = params.get("schema")
        pattern = params.get("pattern") or params.get("name_pattern")
        result = qcat_ops.list_all_functions(items, schema=schema, name_pattern=pattern)
        answer = qcat_fmt.render_list_all_functions(items, schema=schema, name_pattern=pattern)
        entities = [{"kind": "function", "name": n, "safe_name": n} for n in result]
        return {"answer": answer, "entities": entities}

    if intent == "compare_sql":
        # Extract kinds if provided, otherwise None (auto-detect)
        kind_a = params.get("kind_a")
        kind_b = params.get("kind_b")
        result = qcat_ops.compare_sql(items, kind_a, name_a, kind_b, name_b)
        return {"answer": qcat_fmt.render_compare_sql(items, kind_a, name_a, kind_b, name_b), "unified_diff": result.get("unified_diff")}

    if intent == "find_similar_sql":
        result = qcat_ops.find_similar_sql(items, emb, name, k)
        threshold = params.get("threshold", 50.0)
        return {"answer": qcat_fmt.render_find_similar_sql(items, kind, name, threshold), "entities": result}

    return {"answer": f"Sorry, I couldn't handle qcat intent `{intent}`."}

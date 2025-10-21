# cluster/agent.py
"""
Semantic agent for cluster operations.
Follows the same workflow as qcat/agent.py:
  1. LLM classifies intent
  2. Check confidence threshold
  3. Dispatch to deterministic handlers (cluster/ops.py)
  4. Format results as markdown
"""
from __future__ import annotations
from typing import Any, Dict, Optional
from cluster.llm_intent import classify_intent
from cluster.intents import label_of, INTENTS
from cluster import formatters as F
from cluster import ops as O

# Import backend service type (will be passed in)
# from cluster.backend import ClusterService  # Import type only

CONFIRM_THRESHOLD = 0.70  # propose if below this

def agent_answer(
    query: str,
    cluster_service: Any,  # ClusterService instance
    intent_override: Optional[Dict[str, Any]] = None,
    accept_proposal: bool = False,
) -> Dict[str, Any]:
    """
    Agentic dispatcher for cluster operations:
      - classify prompt -> intent (+ args) with confidence
      - if confidence >= threshold OR user accepted proposal -> execute
      - else return a proposal (needs_confirmation=True)
    """
    # 1) Pick intent: override > classify
    if intent_override:
        L = dict(intent_override)
        L.setdefault("confidence", 1.0)
        L.setdefault("source", "override")
    else:
        L = classify_intent(query)

    print(f"[cluster.agent] Classified intent: {L}")

    # If user accepted proposal, force execution
    if accept_proposal and L.get("intent") != "semantic":
        L["confidence"] = max(0.99, float(L.get("confidence", 0)))

    intent = L.get("intent")

    # 2) If not confident, show available commands
    if intent == "semantic" or float(L.get("confidence", 0)) < CONFIRM_THRESHOLD:
        # Build list of available cluster commands
        from cluster.intents import INTENT_LABELS
        commands_list = "\n".join([f"- **{label}** (`{intent_id}`)" for intent_id, label in INTENT_LABELS.items()])

        return {
            "answer": f"""## Could not resolve your cluster command

I tried to understand your request but couldn't confidently determine what cluster operation you want.

### Available Cluster Management Commands
{commands_list}

**Tip**: Try rephrasing your request to match one of the above command patterns.
""",
            "needs_confirmation": True,
            "proposal": L,
        }

    # 3) Confident: execute via ops (deterministic) -> format
    try:
        state = cluster_service.state

        if intent == "rename_cluster":
            result = O.rename_cluster(state, L.get("cluster_id"), L.get("new_name"))
            cluster_service._save_snapshot()
            answer = F.render_rename_cluster(result)
            return {"answer": answer}

        if intent == "rename_group":
            result = O.rename_group(state, L.get("group_id"), L.get("new_name"))
            cluster_service._save_snapshot()
            answer = F.render_rename_group(result)
            return {"answer": answer}

        if intent == "move_group":
            result = O.move_group(state, L.get("group_id"), L.get("cluster_id"))
            cluster_service._save_snapshot()
            answer = F.render_move_group(result)
            return {"answer": answer}

        if intent == "move_procedure":
            result = O.move_procedure(state, L.get("procedure"), L.get("cluster_id"))
            cluster_service._save_snapshot()
            answer = F.render_move_procedure(result)
            return {"answer": answer}

        if intent == "delete_procedure":
            # Try both field names (LLM might use either)
            procedure_name = L.get("procedure_name") or L.get("procedure")
            if not procedure_name:
                return {"answer": "Error: No procedure name provided for deletion."}
            result = O.delete_procedure(state, procedure_name)
            cluster_service._save_snapshot()
            answer = F.render_delete_procedure(result)
            return {"answer": answer}

        if intent == "delete_table":
            result = O.delete_table(state, L.get("table_name"))
            cluster_service._save_snapshot()
            answer = F.render_delete_table(result)
            return {"answer": answer}

        if intent == "add_cluster":
            result = O.add_cluster(state, L.get("cluster_id"), L.get("display_name"))
            cluster_service._save_snapshot()
            answer = F.render_add_cluster(result)
            return {"answer": answer}

        if intent == "delete_cluster":
            result = O.delete_cluster(state, L.get("cluster_id"))
            cluster_service._save_snapshot()
            answer = F.render_delete_cluster(result)
            return {"answer": answer}

        if intent == "restore_procedure":
            result = O.restore_procedure(
                state,
                L.get("procedure_name"),
                L.get("target_cluster_id"),
                L.get("force_new_group", False)
            )
            cluster_service._save_snapshot()
            answer = F.render_restore_procedure(result)
            return {"answer": answer}

        if intent == "restore_table":
            result = O.restore_table(state, L.get("trash_index"))
            cluster_service._save_snapshot()
            answer = F.render_restore_table(result)
            return {"answer": answer}

        if intent == "list_trash":
            result = O.list_trash(state)
            answer = F.render_list_trash(result)
            return {"answer": answer}

        if intent == "empty_trash":
            result = O.empty_trash(state)
            cluster_service._save_snapshot()
            answer = F.render_empty_trash(result)
            return {"answer": answer}

        if intent == "get_cluster_summary":
            summary = O.get_cluster_summary(state)
            answer = F.render_cluster_summary(summary)
            return {"answer": answer}

        if intent == "get_cluster_detail":
            detail = O.get_cluster_detail(state, L.get("cluster_id"))
            answer = F.render_cluster_detail(detail)
            return {"answer": answer}

        # fallback
        return {"answer": f"Sorry, I couldn't handle intent `{intent}`."}

    except Exception as e:
        return {"answer": f"Error while executing intent `{intent}`: {e!r}"}

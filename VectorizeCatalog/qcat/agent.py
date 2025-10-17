# qcat/agent.py
from __future__ import annotations
from typing import Any, Dict, Optional, Tuple, List
from qcat import ops as K
from qcat import formatters as F
from qcat.llm_intent import classify_intent
from qcat.intents import label_of, INTENTS

CONFIRM_THRESHOLD = 0.70  # propose if below this

def _extract_entities_from_result(intent: str, result: Any, intent_params: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Extract entity information from intent operation results.
    Returns list of {kind, name} dicts.
    Excludes list_all_* intents as per requirement.
    """
    entities = []

    # Skip list_all_* intents
    if intent.startswith("list_all_"):
        return entities

    try:
        # procs_access_table, procs_update_table - returns List[Dict] of procedures
        if intent in ("procs_access_table", "procs_update_table"):
            for proc in result:
                if isinstance(proc, dict):
                    name = K._as_display(proc)
                    entities.append({"kind": "procedure", "name": name})

        # views_access_table - returns List[Dict] of views
        elif intent == "views_access_table":
            for view in result:
                if isinstance(view, dict):
                    name = K._as_display(view)
                    entities.append({"kind": "view", "name": name})

        # tables_accessed_by_procedure - returns (reads, writes) tuple of table names
        elif intent == "tables_accessed_by_procedure":
            if isinstance(result, tuple) and len(result) == 2:
                reads, writes = result
                for table_name in reads:
                    entities.append({"kind": "table", "name": table_name})
                for table_name in writes:
                    entities.append({"kind": "table", "name": table_name})

        # tables_accessed_by_view - returns List[str] of table names
        elif intent == "tables_accessed_by_view":
            for table_name in result:
                entities.append({"kind": "table", "name": table_name})

        # Also add the target entity from the intent params
        if "name" in intent_params:
            target_name = intent_params["name"]
            # Determine kind from intent
            if "table" in intent:
                entities.append({"kind": "table", "name": target_name})
            elif "proc" in intent or "procedure" in intent:
                entities.append({"kind": "procedure", "name": target_name})
            elif "view" in intent:
                entities.append({"kind": "view", "name": target_name})

    except Exception as e:
        print(f"[agent] Error extracting entities from {intent}: {e}")

    return entities

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
        guess_label = label_of(guess) if guess in INTENTS else "semantic"
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
        # Execute operation and collect entities
        op_result = None
        entities = []

        if intent == "list_all_tables":
            answer = F.render_list_all_tables(items, schema=L.get("schema"), name_pattern=L.get("pattern"))
            return {"answer": answer}

        if intent == "list_all_views":
            answer = F.render_list_all_views(items, schema=L.get("schema"), name_pattern=L.get("pattern"))
            return {"answer": answer}

        if intent == "list_all_procedures":
            answer = F.render_list_all_procedures(items, schema=L.get("schema"), name_pattern=L.get("pattern"))
            return {"answer": answer}

        if intent == "list_all_functions":
            answer = F.render_list_all_functions(items, schema=L.get("schema"), name_pattern=L.get("pattern"))
            return {"answer": answer}

        if intent == "list_columns_of_table":
            answer = F.render_list_columns_of_table(items, L.get("name"), schema_filter)
            # Add the table itself to memory
            entities.append({"kind": "table", "name": L.get("name")})
            return {"answer": answer, "entities": entities}

        if intent == "procs_access_table":
            op_result = K.procs_access_table(items, L.get("name"), fuzzy=False)
            answer = F.render_procs_access_table(items, L.get("name"))
            entities = _extract_entities_from_result(intent, op_result, L)
            return {"answer": answer, "entities": entities}

        if intent == "procs_update_table":
            op_result = K.procs_update_table(items, L.get("name"))
            answer = F.render_procs_update_table(items, L.get("name"))
            entities = _extract_entities_from_result(intent, op_result, L)
            return {"answer": answer, "entities": entities}

        if intent == "views_access_table":
            op_result = K.views_access_table(items, L.get("name"))
            answer = F.render_views_access_table(items, L.get("name"))
            entities = _extract_entities_from_result(intent, op_result, L)
            return {"answer": answer, "entities": entities}

        if intent == "tables_accessed_by_procedure":
            op_result = K.tables_accessed_by_procedure(items, L.get("name"))
            answer = F.render_tables_accessed_by_procedure(items, L.get("name"))
            entities = _extract_entities_from_result(intent, op_result, L)
            return {"answer": answer, "entities": entities}

        if intent == "tables_accessed_by_view":
            op_result = K.tables_accessed_by_view(items, L.get("name"))
            answer = F.render_tables_accessed_by_view(items, L.get("name"))
            entities = _extract_entities_from_result(intent, op_result, L)
            return {"answer": answer, "entities": entities}

        if intent == "unaccessed_tables":
            op_result = K.unaccessed_tables(items)
            answer = F.render_unaccessed_tables(items)
            # Extract table names from result
            for table_name in op_result:
                entities.append({"kind": "table", "name": table_name})
            return {"answer": answer, "entities": entities}

        if intent == "procs_called_by_procedure":
            op_result = K.procs_called_by_procedure(items, L.get("name"))
            answer = F.render_procs_called_by_procedure(items, L.get("name"))
            # Add target procedure and called procedures
            entities.append({"kind": "procedure", "name": L.get("name")})
            for proc_name in op_result:
                entities.append({"kind": "procedure", "name": proc_name})
            return {"answer": answer, "entities": entities}

        if intent == "call_tree":
            answer = F.render_call_tree(items, L.get("name"))
            # Just add the root procedure
            entities.append({"kind": "procedure", "name": L.get("name")})
            return {"answer": answer, "entities": entities}

        if intent == "columns_returned_by_procedure":
            answer = F.render_columns_returned_by_procedure(items, L.get("name"))
            entities.append({"kind": "procedure", "name": L.get("name")})
            return {"answer": answer, "entities": entities}

        if intent == "unused_columns_of_table":
            answer = F.render_unused_columns_of_table(items, L.get("name"))
            entities.append({"kind": "table", "name": L.get("name")})
            return {"answer": answer, "entities": entities}

        if intent == "sql_of_entity":
            print(f"[agent_answer] Executing sql_of_entity with kind={L.get('kind')} name={L.get('name')}")
            answer = F.render_sql_of_entity(items, L.get("kind") or "any", L.get("name"))
            # Add the entity - infer kind from catalog if not specified
            kind = L.get("kind") or "any"
            entity_name = L.get("name")

            if kind == "any" and entity_name:
                # Try to infer kind by looking up the entity in the catalog
                found_item = K._get_entity(K.as_items_list(items), None, entity_name)
                if found_item:
                    item_kind = (found_item.get("kind") or found_item.get("Kind") or "").lower()
                    if item_kind in ("procedure", "proc"):
                        kind = "procedure"
                    elif item_kind == "table":
                        kind = "table"
                    elif item_kind == "view":
                        kind = "view"
                    elif item_kind == "function":
                        kind = "function"

            if kind != "any" and entity_name:
                entities.append({"kind": kind, "name": entity_name})

            return {"answer": answer, "entities": entities, "contains_sql": True}

        if intent == "compare_sql":
            left = L.get("left")
            right = L.get("right")
            kind = L.get("kind") or "any"
            result = F.render_compare_sql(items, kind, left, kind, right)

            # Add both entities - infer kind if not specified
            if kind == "any" and left:
                # Try to infer kind from the first entity
                found_item = K._get_entity(K.as_items_list(items), None, left)
                if found_item:
                    item_kind = (found_item.get("kind") or found_item.get("Kind") or "").lower()
                    if item_kind in ("procedure", "proc"):
                        kind = "procedure"
                    elif item_kind == "table":
                        kind = "table"
                    elif item_kind == "view":
                        kind = "view"
                    elif item_kind == "function":
                        kind = "function"

            if kind != "any":
                if left:
                    entities.append({"kind": kind, "name": left})
                if right:
                    entities.append({"kind": kind, "name": right})

            return {**result, "entities": entities, "contains_sql": True}

        if intent == "find_similar_sql":
            name = L.get("name")
            kind = L.get("kind") or "any"
            threshold = L.get("threshold", 50.0)

            # Render the similar entities
            answer = F.render_find_similar_sql(items, kind, name, threshold)

            # Add the source entity to memory
            if kind == "any" and name:
                # Infer kind from catalog
                found_item = K._get_entity(K.as_items_list(items), None, name)
                if found_item:
                    item_kind = (found_item.get("kind") or found_item.get("Kind") or "").lower()
                    if item_kind in ("procedure", "proc"):
                        kind = "procedure"
                    elif item_kind == "table":
                        kind = "table"
                    elif item_kind == "view":
                        kind = "view"
                    elif item_kind == "function":
                        kind = "function"

            if kind != "any" and name:
                entities.append({"kind": kind, "name": name})

            # Extract entities from results
            results = K.find_similar_sql(K.as_items_list(items), kind, name, threshold)
            for entity_name, _ in results:
                if kind != "any":
                    entities.append({"kind": kind, "name": entity_name})

            return {"answer": answer, "entities": entities}

        # fallback – shouldn't happen
        return {"answer": "Sorry, I couldn't resolve your intent."}

    except Exception as e:
        return {"answer": f"Error while executing intent `{intent}`: {e!r}"}

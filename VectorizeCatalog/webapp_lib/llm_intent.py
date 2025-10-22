# webapp/llm_intent.py
"""
Unified intent classifier for webapp.
Knows about BOTH cluster and qcat intents.
Uses LLM to classify user query into the correct backend + intent.
"""
from __future__ import annotations
import os
import json
import requests
from typing import Dict, Any, List

# Import intents from both backends
from cluster.intents import INTENTS as CLUSTER_INTENTS, INTENT_LABELS as CLUSTER_LABELS
from qcat.intents import INTENTS as QCAT_INTENTS, INTENT_LABELS as QCAT_LABELS, normalize_entity_name

# LM Studio configuration
_LM_URL = os.getenv("WEBAPP_LMSTUDIO_URL", os.getenv("QCAT_LMSTUDIO_URL", "http://127.0.0.1:1234/v1/chat/completions"))
_LM_MODEL = os.getenv("WEBAPP_LMSTUDIO_MODEL", os.getenv("QCAT_LMSTUDIO_MODEL", "qwen2.5-32b-instruct"))
_LM_TIMEOUT = float(os.getenv("WEBAPP_LMSTUDIO_TIMEOUT", os.getenv("QCAT_LMSTUDIO_TIMEOUT", "12")))
_USE_LLM = os.getenv("WEBAPP_USE_LLM", "1").strip() not in ("0", "false", "False", "")

# Build unified intent list with backend mapping
ALL_INTENTS = {}
for intent in CLUSTER_INTENTS:
    ALL_INTENTS[intent] = {"backend": "cluster", "label": CLUSTER_LABELS[intent]}
for intent in QCAT_INTENTS:
    ALL_INTENTS[intent] = {"backend": "qcat", "label": QCAT_LABELS[intent]}

# Create system prompt with ALL intents
_CLUSTER_INTENTS_STR = "\n".join([f"  - {intent}: {CLUSTER_LABELS[intent]}" for intent in CLUSTER_INTENTS])
_QCAT_INTENTS_STR = "\n".join([f"  - {intent}: {QCAT_LABELS[intent]}" for intent in QCAT_INTENTS])

_SYS = f"""You are an intent parser for a unified SQL catalog system.
You must classify user queries into ONE of these intents and determine which backend to use.

CLUSTER INTENTS (cluster management operations):
{_CLUSTER_INTENTS_STR}

QCAT INTENTS (catalog queries and semantic search):
{_QCAT_INTENTS_STR}

Return STRICT JSON ONLY (no prose). Output schema:
{{
  "intent": "<one of above>",
  "backend": "cluster" or "qcat",
  "confidence": <0.0..1.0>,
  "cluster_id": "<cluster identifier>",
  "group_id": "<group identifier>",
  "procedure": "<procedure name>",
  "procedure_name": "<procedure name>",
  "table_name": "<table name>",
  "new_name": "<new display name>",
  "display_name": "<display name>",
  "target_cluster_id": "<target cluster>",
  "trash_index": <integer index>,
  "force_new_group": <true|false>,
  "name": "<entity name>",
  "name_a": "<first entity name>",
  "name_b": "<second entity name>",
  "kind": "<table|view|procedure|function>",
  "kind_a": "<kind of first entity>",
  "kind_b": "<kind of second entity>"
}}

Rules:
- Normalize identifiers: remove brackets, quotes, backticks
- CLUSTER BACKEND: rename/move/delete/add clusters or groups, trash operations
- QCAT BACKEND: "which procedures access", "show tables", "compare SQL", "list columns", etc.
- If unclear, guess the most likely intent and set confidence <= 0.65
- Never include commentary; return only valid JSON

Examples:
CLUSTER:
- "delete procedure dbo.addLogs" -> {{"intent": "delete_procedure", "backend": "cluster", "confidence": 1.0, "procedure_name": "dbo.addLogs"}}
- "delete table dbo.stock type" -> {{"intent": "delete_table", "backend": "cluster", "confidence": 1.0, "table_name": "dbo.stock type"}}
- "rename cluster C1 to Orders" -> {{"intent": "rename_cluster", "backend": "cluster", "confidence": 1.0, "cluster_id": "C1", "new_name": "Orders"}}
- "move procedure dbo.ProcessOrder to cluster C2" -> {{"intent": "move_procedure", "backend": "cluster", "confidence": 1.0, "procedure": "dbo.ProcessOrder", "cluster_id": "C2"}}

QCAT:
- "which procedures access Order table" -> {{"intent": "procs_access_table", "backend": "qcat", "confidence": 1.0, "name": "Order", "kind": "table"}}
- "show me all tables" -> {{"intent": "list_all_tables", "backend": "qcat", "confidence": 1.0}}
- "compare dbo.Order with dbo.Order_Archive" -> {{"intent": "compare_sql", "backend": "qcat", "confidence": 1.0, "name_a": "dbo.Order", "name_b": "dbo.Order_Archive", "kind_a": null, "kind_b": null}}
- "compare table dbo.Order with table dbo.Order_Archive" -> {{"intent": "compare_sql", "backend": "qcat", "confidence": 1.0, "name_a": "dbo.Order", "kind_a": "table", "name_b": "dbo.Order_Archive", "kind_b": "table"}}
- "list columns of Customer table" -> {{"intent": "list_columns_of_table", "backend": "qcat", "confidence": 1.0, "name": "Customer", "kind": "table"}}
"""


def _safe_json_loads(s: str) -> Dict[str, Any] | None:
    """Safely parse JSON from LLM response"""
    if not s:
        return None
    # strip code fences if any
    if "```" in s:
        parts = s.split("```")
        for part in parts:
            if part.strip().startswith("{"):
                s = part
                break
    # find first/last braces
    i = s.find("{")
    j = s.rfind("}")
    if i >= 0 and j >= 0 and j > i:
        s = s[i:j+1]
    try:
        return json.loads(s)
    except Exception:
        return None


def _normalize_llm_fields(obj: dict) -> dict:
    """Normalize and validate LLM output"""
    out = dict(obj or {})

    # confidence
    if "confidence" in out:
        try:
            out["confidence"] = max(0.0, min(1.0, float(out["confidence"])))
        except Exception:
            out["confidence"] = 0.5
    else:
        out["confidence"] = 0.5

    # normalize name fields
    for k in ("cluster_id", "group_id", "procedure", "procedure_name", "table_name",
              "new_name", "display_name", "target_cluster_id", "name", "name_a", "name_b"):
        if out.get(k):
            out[k] = normalize_entity_name(str(out[k]))

    # booleans defaults
    if "force_new_group" not in out:
        out["force_new_group"] = False

    # trash_index
    if "trash_index" in out and out["trash_index"] is not None:
        try:
            out["trash_index"] = int(out["trash_index"])
        except Exception:
            out["trash_index"] = None

    # backend (should be set by LLM, but validate)
    intent = out.get("intent")
    if intent in ALL_INTENTS:
        out["backend"] = ALL_INTENTS[intent]["backend"]
    elif "backend" not in out:
        out["backend"] = "unknown"

    return out


def _lmstudio_classify(prompt: str) -> Dict[str, Any] | None:
    """Use LM Studio to classify unified intent"""
    if not _USE_LLM:
        return None
    try:
        payload = {
            "model": _LM_MODEL,
            "messages": [
                {"role": "system", "content": _SYS},
                {"role": "user", "content": prompt.strip()},
            ],
            "temperature": 0.0,
            "max_tokens": 256,
            "stream": False,
        }
        r = requests.post(_LM_URL, json=payload, timeout=_LM_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        txt = (data.get("choices") or [{}])[0].get("message", {}).get("content", "")
        obj = _safe_json_loads(txt)
        if not isinstance(obj, dict):
            return None
        obj = _normalize_llm_fields(obj)
        intent = obj.get("intent")
        if intent not in ALL_INTENTS:
            return None
        obj["source"] = "llm"
        return obj
    except Exception as e:
        print(f"[webapp.llm_intent] LLM classification failed: {e}")
        return None


def classify_intent(prompt: str) -> Dict[str, Any]:
    """
    Unified intent classifier - knows about BOTH cluster and qcat intents.
    Returns: {"intent": ..., "backend": "cluster"|"qcat", "confidence": float, "source": "llm"|"failed", ...}

    If LLM fails, returns low confidence so agent can show available commands.
    """
    # Try LLM classification
    llm = _lmstudio_classify(prompt)
    if llm is not None:
        return llm

    # LLM failed - return low confidence fallback
    return {
        "intent": "semantic",
        "backend": "unknown",
        "confidence": 0.0,
        "source": "failed",
        "query": prompt
    }

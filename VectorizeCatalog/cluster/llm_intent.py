# cluster/llm_intent.py
from __future__ import annotations
import os, re, json, requests
from typing import Dict, Optional, Any, List
from cluster.intents import list_intents, normalize_name

# =============== LM Studio config ==================
_LM_URL = os.getenv("CLUSTER_LMSTUDIO_URL", os.getenv("QCAT_LMSTUDIO_URL", "http://127.0.0.1:1234/v1/chat/completions"))
_LM_MODEL = os.getenv("CLUSTER_LMSTUDIO_MODEL", os.getenv("QCAT_LMSTUDIO_MODEL", "qwen2.5-32b-instruct"))
_LM_TIMEOUT = float(os.getenv("CLUSTER_LMSTUDIO_TIMEOUT", os.getenv("QCAT_LMSTUDIO_TIMEOUT", "12")))
_USE_LLM = os.getenv("CLUSTER_USE_LLM", "1").strip() not in ("0", "false", "False", "")

_ALLOWED_INTENTS = set(list_intents())

# =============== Utilities =========================
_RX_NAME = re.compile(r"[`'\"]([^`'\"]+)[`'\"]|(\S+)")

def _extract_names(text: str, count: int = 1) -> List[str]:
    """Extract up to 'count' quoted or unquoted names from text"""
    matches = _RX_NAME.findall(text or "")
    names = []
    for match in matches:
        name = match[0] if match[0] else match[1]
        if name:
            normalized = normalize_name(name)
            if normalized and normalized not in names:
                names.append(normalized)
        if len(names) >= count:
            break
    return names

def _yes(text: str, *words: str) -> bool:
    """Check if any word appears in text"""
    t = f" {text.lower()} "
    return any(f" {w} " in t for w in words)

def _intent_conf(intent: str, conf: float, **kwargs) -> Dict[str, Any]:
    """Build intent result dict"""
    out = {"intent": intent, "confidence": float(conf), "source": "heuristic"}
    out.update(kwargs)
    return out

# =============== Heuristic (fallback) classifier ==========
def _classify_heuristic(prompt: str) -> Dict[str, Any]:
    """Fallback heuristic-based intent classification"""
    q = (prompt or "").strip()
    ql = q.lower()

    # rename cluster
    if _yes(ql, "rename cluster"):
        names = _extract_names(q, count=2)
        if len(names) >= 2:
            return _intent_conf("rename_cluster", 0.95, cluster_id=names[0], new_name=names[1])
        elif len(names) == 1:
            return _intent_conf("rename_cluster", 0.65, cluster_id=names[0], new_name=None)
        return _intent_conf("rename_cluster", 0.60, cluster_id=None, new_name=None)

    # rename group
    if _yes(ql, "rename group", "rename procedure group"):
        names = _extract_names(q, count=2)
        if len(names) >= 2:
            return _intent_conf("rename_group", 0.95, group_id=names[0], new_name=names[1])
        elif len(names) == 1:
            return _intent_conf("rename_group", 0.65, group_id=names[0], new_name=None)
        return _intent_conf("rename_group", 0.60, group_id=None, new_name=None)

    # move group
    if _yes(ql, "move group", "move procedure group") and _yes(ql, "to cluster", "cluster"):
        names = _extract_names(q, count=2)
        if len(names) >= 2:
            return _intent_conf("move_group", 0.95, group_id=names[0], cluster_id=names[1])
        elif len(names) == 1:
            return _intent_conf("move_group", 0.65, group_id=names[0], cluster_id=None)
        return _intent_conf("move_group", 0.60, group_id=None, cluster_id=None)

    # move procedure
    if _yes(ql, "move procedure", "move proc") and _yes(ql, "to cluster", "cluster"):
        names = _extract_names(q, count=2)
        if len(names) >= 2:
            return _intent_conf("move_procedure", 0.95, procedure=names[0], cluster_id=names[1])
        elif len(names) == 1:
            return _intent_conf("move_procedure", 0.65, procedure=names[0], cluster_id=None)
        return _intent_conf("move_procedure", 0.60, procedure=None, cluster_id=None)

    # delete procedure
    if _yes(ql, "delete procedure", "delete proc", "remove procedure"):
        names = _extract_names(q, count=1)
        if names:
            return _intent_conf("delete_procedure", 0.95, procedure_name=names[0])
        return _intent_conf("delete_procedure", 0.60, procedure_name=None)

    # delete table
    if _yes(ql, "delete table", "remove table"):
        names = _extract_names(q, count=1)
        if names:
            return _intent_conf("delete_table", 0.95, table_name=names[0])
        return _intent_conf("delete_table", 0.60, table_name=None)

    # add cluster
    if _yes(ql, "add cluster", "create cluster", "new cluster"):
        names = _extract_names(q, count=2)
        if len(names) >= 2:
            return _intent_conf("add_cluster", 0.95, cluster_id=names[0], display_name=names[1])
        elif len(names) == 1:
            return _intent_conf("add_cluster", 0.85, cluster_id=names[0], display_name=None)
        return _intent_conf("add_cluster", 0.60, cluster_id=None, display_name=None)

    # delete cluster
    if _yes(ql, "delete cluster", "remove cluster"):
        names = _extract_names(q, count=1)
        if names:
            return _intent_conf("delete_cluster", 0.95, cluster_id=names[0])
        return _intent_conf("delete_cluster", 0.60, cluster_id=None)

    # restore procedure
    if _yes(ql, "restore procedure", "restore proc"):
        names = _extract_names(q, count=2)
        if len(names) >= 2:
            return _intent_conf("restore_procedure", 0.95, procedure_name=names[0], target_cluster_id=names[1])
        elif len(names) == 1:
            return _intent_conf("restore_procedure", 0.65, procedure_name=names[0], target_cluster_id=None)
        return _intent_conf("restore_procedure", 0.60, procedure_name=None, target_cluster_id=None)

    # restore table
    if _yes(ql, "restore table"):
        # Try to extract index number
        match = re.search(r"\b(\d+)\b", q)
        if match:
            return _intent_conf("restore_table", 0.95, trash_index=int(match.group(1)))
        return _intent_conf("restore_table", 0.60, trash_index=None)

    # list trash
    if _yes(ql, "list trash", "show trash", "trash items", "what's in trash"):
        return _intent_conf("list_trash", 0.95)

    # empty trash
    if _yes(ql, "empty trash", "clear trash", "delete all trash"):
        return _intent_conf("empty_trash", 0.95)

    # get cluster summary
    if _yes(ql, "cluster summary", "show clusters", "list clusters", "overview"):
        return _intent_conf("get_cluster_summary", 0.85)

    # get cluster detail
    if _yes(ql, "cluster detail", "show cluster", "cluster info") or (_yes(ql, "details") and _yes(ql, "cluster")):
        names = _extract_names(q, count=1)
        if names:
            return _intent_conf("get_cluster_detail", 0.90, cluster_id=names[0])
        return _intent_conf("get_cluster_detail", 0.65, cluster_id=None)

    return {"intent": "semantic", "confidence": 0.3, "source": "fallback", "query": prompt}

# =============== LM Studio (Qwen) classifier =============
_SYS = """You are an intent parser for a SQL cluster management system.
Return STRICT JSON ONLY (no prose). Choose one of these intents:
{intents}

Output JSON schema:
{{
  "intent": "<one of above>",
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
  "force_new_group": <true|false>
}}

Rules:
- Normalize identifiers: remove brackets, quotes, backticks
- If user asks to rename, extract original name and new name
- If user asks to move, extract source and target
- If unclear, guess the most likely intent and set confidence <= 0.65
- Never include commentary; return only valid JSON

Examples:
- "delete procedure dbo.addLogs" -> {{"intent": "delete_procedure", "confidence": 1.0, "procedure_name": "dbo.addLogs"}}
- "delete table dbo.stock type" -> {{"intent": "delete_table", "confidence": 1.0, "table_name": "dbo.stock type"}}
- "remove procedure `order.process`" -> {{"intent": "delete_procedure", "confidence": 1.0, "procedure_name": "order.process"}}
- "remove table [Customer_Old]" -> {{"intent": "delete_table", "confidence": 1.0, "table_name": "Customer_Old"}}
- "delete `dbo.Orders`" -> {{"intent": "delete_table", "confidence": 0.7, "table_name": "dbo.Orders"}}
"""

def _safe_json_loads(s: str) -> Optional[dict]:
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
              "new_name", "display_name", "target_cluster_id"):
        if out.get(k):
            out[k] = normalize_name(str(out[k]))

    # booleans defaults
    if "force_new_group" not in out:
        out["force_new_group"] = False

    # trash_index
    if "trash_index" in out and out["trash_index"] is not None:
        try:
            out["trash_index"] = int(out["trash_index"])
        except Exception:
            out["trash_index"] = None

    return out

def _lmstudio_classify(prompt: str) -> Optional[dict]:
    """Use LM Studio to classify cluster intent"""
    if not _USE_LLM:
        return None
    try:
        payload = {
            "model": _LM_MODEL,
            "messages": [
                {"role": "system", "content": _SYS.format(intents=json.dumps(sorted(list(_ALLOWED_INTENTS))))},
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
        if intent not in _ALLOWED_INTENTS:
            return None
        obj["source"] = "llm"
        return obj
    except Exception as e:
        print(f"[cluster.llm_intent] LLM classification failed: {e}")
        return None

# =============== Public API ================================
def classify_intent(prompt: str) -> Dict[str, Any]:
    """
    Cluster intent classifier using LLM ONLY (no regex/heuristic fallback).
    Returns: {"intent": ..., "confidence": float, "source": "llm"|"failed", ...}

    If LLM fails, returns low confidence so agent can show available commands.
    """
    # Try LLM classification
    llm = _lmstudio_classify(prompt)
    if llm is not None:
        return llm

    # LLM failed - return low confidence semantic fallback
    # Agent will handle showing available commands
    return {
        "intent": "semantic",
        "confidence": 0.0,
        "source": "failed",
        "query": prompt
    }

# qcat/llm_intent.py
from __future__ import annotations
import os, re, json, requests
from typing import Dict, Optional, Any, List
from qcat.intents import list_intents, normalize_entity_name, detect_kind_from_words

# =============== LM Studio config ==================
_LM_URL = os.getenv("QCAT_LMSTUDIO_URL", "http://127.0.0.1:1234/v1/chat/completions")
_LM_MODEL = os.getenv("QCAT_LMSTUDIO_MODEL", "qwen2.5-32b-instruct")  # any model name LM Studio exposes
_LM_TIMEOUT = float(os.getenv("QCAT_LMSTUDIO_TIMEOUT", "12"))
_USE_LLM = os.getenv("QCAT_USE_LLM", "1").strip() not in ("0", "false", "False", "")

_ALLOWED_INTENTS = set(list_intents())  # import from qcat.intents

# =============== Utilities =========================
_RX_ENTITY = re.compile(
    r"(?:`([^`]+)`|\[([^\]]+)\]\.\[([^\]]+)\]|([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)|\[([^\]]+)\]|([A-Za-z0-9_]+))"
)

def _extract_first_entity(text: str) -> Optional[str]:
    m = _RX_ENTITY.search(text or "")
    if not m:
        return None
    if m.group(1):  # `...`
        return normalize_entity_name(m.group(1))
    if m.group(2) and m.group(3):  # [schema].[name]
        return normalize_entity_name(f"{m.group(2)}.{m.group(3)}")
    if m.group(4) and m.group(5):  # schema.name
        return normalize_entity_name(f"{m.group(4)}.{m.group(5)}")
    if m.group(6):  # [name]
        return normalize_entity_name(m.group(6))
    if m.group(7):  # name
        return normalize_entity_name(m.group(7))
    return None

def _yes(text: str, *words: str) -> bool:
    t = f" {text.lower()} "
    return any(f" {w} " in t for w in words)

def _intent_conf(intent: str, conf: float, **kwargs) -> Dict[str, Any]:
    out = {"intent": intent, "confidence": float(conf), "source": "heuristic"}
    out.update(kwargs)
    return out

# =============== Heuristic (fallback) classifier ==========
def _classify_heuristic(prompt: str) -> Dict[str, Any]:
    q = (prompt or "").strip()
    ql = q.lower()

    # compare
    if re.search(r"\b(compare|diff(erence)?|versus|vs)\b", ql):
        # find two entity-ish tokens
        ents = [normalize_entity_name(m.group(0)) for m in re.finditer(
            r"(\[[^\]]+\]\.\[[^\]]+\]|`[^`]+`|[A-Za-z0-9_]+\.[A-Za-z0-9_]+|\[[^\]]+\]|[A-Za-z0-9_]+)", q)]
        uniq: List[str] = []
        for e in ents:
            if e and e not in uniq:
                uniq.append(e)
        kind = detect_kind_from_words(ql)
        if len(uniq) >= 2:
            return {"intent": "compare_sql", "left": uniq[0], "right": uniq[1], "kind": kind,
                    "confidence": 0.95, "source": "heuristic"}
        return {"intent": "compare_sql", "left": None, "right": None, "kind": kind,
                "confidence": 0.60, "source": "heuristic"}

    # list all X
    if _yes(ql, "list all table", "list all tables", "show all tables", "how many tables"):
        return _intent_conf("list_all_tables", 0.95)
    if _yes(ql, "list all view", "list all views", "show all views"):
        return _intent_conf("list_all_views", 0.95)
    if _yes(ql, "list all procedure", "list all procedures", "show all procedures", "stored procedures"):
        return _intent_conf("list_all_procedures", 0.95)
    if _yes(ql, "list all function", "list all functions", "show all functions"):
        return _intent_conf("list_all_functions", 0.95)

    # list columns of a table
    if (_yes(ql, "list columns", "list all column", "columns of", "show columns") and "table" in ql) or \
       re.search(r"\bcolumns?\s+of\s+", ql):
        name = _extract_first_entity(q)
        if name:
            return _intent_conf("list_columns_of_table", 0.95, name=name, kind="table",
                                include_via_views=False, fuzzy=False, unused_only=False, schema=None, pattern=None)
        return _intent_conf("list_columns_of_table", 0.6, name=None, kind="table",
                            include_via_views=False, fuzzy=False, unused_only=False, schema=None, pattern=None)

    # which procedures access/update a table
    if _yes(ql, "which procedure", "which procedures", "what procedure", "what procedures"):
        name = _extract_first_entity(q)
        if _yes(ql, "access", "read", "select", "reference"):
            return _intent_conf("procs_access_table", 0.9 if name else 0.6, name=name,
                                include_via_views=True, include_indirect=True)
        if _yes(ql, "update", "insert", "delete", "write", "modify"):
            return _intent_conf("procs_update_table", 0.9 if name else 0.6, name=name)

    # which views access a table
    if _yes(ql, "which view", "which views") and _yes(ql, "access", "read", "select"):
        name = _extract_first_entity(q)
        return _intent_conf("views_access_table", 0.9 if name else 0.6, name=name)

    # what tables accessed by a procedure/view
    if _yes(ql, "what tables") and _yes(ql, "procedure", "proc", "view"):
        name = _extract_first_entity(q)
        if _yes(ql, "procedure", "proc"):
            return _intent_conf("tables_accessed_by_procedure", 0.9 if name else 0.6, name=name)
        if _yes(ql, "view", "views"):
            return _intent_conf("tables_accessed_by_view", 0.9 if name else 0.6, name=name)

    # unaccessed tables
    if _yes(ql, "unaccessed tables", "unused tables", "not accessed", "not updated"):
        return _intent_conf("unaccessed_tables", 0.9)

    # call graph
    if _yes(ql, "which other procedures", "called by", "calls which procedures"):
        name = _extract_first_entity(q)
        return _intent_conf("procs_called_by_procedure", 0.9 if name else 0.6, name=name)
    if _yes(ql, "call tree", "call graph"):
        name = _extract_first_entity(q)
        return _intent_conf("call_tree", 0.9 if name else 0.6, name=name)

    # columns returned by a procedure
    if _yes(ql, "columns returned", "result columns", "return columns", "output columns") and _yes(ql, "procedure", "proc"):
        name = _extract_first_entity(q)
        return _intent_conf("columns_returned_by_procedure", 0.9 if name else 0.6, name=name)

    # unused columns of a table
    if _yes(ql, "unused columns", "unaccessed columns", "not referenced columns") and _yes(ql, "table"):
        name = _extract_first_entity(q)
        return _intent_conf("unused_columns_of_table", 0.9 if name else 0.6, name=name)

    # creation SQL of entity
    if _yes(ql, "print create sql", "creation sql", "sql of", "show create", "get create"):
        name = _extract_first_entity(q)
        kind = detect_kind_from_words(ql) or "any"
        return _intent_conf("sql_of_entity", 0.9 if name else 0.6, name=name, kind=kind, full=True)

    return {"intent": "semantic", "confidence": 0.3, "source": "fallback", "query": prompt}

# =============== LM Studio (Qwen) classifier =============
_SYS = """You are an intent parser for a SQL catalog Q&A system.
Return STRICT JSON ONLY (no prose). Choose one of these intents:
{intents}

Output JSON schema:
{{
  "intent": "<one of above>",
  "confidence": <0.0..1.0>,
  "name": "<entity name if any, like 'dbo.Order'>",
  "kind": "<table|view|procedure|function|any>",
  "left": "<entity name for compare>",
  "right": "<entity name for compare>",
  "include_via_views": <true|false>,
  "include_indirect": <true|false>,
  "unused_only": <true|false>,
  "fuzzy": <true|false>,
  "full": <true|false>,
  "schema": "<schema name if given>",
  "pattern": "<wildcard/regex if given>"
}}

Rules:
- Normalize identifiers: [dbo].[Order] -> dbo.Order (no brackets/backticks).
- If user asks to compare, set intent=compare_sql and fill "left" and "right".
- If user asks for columns of a table, set intent=list_columns_of_table and "kind":"table".
- If unclear, guess the most likely intent and set confidence <= 0.65.
- Never include commentary; return only valid JSON.
"""

def _safe_json_loads(s: str) -> Optional[dict]:
    if not s:
        return None
    # strip code fences if any
    if "```" in s:
        s = s.split("```", 2)[-1]
    # find first/last braces to be safe
    i = s.find("{"); j = s.rfind("}")
    if i >= 0 and j >= 0 and j > i:
        s = s[i:j+1]
    try:
        return json.loads(s)
    except Exception:
        return None

def _normalize_llm_fields(obj: dict) -> dict:
    # coerce/clip
    out = dict(obj or {})
    if "confidence" in out:
        try:
            out["confidence"] = max(0.0, min(1.0, float(out["confidence"])))
        except Exception:
            out["confidence"] = 0.5
    else:
        out["confidence"] = 0.5
    # normalize identifiers
    for k in ("name", "left", "right"):
        if out.get(k):
            out[k] = normalize_entity_name(str(out[k]))
    # default kind
    if not out.get("kind"):
        out["kind"] = "any"
    # booleans defaults
    for k in ("include_via_views", "include_indirect", "unused_only", "fuzzy", "full"):
        if k not in out:
            out[k] = False
    return out

def _lmstudio_classify(prompt: str) -> Optional[dict]:
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
        # gentle guard: if model "guessed", keep conf <= 0.65
        c = float(obj.get("confidence", 0.5))
        if c > 1.0: obj["confidence"] = 1.0
        if c < 0.0: obj["confidence"] = 0.0
        return obj
    except Exception:
        return None

# =============== Public API ================================
def classify_intent(prompt: str) -> Dict[str, Any]:
    """
    Agentic classifier with LM Studio (Qwen) + heuristic fallback.
    Returns a dict: {"intent": ..., "confidence": float, "source": "llm"|"heuristic"|"fallback", ...}
    """
    # 1) Try LLM first
    llm = _lmstudio_classify(prompt)
    if llm is not None:
      # small safety: if the LLM couldn't extract any entity where one is needed, let heuristics try too
      needs_entity = llm["intent"] in {
          "list_columns_of_table", "procs_access_table", "procs_update_table",
          "views_access_table", "tables_accessed_by_procedure", "tables_accessed_by_view",
          "procs_called_by_procedure", "call_tree", "columns_returned_by_procedure",
          "unused_columns_of_table", "sql_of_entity",
      }
      if needs_entity and not llm.get("name") and llm["intent"] != "sql_of_entity":
          # fall back to heuristic for name extraction
          heur = _classify_heuristic(prompt)
          # prefer LLM's intent but borrow name if heuristic found one
          if heur.get("name"):
              llm["name"] = heur["name"]
          # blend confidence a bit
          llm["confidence"] = max(llm.get("confidence", 0.5), heur.get("confidence", 0.5) - 0.1)
      return llm

    # 2) Fallback to heuristics
    return _classify_heuristic(prompt)

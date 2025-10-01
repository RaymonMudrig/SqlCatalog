from __future__ import annotations
import os, json, re
from typing import Dict, Any, Optional, List
import requests

from qcat.intents import list_intents
from qcli.resolver import resolve_items_by_name

LMSTUDIO_URL   = os.getenv("LMSTUDIO_URL",  "http://127.0.0.1:1234/v1")
LMSTUDIO_MODEL = os.getenv("LMSTUDIO_MODEL","qwen2.5-32b-instruct-mlx")

def _safe_json(s: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(s)
    except Exception:
        m = re.search(r"\{.*\}", s, re.S)
        if m:
            try: return json.loads(m.group(0))
            except Exception: return None
        return None

def _post_chat(messages: List[Dict[str, str]], temperature=0.1, max_tokens=300) -> str:
    url = f"{LMSTUDIO_URL}/chat/completions"
    payload = {
        "model": LMSTUDIO_MODEL,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "stream": False,
    }
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data["choices"][0]["message"]["content"]

_SYSTEM = (
    "You classify a user question about a SQL catalog. "
    "Return ONLY compact JSON with fields: "
    '{"intent": <one of ' + ", ".join(list_intents()) + '>, '
    '"name": <string or null>, "kind": <table|view|procedure|function|null>, '
    '"include_via_views": <true|false>, "fuzzy": <true|false>, "unused_only": <true|false>, '
    '"schema": <string or null>, "pattern": <string or null>, '
    '"confidence": <0..1>}.\n'
    "Use SQL LIKE notation for pattern when present (e.g., 'Order%'). "
    "If the user asks for columns of a table, intent MUST be 'list_columns_of_table'. "
    "If the user asks to list all tables/views/procedures/functions, use the corresponding 'list_all_*' intent, "
    "and set name=null. If a schema filter is mentioned (e.g., 'in schema dbo'), set schema to that. "
    "Do not add commentary."
)

_FEWSHOTS = [
    ("Which procedures access table 'Order'?", {
        "intent":"procs_access_table","name":"Order","kind":"table",
        "include_via_views":True,"fuzzy":False,"unused_only":False,
        "schema":None,"pattern":None,"confidence":0.94
    }),
    ("Which procedures update dbo.RT_Order?", {
        "intent":"procs_update_table","name":"dbo.RT_Order","kind":"table",
        "include_via_views":True,"fuzzy":False,"unused_only":False,
        "schema":"dbo","pattern":None,"confidence":0.92
    }),
    ("list all tables in schema dbo", {
        "intent":"list_all_tables","name":None,"kind":"table",
        "include_via_views":False,"fuzzy":False,"unused_only":False,
        "schema":"dbo","pattern":None,"confidence":0.99
    }),
    ("show tables like 'Order%'", {
        "intent":"list_all_tables","name":None,"kind":"table",
        "include_via_views":False,"fuzzy":False,"unused_only":False,
        "schema":None,"pattern":"Order%","confidence":0.98
    }),
    ("list all column of `dbo.Order` table", {
        "intent":"list_columns_of_table","name":"dbo.Order","kind":"table",
        "include_via_views":False,"fuzzy":False,"unused_only":False,
        "schema":"dbo","pattern":None,"confidence":0.96
    }),
    ("columns returned by procedure dbo.GetOrders", {
        "intent":"columns_returned_by_procedure","name":"dbo.GetOrders","kind":"procedure",
        "include_via_views":False,"fuzzy":False,"unused_only":False,
        "schema":"dbo","pattern":None,"confidence":0.93
    }),
    ("create a call tree of procedure 'dbo.Dispatch'", {
        "intent":"call_tree","name":"dbo.Dispatch","kind":"procedure",
        "include_via_views":False,"fuzzy":False,"unused_only":False,
        "schema":"dbo","pattern":None,"confidence":0.9
    }),
    ("print create sql of [dbo].[Order_Archive]", {
        "intent":"sql_of_entity","name":"[dbo].[Order_Archive]","kind":"table",
        "include_via_views":False,"fuzzy":False,"unused_only":False,
        "schema":"dbo","pattern":None,"confidence":0.95
    }),
    ("find unused columns of 'dbo.RT_Order'", {
        "intent":"unused_columns_of_table","name":"dbo.RT_Order","kind":"table",
        "include_via_views":False,"fuzzy":False,"unused_only":True,
        "schema":"dbo","pattern":None,"confidence":0.9
    }),
    ("list all procedures in schema sales matching 'sp_%order%'", {
        "intent":"list_all_procedures","name":None,"kind":"procedure",
        "include_via_views":False,"fuzzy":False,"unused_only":False,
        "schema":"sales","pattern":"sp_%order%","confidence":0.95
    }),
]

def _fewshot_messages() -> List[Dict[str,str]]:
    msgs = [{"role":"system","content":_SYSTEM}]
    for q, y in _FEWSHOTS:
        msgs.append({"role":"user","content":q})
        msgs.append({"role":"assistant","content":json.dumps(y, ensure_ascii=False)})
    return msgs

def classify_intent(prompt: str, items: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    messages = _fewshot_messages() + [{"role":"user","content":prompt}]
    try:
        out = _post_chat(messages, temperature=0.05, max_tokens=300)
    except Exception:
        return None

    data = _safe_json(out) or {}
    intent = data.get("intent")
    name   = data.get("name")
    kind   = data.get("kind")
    include_via_views = bool(data.get("include_via_views", False))
    fuzzy  = bool(data.get("fuzzy", False))
    unused_only = bool(data.get("unused_only", False))
    conf   = float(data.get("confidence") or 0.0)
    schema = data.get("schema")
    pattern= data.get("pattern")

    # Repairs
    ql = (prompt or "").lower()
    if ("column" in ql or "columns" in ql) and "table" in ql:
        if intent not in ("list_columns_of_table", "unused_columns_of_table"):
            intent = "list_columns_of_table"

    if intent and intent.startswith("list_all_"):
        name = None
        fuzzy = False
        include_via_views = False

    if name and not kind:
        for knd in ("table","view","procedure","function"):
            ms = resolve_items_by_name(items, knd, name, strict=True)
            if ms:
                kind = knd
                break

    if not intent:
        return None

    return {
        "intent": intent,
        "name": name,
        "kind": kind,
        "include_via_views": include_via_views,
        "fuzzy": fuzzy,
        "unused_only": unused_only,
        "schema": schema,
        "pattern": pattern,
        "confidence": conf,
        "source": "llm",
    }

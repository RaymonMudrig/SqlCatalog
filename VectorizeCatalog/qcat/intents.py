from __future__ import annotations
import re
from typing import Dict, Any, Optional, List, Literal

__all__ = ["parse_intent", "list_intents", "IntentId"]

# --------------------------------------------------------------------
# Public: supported intents (and a typed alias used by qcat.prompt)
# --------------------------------------------------------------------
def list_intents() -> List[str]:
    return [
        # legacy intents (must remain stable)
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
        "sql_of_entity",
        "list_all_tables",
        "list_all_views",
        "list_all_procedures",
        "list_all_functions",
        # extras kept in the system
        "count_of_kind",
        "compare_sql",
        # fallback
        "semantic",
    ]

# Literal type alias for static checkers and imports from qcat.prompt
IntentId = Literal[
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
    "sql_of_entity",
    "list_all_tables",
    "list_all_views",
    "list_all_procedures",
    "list_all_functions",
    "count_of_kind",
    "compare_sql",
    "semantic",
]

# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------

KIND_WORDS = {
    "table": {"table", "tables"},
    "procedure": {"procedure", "procedures", "proc", "procs"},
    "view": {"view", "views"},
    "function": {"function", "functions", "func", "udf"},
}

_NAME = r"(?:\[[^\]]+\]|`[^`]+`|\"[^\"]+\"|[A-Za-z0-9_]+(?:\s*\.\s*[A-Za-z0-9_]+)?)"

def _clean_name(s: str) -> str:
    s = s.strip()
    if (s.startswith("[") and s.endswith("]")) or (s.startswith("`") and s.endswith("`")) or (s.startswith('"') and s.endswith('"')):
        return s[1:-1]
    return s

def _find_kind_hint(q: str) -> Optional[str]:
    ql = q.lower()
    for kind, words in KIND_WORDS.items():
        if any(re.search(rf"\b{re.escape(w)}\b", ql) for w in words):
            return kind
    return None

# --------------------------------------------------------------------
# Main parser
# --------------------------------------------------------------------

def parse_intent(prompt: str) -> Dict[str, Any]:
    q = (prompt or "").strip()

    # ---- compare / diff intent ----
    m = re.search(rf"(?is)\b(compare|diff|difference|how\s+similar)\b.+?({_NAME}).+?(?:and|vs\.?|versus)\s+({_NAME})", q)
    if m:
        left = _clean_name(m.group(2))
        right = _clean_name(m.group(3))
        return {
            "intent": "compare_sql",
            "left": {"name": left, "kind": _find_kind_hint(q)},
            "right": {"name": right, "kind": _find_kind_hint(q)},
            "side_by_side": True,
            "confidence": 0.9,
            "source": "rules",
        }

    # ---- counts ----
    m = re.search(r"(?i)\bhow\s+many\s+(tables?|procedures?|views?|functions?)\s+(?:are\s+there|do\s+we\s+have)?\b", q)
    if m:
        kw = m.group(1).lower()
        for kind, words in KIND_WORDS.items():
            if any(kw.startswith(w.rstrip('s')) for w in words):
                return {"intent": "count_of_kind", "kind": kind, "confidence": 0.95, "source": "rules"}

    m = re.search(r"(?i)\bcount\s+all\s+(tables?|procedures?|views?|functions?)\b", q)
    if m:
        kw = m.group(1).lower()
        for kind, words in KIND_WORDS.items():
            if any(kw.startswith(w.rstrip('s')) for w in words):
                return {"intent": "count_of_kind", "kind": kind, "confidence": 0.95, "source": "rules"}

    # ---- explicit list-all intents ----
    if re.search(r"(?i)\b(list|show)\s+all\s+tables?\b", q):
        return {"intent": "list_all_tables", "confidence": 0.95, "source": "rules"}
    if re.search(r"(?i)\b(list|show)\s+all\s+procedures?\b", q):
        return {"intent": "list_all_procedures", "confidence": 0.95, "source": "rules"}
    if re.search(r"(?i)\b(list|show)\s+all\s+views?\b", q):
        return {"intent": "list_all_views", "confidence": 0.95, "source": "rules"}
    if re.search(r"(?i)\b(list|show)\s+all\s+functions?\b", q):
        return {"intent": "list_all_functions", "confidence": 0.95, "source": "rules"}

    # ---- which procedures access / update a table ----
    m = re.search(rf"(?is)\b(list|which)\s+(?:all\s+)?procedures?\s+(?:that\s+)?(?:access|read|use|reference)\s+(?:table\s+)?({_NAME})\b", q)
    if m:
        name = _clean_name(m.group(2))
        return {"intent": "procs_access_table", "name": name, "kind": "table", "confidence": 0.95, "source": "rules"}

    m = re.search(rf"(?is)\b(list|which)\s+(?:all\s+)?procedures?\s+(?:that\s+)?(?:update|insert|delete|write)s?\s+(?:table\s+)?({_NAME})\b", q)
    if m:
        name = _clean_name(m.group(2))
        return {"intent": "procs_update_table", "name": name, "kind": "table", "confidence": 0.95, "source": "rules"}

    # ---- which views access a table ----
    m = re.search(rf"(?is)\b(list|which)\s+(?:all\s+)?views?\s+(?:that\s+)?(?:access|use|reference)\s+(?:table\s+)?({_NAME})\b", q)
    if m:
        name = _clean_name(m.group(2))
        return {"intent": "views_access_table", "name": name, "kind": "table", "confidence": 0.95, "source": "rules"}

    # ---- tables accessed by a procedure / by a view ----
    m = re.search(rf"(?is)\b(what|which)\s+tables?\s+(?:are\s+)?(?:accessed|used|read|referenced)\s+by\s+(?:procedure\s+)?({_NAME})\b", q)
    if m:
        name = _clean_name(m.group(2))
        return {"intent": "tables_accessed_by_procedure", "name": name, "kind": "procedure", "confidence": 0.95, "source": "rules"}

    m = re.search(rf"(?is)\b(what|which)\s+tables?\s+(?:are\s+)?(?:accessed|used|read|referenced)\s+by\s+(?:view\s+)?({_NAME})\b", q)
    if m:
        name = _clean_name(m.group(2))
        return {"intent": "tables_accessed_by_view", "name": name, "kind": "view", "confidence": 0.95, "source": "rules"}

    # ---- unaccessed tables ----
    if re.search(r"(?i)\b(unaccessed|unused)\s+tables?\b", q) or \
       re.search(r"(?i)\bwhich\s+tables?\s+(?:are\s+)?not\s+(?:accessed|used|updated)\b", q):
        return {"intent": "unaccessed_tables", "confidence": 0.9, "source": "rules"}

    # ---- procedure calls / call tree ----
    m = re.search(rf"(?is)\bwhich\s+(?:other\s+)?procedures?\s+(?:are\s+)?called\s+by\s+(?:procedure\s+)?({_NAME})\b", q)
    if m:
        name = _clean_name(m.group(1))
        return {"intent": "procs_called_by_procedure", "name": name, "kind": "procedure", "confidence": 0.9, "source": "rules"}

    m = re.search(rf"(?is)\b(call[\s-]*tree|call\s+graph)\s+(?:of|for)\s+(?:procedure\s+)?({_NAME})\b", q)
    if m:
        name = _clean_name(m.group(2))
        return {"intent": "call_tree", "name": name, "kind": "procedure", "confidence": 0.9, "source": "rules"}

    # ---- list columns of table ----
    m = re.search(rf"(?is)\b(list|show)\s+all\s+columns?\s+of\s+({_NAME})\s+(?:table)?\b", q)
    if m:
        name = _clean_name(m.group(2))
        return {
            "intent": "list_columns_of_table",
            "name": name, "kind": "table",
            "include_via_views": False, "fuzzy": False,
            "unused_only": False, "schema": None, "pattern": None,
            "confidence": 0.95, "source": "rules",
        }

    # ---- columns returned by procedure ----
    m = re.search(rf"(?is)\b(list|show|what)\s+columns?\s+(?:are\s+)?(?:returned|output)\s+by\s+(?:procedure\s+)?({_NAME})\b", q)
    if m:
        name = _clean_name(m.group(2))
        return {"intent": "columns_returned_by_procedure", "name": name, "kind": "procedure", "confidence": 0.9, "source": "rules"}

    # ---- unused columns of table ----
    m = re.search(rf"(?is)\b(unused|unaccessed)\s+columns?\s+of\s+(?:table\s+)?({_NAME})\b", q)
    if m:
        name = _clean_name(m.group(2))
        return {"intent": "unused_columns_of_table", "name": name, "kind": "table", "confidence": 0.9, "source": "rules"}

    # ---- show/print create sql / ddl of entity ----
    m = re.search(rf"(?is)\b(show|print)\s+(?:the\s+)?(?:create\s+sql|definition|ddl)\s+(?:of|for)\s+({_NAME})(?:\s+(table|procedure|view|function))?\b", q)
    if m:
        name = _clean_name(m.group(2))
        kind = (m.group(3) or "").lower() or _find_kind_hint(q)
        return {"intent": "sql_of_entity", "name": name, "kind": kind, "confidence": 0.9, "source": "rules"}

    # also accept "sql of X"
    m = re.search(rf"(?is)\bsql\s+(?:of|for)\s+({_NAME})\b", q)
    if m:
        name = _clean_name(m.group(1))
        kind = _find_kind_hint(q)
        return {"intent": "sql_of_entity", "name": name, "kind": kind, "confidence": 0.8, "source": "rules"}

    # ---- fallback to semantic ----
    return {
        "intent": "semantic",
        "query": q,
        "confidence": 0.3,
        "source": "fallback",
    }

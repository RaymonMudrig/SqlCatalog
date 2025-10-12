# qcat/intents.py
from __future__ import annotations
from typing import Dict, List, Optional, Tuple

# Canonical intent ids (must match ops/formatters handlers you already have)
INTENTS: List[str] = [
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
    "compare_sql",
    "find_similar_sql",
]

# Human-friendly labels (optional)
INTENT_LABELS: Dict[str, str] = {
    "procs_access_table": "Which procedures access a table",
    "procs_update_table": "Which procedures update a table",
    "views_access_table": "Which views access a table",
    "tables_accessed_by_procedure": "What tables are accessed by a procedure",
    "tables_accessed_by_view": "What tables are accessed by a view",
    "unaccessed_tables": "Tables not accessed or updated",
    "procs_called_by_procedure": "Which other procedures are called by a procedure",
    "call_tree": "Call tree of a procedure",
    "list_columns_of_table": "List columns of a table",
    "columns_returned_by_procedure": "List columns returned by a procedure",
    "unused_columns_of_table": "Unused/unaccessed columns of a table",
    "sql_of_entity": "Creation SQL of an entity",
    "list_all_tables": "List all tables",
    "list_all_views": "List all views",
    "list_all_procedures": "List all procedures",
    "list_all_functions": "List all functions",
    "compare_sql": "Compare creation SQL of two entities",
    "find_similar_sql": "Find entities with similar SQL to a given entity",
}

def list_intents() -> List[str]:
    return list(INTENTS)

def label_of(intent: str) -> str:
    return INTENT_LABELS.get(intent, intent)

# Simple schema/kind helpers used by llm_intent
KIND_WORDS = {
    "table": ["table", "tables"],
    "view": ["view", "views"],
    "procedure": ["procedure", "procedures", "proc", "procs", "stored procedure", "stored procedures"],
    "function": ["function", "functions", "fn", "udf", "udfs"],
}

def normalize_entity_name(name: str) -> str:
    """
    Normalize names like:
      - [dbo].[Order]  -> dbo.Order
      - `dbo.Order`    -> dbo.Order
      - dbo.[Order]    -> dbo.Order
      - [Order]        -> Order  (no schema)
    """
    if not name:
        return name
    s = name.strip().strip("`").strip()
    # drop outer brackets
    if s.startswith("[") and s.endswith("]"):
        s = s[1:-1]
    # replace ].[ with .
    s = s.replace("].[", ".")
    # remove single brackets around parts
    s = s.replace("[", "").replace("]", "")
    # collapse spaces around dot
    s = ".".join(p.strip() for p in s.split(".") if p.strip())
    return s

def detect_kind_from_words(text: str) -> Optional[str]:
    t = text.lower()
    for k, words in KIND_WORDS.items():
        for w in words:
            if f" {w} " in f" {t} ":
                return k
    return None

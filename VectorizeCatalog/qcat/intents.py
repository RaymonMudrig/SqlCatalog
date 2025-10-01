from __future__ import annotations
from typing import Literal, Dict, List

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
    # list-all intents
    "list_all_tables",
    "list_all_views",
    "list_all_procedures",
    "list_all_functions",
]

INTENT_DESCRIPTIONS: Dict[IntentId, str] = {
    "procs_access_table": "Which procedures access a table",
    "procs_update_table": "Which procedures update (write) a table",
    "views_access_table": "Which views access a table",
    "tables_accessed_by_procedure": "What tables are accessed by a procedure",
    "tables_accessed_by_view": "What tables are accessed by a view",
    "unaccessed_tables": "Which tables are not accessed or updated by any procedures or views",
    "procs_called_by_procedure": "Which other procedures are called by a procedure",
    "call_tree": "Create a call-tree of a procedure",
    "list_columns_of_table": "List columns of a table",
    "columns_returned_by_procedure": "List columns returned by a procedure",
    "unused_columns_of_table": "Find unused/unaccessed/unupdated columns of a table",
    "sql_of_entity": "Print CREATE SQL of a table, procedure or view",
    "list_all_tables": "List all tables (optionally filtered)",
    "list_all_views": "List all views (optionally filtered)",
    "list_all_procedures": "List all procedures (optionally filtered)",
    "list_all_functions": "List all functions (optionally filtered)",
}

def list_intents() -> List[str]:
    return list(INTENT_DESCRIPTIONS.keys())

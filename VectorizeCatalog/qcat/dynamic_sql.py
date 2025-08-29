import re
from typing import List, Optional, Dict, Any
from .name_match import split_safe

def _escape(name: str) -> str:
    return re.escape(name)

def _mk_table_patterns(schema: Optional[str], base: str) -> List[re.Pattern]:
    """
    Build regex patterns that will match references to the table in raw or dynamic SQL.
    Covers: [Order], dbo.Order, [dbo].[Order], "dbo"."Order", etc.
    Looks around FROM/JOIN/UPDATE/INTO/DELETE against table tokens.
    """
    base_re = rf"(?:\[{_escape(base)}\]|\"{_escape(base)}\"|`{_escape(base)}`|{_escape(base)})"
    if schema:
        schema_re = rf"(?:\[{_escape(schema)}\]|\"{_escape(schema)}\"|`{_escape(schema)}`|{_escape(schema)})"
        qual = rf"(?:{schema_re}\s*\.\s*{base_re})"
    else:
        # optionally qualified
        schema_token = r"(?:\[[A-Za-z0-9_]+\]|\"[A-Za-z0-9_]+\"|`[A-Za-z0-9_]+`|[A-Za-z0-9_]+)"
        qual = rf"(?:{schema_token}\s*\.\s*{base_re}|{base_re})"

    ops = r"(?:from|join|update|into|delete\s+from)"
    return [
        re.compile(rf"\b{ops}\s+{qual}\b", re.IGNORECASE),
        # inside quoted strings (dynamic sql) e.g., '... FROM [dbo].[Order] ...'
        re.compile(rf"[\"'`]([^\"'`]*\b{ops}\s+{qual}\b[^\"'`]*)[\"'`]", re.IGNORECASE),
    ]

def proc_hits_table(proc_item: Dict[str, Any], target_table_safe: str) -> bool:
    """
    Check if a procedure's SQL contains direct or dynamic references to the target table.
    """
    sql = (proc_item.get("sql") or "")  # may be None
    if not sql:
        return False
    schema, base = split_safe(target_table_safe)
    pats = _mk_table_patterns(schema or None, base)
    for pat in pats:
        if pat.search(sql):
            return True
    return False

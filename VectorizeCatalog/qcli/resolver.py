from __future__ import annotations
import re
from typing import List, Dict, Any, Optional, Tuple

from qcat.name_match import split_safe

_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")  # pulls tokens from dbo.RT_Order, [dbo].[RT_Order], "dbo"."RT_Order", etc.

def _split_qualified(n: str) -> Tuple[Optional[str], Optional[str]]:
    parts = _TOKEN_RE.findall(n or "")
    if not parts:
        return None, None
    if len(parts) == 1:
        return None, parts[0]
    return parts[0], parts[1]

def _norm_base(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "", (s or "").lower())

def resolve_items_by_name(items: List[Dict[str, Any]], kind: str, name: str, strict: bool = True) -> List[Dict[str, Any]]:
    """
    Robust resolver:
      - kind match is case-insensitive
      - accepts: dbo.RT_Order, dbo·RT_Order, [dbo].[RT_Order], "dbo"."RT_Order", `dbo`.`RT_Order`, [RT_Order], RT_Order
      - strict=True: exact safe/schema match; if none, auto-fallback to UNIQUE base-name match
      - strict=False: also allows fuzzy contains
    """
    kind_l = (kind or "").lower()
    matches = [it for it in items if (it.get("kind") or "").lower() == kind_l]

    schema_in, base_in = _split_qualified(name)
    name_l = (name or "").lower()

    # 1) safe_name exact (normalize 'dbo.RT_Order' → 'dbo·rt_order')
    if schema_in and base_in:
        dot_to_mid = f"{schema_in.lower()}·{base_in.lower()}"
        out = [it for it in matches if (it.get("safe_name") or "").lower() == dot_to_mid]
        if out:
            return out

    # 2) schema.name exact
    if schema_in and base_in:
        out = [it for it in matches
               if ((it.get("schema") or "").lower() == schema_in.lower())
               and ((it.get("name") or "").lower() == base_in.lower()
                    or split_safe(it.get("safe_name") or "")[1].lower() == base_in.lower())]
        if out:
            return out

    # 3) safe_name exact as provided (dbo·RT_Order)
    if "·" in name_l:
        out = [it for it in matches if (it.get("safe_name") or "").lower() == name_l]
        if out:
            return out

    # ---- strict mode auto-fallback: unique base-name match ----
    if strict and base_in:
        target = _norm_base(base_in)
        base_hits = [it for it in matches
                     if _norm_base(split_safe(it.get("safe_name") or "")[1]) == target
                     or _norm_base(it.get("name") or "") == target]
        if len(base_hits) == 1:
            return base_hits
        return []  # ambiguous or missing

    # ---- non-strict (fuzzy) fallbacks ----
    if base_in:
        out = [it for it in matches
               if split_safe(it.get("safe_name") or "")[1].lower() == base_in.lower()
               or (it.get("name") or "").lower() == base_in.lower()]
        if out:
            return out

    needle = (base_in or name).lower()
    out = [it for it in matches
           if needle in (it.get("safe_name") or "").lower()
           or needle in (it.get("name") or "").lower()]
    return out

def list_names(items: List[Dict[str, Any]], kind: str, pattern: Optional[str]) -> List[Tuple[str, str]]:
    patt = re.compile(pattern or ".*", re.IGNORECASE)
    hits = []
    for it in items:
        if (it.get("kind") or "").lower() != kind:
            continue
        safe = it.get("safe_name") or ""
        disp = f"{(it.get('schema') or '')+'.' if it.get('schema') else ''}{it.get('name') or ''}"
        if patt.search(safe) or patt.search(disp):
            hits.append((safe, disp or safe))
    hits.sort(key=lambda t: t[0].lower())
    return hits

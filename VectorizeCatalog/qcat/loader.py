# VectorizeCatalog/qcat/loader.py
from __future__ import annotations
import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

from qcat.paths import CATALOG_JSON, ITEMS_JSON, SQL_EXPORTS_TABLES, SQL_EXPORTS_VIEWS, SQL_EXPORTS_PROCEDURES, SQL_EXPORTS_FUNCTIONS

def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def _ci_get(d: Dict[str, Any], key: str, default=None):
    if key in d: return d[key]
    kl = key.lower()
    for k, v in d.items():
        if isinstance(k, str) and k.lower() == kl: return v
    return default

def _columns_to_list(obj: Any) -> List[Dict[str, Any]]:
    # Accept { "ColName": {Type, Nullable, ...}, ... }  OR  [ {name, type, ...}, ... ]
    out: List[Dict[str, Any]] = []
    if isinstance(obj, list):
        for c in obj:
            if not isinstance(c, dict): continue
            name = c.get("name") or c.get("Name")
            if not name: continue
            out.append({
                "name": name,
                "type": c.get("type") or c.get("Type"),
                "nullable": c.get("nullable") if "nullable" in c else c.get("Nullable"),
                "default": c.get("default") if "default" in c else c.get("Default"),
                "doc": c.get("doc") if "doc" in c else c.get("Doc"),
            })
        return out
    if isinstance(obj, dict):
        for name, meta in obj.items():
            if not isinstance(meta, dict): meta = {}
            out.append({
                "name": name,
                "type": _ci_get(meta, "Type"),
                "nullable": _ci_get(meta, "Nullable"),
                "default": _ci_get(meta, "Default"),
                "doc": _ci_get(meta, "Doc"),
            })
    return out

def _mk_safe(schema: Optional[str], name: str) -> str:
    schema = (schema or "").strip()
    return f"{schema}·{name}" if schema else name

def _sql_export_path(kind: str, schema: Optional[str], name: str) -> Optional[str]:
    base = {
        "table":     SQL_EXPORTS_TABLES,
        "view":      SQL_EXPORTS_VIEWS,
        "procedure": SQL_EXPORTS_PROCEDURES,
        "function":  SQL_EXPORTS_FUNCTIONS,
    }.get((kind or "").lower())
    if not base: return None
    fname = f"{schema}.{name}.sql" if schema else f"{name}.sql"
    p = base / fname
    return str(p) if p.exists() else None

def _lift_table(name: str, obj: Dict[str, Any]) -> Dict[str, Any]:
    schema = _ci_get(obj, "Schema") or ""
    real_name = _ci_get(obj, "Original_Name") or _ci_get(obj, "Safe_Name") or name
    cols = _columns_to_list(_ci_get(obj, "Columns") or _ci_get(obj, "cols") or {})
    doc  = _ci_get(obj, "Doc")
    safe = _mk_safe(schema, real_name)
    refs = _ci_get(obj, "Referenced_By") or []
    return {
        "kind": "table",
        "schema": schema,
        "name": real_name,
        "safe_name": safe,
        "columns": cols,
        "doc": doc,
        "referenced_by": refs,  # list of {Schema?, Safe_Name}
        "sql_export_path": _sql_export_path("table", schema, real_name),
    }

def _lift_routine(kind: str, name: str, obj: Dict[str, Any]) -> Dict[str, Any]:
    schema = _ci_get(obj, "Schema") or ""
    real_name = _ci_get(obj, "Original_Name") or _ci_get(obj, "Safe_Name") or name
    doc  = _ci_get(obj, "Doc")
    safe = _mk_safe(schema, real_name)
    reads  = _ci_get(obj, "Reads")  or []
    writes = _ci_get(obj, "Writes") or []
    rescols= _ci_get(obj, "Result_Columns") or _ci_get(obj, "ResultColumns") or []
    return {
        "kind": kind,
        "schema": schema,
        "name": real_name,
        "safe_name": safe,
        "doc": doc,
        "reads": reads,
        "writes": writes,
        "result_columns": rescols,
        "sql_export_path": _sql_export_path(kind, schema, real_name),
    }

def _walk_group(kind: str, group: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for nm, obj in group.items():
        if kind == "table":
            out.append(_lift_table(nm, obj or {}))
        else:
            out.append(_lift_routine(kind, nm, obj or {}))
    return out

def _catalog_to_items(cat: Dict[str, Any]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for kind, key in (("table","Tables"), ("view","Views"), ("procedure","Procedures"), ("function","Functions")):
        grp = _ci_get(cat, key) or {}
        if isinstance(grp, dict):
            items.extend(_walk_group(kind, grp))
    return items

@lru_cache(None)
def load_catalog(path: Optional[str] = None) -> Dict[str, Any]:
    p = Path(path) if path else CATALOG_JSON
    if not p.exists():
        raise FileNotFoundError(f"catalog.json not found at {p}")
    return _read_json(p)

@lru_cache(None)
def load_items() -> List[Dict[str, Any]]:
    """
    Prefer items.json for semantic search if present.
    Otherwise, synthesize items from catalog.json (deterministic ops use this anyway).
    """
    if ITEMS_JSON.exists():
        try:
            return _read_json(ITEMS_JSON)
        except Exception:
            pass
    cat = load_catalog()
    return _catalog_to_items(cat)

@lru_cache(None)
def load_emb():
    # optional; keep existing behavior if you had an embeddings file elsewhere.
    # Returning None allows code paths that don't need embeddings to proceed.
    return None

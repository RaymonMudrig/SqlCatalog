# qcat/items.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

# Import paths
try:
    from .paths import CATALOG_JSON
except ImportError:
    from paths import CATALOG_JSON

def _read_json(p: Path) -> Optional[dict]:
    """Read JSON file, return None on error."""
    try:
        if p and p.exists():
            return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        pass
    return None

def _build_indices_from_catalog(catalog: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build lightweight name indexes so ops/formatters can work even without items.json.
    """
    idx: Dict[str, Any] = {
        "tables": [],
        "views": [],
        "procedures": [],
        "functions": [],
        "by_kind": {
            "table": {},
            "view": {},
            "procedure": {},
            "function": {},
        },
    }

    for section_name, kind_key in (
        ("Tables", "table"),
        ("Views", "view"),
        ("Procedures", "procedure"),
        ("Functions", "function"),
    ):
        section = (catalog or {}).get(section_name) or {}
        for safe, meta in section.items():
            schema = meta.get("Schema") or ""
            name = meta.get("Safe_Name") or safe
            fq = f"{schema}.{name}" if schema else name
            idx[f"{kind_key}s"].append(fq)
            idx["by_kind"][kind_key][fq] = meta

    return idx

def load_items() -> Tuple[Dict[str, Any], Optional[Any]]:
    """
    Load items from catalog.json.

    Simplified: Always builds from catalog.json (no pre-built items.json needed).
    Returns: (items dict, None) - embeddings always None (not used in current flow)
    """
    catalog = _read_json(Path(CATALOG_JSON)) or {}
    if not catalog:
        raise FileNotFoundError(f"catalog.json not found or empty at {CATALOG_JSON}")

    items: Dict[str, Any] = {"catalog": catalog}
    items.update(_build_indices_from_catalog(catalog))

    # Embeddings not used in current query flow (always return None)
    return items, None

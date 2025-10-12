# qcat/items.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

# Import your existing paths (names preserved)
from qcat.paths import (
    OUTPUT_DIR,
    CATALOG_JSON,
    ITEMS_JSON,
    ITEMS_PATH,
    EMB_PATH,
)

def _read_json(p: Path) -> Optional[dict]:
    try:
        if p and p.exists():
            return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        pass
    return None

def _maybe_load_numpy(path: Path):
    """Try to load .npy embeddings if numpy is available."""
    try:
        import numpy as np  # optional
        return np.load(str(path), allow_pickle=True)
    except Exception:
        return None

def _load_embeddings(items_path_used: Optional[Path]) -> Optional[Any]:
    """
    Try several embedding locations:
      1) EMB_PATH (global: output/vector_index/embeddings.npy)
      2) items_path_used.with_suffix('.emb.json')
      3) items_path_used.parent / 'embeddings.npy'
    Accept either .npy or .json formats.
    """
    # 1) global EMB_PATH
    if EMB_PATH and EMB_PATH.exists():
        if EMB_PATH.suffix.lower() == ".npy":
            emb = _maybe_load_numpy(EMB_PATH)
            if emb is not None:
                return emb
        else:
            emb = _read_json(EMB_PATH)
            if emb is not None:
                return emb

    if not items_path_used:
        return None

    # 2) colocated .emb.json
    emb_json = items_path_used.with_suffix(".emb.json")
    if emb_json.exists():
        emb = _read_json(emb_json)
        if emb is not None:
            return emb

    # 3) colocated embeddings.npy
    emb_npy = items_path_used.parent / "embeddings.npy"
    if emb_npy.exists():
        emb = _maybe_load_numpy(emb_npy)
        if emb is not None:
            return emb

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
    Load 'items' (and optional embeddings) used by the backend.

    Priority for items.json:
      1) ITEMS_JSON (VectorizeCatalog/items.json)
      2) ITEMS_PATH (output/vector_index/items.json)
      3) OUTPUT_DIR/items.json

    Fallback:
      - Build minimal items from CATALOG_JSON (output/catalog.json)
    """
    candidates = [
        Path(ITEMS_JSON),
        Path(ITEMS_PATH),
        OUTPUT_DIR / "items.json",
    ]

    # Try to load items.json from candidates
    for cand in candidates:
        items = _read_json(cand)
        if isinstance(items, dict):
            # Enrich with catalog if missing
            if "catalog" not in items:
                cat = _read_json(Path(CATALOG_JSON))
                if cat:
                    items["catalog"] = cat

            # Load embeddings (optional)
            emb = _load_embeddings(cand)
            return items, emb

    # No items.json anywhere -> fallback to catalog-only mode
    catalog = _read_json(Path(CATALOG_JSON)) or {}
    items: Dict[str, Any] = {"catalog": catalog}
    items.update(_build_indices_from_catalog(catalog))
    emb = None
    return items, emb

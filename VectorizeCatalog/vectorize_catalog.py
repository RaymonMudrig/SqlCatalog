#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Builds a semantic index from ../output/catalog.json and ../output/sql_exports,
including:
  • tables (with columns, pk/fk, indexes, doc, unused flag)
  • views (reads, projected columns, doc)
  • procedures (reads/writes/calls, column refs, doc)
  • functions (discovered from sql_exports if present)
  • UNUSED tables & columns as separate, searchable items

Artifacts:
  ../output/vector_index/
      embeddings.npy       # float32 [N, D]
      items.json           # metadata per item (kind/schema/name/safe_name/.../sql)
      meta.json            # model, dim, created_at, counts
  ../output/sql_entities.json  # simple list of entities + SQL (easy to “list” later)

Environment (optional):
  LMSTUDIO_BASE_URL=http://localhost:1234/v1
  LMSTUDIO_API_KEY=lm-studio
  EMBED_MODEL=text-embedding-nomic-embed-text-v1.5
  USE_LMSTUDIO=1  (else uses sentence-transformers)
  SQL_OUTPUT_DIR=/abs/path/to/output   (defaults to ../output relative to this file)
"""

from __future__ import annotations
import os, json, re, sys, time
from pathlib import Path
from typing import Dict, List, Any, Tuple, Iterable, Optional

import numpy as np

# Optional: local embeddings if not using LM Studio
USE_LMSTUDIO = os.getenv("USE_LMSTUDIO", "1") != "0"
LMSTUDIO_BASE_URL = os.getenv("LMSTUDIO_BASE_URL", "http://localhost:1234/v1")
LMSTUDIO_API_KEY  = os.getenv("LMSTUDIO_API_KEY", "lm-studio")
EMBED_MODEL       = os.getenv("EMBED_MODEL", "text-embedding-nomic-embed-text-v1.5")

BASE = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = Path(os.getenv("SQL_OUTPUT_DIR") or (BASE.parent / "output")).resolve()
CATALOG_PATH  = DEFAULT_OUTPUT_DIR / "catalog.json"
EXPORTS_DIR   = DEFAULT_OUTPUT_DIR / "sql_exports"      # tables/, views/, procedures/, functions/
INDEX_DIR     = DEFAULT_OUTPUT_DIR / "vector_index"
SQL_LIST_PATH = DEFAULT_OUTPUT_DIR / "sql_entities.json"

BATCH = int(os.getenv("EMBED_BATCH", "64"))

# ------------------------------ Embedding backends ------------------------------

def embed_texts_lmstudio(texts: List[str], model: str) -> np.ndarray:
    import requests
    url = f"{LMSTUDIO_BASE_URL.rstrip('/')}/embeddings"
    headers = {"Authorization": f"Bearer {LMSTUDIO_API_KEY}", "Content-Type": "application/json"}
    vecs: List[List[float]] = []
    for i in range(0, len(texts), BATCH):
        chunk = texts[i:i+BATCH]
        payload = {"model": model, "input": chunk}
        r = requests.post(url, headers=headers, json=payload, timeout=600)
        r.raise_for_status()
        data = r.json()
        vecs.extend(item["embedding"] for item in data["data"])
    arr = np.array(vecs, dtype=np.float32)
    norms = np.linalg.norm(arr, axis=1, keepdims=True) + 1e-8
    return arr / norms

def embed_texts_local(texts: List[str]) -> np.ndarray:
    from sentence_transformers import SentenceTransformer
    model_name = os.getenv("LOCAL_EMBED_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
    m = SentenceTransformer(model_name)
    vecs = m.encode(texts, batch_size=max(16, BATCH//2), show_progress_bar=True,
                    convert_to_numpy=True, normalize_embeddings=True)
    return vecs.astype(np.float32)

def embed_texts(texts: List[str], model: str) -> np.ndarray:
    if USE_LMSTUDIO:
        return embed_texts_lmstudio(texts, model)
    return embed_texts_local(texts)

# ------------------------------ Utilities ------------------------------

def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))

def try_read(path: Path) -> Optional[str]:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return None

def safe_join(*parts: str) -> str:
    return "·".join([p for p in parts if p])

def summarize_list(items: Iterable[str], limit: int = 12) -> str:
    items = [i for i in items if i]
    if len(items) <= limit:
        return ", ".join(items)
    return ", ".join(items[:limit]) + f", … (+{len(items)-limit} more)"

def load_sql_export(kind: str, safe_name: str) -> Tuple[Optional[str], Optional[str]]:
    p = (EXPORTS_DIR / kind / f"{safe_name}.sql").resolve()
    if p.exists():
        return (str(p), try_read(p))
    return (None, None)

def build_table_text(t: Dict[str, Any]) -> str:
    cols = t.get("columns") or t.get("Columns") or {}
    col_lines = []
    for cname, cinfo in cols.items():
        typ = cinfo.get("type") or cinfo.get("Type")
        nullable = cinfo.get("nullable") if "nullable" in cinfo else cinfo.get("Nullable")
        default = cinfo.get("default") if "default" in cinfo else cinfo.get("Default")
        cdoc = cinfo.get("doc") if "doc" in cinfo else cinfo.get("Doc")
        line = f"- {cname}: {typ} " + ("(nullable)" if nullable else "(not null)")
        if default:
            line += f" default {default}"
        if cdoc:
            line += f" — {cdoc}"
        col_lines.append(line)

    pk = t.get("primary_key") or t.get("Primary_Key") or []
    fks = t.get("foreign_keys") or t.get("Foreign_Keys") or []
    idxs = t.get("indexes") or t.get("Indexes") or {}

    # Format FKs safely (no nested f-strings)
    fk_lines: List[str] = []
    for fk in fks:
        lc = fk.get("local") or fk.get("Local_Column")
        rs = fk.get("ref_schema") or fk.get("Ref_Schema")
        rt = fk.get("ref_table") or fk.get("Ref_Table")
        rc = fk.get("ref_column") or fk.get("Ref_Column")
        fk_lines.append(f"{lc} -> {rs}.{rt}.{rc}")

    # Format indexes safely
    idx_lines: List[str] = []
    for k, v in (idxs.items() if isinstance(idxs, dict) else []):
        if isinstance(v, list):
            inner = ",".join(v)
        else:
            inner = str(v)
        idx_lines.append(f"{k}({inner})")

    parts = [
        f"TABLE {t.get('schema','')}.{t.get('Original_Name') or t.get('original_name') or t.get('name','')}".strip("."),
        f"safe: {t.get('Safe_Name') or t.get('safe_name')}",
        f"doc: {t.get('Doc') or t.get('doc') or ''}".strip(),
        f"primary key: {summarize_list(pk)}" if pk else "",
        f"foreign keys: {summarize_list(fk_lines)}" if fk_lines else "",
        f"indexes: {summarize_list(idx_lines)}" if idx_lines else "",
        "columns:",
        *col_lines
    ]
    return "\n".join([p for p in parts if p])

def build_view_text(v: Dict[str, Any]) -> str:
    cols = v.get("Columns") or v.get("columns") or []
    reads = v.get("Reads") or v.get("reads") or []
    read_names = []
    for r in reads:
        read_names.append(r.get("Safe_Name") or r.get("safe_name") or safe_join(r.get("Schema") or r.get("schema"), r.get("Name") or r.get("name")))
    parts = [
        f"VIEW {v.get('schema','')}.{v.get('Original_Name') or v.get('original_name') or v.get('name','')}".strip("."),
        f"safe: {v.get('Safe_Name') or v.get('safe_name')}",
        f"doc: {v.get('Doc') or v.get('doc') or ''}".strip(),
        f"projects columns: {summarize_list(cols)}" if cols else "",
        f"reads tables: {summarize_list(read_names)}" if read_names else ""
    ]
    return "\n".join([p for p in parts if p])

def build_proc_text(p: Dict[str, Any]) -> str:
    reads = p.get("Reads") or p.get("reads") or []
    writes = p.get("Writes") or p.get("writes") or []
    calls = p.get("Calls") or p.get("calls") or []
    colrefs = p.get("Column_Refs") or p.get("column_refs") or {}

    def names(lst):
        out = []
        for r in lst:
            out.append(r.get("Safe_Name") or r.get("safe_name") or safe_join(r.get("Schema") or r.get("schema"), r.get("Name") or r.get("name")))
        return out

    colref_lines = []
    for tbl, cols in colrefs.items():
        if isinstance(cols, (list, tuple, set)):
            colref_lines.append(f"- {tbl}: {summarize_list(sorted(list(cols)))}")
        else:
            colref_lines.append(f"- {tbl}: {cols}")

    parts = [
        f"PROCEDURE {p.get('schema','')}.{p.get('Original_Name') or p.get('original_name') or p.get('name','')}".strip("."),
        f"safe: {p.get('Safe_Name') or p.get('safe_name')}",
        f"doc: {p.get('Doc') or p.get('doc') or ''}".strip(),
        f"reads: {summarize_list(names(reads))}" if reads else "",
        f"writes: {summarize_list(names(writes))}" if writes else "",
        f"calls: {summarize_list(names(calls))}" if calls else "",
        "column refs:" if colrefs else "",
        *colref_lines
    ]
    return "\n".join([p for p in parts if p])

def build_function_text(name_safe: str, sql: Optional[str]) -> str:
    return f"FUNCTION {name_safe}\n" + (f"sql:\n{sql}" if sql else "")

# ------------------------------ Index building ------------------------------

def main():
    INDEX_DIR.mkdir(parents=True, exist_ok=True)
    if not CATALOG_PATH.exists():
        print(f"❗ catalog.json not found at {CATALOG_PATH}", file=sys.stderr)
        sys.exit(2)

    cat = read_json(CATALOG_PATH)

    items: List[Dict[str, Any]] = []
    sql_entities: List[Dict[str, Any]] = []

    # ---- Tables ----
    for safe_name, t in (cat.get("Tables") or cat.get("tables") or {}).items():
        schema = t.get("Schema") or t.get("schema") or ""
        original = t.get("Original_Name") or t.get("original_name") or ""
        doc = t.get("Doc") or t.get("doc")
        is_unused = bool(t.get("Is_Unused") or t.get("is_unused") or False)

        sql_path, sql_text = load_sql_export("tables", safe_name)
        text = build_table_text(t)
        if doc:
            text += f"\nnotes: {doc}"
        if is_unused:
            text += "\nstatus: unused, unaccessed, not referenced by views or procedures"

        items.append({
            "id": f"table::{safe_name}",
            "kind": "table",
            "schema": schema,
            "name": original,
            "safe_name": safe_name,
            "is_unused": is_unused,
            "doc": doc,
            "text": text,
            "sql": sql_text,
            "sql_path": sql_path
        })

        # Add one item per COLUMN
        columns = t.get("Columns") or t.get("columns") or {}
        for col_name, cinfo in columns.items():
            # Determine referenced_in with either case
            ref_in = cinfo.get("Referenced_In") if "Referenced_In" in cinfo else cinfo.get("referenced_in", [])
            col_unused = not bool(ref_in)

            typ = cinfo.get("type") or cinfo.get("Type")
            nullable = cinfo.get("nullable") if "nullable" in cinfo else cinfo.get("Nullable")
            default = cinfo.get("default") if "default" in cinfo else cinfo.get("Default")
            cdoc = cinfo.get("doc") if "doc" in cinfo else cinfo.get("Doc")

            col_text = f"COLUMN {schema}.{original}.{col_name}\n"
            col_text += f"type: {typ}, {'nullable' if nullable else 'not null'}"
            if default:
                col_text += f", default: {default}"
            if cdoc:
                col_text += f"\nnotes: {cdoc}"
            if col_unused:
                col_text += "\nstatus: unused, unaccessed, not referenced by any view or procedure"

            items.append({
                "id": f"column::{safe_name}.{col_name}",
                "kind": "column",
                "schema": schema,
                "table": original,
                "name": col_name,
                "safe_table": safe_name,
                "is_unused": col_unused,
                "doc": cdoc,
                "text": col_text,
                "sql": sql_text,
                "sql_path": sql_path
            })

        if sql_text is not None:
            sql_entities.append({
                "kind": "table", "safe_name": safe_name, "schema": schema, "name": original,
                "sql_path": sql_path, "sql": sql_text
            })

    # ---- Views ----
    for safe_name, v in (cat.get("Views") or cat.get("views") or {}).items():
        schema = v.get("Schema") or v.get("schema") or ""
        original = v.get("Original_Name") or v.get("original_name") or ""
        sql_path, sql_text = load_sql_export("views", safe_name)
        text = build_view_text(v)
        items.append({
            "id": f"view::{safe_name}",
            "kind": "view",
            "schema": schema,
            "name": original,
            "safe_name": safe_name,
            "doc": v.get("Doc") or v.get("doc"),
            "text": text,
            "sql": sql_text,
            "sql_path": sql_path
        })
        if sql_text is not None:
            sql_entities.append({
                "kind": "view", "safe_name": safe_name, "schema": schema, "name": original,
                "sql_path": sql_path, "sql": sql_text
            })

    # ---- Procedures ----
    for safe_name, p in (cat.get("Procedures") or cat.get("procedures") or {}).items():
        schema = p.get("Schema") or p.get("schema") or ""
        original = p.get("Original_Name") or p.get("original_name") or ""
        sql_path, sql_text = load_sql_export("procedures", safe_name)
        text = build_proc_text(p)
        items.append({
            "id": f"procedure::{safe_name}",
            "kind": "procedure",
            "schema": schema,
            "name": original,
            "safe_name": safe_name,
            "doc": p.get("Doc") or p.get("doc"),
            "text": text,
            "sql": sql_text,
            "sql_path": sql_path
        })
        if sql_text is not None:
            sql_entities.append({
                "kind": "procedure", "safe_name": safe_name, "schema": schema, "name": original,
                "sql_path": sql_path, "sql": sql_text
            })

    # ---- Functions (discover via exports; catalog may not include them) ----
    func_dir = EXPORTS_DIR / "functions"
    if func_dir.exists():
        for pth in sorted(func_dir.glob("*.sql")):
            safe_name = pth.stem
            sql_text = try_read(pth)
            schema = safe_name.split("·")[0] if "·" in safe_name else ""
            name    = safe_name.split("·")[1] if "·" in safe_name else safe_name
            items.append({
                "id": f"function::{safe_name}",
                "kind": "function",
                "schema": schema,
                "name": name,
                "safe_name": safe_name,
                "doc": None,
                "text": build_function_text(safe_name, None),
                "sql": sql_text,
                "sql_path": str(pth)
            })
            if sql_text is not None:
                sql_entities.append({
                    "kind": "function", "safe_name": safe_name, "schema": schema, "name": name,
                    "sql_path": str(pth), "sql": sql_text
                })

    # ---- Also add explicit UNUSED lists from catalog ----
    for safe_name in cat.get("Unused_Tables") or cat.get("unused_tables") or []:
        if not any(it["id"] == f"table::{safe_name}" for it in items):
            sql_path, sql_text = load_sql_export("tables", safe_name)
            schema = safe_name.split("·")[0] if "·" in safe_name else ""
            name   = safe_name.split("·")[1] if "·" in safe_name else safe_name
            text = f"TABLE {schema}.{name}\nsafe: {safe_name}\nstatus: unused, unaccessed, not referenced"
            items.append({
                "id": f"table::{safe_name}",
                "kind": "table",
                "schema": schema,
                "name": name,
                "safe_name": safe_name,
                "is_unused": True,
                "doc": None,
                "text": text,
                "sql": sql_text,
                "sql_path": sql_path
            })

    for uc in cat.get("Unused_Columns") or cat.get("unused_columns") or []:
        if isinstance(uc, dict):
            safe_table = uc.get("Table") or uc.get("table")
            col_name   = uc.get("Column") or uc.get("column")
        elif isinstance(uc, (list, tuple)) and len(uc) >= 2:
            safe_table, col_name = uc[0], uc[1]
        else:
            continue
        if not safe_table or not col_name:
            continue
        sql_path, sql_text = load_sql_export("tables", safe_table)
        schema = safe_table.split("·")[0] if "·" in safe_table else ""
        table  = safe_table.split("·")[1] if "·" in safe_table else safe_table
        text = f"COLUMN {schema}.{table}.{col_name}\nsafe table: {safe_table}\nstatus: unused, unaccessed, not referenced"
        items.append({
            "id": f"column::{safe_table}.{col_name}",
            "kind": "column",
            "schema": schema,
            "table": table,
            "name": col_name,
            "safe_table": safe_table,
            "is_unused": True,
            "doc": None,
            "text": text,
            "sql": sql_text,
            "sql_path": sql_path
        })

    # ---- Write the SQL entity list (for “listing the SQL creation of entities”) ----
    SQL_LIST_PATH.write_text(json.dumps(sql_entities, ensure_ascii=False, indent=2), encoding="utf-8")

    # ---- Build embeddings ----
    texts = [it["text"] for it in items]
    print(f"[vectorize_catalog] Embedding {len(texts)} items using "
          f"{'LM Studio '+EMBED_MODEL if USE_LMSTUDIO else 'sentence-transformers'}")
    emb = embed_texts(texts, EMBED_MODEL)
    dim = int(emb.shape[1])

    # ---- Save artifacts ----
    INDEX_DIR.mkdir(parents=True, exist_ok=True)
    np.save(INDEX_DIR / "embeddings.npy", emb)

    with (INDEX_DIR / "items.json").open("w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)

    meta = {
        "model": EMBED_MODEL if USE_LMSTUDIO else os.getenv("LOCAL_EMBED_MODEL", "sentence-transformers/all-MiniLM-L6-v2"),
        "provider": "lmstudio" if USE_LMSTUDIO else "sentence-transformers",
        "dimension": dim,
        "count": len(items),
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S %z"),
        "catalog_path": str(CATALOG_PATH),
        "exports_dir": str(EXPORTS_DIR),
        "index_dir": str(INDEX_DIR)
    }
    (INDEX_DIR / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[vectorize_catalog] Wrote: {INDEX_DIR/'embeddings.npy'}")
    print(f"[vectorize_catalog] Wrote: {INDEX_DIR/'items.json'}")
    print(f"[vectorize_catalog] Wrote: {INDEX_DIR/'meta.json'}")
    print(f"[vectorize_catalog] Wrote: {SQL_LIST_PATH} (entity SQL listing)")

if __name__ == "__main__":
    main()

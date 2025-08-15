# vectorize_catalog.py
import os, json
from pathlib import Path
import numpy as np

CATALOG_PATH = "./output/catalog.json"
INDEX_DIR = "./output/vector_index"

# Default to LM Studio embedding server + your model
BASE_URL = os.environ.get("LMSTUDIO_BASE_URL", "http://localhost:1234/v1")
API_KEY  = os.environ.get("LMSTUDIO_API_KEY", "lm-studio")
EMBED_MODEL = os.environ.get("EMBED_MODEL", "text-embedding-nomic-embed-text-v1.5")
USE_LMSTUDIO = os.environ.get("USE_LMSTUDIO", "1") == "1"  # default ON for your setup

def short(s, n=120):
    s = " ".join(str(s).split())
    return s if len(s) <= n else s[:n-1] + "…"

def join_cols(cols_dict, max_cols=60):
    parts = []
    for i, (col, info) in enumerate(cols_dict.items()):
        if i >= max_cols:
            parts.append(f"... (+{len(cols_dict)-max_cols} more cols)")
            break
        t = info.get("Type") or info.get("type") or ""
        nn = "NULL" if (info.get("Nullable") or info.get("nullable")) else "NOT NULL"
        dv = info.get("Default") or info.get("default")
        parts.append(f"{col}: {t} {nn}" + (f" DEFAULT {dv}" if dv else ""))
    return "; ".join(parts)

def as_text_for_table(key, t):
    schema = t.get("Schema") or t.get("schema") or "dbo"
    safe = t.get("Safe_Name") or t.get("safe_name") or key
    orig = t.get("Original_Name") or t.get("original_name") or key
    cols = t.get("Columns") or t.get("columns") or {}
    pk = t.get("Primary_Key") or t.get("primary_key") or []
    fks = t.get("Foreign_Keys") or t.get("foreign_keys") or []
    idx = t.get("Indexes") or t.get("indexes") or {}

    fk_lines = []
    for fk in fks:
        ref_schema = fk.get("Referenced_Schema") or fk.get("referenced_schema")
        ref_tab = fk.get("Referenced_Table") or fk.get("referenced_table")
        ref_col = fk.get("Referenced_Column") or fk.get("referenced_column")
        col = fk.get("Column") or fk.get("column")
        fk_lines.append(f"FK {col} -> {ref_schema}.{ref_tab}({ref_col})")

    idx_lines = []
    for iname, cols_l in idx.items():
        if isinstance(cols_l, dict) and "columns" in cols_l:
            cols_l = cols_l["columns"]
        idx_lines.append(f"INDEX {iname} ({', '.join(cols_l)})")

    text = (
        f"TABLE {schema}.{orig} (safe: {safe}). "
        f"PrimaryKey: {', '.join(pk) if pk else 'none'}. "
        f"Columns: {join_cols(cols)}. "
    )
    if fk_lines:
        text += " " + " ".join(fk_lines) + "."
    if idx_lines:
        text += " " + " ".join(idx_lines) + "."
    return text

def as_text_for_view(name, v):
    schema = v.get("Schema") or v.get("schema") or "dbo"
    cols = v.get("Columns") or v.get("columns") or []
    reads = v.get("Reads") or v.get("reads") or []
    reads_str = ", ".join([f"{(r.get('schema') or r.get('Schema') or 'dbo')}.{r.get('name') or r.get('Name')}" for r in reads])
    return f"VIEW {schema}.{name}. Columns: {', '.join(cols) if cols else '(unknown)'}; Reads: {reads_str or 'none'}."

def as_text_for_proc(name, p):
    schema = p.get("Schema") or p.get("schema") or "dbo"
    params = p.get("Params") or p.get("params") or []
    reads = p.get("Reads") or p.get("reads") or []
    writes = p.get("Writes") or p.get("writes") or []
    calls = p.get("Calls") or p.get("calls") or []
    access = p.get("Access") or ("read" if not writes else "write")
    prm_str = ", ".join([f"@{prm.get('Name') or prm.get('name')} {prm.get('Type') or prm.get('type')}" for prm in params]) or "(no params)"
    r_str = ", ".join([f"{(r.get('schema') or r.get('Schema') or 'dbo')}.{r.get('name') or r.get('Name')}" for r in reads]) or "none"
    w_str = ", ".join([f"{(w.get('schema') or w.get('Schema') or 'dbo')}.{w.get('name') or w.get('Name')}" for w in writes]) or "none"
    c_str = ", ".join([f"{(c.get('schema') or c.get('Schema') or 'dbo')}.{c.get('name') or c.get('Name')}" for c in calls]) or "none"
    return f"PROC {schema}.{name} [{access}]. Params: {prm_str}. Reads: {r_str}. Writes: {w_str}. Calls: {c_str}."

def as_text_for_func(name, f):
    schema = f.get("Schema") or f.get("schema") or "dbo"
    params = f.get("Params") or f.get("params") or []
    reads = f.get("Reads") or f.get("reads") or []
    writes = f.get("Writes") or f.get("writes") or []
    calls = f.get("Calls") or f.get("calls") or []
    access = f.get("Access") or ("read" if not writes else "write")
    prm_str = ", ".join([f"@{prm.get('Name') or prm.get('name')} {prm.get('Type') or prm.get('type')}" for prm in params]) or "(no params)"
    r_str = ", ".join([f"{(r.get('schema') or r.get('Schema') or 'dbo')}.{r.get('name') or r.get('Name')}" for r in reads]) or "none"
    w_str = ", ".join([f"{(w.get('schema') or w.get('Schema') or 'dbo')}.{w.get('name') or w.get('Name')}" for w in writes]) or "none"
    c_str = ", ".join([f"{(c.get('schema') or c.get('Schema') or 'dbo')}.{c.get('name') or c.get('Name')}" for c in calls]) or "none"
    return f"FUNC {schema}.{name} [{access}]. Params: {prm_str}. Reads: {r_str}. Writes: {w_str}. Calls: {c_str}."

def build_documents(cat):
    docs = []
    # Tables
    for safe_key, t in (cat.get("Tables") or cat.get("tables") or {}).items():
        text = as_text_for_table(safe_key, t)
        docs.append({
            "id": f"table::{(t.get('Schema') or t.get('schema') or 'dbo')}.{t.get('Original_Name') or t.get('original_name') or safe_key}",
            "kind": "table",
            "schema": t.get("Schema") or t.get("schema") or "dbo",
            "name": t.get("Original_Name") or t.get("original_name") or safe_key,
            "safe_name": t.get("Safe_Name") or t.get("safe_name") or safe_key,
            "text": text
        })
    # Views
    for vname, v in (cat.get("Views") or cat.get("views") or {}).items():
        text = as_text_for_view(vname, v)
        docs.append({
            "id": f"view::{(v.get('Schema') or v.get('schema') or 'dbo')}.{vname}",
            "kind": "view",
            "schema": v.get("Schema") or v.get("schema") or "dbo",
            "name": vname,
            "safe_name": vname,
            "text": text
        })
    # Procedures
    for pname, p in (cat.get("Procedures") or cat.get("procedures") or {}).items():
        text = as_text_for_proc(pname, p)
        docs.append({
            "id": f"proc::{(p.get('Schema') or p.get('schema') or 'dbo')}.{pname}",
            "kind": "procedure",
            "schema": p.get("Schema") or p.get("schema") or "dbo",
            "name": pname,
            "safe_name": pname,
            "access": p.get("Access") or ("read" if not p.get('Writes') and not p.get('writes') else "write"),
            "text": text
        })
    # Functions
    for fname, fobj in (cat.get("Functions") or cat.get("functions") or {}).items():
        text = as_text_for_func(fname, fobj)
        f_schema = fobj.get("Schema") or fobj.get("schema") or "dbo"
        access = fobj.get("Access") or ("read" if not fobj.get('Writes') and not fobj.get('writes') else "write")
        docs.append({
            "id": f"func::{f_schema}.{fname}",
            "kind": "function",
            "schema": f_schema,
            "name": fname,
            "safe_name": fname,
            "access": access,
            "text": text
        })
    return docs

# ---------- Embeddings ----------
def embed_with_lmstudio(texts):
    import requests
    headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}
    data = {"model": EMBED_MODEL, "input": texts}
    r = requests.post(f"{BASE_URL}/embeddings", headers=headers, json=data, timeout=600)
    r.raise_for_status()
    out = r.json()
    if "data" not in out or not out["data"]:
        raise RuntimeError(f"LM Studio returned no embeddings. Response: {out}")
    return [d["embedding"] for d in out["data"]]

def embed_with_st(texts):
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer(os.environ.get("ST_MODEL", "sentence-transformers/all-MiniLM-L6-v2"))
    return model.encode(texts, batch_size=32, convert_to_numpy=True, show_progress_bar=True, normalize_embeddings=True).tolist()

def batched(seq, n=64):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def main():
    Path(INDEX_DIR).mkdir(parents=True, exist_ok=True)

    # BOM-safe read
    with open(CATALOG_PATH, encoding="utf-8-sig") as f:
        root = json.load(f)
    catalog_obj = root.get("Catalog") or root   # supports both the .NET export wrapper or plain catalog

    docs = build_documents(catalog_obj)
    if not docs:
        print("No documents found in catalog.")
        return

    # Compute embeddings
    texts = [d["text"] for d in docs]
    embeddings = []
    if USE_LMSTUDIO:
        print(f"Using LM Studio embeddings @ {BASE_URL} model={EMBED_MODEL}")
        # warm-up to detect dim
        warm = embed_with_lmstudio(["__dim_probe__"])[0]
        dim = len(warm)
        print(f"Detected embedding dimension: {dim}")
        # batch the rest including the probe separately
        for chunk in batched(texts, 64):
            embeddings.extend(embed_with_lmstudio(chunk))
    else:
        print("Using sentence-transformers fallback (CPU).")
        veclist = embed_with_st(texts)
        dim = len(veclist[0]) if veclist else 0
        embeddings = veclist

    import numpy as np
    embs = np.array(embeddings, dtype="float32")
    # Normalize for cosine similarity
    embs = embs / (np.linalg.norm(embs, axis=1, keepdims=True) + 1e-12)

    # Save index
    np.save(os.path.join(INDEX_DIR, "embeddings.npy"), embs)
    with open(os.path.join(INDEX_DIR, "items.json"), "w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False, indent=2)
    with open(os.path.join(INDEX_DIR, "meta.json"), "w", encoding="utf-8") as f:
        json.dump({"model": EMBED_MODEL, "use_lmstudio": USE_LMSTUDIO, "dim": int(dim)}, f, indent=2)

    print(f"✅ Indexed {len(docs)} objects (dim={dim}) → {INDEX_DIR}/embeddings.npy + items.json + meta.json")

if __name__ == "__main__":
    main()

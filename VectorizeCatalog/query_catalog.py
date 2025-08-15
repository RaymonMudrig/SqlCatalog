# query_catalog.py
import os, json, numpy as np

INDEX_DIR = "./output/vector_index"

BASE_URL = os.environ.get("LMSTUDIO_BASE_URL", "http://localhost:1234/v1")
API_KEY  = os.environ.get("LMSTUDIO_API_KEY", "lm-studio")
EMBED_MODEL = os.environ.get("EMBED_MODEL", "text-embedding-nomic-embed-text-v1.5")
USE_LMSTUDIO = os.environ.get("USE_LMSTUDIO", "1") == "1"  # default ON to match vectorize script

def load_index():
    embs = np.load(os.path.join(INDEX_DIR, "embeddings.npy"))
    with open(os.path.join(INDEX_DIR, "items.json"), encoding="utf-8-sig") as f:
        items = json.load(f)
    meta = {}
    try:
        meta = json.load(open(os.path.join(INDEX_DIR, "meta.json"), encoding="utf-8"))
    except Exception:
        pass
    return embs.astype("float32"), items, meta

def embed_query_lmstudio(texts):
    import requests
    headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}
    data = {"model": EMBED_MODEL, "input": texts}
    r = requests.post(f"{BASE_URL}/embeddings", headers=headers, json=data, timeout=60)
    r.raise_for_status()
    out = r.json()
    if "data" not in out or not out["data"]:
        raise RuntimeError(f"LM Studio returned no embeddings. Raw: {out}")
    vecs = [d["embedding"] for d in out["data"]]
    arr = np.array(vecs, dtype="float32")
    return arr

def embed_query_st(texts):
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer(os.environ.get("ST_MODEL", "sentence-transformers/all-MiniLM-L6-v2"))
    arr = model.encode(texts, convert_to_numpy=True)
    return arr.astype("float32")

def embed_query(texts):
    return embed_query_lmstudio(texts) if USE_LMSTUDIO else embed_query_st(texts)

def normalize(a):
    norms = np.linalg.norm(a, axis=1, keepdims=True) + 1e-12
    return a / norms

def search(query, k=10, kind=None, schema=None):
    embs, items, meta = load_index()
    d_index = embs.shape[1]

    q_vec = embed_query([query])
    if q_vec.ndim == 1:
        q_vec = q_vec[None, :]
    if q_vec.shape[1] != d_index:
        raise ValueError(
            f"Embedding dimension mismatch: index={d_index}, query={q_vec.shape[1]}.\n"
            f"Index meta: {meta}. Ensure EMBED_MODEL and USE_LMSTUDIO match your vectorization run."
        )

    embs = normalize(embs)
    q_vec = normalize(q_vec)
    q = q_vec[0]

    # Optional filters
    mask = np.ones(len(items), dtype=bool)
    if kind:
        mask &= np.array([it["kind"] == kind for it in items])
    if schema:
        mask &= np.array([(it.get("schema") or "dbo").lower() == schema.lower() for it in items])

    idxs = np.where(mask)[0]
    if idxs.size == 0:
        return []

    sims = embs[idxs] @ q
    order = np.argsort(-sims)[:k]
    top = idxs[order]

    results = []
    for i in top:
        it = items[i]
        results.append({
            "score": float((embs[i] @ q)),
            "id": it["id"],
            "kind": it["kind"],
            "schema": it.get("schema"),
            "name": it.get("name"),
            "safe_name": it.get("safe_name"),
            "preview": (it["text"][:300] + ("…" if len(it["text"]) > 300 else "")),
        })
    return results

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("query", help="semantic query")
    p.add_argument("--k", type=int, default=10)
    p.add_argument("--kind", choices=["table","view","procedure","function"])
    p.add_argument("--schema")
    args = p.parse_args()

    hits = search(args.query, k=args.k, kind=args.kind, schema=args.schema)
    for r in hits:
        print(f"[{r['score']:.3f}] {r['kind']} {r['schema']}.{r['name']} -> {r['id']}")
        print(f"    {r['preview']}\n")

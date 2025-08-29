# rag_chat.py
import os, json, numpy as np, textwrap, requests

INDEX_DIR = "../output/vector_index"

# Embedding server/model (LM Studio)
EMBED_BASE_URL = os.environ.get("LMSTUDIO_BASE_URL", "http://localhost:1234/v1")
EMBED_API_KEY  = os.environ.get("LMSTUDIO_API_KEY", "lm-studio")
EMBED_MODEL    = os.environ.get("EMBED_MODEL", "text-embedding-nomic-embed-text-v1.5")

# Chat server/model (LM Studio with Qwen)
CHAT_BASE_URL = os.environ.get("LMSTUDIO_CHAT_BASE_URL", EMBED_BASE_URL)  # often same server
CHAT_API_KEY  = os.environ.get("LMSTUDIO_CHAT_API_KEY", "lm-studio")
CHAT_MODEL    = os.environ.get("CHAT_MODEL", "qwen2.5-32b-instruct-mlx")

def load_index():
    embs = np.load(f"{INDEX_DIR}/embeddings.npy")
    items = json.load(open(f"{INDEX_DIR}/items.json", encoding="utf-8-sig"))
    meta  = {}
    try:
        meta = json.load(open(f"{INDEX_DIR}/meta.json", encoding="utf-8"))
    except Exception:
        pass
    return embs.astype("float32"), items, meta

def normalize(a):
    norms = np.linalg.norm(a, axis=1, keepdims=True) + 1e-12
    return a / norms

def embed_query(text):
    headers = {"Authorization": f"Bearer {EMBED_API_KEY}", "Content-Type": "application/json"}
    data = {"model": EMBED_MODEL, "input": [text]}
    r = requests.post(f"{EMBED_BASE_URL}/embeddings", headers=headers, json=data, timeout=60)
    r.raise_for_status()
    out = r.json()
    vec = np.array(out["data"][0]["embedding"], dtype="float32")
    vec /= (np.linalg.norm(vec) + 1e-12)
    return vec

def retrieve(query, k=8, kind=None, schema=None):
    embs, items, meta = load_index()
    d = embs.shape[1]
    q = embed_query(query)
    if q.shape[0] != d:
        raise ValueError(f"Embedding dim mismatch: index={d}, query={q.shape[0]}. "
                         f"Rebuild index with the same embedding model you're querying.")

    embs = normalize(embs)

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
            "score": float(embs[i] @ q),
            "id": it["id"],
            "kind": it["kind"],
            "schema": it.get("schema"),
            "name": it.get("name"),
            "safe_name": it.get("safe_name"),
            "text": it["text"],
        })
    return results

def build_context(chunks, max_chars=6000):
    # Concatenate top chunks into a single context block with light headers
    out = []
    total = 0
    for ch in chunks:
        header = f"[{ch['kind']} {ch['schema']}.{ch['name']}]"
        body = ch["text"]
        block = header + "\n" + body.strip()
        if total + len(block) > max_chars:
            break
        out.append(block)
        total += len(block)
    return "\n\n---\n\n".join(out)

def ask_qwen(query, context):
    system_prompt = (
        "You are a precise assistant for a SQL catalog. "
        "Answer ONLY using the provided context. "
        "If the answer isn't in the context, say you cannot find it. "
        "Prefer listing exact table/procedure/view names and relevant columns."
    )
    user_prompt = textwrap.dedent(f"""
    Question:
    {query}

    Context:
    {context}
    """).strip()

    headers = {"Authorization": f"Bearer {CHAT_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": CHAT_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt}
        ],
        "temperature": 0.2,
        "max_tokens": 800
    }
    r = requests.post(f"{CHAT_BASE_URL}/chat/completions", headers=headers, json=payload, timeout=120)
    r.raise_for_status()
    out = r.json()
    return out["choices"][0]["message"]["content"].strip()

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("query")
    p.add_argument("--k", type=int, default=8)
    p.add_argument("--kind", choices=["table","view","procedure","function"])
    p.add_argument("--schema")
    args = p.parse_args()

    hits = retrieve(args.query, k=args.k, kind=args.kind, schema=args.schema)
    if not hits:
        print("No relevant items found.")
        raise SystemExit(0)

    ctx = build_context(hits)
    answer = ask_qwen(args.query, ctx)

    print("\n=== Top matches ===")
    for h in hits:
        print(f"[{h['score']:.3f}] {h['kind']} {h['schema']}.{h['name']} -> {h['id']}")
    print("\n=== Answer (Qwen) ===\n")
    print(answer)

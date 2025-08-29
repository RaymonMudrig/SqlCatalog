import os
import numpy as np

USE_LMSTUDIO = os.getenv("USE_LMSTUDIO", "1") != "0"
LMSTUDIO_BASE_URL = os.getenv("LMSTUDIO_BASE_URL", "http://localhost:1234/v1")
LMSTUDIO_API_KEY  = os.getenv("LMSTUDIO_API_KEY", "lm-studio")
EMBED_MODEL       = os.getenv("EMBED_MODEL", "text-embedding-nomic-embed-text-v1.5")

def embed_query(text: str) -> np.ndarray:
    if USE_LMSTUDIO:
        import requests
        url = f"{LMSTUDIO_BASE_URL.rstrip('/')}/embeddings"
        headers = {"Authorization": f"Bearer {LMSTUDIO_API_KEY}", "Content-Type": "application/json"}
        payload = {"model": EMBED_MODEL, "input": [text]}
        r = requests.post(url, headers=headers, json=payload, timeout=60)
        r.raise_for_status()
        vec = np.array(r.json()["data"][0]["embedding"], dtype=np.float32)
        return vec / (np.linalg.norm(vec) + 1e-8)
    from sentence_transformers import SentenceTransformer
    model_name = os.getenv("LOCAL_EMBED_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
    m = SentenceTransformer(model_name)
    vec = m.encode([text], convert_to_numpy=True, normalize_embeddings=True)[0]
    return vec.astype(np.float32)

def cosine_scores(qvec: np.ndarray, mat: np.ndarray) -> np.ndarray:
    return mat @ qvec  # both normalized

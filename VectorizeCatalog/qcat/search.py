from typing import List, Dict, Any, Optional
import numpy as np
from .embeddings import embed_query, cosine_scores

def semantic_search(query: str, items: List[Dict[str, Any]], emb: np.ndarray,
                    k: int, kind: str, schema: Optional[str], unused_only: bool):
    qvec = embed_query(query)
    mask = np.ones(len(items), dtype=bool)
    if kind != "any":
        mask &= np.array([it.get("kind") == kind for it in items])
    if schema:
        sch = schema.lower()
        mask &= np.array([(it.get("schema") or "").lower() == sch for it in items])
    if unused_only:
        mask &= np.array([bool(it.get("is_unused")) for it in items])
    cand_idx = np.where(mask)[0]
    if cand_idx.size == 0:
        return []
    submat = emb[cand_idx]
    scores = submat @ qvec
    order = np.argsort(-scores)[:k]
    picked = []
    for idx in order:
        global_idx = cand_idx[int(idx)]
        it = items[int(global_idx)].copy()
        it["_score"] = float(scores[int(idx)])
        picked.append(it)
    return picked

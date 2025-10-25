import re
from typing import List, Tuple, Dict, Any

# Embeddings are no longer used in the current query flow
# This module only uses deterministic name matching


# --- Name splitting / tokens ---
def split_safe(safe_name: str) -> Tuple[str, str]:
    if "·" in safe_name:
        s, n = safe_name.split("·", 1)
        return s, n
    return "", safe_name

_CAMEL_RE = re.compile(r"[A-Z]?[a-z]+|[A-Z]+(?![a-z])|\d+")
_WORD_RE  = re.compile(r"[A-Za-z0-9_]+")

def tokens_for(name: str) -> List[str]:
    toks = []
    for t in _WORD_RE.findall(name):
        toks.append(t)
        toks.extend([m.group(0) for m in _CAMEL_RE.finditer(t) if m.group(0).lower() != t.lower()])
    seen=set(); out=[]
    for t in [x.lower() for x in toks if x]:
        if t not in seen: out.append(t); seen.add(t)
    return out

# --- Quoted & kind detection ---
def extract_quoted_names(q: str) -> List[str]:
    quoted = re.findall(r"(?:'([^']+)')|(?:\"([^\"]+)\")|(?:\[((?:[^\]]|])+)\])|(?:`([^`]+)`)", q)
    out = []
    for tup in quoted:
        for s in tup:
            if s: out.append(s.strip())
    return out

def detect_kind(q: str):
    ql = q.lower()
    if re.search(r"\b(proc|procedure|stored procedure)s?\b", ql): return "procedure"
    if re.search(r"\bviews?\b", ql): return "view"
    if re.search(r"\btables?\b", ql): return "table"
    if re.search(r"\bcolumns?\b", ql): return "column"
    if re.search(r"\bfunctions?\b", ql): return "function"
    return None

# --- Matching strategies ---
def ranked_match_score(hint: str, item: Dict[str, Any]):
    h = hint.lower()
    safe = (item.get("safe_name") or "").lower()
    schema = (item.get("schema") or "").lower()
    base = split_safe(item.get("safe_name") or "")[1].lower()
    name = (item.get("name") or "").lower()
    if "." in h and "·" not in h:
        hs, hb = h.split(".", 1)
        if hs == schema and (hb == base or hb == name): return 0
    if "·" in h and h == safe: return 0
    if h == base or h == name: return 1
    htoks = {h}
    if htoks & set(tokens_for(base)) or htoks & set(tokens_for(name)): return 2
    if base.startswith(h) or name.startswith(h): return 3
    if base.endswith(h) or name.endswith(h):   return 4
    if h in base or h in name or h in safe:    return 5
    return None

def matches_mode(hint: str, item: Dict[str, Any], mode: str) -> bool:
    h = hint.lower()
    safe = (item.get("safe_name") or "").lower()
    schema = (item.get("schema") or "").lower()
    base = split_safe(item.get("safe_name") or "")[1].lower()
    name = (item.get("name") or "").lower()
    if mode == "exact":
        if "·" in h and h == safe: return True
        if "." in h and "·" not in h:
            hs, hb = h.split(".", 1)
            return hs == schema and (hb == base or hb == name)
        return h == base or h == name
    if mode == "word":
        htoks = {h}
        return bool(htoks & set(tokens_for(base)) or htoks & set(tokens_for(name)))
    if mode == "substring":
        return h in base or h in name or h in safe
    # smart:
    return ranked_match_score(hint, item) is not None

# --- Candidate pickers ---
def choose_candidates_by_kind(q: str, items: List[Dict[str, Any]], kind: str, k: int = 3, name_mode: str = "smart") -> List[str]:
    inds = [i for i, it in enumerate(items) if it.get("kind") == kind]
    kind_items = [items[i] for i in inds]
    hints = extract_quoted_names(q)
    candidates: List[str] = []

    if hints:
        if name_mode == "smart":
            ranked = []
            for it in kind_items:
                for h in hints:
                    sc = ranked_match_score(h, it)
                    if sc is not None:
                        ranked.append((sc, it["safe_name"]))
            ranked.sort(key=lambda t: (t[0], len(split_safe(t[1])[1])))
            for _, s in ranked: candidates.append(s)
        else:
            for it in kind_items:
                for h in hints:
                    if matches_mode(h, it, name_mode):
                        candidates.append(it["safe_name"])
        seen=set(); out=[]
        for s in candidates:
            if s not in seen: out.append(s); seen.add(s)
        return out[:k] if out else []

    # Semantic fallback removed - embeddings no longer used
    # Falls through to token substring fallback below

    # token substring fallback
    words = [w for w in re.split(r"[^A-Za-z0-9_]+", q) if w]
    for w in words:
        wl = w.lower()
        for it in kind_items:
            base = split_safe(it.get("safe_name") or "")[1].lower()
            name = (it.get("name") or "").lower()
            if wl in base or wl in name:
                candidates.append(it["safe_name"])
    seen=set(); out=[]
    for s in candidates:
        if s not in seen: out.append(s); seen.add(s)
    return out[:k]

def choose_table_candidates(q, items, k=3, name_mode="smart"):
    return choose_candidates_by_kind(q, items, "table", k, name_mode)

def choose_proc_candidates(q, items, k=3, name_mode="smart"):
    return choose_candidates_by_kind(q, items, "procedure", k, name_mode)

def all_tables_matching_hints(hints: List[str], items: List[Dict[str, Any]], name_mode: str) -> List[str]:
    tables = [it for it in items if it.get("kind") == "table"]
    out: List[str] = []
    for h in hints:
        hl = h.lower()
        for it in tables:
            schema, base = split_safe(it.get("safe_name") or "")
            name = (it.get("name") or "")
            if base.lower() == hl or name.lower() == hl:
                out.append(it["safe_name"]); continue
            if "." in hl and "·" not in hl:
                hs, hb = hl.split(".", 1)
                if hs == (schema or "").lower() and (hb == base.lower() or hb == name.lower()):
                    out.append(it["safe_name"]); continue
            if name_mode == "smart" and "·" in h and (it.get("safe_name") or "").lower() == hl:
                out.append(it["safe_name"]); continue
    seen=set(); uniq=[]
    for s in out:
        if s not in seen: uniq.append(s); seen.add(s)
    return uniq

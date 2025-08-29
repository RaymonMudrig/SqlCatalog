#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
query_catalog.py — semantic & relation-aware queries over ../output/*

New controls:
  --table NAME              Force the target table for relation queries (e.g., "Order" or "dbo.Order")
  --include-via-views       Also include procedures that read views which read the table (transitive reads)
  --name-match {smart,exact,word,substring}  (smarter name matching; default smart)

Relation intents supported:
  • which procedures access table 'X' (reads/writes)
  • which views read table 'X'
  • which columns are used by proc 'Y' (optionally from table 'Z')
  • what tables does proc 'Y' write to
  • what tables does proc 'Y' read

Plus general semantic search and optional LM Studio chat synthesis (--answer).
"""

from __future__ import annotations
import os, re, sys, json, argparse, textwrap
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional

import numpy as np

# ---------- Paths (aligned with vectorize_catalog.py) ----------
BASE = Path(__file__).resolve().parent
OUTPUT_DIR = Path(os.getenv("SQL_OUTPUT_DIR") or (BASE.parent / "output")).resolve()
INDEX_DIR  = OUTPUT_DIR / "vector_index"
CATALOG    = OUTPUT_DIR / "catalog.json"

ITEMS_PATH = INDEX_DIR / "items.json"
EMB_PATH   = INDEX_DIR / "embeddings.npy"

# ---------- Embedding & chat ----------
USE_LMSTUDIO = os.getenv("USE_LMSTUDIO", "1") != "0"
LMSTUDIO_BASE_URL = os.getenv("LMSTUDIO_BASE_URL", "http://localhost:1234/v1")
LMSTUDIO_API_KEY  = os.getenv("LMSTUDIO_API_KEY", "lm-studio")
EMBED_MODEL       = os.getenv("EMBED_MODEL", "text-embedding-nomic-embed-text-v1.5")
CHAT_MODEL        = os.getenv("CHAT_MODEL", "qwen2.5-32b-instruct-mlx")  # e.g., qwen2.5-32b-instruct-mlx

def embed_query(text: str) -> np.ndarray:
    if USE_LMSTUDIO:
        import requests
        url = f"{LMSTUDIO_BASE_URL.rstrip('/')}/embeddings"
        headers = {"Authorization": f"Bearer {LMSTUDIO_API_KEY}", "Content-Type": "application/json"}
        payload = {"model": EMBED_MODEL, "input": [text]}
        r = requests.post(url, headers=headers, json=payload, timeout=60)
        r.raise_for_status()
        vec = np.array(r.json()["data"][0]["embedding"], dtype=np.float32)
        vec = vec / (np.linalg.norm(vec) + 1e-8)
        return vec
    from sentence_transformers import SentenceTransformer
    model_name = os.getenv("LOCAL_EMBED_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
    m = SentenceTransformer(model_name)
    vec = m.encode([text], convert_to_numpy=True, normalize_embeddings=True)[0]
    return vec.astype(np.float32)

# ---------- Utils ----------
def load_items() -> List[Dict[str, Any]]:
    return json.loads(ITEMS_PATH.read_text(encoding="utf-8"))

def load_emb() -> np.ndarray:
    emb = np.load(EMB_PATH)
    return emb.astype(np.float32)

def load_catalog() -> Dict[str, Any]:
    return json.loads(CATALOG.read_text(encoding="utf-8"))

def cosine_scores(qvec: np.ndarray, mat: np.ndarray) -> np.ndarray:
    return mat @ qvec  # both normalized

def print_item(item: Dict[str, Any], score: Optional[float], show_sql: bool):
    kind  = item.get("kind")
    schema= item.get("schema") or ""
    disp_name = f"{schema+'.' if schema else ''}{item.get('name') or item.get('safe_name')}"
    if score is not None:
        print(f"[{score:.3f}] {kind} {disp_name} -> {item.get('id')}")
    else:
        print(f"{kind} {disp_name} -> {item.get('id')}")
    text = (item.get("text") or "").strip()
    if text:
        preview = "\n    " + "\n    ".join(text.splitlines()[:6])
        print(preview)
    if show_sql and item.get("sql_path"):
        print(f"    SQL: {item['sql_path']}")
        sql = (item.get("sql") or "").strip()
        if sql:
            snippet = "\n".join(sql.splitlines()[:20])
            print("    --- SQL (first 20 lines) ---")
            for ln in snippet.splitlines():
                print("    " + ln)
            if len(sql.splitlines()) > 20:
                print("    ...")

def extract_quoted_names(q: str) -> List[str]:
    # 'Order', "Order", [Order], `Order`
    quoted = re.findall(r"(?:'([^']+)')|(?:\"([^\"]+)\")|(?:\[((?:[^\]]|])+)\])|(?:`([^`]+)`)", q)
    out = []
    for tup in quoted:
        for s in tup:
            if s:
                out.append(s.strip())
    return out

def detect_kind(q: str) -> Optional[str]:
    ql = q.lower()
    if re.search(r"\b(proc|procedure|stored procedure)s?\b", ql): return "procedure"
    if re.search(r"\bviews?\b", ql): return "view"
    if re.search(r"\btables?\b", ql): return "table"
    if re.search(r"\bcolumns?\b", ql): return "column"
    if re.search(r"\bfunctions?\b", ql): return "function"
    return None

# ---------- Name matching helpers ----------
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
        if t not in seen:
            out.append(t); seen.add(t)
    return out

def ranked_match_score(hint: str, item: Dict[str, Any]) -> Optional[int]:
    h = hint.lower()
    safe = (item.get("safe_name") or "").lower()
    schema = (item.get("schema") or "").lower()
    base = split_safe(item.get("safe_name") or "")[1].lower()
    name = (item.get("name") or "").lower()
    if "." in h and "·" not in h:
        hs, hb = h.split(".", 1)
        if hs == schema and (hb == base or hb == name): return 0
    if "·" in h:
        if h == safe: return 0
    if h == base or h == name: return 1
    htoks = {h}
    base_toks = set(tokens_for(base)); name_toks = set(tokens_for(name))
    if htoks & base_toks or htoks & name_toks: return 2
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
    return ranked_match_score(hint, item) is not None

# ---------- Candidate pickers ----------
def choose_candidates_by_kind(q: str, items: List[Dict[str, Any]], kind: str, k: int = 3, name_mode: str = "smart") -> List[str]:
    inds = [i for i, it in enumerate(items) if it.get("kind") == kind]
    kind_items = [items[i] for i in inds]
    hints = extract_quoted_names(q)
    candidates: List[str] = []

    if hints:
        if name_mode == "smart":
            ranked: List[Tuple[int, str]] = []
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

    try:
        emb = load_emb()
    except Exception:
        emb = None
    if emb is not None and inds:
        mat = emb[inds]
        qvec = embed_query(q)
        scores = cosine_scores(qvec, mat)
        order = np.argsort(-scores)[:max(5, k)]
        return [kind_items[int(i)]["safe_name"] for i in order][:k]

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

def choose_table_candidates(q: str, items: List[Dict[str, Any]], k: int = 3, name_mode: str = "smart") -> List[str]:
    return choose_candidates_by_kind(q, items, "table", k, name_mode)

def choose_proc_candidates(q: str, items: List[Dict[str, Any]], k: int = 3, name_mode: str = "smart") -> List[str]:
    return choose_candidates_by_kind(q, items, "procedure", k, name_mode)

# ---------- Broad fallback: scan all tables by exact base/name ----------
def all_tables_matching_hints(hints: List[str], items: List[Dict[str, Any]], name_mode: str) -> List[str]:
    tables = [it for it in items if it.get("kind") == "table"]
    out: List[str] = []
    for h in hints:
        hl = h.lower()
        for it in tables:
            schema, base = split_safe(it.get("safe_name") or "")
            name = (it.get("name") or "")
            # exact base match always allowed as fallback
            if base.lower() == hl or name.lower() == hl:
                out.append(it["safe_name"]); continue
            # exact schema-qualified (dbo.Order)
            if "." in hl and "·" not in hl:
                hs, hb = hl.split(".", 1)
                if hs == (schema or "").lower() and (hb == base.lower() or hb == name.lower()):
                    out.append(it["safe_name"]); continue
            # smart mode also accepts explicit safe_name
            if name_mode == "smart" and "·" in h and (it.get("safe_name") or "").lower() == hl:
                out.append(it["safe_name"]); continue
    # dedupe, keep order
    seen=set(); uniq=[]
    for s in out:
        if s not in seen: uniq.append(s); seen.add(s)
    return uniq

# ---------- LM Studio chat answer ----------
def llm_answer(question: str, picked_items: List[Dict[str, Any]]) -> None:
    if not CHAT_MODEL:
        return
    try:
        import requests
    except Exception:
        print("[LLM answer] requests not available; skipping.", file=sys.stderr)
        return

    ctx_lines = []
    for it in picked_items[:8]:
        ctx = (it.get("text") or "").strip().replace("\r", "")
        ctx = "\n".join(ctx.splitlines()[:40])
        ctx_lines.append(f"- {it.get('id')}: {ctx}")

    messages = [
        {"role": "system", "content": "You are a helpful SQL catalog assistant. Be concise and cite entity IDs you used."},
        {"role": "user", "content": textwrap.dedent(f"""
            Question: {question}

            Context (top results):
            {chr(10).join(ctx_lines)}

            Task: Answer the question directly. For relation questions, list the entities and their roles (READ/WRITE),
            and include their IDs.
        """).strip()},
    ]
    url = f"{LMSTUDIO_BASE_URL.rstrip('/')}/chat/completions"
    headers = {"Authorization": f"Bearer {LMSTUDIO_API_KEY}", "Content-Type": "application/json"}
    try:
        r = requests.post(url, headers=headers, json={"model": CHAT_MODEL, "messages": messages}, timeout=120)
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"].strip()
        print("\n[LLM answer]")
        print(content)
    except Exception as e:
        print(f"[LLM answer] Failed: {e}", file=sys.stderr)

# ---------- Relation detectors ----------
def is_relation_proc_access_table(q: str) -> bool:
    ql = q.lower()
    return (bool(re.search(r"\b(proc|procedure|stored procedure)s?\b", ql))
            and bool(re.search(r"\btable\b", ql))
            and bool(re.search(r"\b(access|use|uses|read|reads|write|writes|update|insert|delete|select|reference|references)\b", ql)))

def is_relation_views_read_table(q: str) -> bool:
    ql = q.lower()
    return (bool(re.search(r"\bviews?\b", ql))
            and bool(re.search(r"\btable\b", ql))
            and bool(re.search(r"\b(read|reads|select|access|uses?)\b", ql)))

def is_relation_cols_used_by_proc(q: str) -> bool:
    ql = q.lower()
    return (bool(re.search(r"\bcolumns?\b", ql))
            and bool(re.search(r"\b(proc|procedure|stored procedure)s?\b", ql))
            and bool(re.search(r"\b(use|uses|read|reads|select|reference|references)\b", ql)))

def is_relation_tables_written_by_proc(q: str) -> bool:
    ql = q.lower()
    return (bool(re.search(r"\btables?\b", ql))
            and bool(re.search(r"\b(proc|procedure|stored procedure)s?\b", ql))
            and bool(re.search(r"\b(write|writes|update|insert|delete|merge)\b", ql)))

def is_relation_tables_read_by_proc(q: str) -> bool:
    ql = q.lower()
    return (bool(re.search(r"\btables?\b", ql))
            and bool(re.search(r"\b(proc|procedure|stored procedure)s?\b", ql))
            and bool(re.search(r"\b(read|reads|select|access|uses?)\b", ql)))

# ---------- Relation answer helpers ----------
def refs_contains_table(lst, tbl_safe: str) -> bool:
    for r in lst:
        rsafe = r.get("Safe_Name")
        if rsafe and rsafe.lower() == tbl_safe.lower():
            return True
        rsch = r.get("Schema") or r.get("schema") or ""
        rnm  = r.get("Name") or r.get("name") or ""
        combo = (rsch + "·" + rnm) if rsch else rnm
        if combo.lower() == tbl_safe.lower():
            return True
    return False

# ---------- Core relation answers ----------
def answer_procs_accessing_table(query: str, items: List[Dict[str, Any]], show_sql: bool,
                                 name_mode: str, forced_table: Optional[str],
                                 include_via_views: bool) -> Tuple[bool, List[Dict[str, Any]]]:
    # Trigger if intent OR forced_table provided
    if not (forced_table or is_relation_proc_access_table(query)):
        return False, []
    catalog = load_catalog()
    procs  = catalog.get("Procedures") or catalog.get("procedures") or {}
    views  = catalog.get("Views") or catalog.get("views") or {}

    proc_item_by_safe = {it.get("safe_name"): it for it in items if it.get("kind") == "procedure"}
    table_items = [it for it in items if it.get("kind") == "table"]

    # Resolve candidate tables
    candidates: List[str] = []
    hints = [forced_table] if forced_table else extract_quoted_names(query)
    if forced_table and "·" in forced_table:
        candidates = [forced_table]
    else:
        # union of top candidates AND full exact-base scan
        candidates = choose_table_candidates(query, items, k=8, name_mode=name_mode)
        candidates = list(dict.fromkeys(candidates + all_tables_matching_hints(hints, items, name_mode)))

    if not candidates:
        return False, []

    # Precompute views that read each table (for transitive proc reads)
    view_reads_map: Dict[str, List[str]] = {}  # table_safe -> [view_safe...]
    if include_via_views:
        for vsafe, vobj in views.items():
            reads = vobj.get("Reads") or vobj.get("reads") or []
            for r in reads:
                rsafe = r.get("Safe_Name")
                if not rsafe:
                    s = r.get("Schema") or r.get("schema") or ""
                    n = r.get("Name") or r.get("name") or ""
                    rsafe = (s + "·" + n) if s else n
                if not rsafe: continue
                view_reads_map.setdefault(rsafe, []).append(vsafe)

    picked: List[Dict[str, Any]] = []

    for tbl_safe in candidates:
        schema, base = split_safe(tbl_safe)
        disp = f"{schema+'.' if schema else ''}{base}"
        title = f"=== Procedures that access table {disp} (safe: {tbl_safe}) ==="
        if include_via_views:
            title += "  [including via views]"
        print(f"\n{title}")

        # Views that read the table (if enabled)
        via_views = set(view_reads_map.get(tbl_safe, [])) if include_via_views else set()

        refs: List[Tuple[str, str]] = []
        for psafe, pobj in procs.items():
            reads  = pobj.get("Reads") or pobj.get("reads") or []
            writes = pobj.get("Writes") or pobj.get("writes") or []

            state_flags = []
            if refs_contains_table(reads, tbl_safe):  state_flags.append("READ")
            if refs_contains_table(writes, tbl_safe): state_flags.append("WRITE")

            if include_via_views and not state_flags and reads:
                # if procedure reads a view that reads this table -> mark READ (transitive)
                for r in reads:
                    rsafe = r.get("Safe_Name")
                    if not rsafe:
                        s = r.get("Schema") or r.get("schema") or ""
                        n = r.get("Name") or r.get("name") or ""
                        rsafe = (s + "·" + n) if s else n
                    if rsafe and rsafe in via_views:
                        state_flags.append("READ(via view)")
                        break

            if state_flags:
                refs.append((psafe, "/".join(state_flags)))

        if not refs:
            print("  (no procedures found)")
        else:
            for psafe, how in sorted(refs, key=lambda x: (0 if "WRITE" in x[1] else 1, x[0].lower())):
                it = proc_item_by_safe.get(psafe)
                if it:
                    print_item(it, score=None, show_sql=show_sql)
                    print(f"    ACCESS: {how}")
                    picked.append(it)
                else:
                    print(f"procedure {psafe}  ACCESS: {how}")

    seen=set(); picked=[it for it in picked if not (it.get('id') in seen or seen.add(it.get('id')))]
    return True, picked

def answer_views_reading_table(query: str, items: List[Dict[str, Any]], show_sql: bool, name_mode: str,
                               forced_table: Optional[str]) -> Tuple[bool, List[Dict[str, Any]]]:
    if not (forced_table or is_relation_views_read_table(query)):
        return False, []
    catalog = load_catalog()
    views = catalog.get("Views") or catalog.get("views") or {}

    view_item_by_safe = {it.get("safe_name"): it for it in items if it.get("kind") == "view"}
    hints = [forced_table] if forced_table else extract_quoted_names(query)
    candidates = []
    if forced_table and "·" in forced_table:
        candidates = [forced_table]
    else:
        candidates = choose_table_candidates(query, items, k=8, name_mode=name_mode)
        candidates = list(dict.fromkeys(candidates + all_tables_matching_hints(hints, items, name_mode)))

    if not candidates:
        return False, []
    picked: List[Dict[str, Any]] = []

    for tbl_safe in candidates:
        schema, base = split_safe(tbl_safe)
        disp = f"{schema+'.' if schema else ''}{base}"
        print(f"\n=== Views that read table {disp} (safe: {tbl_safe}) ===")

        hits: List[str] = []
        for vsafe, vobj in views.items():
            reads = vobj.get("Reads") or vobj.get("reads") or []
            if refs_contains_table(reads, tbl_safe):
                hits.append(vsafe)

        if not hits:
            print("  (no views found)")
        else:
            for vsafe in sorted(set(hits), key=str.lower):
                it = view_item_by_safe.get(vsafe)
                if it:
                    print_item(it, score=None, show_sql=show_sql)
                    picked.append(it)
                else:
                    print(f"view {vsafe}  READ")
    seen=set(); picked=[it for it in picked if not (it.get('id') in seen or seen.add(it.get('id')))]
    return True, picked

def answer_columns_used_by_proc(query: str, items: List[Dict[str, Any]], show_sql: bool, name_mode: str) -> Tuple[bool, List[Dict[str, Any]]]:
    ql = query.lower()
    if not (bool(re.search(r"\bcolumns?\b", ql))
            and bool(re.search(r"\b(proc|procedure|stored procedure)s?\b", ql))
            and bool(re.search(r"\b(use|uses|read|reads|select|reference|references)\b", ql))):
        return False, []
    catalog = load_catalog()
    procs = catalog.get("Procedures") or catalog.get("procedures") or {}

    proc_item_by_safe = {it.get("safe_name"): it for it in items if it.get("kind") == "procedure"}
    table_hints = set(choose_table_candidates(query, items, k=8, name_mode=name_mode))

    candidates = choose_proc_candidates(query, items, k=8, name_mode=name_mode)
    if not candidates:
        return False, []
    picked: List[Dict[str, Any]] = []

    for psafe in candidates:
        pobj = procs.get(psafe)
        if not pobj:
            continue
        print(f"\n=== Columns used by procedure {psafe} ===")
        it = proc_item_by_safe.get(psafe)
        if it:
            print_item(it, score=None, show_sql=show_sql)
            picked.append(it)

        colrefs = pobj.get("Column_Refs") or pobj.get("column_refs") or {}
        if not colrefs:
            print("  (no column refs recorded)")
            continue

        keys = list(colrefs.keys())
        if table_hints:
            keys = [k for k in keys if k in table_hints or any(k.lower().endswith(("·"+t.split("·")[-1]).lower()) for t in table_hints)]
        if not keys:
            print("  (no matching tables among column refs)")
            continue

        for tbl in sorted(keys, key=str.lower):
            cols = colrefs.get(tbl) or []
            if isinstance(cols, dict): cols = list(cols.keys())
            print(f"  - {tbl}: {', '.join(sorted(cols)) if cols else '(none)'}")

    seen=set(); picked=[it for it in picked if not (it.get('id') in seen or seen.add(it.get('id')))]
    return True, picked

def answer_tables_written_by_proc(query: str, items: List[Dict[str, Any]], show_sql: bool, name_mode: str) -> Tuple[bool, List[Dict[str, Any]]]:
    ql = query.lower()
    if not (bool(re.search(r"\btables?\b", ql))
            and bool(re.search(r"\b(proc|procedure|stored procedure)s?\b", ql))
            and bool(re.search(r"\b(write|writes|update|insert|delete|merge)\b", ql))):
        return False, []
    catalog = load_catalog()
    procs = catalog.get("Procedures") or catalog.get("procedures") or {}

    proc_item_by_safe = {it.get("safe_name"): it for it in items if it.get("kind") == "procedure"}
    candidates = choose_proc_candidates(query, items, k=8, name_mode=name_mode)
    if not candidates:
        return False, []
    picked: List[Dict[str, Any]] = []

    for psafe in candidates:
        pobj = procs.get(psafe)
        if not pobj:
            continue
        print(f"\n=== Tables WRITTEN by procedure {psafe} ===")
        it = proc_item_by_safe.get(psafe)
        if it:
            print_item(it, score=None, show_sql=show_sql)
            picked.append(it)
        writes = pobj.get("Writes") or pobj.get("writes") or []
        if not writes:
            print("  (no WRITE targets)")
            continue
        names = []
        for r in writes:
            if r.get("Safe_Name"): names.append(r["Safe_Name"])
            else:
                s = r.get("Schema") or r.get("schema") or ""
                n = r.get("Name") or r.get("name") or ""
                names.append((s + "·" + n) if s else n)
        for n in sorted(set(names), key=str.lower):
            print(f"  - {n}")
    seen=set(); picked=[it for it in picked if not (it.get('id') in seen or seen.add(it.get('id')))]
    return True, picked

def answer_tables_read_by_proc(query: str, items: List[Dict[str, Any]], show_sql: bool, name_mode: str) -> Tuple[bool, List[Dict[str, Any]]]:
    ql = query.lower()
    if not (bool(re.search(r"\btables?\b", ql))
            and bool(re.search(r"\b(proc|procedure|stored procedure)s?\b", ql))
            and bool(re.search(r"\b(read|reads|select|access|uses?)\b", ql))):
        return False, []
    catalog = load_catalog()
    procs = catalog.get("Procedures") or catalog.get("procedures") or {}

    proc_item_by_safe = {it.get("safe_name"): it for it in items if it.get("kind") == "procedure"}
    candidates = choose_proc_candidates(query, items, k=8, name_mode=name_mode)
    if not candidates:
        return False, []
    picked: List[Dict[str, Any]] = []

    for psafe in candidates:
        pobj = procs.get(psafe)
        if not pobj:
            continue
        print(f"\n=== Tables READ by procedure {psafe} ===")
        it = proc_item_by_safe.get(psafe)
        if it:
            print_item(it, score=None, show_sql=show_sql)
            picked.append(it)
        reads = pobj.get("Reads") or pobj.get("reads") or []
        if not reads:
            print("  (no READ sources)")
            continue
        names = []
        for r in reads:
            if r.get("Safe_Name"): names.append(r["Safe_Name"])
            else:
                s = r.get("Schema") or r.get("schema") or ""
                n = r.get("Name") or r.get("name") or ""
                names.append((s + "·" + n) if s else n)
        for n in sorted(set(names), key=str.lower):
            print(f"  - {n}")
    seen=set(); picked=[it for it in picked if not (it.get('id') in seen or seen.add(it.get('id')))]
    return True, picked

# ---------- General semantic search ----------
def semantic_search(query: str, items: List[Dict[str, Any]], emb: np.ndarray,
                    k: int, kind: str, schema: Optional[str], unused_only: bool,
                    show_sql: bool) -> List[Dict[str, Any]]:
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
        print("No items match the given filters.")
        return []
    submat = emb[cand_idx]
    scores = submat @ qvec
    order = np.argsort(-scores)[:k]

    picked: List[Dict[str, Any]] = []
    for idx in order:
        global_idx = cand_idx[int(idx)]
        it = items[int(global_idx)]
        print_item(it, float(scores[int(idx)]), show_sql)
        picked.append(it)
    return picked

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("query")
    ap.add_argument("--k", type=int, default=12)
    ap.add_argument("--kind", choices=["any","table","view","procedure","function","column"], default="any")
    ap.add_argument("--schema")
    ap.add_argument("--unused-only", action="store_true")
    ap.add_argument("--show-sql", action="store_true")
    ap.add_argument("--answer", action="store_true", help="Use LM Studio CHAT_MODEL to synthesize an answer from top results")
    ap.add_argument("--answer-top", type=int, default=8, help="How many top items to send to the LLM (max 8 used)")
    ap.add_argument("--name-match", choices=["smart","exact","word","substring"], default="smart",
                    help="How quoted names are matched; default smart = exact > word > prefix/suffix > substring")
    ap.add_argument("--table", help="Force the target table for relation queries, e.g. 'Order' or 'dbo.Order'")
    ap.add_argument("--include-via-views", action="store_true", help="Include procedures that read views which read the table")
    args = ap.parse_args()

    if not ITEMS_PATH.exists() or not EMB_PATH.exists() or not CATALOG.exists():
        print("Index or catalog files not found. Run vectorize_catalog.py first.", file=sys.stderr)
        sys.exit(2)
    items = load_items()
    emb   = load_emb()

    # Relation intents (priority). Now pass forced_table and via-views flags.
    for handler in (
        lambda q,i,s: answer_procs_accessing_table(q,i,s,args.name_match,args.table,args.include_via_views),
        lambda q,i,s: answer_views_reading_table(q,i,s,args.name_match,args.table),
        lambda q,i,s: answer_columns_used_by_proc(q,i,s,args.name_match),
        lambda q,i,s: answer_tables_written_by_proc(q,i,s,args.name_match),
        lambda q,i,s: answer_tables_read_by_proc(q,i,s,args.name_match),
    ):
        handled, picked = handler(args.query, items, args.show_sql)
        if handled:
            if args.answer and picked:
                llm_answer(args.query, picked[:max(1, min(args.answer_top, 8))])
            return

    # Bias kind if user mentioned it
    auto_kind = detect_kind(args.query)
    kind = args.kind if args.kind != "any" else (auto_kind or "any")

    picked = semantic_search(args.query, items, emb, k=args.k, kind=kind,
                             schema=args.schema, unused_only=args.unused_only,
                             show_sql=args.show_sql)
    if args.answer and picked:
        llm_answer(args.query, picked[:max(1, min(args.answer_top, 8))])

if __name__ == "__main__":
    main()

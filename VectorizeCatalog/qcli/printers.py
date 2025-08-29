from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, Iterable
import re

from qcat.paths import OUTPUT_DIR, SQL_FILES_DIR
from qcat.name_match import split_safe

def print_item(item: Dict[str, Any], score: Optional[float], show_sql: bool = False) -> None:
    kind  = (item.get("kind") or "").lower()
    schema= item.get("schema") or ""
    name  = item.get("name") or item.get("safe_name") or ""
    disp_name = f"{schema+'.' if schema else ''}{name}"
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

# ---------- helpers to find DDL in ../sql_files when exports are missing ----------

_GO_RE = re.compile(r"(?im)^\s*GO\s*$")

def _id_alts(s: str) -> str:
    esc = re.escape(s)
    return rf"(?:\[{esc}\]|\"{esc}\"|`{esc}`|{esc})"

def _qualified_alts(schema: Optional[str], base: str) -> str:
    base_alt = _id_alts(base)
    if schema:
        sch_alt = _id_alts(schema)
        return rf"(?:{sch_alt}\s*\.\s*{base_alt})"
    # schema optional
    sch_any = r"(?:\[[A-Za-z0-9_]+\]|\"[A-Za-z0-9_]+\"|`[A-Za-z0-9_]+`|[A-Za-z0-9_]+)"
    return rf"(?:{sch_any}\s*\.\s*{base_alt}|{base_alt})"

def _ddl_pattern(kind: str, schema: Optional[str], base: str) -> re.Pattern:
    obj = _qualified_alts(schema, base)
    if kind == "table":
        head = r"(?:CREATE|ALTER)\s+TABLE"
    elif kind == "view":
        head = r"(?:CREATE|ALTER)\s+VIEW"
    elif kind == "procedure":
        head = r"(?:CREATE|ALTER)\s+PROC(?:EDURE)?"
    elif kind == "function":
        head = r"(?:CREATE|ALTER)\s+FUNCTION"
    else:
        head = r"(?:CREATE|ALTER)\s+(?:TABLE|VIEW|PROC(?:EDURE)?|FUNCTION)"
    return re.compile(rf"(?is)\b{head}\s+{obj}\b")

def _slice_to_next_go(text: str, start: int) -> str:
    m = _GO_RE.search(text, pos=start)
    return text[start:].strip() if not m else text[start:m.start()].strip()

def _search_sources_for(kind: str, schema: Optional[str], base: str) -> Tuple[Optional[str], Optional[str]]:
    """Scan ../sql_files recursively; return (ddl_text, source_path) if found."""
    pat = _ddl_pattern(kind, schema, base)
    if not SQL_FILES_DIR.exists():
        return None, None
    for p in sorted(SQL_FILES_DIR.rglob("*.sql")):
        try:
            txt = p.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        m = pat.search(txt)
        if not m:
            # try without schema if provided (sometimes sources omit it)
            if schema:
                pat2 = _ddl_pattern(kind, None, base)
                m = pat2.search(txt)
                if not m:
                    continue
            else:
                continue
        snippet = _slice_to_next_go(txt, m.start())
        if snippet:
            return snippet, str(p)
    return None, None

# ---------- export path guesser (handles middle-dot vs dot, spaces vs underscores, etc.) ----------

def _sanitize_base_variants(base: str) -> Iterable[str]:
    """Generate plausible base filename variants used by exporters."""
    yield base
    if " " in base:
        yield base.replace(" ", "_")
    # replace any non [A-Za-z0-9_] (keep underscore)
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", base)
    if cleaned != base:
        yield cleaned
    # collapse multiple underscores
    collapsed = re.sub(r"_+", "_", cleaned)
    if collapsed != cleaned:
        yield collapsed

def _safe_variants(schema: Optional[str], base: str) -> Iterable[str]:
    """
    Produce filename stems like:
      'dbo·RT_Order', 'dbo.RT_Order', 'dbo·RT Order', 'RT_Order', etc.
    """
    bases = list(dict.fromkeys(_sanitize_base_variants(base)))
    seps = ["·", "."] if schema else [""]
    for b in bases:
        if schema:
            for sep in seps:
                yield f"{schema}{sep}{b}"
        yield b  # base-only as a last resort

def _candidate_export_paths(kind: str, schema: Optional[str], base: str) -> Iterable[Path]:
    sub = {"table":"tables","view":"views","procedure":"procedures","function":"functions"}.get(kind)
    if not sub:
        return []
    folder = OUTPUT_DIR / "sql_exports" / sub
    for stem in _safe_variants(schema, base):
        yield (folder / f"{stem}.sql")

# ---------- main SQL resolution ----------

def read_sql_from_item(item: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    """
    Returns (sql_text, path_used). Order:
      1) embedded in index
      2) recorded sql_path
      3) ../output/sql_exports/{kind}/{safe or variants}.sql  <-- robust variant search
      4) scan ../sql_files for CREATE/ALTER <object> ... up to next GO
    """
    sql = (item.get("sql") or "")
    path = item.get("sql_path")
    p: Optional[Path] = None

    # 1) embedded
    if sql:
        return sql, path

    # 2) recorded path exactly
    if not sql and path:
        try:
            p = Path(path)
            if p.exists():
                sql = p.read_text(encoding="utf-8", errors="ignore")
                if sql:
                    return sql, str(p)
        except Exception:
            pass

    # 3) exported file with robust name matching
    kind = (item.get("kind") or "").lower()
    schema = (item.get("schema") or None)
    # Prefer the safe base from safe_name; fallback to display name
    safe = item.get("safe_name") or ""
    _, safe_base = split_safe(safe)
    base = safe_base or (item.get("name") or "")

    for cand in _candidate_export_paths(kind, schema, base):
        try:
            if cand.exists():
                sql = cand.read_text(encoding="utf-8", errors="ignore")
                if sql:
                    return sql, str(cand)
        except Exception:
            continue

    # 4) scan sources in ../sql_files
    if base:
        sql2, src = _search_sources_for(kind, schema, base)
        if sql2:
            return sql2, src

    return "", None

def print_sql_blob(item: Dict[str, Any], head: Optional[int], full: bool) -> None:
    sql, path = read_sql_from_item(item)
    header = f"{(item.get('kind') or '').upper()} SQL — {item.get('schema')+'.' if item.get('schema') else ''}{item.get('name') or item.get('safe_name')}"
    print(f"\n{header}")
    if path:
        print(f"Path: {path}")
    if not sql:
        print("(no SQL found on disk or in index)")
        return
    if full or head is None:
        print(sql)
        return
    lines = sql.splitlines()
    print("\n".join(lines[:head]))
    if len(lines) > head:
        print("...")

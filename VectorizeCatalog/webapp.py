#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Optional, List, Literal, Dict, Any
from pydantic import BaseModel, Field
try:
    # pydantic v2
    from pydantic import ConfigDict
    HAS_V2 = True
except Exception:
    HAS_V2 = False

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse, Response

# Shared libs
from qcat.loader import load_items, load_emb
from qcat.search import semantic_search
from qcat.relations import procs_accessing_table
from qcat.name_match import detect_kind
from qcli.resolver import resolve_items_by_name, list_names
from qcli.printers import read_sql_from_item

# ------------------------------------------------------------
# App setup
# ------------------------------------------------------------

app = FastAPI(title="VectorizeCatalog API", version="2.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Lazy globals
ITEMS: List[Dict[str, Any]] = []
EMB = None

def _ensure_loaded():
    global ITEMS, EMB
    if not ITEMS:
        ITEMS = load_items()
    if EMB is None:
        EMB = load_emb()

# ------------------------------------------------------------
# Models
# ------------------------------------------------------------

KindLiteral = Literal["any", "table", "view", "procedure", "function", "column"]
NameModeLiteral = Literal["smart", "exact", "word", "substring"]

class SemanticBody(BaseModel):
    query: str
    k: int = 12
    kind: KindLiteral = "any"
    # Avoid pydantic "schema" shadowing warning by using schema_name with alias "schema"
    schema_name: Optional[str] = Field(default=None, alias="schema")
    unused_only: bool = False
    if HAS_V2:
        model_config = ConfigDict(populate_by_name=True)

class ProcsAccessBody(BaseModel):
    query: str
    name_mode: NameModeLiteral = "smart"
    forced_table: Optional[str] = None
    include_via_views: bool = False
    include_dynamic: bool = True  # keep dynamic-sql detection on by default

class SqlOfBody(BaseModel):
    kind: Literal["table", "view", "procedure", "function"]
    name: str
    head: Optional[int] = None     # None = full SQL
    fuzzy: bool = False            # allow fuzzy name match (may return multiple)

class ListNamesBody(BaseModel):
    kind: Literal["table", "view", "procedure", "function"]
    pattern: Optional[str] = None  # regex (case-insensitive)

# ------------------------------------------------------------
# Root UI + favicon
# ------------------------------------------------------------

_INDEX_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>VectorizeCatalog</title>
<style>
  :root { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; }
  body { margin: 24px; }
  .row { display:flex; gap:12px; align-items:center; flex-wrap:wrap; margin-bottom:10px; }
  input[type=text], textarea, select { padding:8px; min-width: 320px; }
  button { padding:8px 12px; cursor:pointer; }
  pre { background:#0b1020; color:#d0d6f7; padding:12px; border-radius:8px; overflow:auto; max-height:50vh; }
  .box { border:1px solid #ddd; border-radius:8px; padding:12px; margin-top:12px;}
  .muted { color:#666; font-size:12px; }
  .grid { display:grid; gap:8px; grid-template-columns: repeat(auto-fit, minmax(220px,1fr)); }
</style>
</head>
<body>
<h1>VectorizeCatalog</h1>

<div class="box">
  <h3>Semantic & Relations</h3>
  <div class="row">
    <label>Mode:
      <select id="mode">
        <option value="semantic">Semantic</option>
        <option value="procs_access_table">Procedures Accessing Table</option>
        <option value="sql_of">Print SQL of Entity</option>
        <option value="list_names">List Names</option>
      </select>
    </label>
    <label>Kind:
      <select id="kind">
        <option>any</option><option>table</option><option>view</option>
        <option>procedure</option><option>function</option><option>column</option>
      </select>
    </label>
    <label>Name-match:
      <select id="name_mode">
        <option>smart</option><option>exact</option><option>word</option><option>substring</option>
      </select>
    </label>
  </div>

  <div class="grid">
    <label>Query / Name
      <input id="query" type="text" placeholder="e.g. Which procedure access table 'Order'? or dbo.RT_Order"/>
    </label>
    <label>Schema
      <input id="schema" type="text" placeholder="optional schema filter for semantic"/>
    </label>
    <label>K
      <input id="k" type="number" min="1" max="50" value="12"/>
    </label>
    <label>Head (lines)
      <input id="head" type="number" min="1" max="999" value="120"/>
    </label>
  </div>

  <div class="row">
    <label><input id="unused_only" type="checkbox"/> unused_only</label>
    <label><input id="include_via_views" type="checkbox"/> include_via_views</label>
    <label><input id="fuzzy" type="checkbox"/> fuzzy (sql_of)</label>
  </div>

  <div class="row">
    <label>Forced table (relations)
      <input id="forced_table" type="text" placeholder="e.g. Order or dbo.Order"/>
    </label>
    <button id="run">Run</button>
  </div>

  <div class="muted">Tips:
    <ul>
      <li><b>Semantic</b>: uses /api/semantic with kind/schema/unused_only</li>
      <li><b>Procedures Accessing Table</b>: uses /api/procs_access_table with name_mode / forced_table / include_via_views</li>
      <li><b>Print SQL of Entity</b>: uses /api/sql_of with kind/name/head/fuzzy</li>
      <li><b>List Names</b>: uses /api/list_names with kind/pattern (use Query field as pattern)</li>
    </ul>
  </div>
</div>

<h3>Output</h3>
<pre id="out">(results will appear here)</pre>

<script>
const out = document.getElementById('out');
function show(obj){ out.textContent = (typeof obj === 'string') ? obj : JSON.stringify(obj, null, 2); }

document.getElementById('run').addEventListener('click', async () => {
  const mode = document.getElementById('mode').value;
  const kind = document.getElementById('kind').value;
  const name_mode = document.getElementById('name_mode').value;
  const query = document.getElementById('query').value;
  const schema = document.getElementById('schema').value || null;
  const k = parseInt(document.getElementById('k').value || '12', 10);
  const head = parseInt(document.getElementById('head').value || '120', 10);
  const unused_only = document.getElementById('unused_only').checked;
  const include_via_views = document.getElementById('include_via_views').checked;
  const fuzzy = document.getElementById('fuzzy').checked;
  const forced_table = document.getElementById('forced_table').value || null;

  try {
    let url = '';
    let body = {};
    if (mode === 'semantic') {
      url = '/api/semantic';
      body = { query, k, kind, schema, unused_only };
    } else if (mode === 'procs_access_table') {
      url = '/api/procs_access_table';
      body = { query, name_mode, forced_table, include_via_views, include_dynamic: true };
    } else if (mode === 'sql_of') {
      url = '/api/sql_of';
      let headVal = isNaN(head) ? null : head;
      body = { kind: (kind === 'any' ? 'table' : kind), name: query, head: headVal, fuzzy };
    } else if (mode === 'list_names') {
      url = '/api/list_names';
      body = { kind: (kind === 'any' ? 'table' : kind), pattern: query || null };
    }
    const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    const txt = await r.text();
    try { show(JSON.parse(txt)); } catch { show(txt); }
  } catch (e) {
    show(String(e));
  }
});
</script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
def index():
    return HTMLResponse(_INDEX_HTML)

@app.get("/favicon.ico")
def favicon():
    # No-content favicon to avoid 404 noise
    return Response(status_code=204)

# ------------------------------------------------------------
# JSON APIs
# ------------------------------------------------------------

@app.post("/api/semantic")
def api_semantic(body: SemanticBody):
    _ensure_loaded()
    qkind = body.kind if body.kind != "any" else (detect_kind(body.query) or "any")
    picked = semantic_search(
        body.query, ITEMS, EMB,
        k=body.k, kind=qkind, schema=body.schema_name, unused_only=body.unused_only
    ) or []
    return JSONResponse({"picked": picked})

@app.post("/api/procs_access_table")
def api_procs_access_table(body: ProcsAccessBody):
    _ensure_loaded()
    handled, picked, sections = procs_accessing_table(
        body.query, ITEMS,
        name_mode=body.name_mode,
        forced_table=body.forced_table,
        include_via_views=body.include_via_views,
        include_dynamic=body.include_dynamic,
    )
    return JSONResponse({
        "handled": handled,
        "picked": picked,
        "sections": sections,
    })

@app.post("/api/sql_of")
def api_sql_of(body: SqlOfBody):
    """
    Returns the SQL DDL for a given entity name.
    - Exact match first; if not found and fuzzy=True, falls back to broader matches.
    - If head is provided (>=0), trims returned SQL to first N lines.
    """
    _ensure_loaded()
    strict = not body.fuzzy
    matches = resolve_items_by_name(ITEMS, body.kind, body.name, strict=strict)

    if not matches:
        return JSONResponse({"error": "not found"}, status_code=404)

    out = []
    for it in matches:
        sql, path = read_sql_from_item(it)
        if sql and body.head is not None and body.head >= 0:
            lines = sql.splitlines()
            sql = "\n".join(lines[:body.head]) + ("" if len(lines) <= body.head else "\n...")
        out.append({
            "id": it.get("id"),
            "kind": it.get("kind"),
            "schema": it.get("schema"),
            "name": it.get("name") or it.get("safe_name"),
            "safe_name": it.get("safe_name"),
            "sql_path": it.get("sql_path") or path,
            "sql": sql or "",
        })

    return JSONResponse({"matches": out})

@app.post("/api/list_names")
def api_list_names(body: ListNamesBody):
    """
    Lists entity display and safe names that match a regex pattern.
    Mirrors CLI: `--list-names --kind <k> --pattern <regex>`.
    """
    _ensure_loaded()
    hits = list_names(ITEMS, body.kind, body.pattern)
    return JSONResponse({
        "names": [{"display": disp, "safe_name": safe} for safe, disp in hits]
    })

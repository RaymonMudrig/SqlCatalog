#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from qcat.loader import load_items, load_emb
from qcat.agent import agent_answer
from qcli.resolver import resolve_items_by_name
from qcli.printers import read_sql_from_item
from qcat.llm_intent import classify_intent
from qcat.prompt import parse_prompt  # fallback

APP_DIR = Path(__file__).resolve().parent
STATIC_DIR = APP_DIR / "static"

app = FastAPI(title="VectorizeCatalog", version="4.2.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=False,
                   allow_methods=["*"], allow_headers=["*"])

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

ITEMS: List[Dict[str, Any]] = []
EMB = None
def _ensure_loaded():
    global ITEMS, EMB
    if not ITEMS: ITEMS = load_items()
    if EMB is None: EMB = load_emb()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/favicon.ico")
def favicon():
    return Response(status_code=204)

@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")

class AskBody(BaseModel):
    prompt: str
    k: int = 12

@app.post("/api/ask")
def api_ask(body: AskBody):
    _ensure_loaded()

    # 1) LLM intent first
    parsed = classify_intent(body.prompt, ITEMS)

    # 2) Fallback heuristic parser
    if not parsed:
        parsed = parse_prompt(body.prompt, ITEMS)

    intent = parsed.get("intent")
    name   = parsed.get("name")
    kind   = parsed.get("kind")
    fuzzy  = bool(parsed.get("fuzzy"))
    include_via_views = bool(parsed.get("include_via_views"))
    unused_only = bool(parsed.get("unused_only"))
    schema = parsed.get("schema")
    pattern= parsed.get("pattern")

    # Direct DDL print
    if intent == "sql_of_entity" and name:
        kinds_to_try = [kind] if kind else ["table","view","procedure","function"]
        matches: List[Dict[str, Any]] = []
        for knd in kinds_to_try:
            ms = resolve_items_by_name(ITEMS, knd, name, strict=not fuzzy)
            if ms:
                matches = ms; kind = knd; break
        if not matches:
            return JSONResponse({"answer": f"No object found for {name!r}.",
                                 "parsed": parsed, "raw": {}}, status_code=200)
        it = matches[0]
        sql, path = read_sql_from_item(it)
        if not sql:
            ans = f"Definition for `{(it.get('schema')+'.' if it.get('schema') else '')}{it.get('name') or it.get('safe_name')}` not found on disk."
            return JSONResponse({"answer": ans, "parsed": parsed, "raw": {"match": it}}, status_code=200)
        lines = sql.splitlines()
        shown = "\n".join(lines[:200]) + ("\n..." if len(lines) > 200 else "")
        hdr = f"{(it.get('kind') or '').upper()} — {(it.get('schema')+'.' if it.get('schema') else '')}{it.get('name') or it.get('safe_name')}"
        ans = f"**{hdr}**\n\n```\n{shown}\n```"
        return JSONResponse({"answer": ans, "parsed": parsed, "raw": {"match": it, "path": path}}, status_code=200)

    # Agent with deterministic ops for listing/numeric intents, semantic only for narratives
    out = agent_answer(
        query=body.prompt, items=ITEMS, emb=EMB,
        k=body.k, kind="any", schema=None, unused_only=unused_only,
        name_mode="smart", forced_table=None,
        include_via_views=include_via_views, include_dynamic=True,
        intent=intent, name=name, fuzzy=fuzzy,
        schema_filter=schema, name_pattern=pattern,
    )
    return JSONResponse({"answer": out.get("answer"), "parsed": parsed, "raw": out}, status_code=200)

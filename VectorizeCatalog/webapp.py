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

@app.post("/api/ask")
def api_ask(body: AskBody):
    _ensure_loaded()
    out = agent_answer(
        query=body.prompt, items=ITEMS, emb=EMB,
        schema_filter=None, name_pattern=None,
    )
    # IMPORTANT: return the dict unmodified (contains `answer` and maybe `unified_diff`)
    return out if isinstance(out, dict) else {"answer": str(out)}
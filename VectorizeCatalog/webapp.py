# webapp.py
from __future__ import annotations
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, ConfigDict, AliasChoices
from typing import Optional, Any, Dict
import json
from qcat.items import load_items
from qcat.paths import BASE, OUTPUT_DIR, ITEMS_JSON
from qcat.agent import agent_answer

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# Serve exactly from VectorizeCatalog/static
STATIC_DIR = BASE / "static"

# /static/... -> VectorizeCatalog/static/...
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# / -> VectorizeCatalog/static/index.html
@app.get("/", response_class=HTMLResponse)
def index():
    return FileResponse(str(STATIC_DIR / "index.html"))

# Optional: also serve /index.html explicitly
@app.get("/index.html", response_class=HTMLResponse)
def index_html():
    return FileResponse(str(STATIC_DIR / "index.html"))

ITEMS, EMB = load_items()

class AskBody(BaseModel):
    prompt: str
    k: int = 10

    # use alias to avoid BaseModel.schema clash
    schema_name: str | None = Field(
        default=None,
        validation_alias="schema",
        serialization_alias="schema",
    )

    pattern: str | None = None
    name_match: str | None = None
    include_via_views: bool = False
    fuzzy: bool = False
    unused_only: bool = False

    # existing override support
    intent_override: str | None = Field(
        default=None,
        validation_alias=AliasChoices("intent_override", "intent"),
        serialization_alias="intent_override",
    )

    # NEW: whether the user accepts the agent’s proposed intent
    accept_proposal: bool = False

    model_config = ConfigDict(populate_by_name=True)


class SemanticBody(BaseModel):
    query: str
    k: int = 10
    kind: str | None = None

    schema_name: str | None = Field(
        default=None,
        validation_alias="schema",
        serialization_alias="schema",
    )

    pattern: str | None = None
    name_match: str | None = None
    include_via_views: bool = True
    fuzzy: bool = False
    unused_only: bool = False

    # keep consistent; optional here
    intent_override: str | None = Field(
        default=None,
        validation_alias=AliasChoices("intent_override", "intent"),
        serialization_alias="intent_override",
    )

    # Optional: mirror AskBody if you also use it on /api/semantic
    accept_proposal: bool = False

    model_config = ConfigDict(populate_by_name=True)

@app.post("/api/ask")
def api_ask(body: AskBody):
    out = agent_answer(
        query=body.prompt, items=ITEMS, emb=EMB,
        schema_filter=body.schema, name_pattern=body.pattern,
        intent_override=body.intent_override,
        accept_proposal=body.accept_proposal,
    )
    return out

# Simple health
@app.get("/api/ping")
def ping():
    return {"ok": True}

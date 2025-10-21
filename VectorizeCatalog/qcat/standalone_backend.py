# qcat_backend.py - Semantic search backend (standalone mode)
from __future__ import annotations
from fastapi import FastAPI, Request, Cookie, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, ConfigDict, AliasChoices
from typing import Optional, Any, Dict, List
import json
import uuid

try:
    from .items import load_items
    from .paths import BASE, OUTPUT_DIR, ITEMS_JSON
    from .agent import agent_answer
except ImportError:
    from items import load_items
    from paths import BASE, OUTPUT_DIR, ITEMS_JSON
    from agent import agent_answer

app = FastAPI()

# In-memory session storage for entity memory
# session_id -> {tables: set, procedures: set, views: set, functions: set}
SESSION_MEMORY: Dict[str, Dict[str, set]] = {}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# Serve from VectorizeCatalog/static/qcat (standalone mode)
STATIC_DIR = BASE / "static" / "qcat"

# Mount static files at /qcat-ui/
app.mount("/qcat-ui", StaticFiles(directory=str(STATIC_DIR)), name="qcat-ui")

FAVICON_PATH = STATIC_DIR / "favicon.ico"

# Root redirects to qcat UI
@app.get("/", response_class=HTMLResponse)
def index():
    return FileResponse(str(STATIC_DIR / "index.html"))

# Optional: also serve /index.html explicitly
@app.get("/index.html", response_class=HTMLResponse)
def index_html():
    return FileResponse(str(STATIC_DIR / "index.html"))

@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    if FAVICON_PATH.exists():
        return FileResponse(str(FAVICON_PATH))
    raise HTTPException(status_code=404)

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

    # NEW: whether the user accepts the agent's proposed intent
    accept_proposal: bool = False

    # Session ID for memory tracking
    session_id: str | None = None

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
    # Generate session ID if not provided
    session_id = body.session_id or str(uuid.uuid4())

    # Initialize session memory if needed
    if session_id not in SESSION_MEMORY:
        SESSION_MEMORY[session_id] = {
            "tables": set(),
            "procedures": set(),
            "views": set(),
            "functions": set()
        }

    # Get answer from agent
    out = agent_answer(
        query=body.prompt, items=ITEMS, emb=EMB,
        schema_filter=body.schema_name, name_pattern=body.pattern,
        intent_override=body.intent_override,
        accept_proposal=body.accept_proposal,
    )

    # Update memory with entities from this query
    entities = out.get("entities", [])
    for entity in entities:
        kind = entity.get("kind")
        name = entity.get("name")
        if kind and name:
            plural = kind + "s"  # table->tables, procedure->procedures, etc
            if plural in SESSION_MEMORY[session_id]:
                SESSION_MEMORY[session_id][plural].add(name)

    # Return memory alongside answer
    memory = {
        "tables": sorted(SESSION_MEMORY[session_id]["tables"]),
        "procedures": sorted(SESSION_MEMORY[session_id]["procedures"]),
        "views": sorted(SESSION_MEMORY[session_id]["views"]),
        "functions": sorted(SESSION_MEMORY[session_id]["functions"])
    }

    return {**out, "session_id": session_id, "memory": memory}

# Clear memory endpoint
@app.post("/api/clear_memory")
def clear_memory(body: Dict[str, Any]):
    session_id = body.get("session_id")
    if session_id and session_id in SESSION_MEMORY:
        SESSION_MEMORY[session_id] = {
            "tables": set(),
            "procedures": set(),
            "views": set(),
            "functions": set()
        }
        return {"ok": True, "message": "Memory cleared"}
    return {"ok": False, "message": "Session not found"}

# Simple health
@app.get("/api/ping")
def ping():
    return {"ok": True}

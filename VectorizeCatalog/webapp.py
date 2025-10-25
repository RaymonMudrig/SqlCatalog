# webapp.py - Unified SQL Catalog + Cluster Analysis Backend
from __future__ import annotations
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.requests import Request
from pydantic import BaseModel, Field, ConfigDict, AliasChoices
from typing import Optional, Any, Dict, List
import json
import uuid
from pathlib import Path

# Import qcat components
from qcat.items import load_items
from qcat.paths import BASE, OUTPUT_DIR, ITEMS_JSON
from qcat.backend import QcatService

# Import cluster backend components
from cluster.backend import ClusterService, ClusterState

# Import webapp unified agent
from webapp_lib.agent import agent_answer as webapp_agent_answer

app = FastAPI(title="SQL Catalog - Unified")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static file directories
STATIC_DIR = BASE / "static"
QCAT_STATIC = STATIC_DIR / "qcat"
CLUSTER_STATIC = STATIC_DIR / "cluster"

# Mount static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
app.mount("/qcat-ui", StaticFiles(directory=str(QCAT_STATIC)), name="qcat-ui")
app.mount("/cluster-ui", StaticFiles(directory=str(CLUSTER_STATIC)), name="cluster-ui")

# Initialize backends
ITEMS, EMB = load_items()

# Initialize qcat service
QCAT_SERVICE = QcatService(ITEMS, EMB)

# Initialize cluster service
CLUSTER_SNAPSHOT_PATH = OUTPUT_DIR / "cluster" / "clusters.json"
CLUSTER_SERVICE = ClusterService(CLUSTER_SNAPSHOT_PATH)

# Session memory for qcat
SESSION_MEMORY: Dict[str, Dict[str, set]] = {}

# ============================================================================
# ROOT - Serve unified UI
# ============================================================================

@app.get("/", response_class=HTMLResponse)
def index():
    """Serve the unified SQL Catalog interface"""
    return FileResponse(str(STATIC_DIR / "index.html"))

@app.get("/index.html", response_class=HTMLResponse)
def index_html():
    return FileResponse(str(STATIC_DIR / "index.html"))

@app.get("/favicon.ico")
def favicon():
    """Serve favicon"""
    return FileResponse(str(STATIC_DIR / "favicon.ico"))

# ============================================================================
# QCAT ROUTES - Semantic Search
# ============================================================================

class AskBody(BaseModel):
    prompt: str
    k: int = 10
    schema_name: str | None = Field(default=None, validation_alias="schema", serialization_alias="schema")
    pattern: str | None = None
    name_match: str | None = None
    include_via_views: bool = False
    fuzzy: bool = False
    unused_only: bool = False
    intent_override: str | None = Field(default=None, validation_alias=AliasChoices("intent_override", "intent"))
    accept_proposal: bool = False
    session_id: str | None = None
    model_config = ConfigDict(populate_by_name=True)

@app.post("/api/qcat/ask")
def qcat_ask(body: AskBody):
    """Semantic search endpoint (uses qcat agent directly)"""
    session_id = body.session_id or str(uuid.uuid4())

    if session_id not in SESSION_MEMORY:
        SESSION_MEMORY[session_id] = {
            "tables": set(), "procedures": set(),
            "views": set(), "functions": set()
        }

    # Use qcat agent directly for this endpoint
    from qcat.agent import agent_answer as qcat_agent_answer
    out = qcat_agent_answer(
        query=body.prompt, items=ITEMS, emb=EMB,
        schema_filter=body.schema_name, name_pattern=body.pattern,
        intent_override=body.intent_override,
        accept_proposal=body.accept_proposal,
    )

    # Update memory
    entities = out.get("entities", [])
    for entity in entities:
        kind = entity.get("kind")
        name = entity.get("name")
        if kind and name:
            plural = kind + "s"
            if plural in SESSION_MEMORY[session_id]:
                SESSION_MEMORY[session_id][plural].add(name)

    memory = {k: sorted(v) for k, v in SESSION_MEMORY[session_id].items()}
    return {**out, "session_id": session_id, "memory": memory}

@app.post("/api/qcat/clear_memory")
def qcat_clear_memory(body: Dict[str, Any]):
    """Clear session memory"""
    session_id = body.get("session_id")
    if session_id and session_id in SESSION_MEMORY:
        SESSION_MEMORY[session_id] = {
            "tables": set(), "procedures": set(),
            "views": set(), "functions": set()
        }
        return {"ok": True, "message": "Memory cleared"}
    return {"ok": False, "message": "Session not found"}

# ============================================================================
# CLUSTER ROUTES - Cluster Management
# ============================================================================

@app.get("/api/cluster/summary")
def cluster_summary():
    """Get cluster summary"""
    return CLUSTER_SERVICE.summary()

@app.get("/api/cluster/trash")
def cluster_list_trash():
    """List all items in trash"""
    return CLUSTER_SERVICE.list_trash()

@app.post("/api/cluster/trash/restore")
def cluster_restore_trash(body: Dict[str, Any]):
    """Restore item from trash"""
    item_type = body.get("item_type")
    if item_type == "procedure":
        procedure_name = body.get("procedure_name")
        target_cluster_id = body.get("target_cluster_id")
        force_new_group = body.get("force_new_group", False)
        return CLUSTER_SERVICE.restore_procedure(procedure_name, target_cluster_id, force_new_group)
    elif item_type == "table":
        trash_index = body.get("trash_index")
        return CLUSTER_SERVICE.restore_table(trash_index)
    else:
        return {"ok": False, "message": f"Unknown item_type: {item_type}"}

@app.post("/api/cluster/trash/empty")
def cluster_empty_trash():
    """Empty trash permanently"""
    return CLUSTER_SERVICE.empty_trash()

@app.get("/api/cluster/{cluster_id}")
def cluster_detail(cluster_id: str):
    """Get cluster details"""
    return CLUSTER_SERVICE.cluster_detail(cluster_id)

@app.get("/api/cluster/svg/summary")
def cluster_svg_summary():
    """Get summary SVG"""
    return HTMLResponse(content=CLUSTER_SERVICE.summary_svg(), media_type="image/svg+xml")

@app.get("/api/cluster/svg/{cluster_id}")
def cluster_svg_detail(cluster_id: str):
    """Get cluster detail SVG"""
    return HTMLResponse(content=CLUSTER_SERVICE.cluster_svg(cluster_id), media_type="image/svg+xml")

@app.post("/api/cluster/command")
def cluster_command(body: Dict[str, Any]):
    """Execute cluster command"""
    if "command" in body:
        result = CLUSTER_SERVICE.execute_text(body["command"])
    else:
        result = CLUSTER_SERVICE.execute(body)
    return result

@app.post("/api/cluster/reload")
def cluster_reload():
    """Reload clusters from snapshot"""
    summary = CLUSTER_SERVICE.reload()
    return {"ok": True, "summary": summary}

@app.post("/api/cluster/rebuild")
def cluster_rebuild():
    """Rebuild clusters from catalog.json (DESTRUCTIVE - creates fresh clusters)"""
    return CLUSTER_SERVICE.rebuild_from_catalog()

# ============================================================================
# UNIFIED COMMAND ROUTER
# ============================================================================

class UnifiedCommand(BaseModel):
    """Unified command that routes to either qcat or cluster"""
    command: str
    session_id: str | None = None

@app.post("/api/command")
def unified_command(body: UnifiedCommand):
    """
    Route command to appropriate backend using UNIFIED intent classification.

    New architecture:
      1. Single LLM call with ALL intents (cluster + qcat)
      2. Dispatch to cluster.ops or qcat.ops directly
      3. Format with respective formatters

    This ensures "which procedures access X" goes to qcat, not cluster!
    """
    # Call unified webapp agent (single LLM call with ALL intents)
    try:
        result = webapp_agent_answer(
            query=body.command,
            qcat_service=QCAT_SERVICE,
            cluster_service=CLUSTER_SERVICE,
            intent_override=None,
            accept_proposal=False,
        )
    except Exception as e:
        # Catch any unhandled exceptions and return JSON error
        import traceback
        error_detail = str(e)
        print(f"[webapp] Error in unified_command: {error_detail}")
        traceback.print_exc()

        return {
            "type": "error",
            "result": {
                "answer": f"## Internal Error\n\nAn error occurred while processing your command:\n\n```\n{error_detail}\n```\n\nPlease check the server logs for more details.",
                "ok": False,
                "status": "error",
                "error": error_detail
            },
            "ok": False
        }

    # Update session memory if qcat entities were returned
    # BUT skip list_all_* intents (they would populate entire entity list)
    if "entities" in result:
        # Create or reuse session_id
        session_id = body.session_id or str(uuid.uuid4())

        if session_id not in SESSION_MEMORY:
            SESSION_MEMORY[session_id] = {
                "tables": set(), "procedures": set(),
                "views": set(), "functions": set()
            }

        # Check if this is a list_all_* intent (should not populate entity memory)
        intent = result.get("intent")
        if not (intent and intent.startswith("list_all_")):
            entities = result.get("entities", [])
            for entity in entities:
                kind = entity.get("kind")
                name = entity.get("name")
                if kind and name:
                    plural = kind + "s"
                    if plural in SESSION_MEMORY[session_id]:
                        SESSION_MEMORY[session_id][plural].add(name)

        memory = {k: sorted(v) for k, v in SESSION_MEMORY[session_id].items()}
        result["session_id"] = session_id
        result["memory"] = memory

    # Check if needs confirmation
    if result.get("needs_confirmation"):
        return {
            "type": "error",
            "result": result,
            "ok": False
        }

    # Check if operation failed (answer contains "Error")
    answer = result.get("answer", "")
    # Handle case where answer might not be a string (e.g., dict for compare_sql)
    if isinstance(answer, str) and (answer.startswith("Error") or answer.startswith("✗")):
        return {
            "type": "error",
            "result": result,
            "ok": False,
            "status": "error"
        }

    # Success - mark as ok
    result["ok"] = True
    result["status"] = "ok"

    # Determine type from result (for backward compatibility)
    if "entities" in result or "unified_diff" in result:
        result_type = "qcat"
    else:
        result_type = "cluster"

    return {
        "type": result_type,
        "result": result,
        "ok": True,
        "status": "ok"
    }

# ============================================================================
# HEALTH CHECK
# ============================================================================

@app.get("/api/ping")
def ping():
    return {"ok": True, "backends": ["qcat", "cluster"]}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

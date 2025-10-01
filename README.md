# Sql Catalog Agentic Mode

# SqlCatalog
Set of tools to cataloging SQL script creating a database and make query on it.
Cataloging result:
- File catalog.json
- Export of each sql create statement of tables, views, function and procedures.

# SqlCatalog Agentic

Here’s the **Python-only** inventory for the current (working) version — i.e., the web UI + CLI that deduces intent, uses `catalog.json` deterministically, and (optionally) falls back to `items.json` for semantic bits.

## Top-level

* `webapp.py` — FastAPI app (serves `/`, `/static/*`, and `POST /api/ask` to run the agent).
* `cli.py` — thin launcher that delegates to `qcli.main`.

## Core engine (`qcat/`)

* `qcat/__init__.py` — empty (marks package).
* `qcat/paths.py` — resolves paths (`../output/catalog.json`, `../output/sql_exports/*`, etc.).
* `qcat/loader.py` — loads `catalog.json`; synthesizes items if `items.json` is absent.
* `qcat/graph.py` — builds read/write/call graphs from catalog + exported SQL (no list-arg caching).
* `qcat/ops.py` — **deterministic** operations (list columns, readers/writers, call tree, unused cols, etc.).
* `qcat/formatters.py` — renders markdown (e.g., `render_procs_access_table`, `render_list_columns_of_table`, …).
* `qcat/agent.py` — intent router for `/api/ask` (parses prompt → calls the right formatter/op).
* `qcat/intent.py` — prompt → intent parser (LLM or rules; used by `agent.py`).
* `qcat/name_match.py` — name utilities (e.g., `split_safe`, simple matching helpers).

> Note: `qcat/relations.py` is **not required** in this version (replaced by `ops.py` + `graph.py`). You can drop it if present.

## CLI layer (`qcli/`)

* `qcli/__init__.py` — empty (marks package).
* `qcli/main.py` — argument parsing entrypoint; dispatches to local ops or server.
* `qcli/args.py` — defines argparse flags (same semantics as web prompt but explicit flags).
* `qcli/server.py` — tiny client for calling the FastAPI server.
* `qcli/printers.py` — pretty-printers; also includes `read_sql_from_item()` (reads SQL from `../output/sql_exports/...` or item).

## Optional (only if you still run vectorization/semantic indexing)

* `vectorize_catalog.py` — builds `items.json` + embeddings (not needed for deterministic intents).
* Any `llm_client.py` you use to talk to LM Studio / OpenAI (if your `intent.py` uses LLMs).

---

### Minimal set (if you want the leanest working stack)

* `webapp.py`
* `cli.py`
* `qcat/paths.py`, `qcat/loader.py`, `qcat/graph.py`, `qcat/ops.py`, `qcat/formatters.py`, `qcat/agent.py`, `qcat/intent.py`, `qcat/name_match.py`, `qcat/__init__.py`
* `qcli/main.py`, `qcli/args.py`, `qcli/server.py`, `qcli/printers.py`, `qcli/__init__.py`


# VectorizeCatalog
This is first model attemp to create this system, keep just for study purposes.

## vectorize explicitly (optional – query/RAG will auto-build if missing)
python vectorize_catalog.py

## semantic retrieval

### One time query
python query_catalog.py "procedures that update client cash table" --kind procedure --schema dbo

### RAG answer with Qwen
python rag_chat.py "Which views read from Order tables?" --kind view

### deterministic utility inside RAG script
python rag_chat.py "what table that is not accessed by any procedure" --op unaccessed --schema dbo --no-chat

## client server mode

### query client
python cli.py  "what table that is not accessed by any procedure" --kind table

### using server
python webapp.py

Runs a server and give a html page to perform queries.

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**VectorizeCatalog** is a semantic SQL catalog search and relationship analysis system for SQL Server databases. It combines vector embeddings, graph-based relationship tracking, and LLM-powered intent classification to enable natural language queries over database schemas.

The system processes SQL Server catalog metadata (tables, views, procedures, functions) along with their relationships (reads/writes/calls, foreign keys, column references) and provides:
- Semantic search over database entities using vector embeddings
- Relationship graph queries (e.g., "which procedures access table X", "call tree of procedure Y")
- LLM-powered natural language intent classification
- SQL comparison and diffing with similarity scoring
- Detection of unused tables and columns
- Both CLI and web UI (FastAPI + static frontend)

## Core Architecture

### Architectural Principle: LLM as Intent Classifier + Deterministic Operations

**Key Design Decision**: This system uses LLMs ONLY for intent classification, never for data retrieval or manipulation. All operations on catalog data are deterministic and reliable.

**Flow**:
```
Natural Language Query
    ↓
[LLM] Intent Classification (llm_intent.py)
    → Returns: {"intent": "procs_access_table", "name": "Order", "confidence": 0.85}
    ↓
[Deterministic] Operation Execution (ops.py)
    → Pure function: procs_access_table(items, "Order")
    → Walks catalog.json graph (Referenced_By, Reads, Writes)
    → Returns: List of procedure objects
    ↓
[Deterministic] Formatting (formatters.py)
    → Renders to Markdown
```

**Why This Matters**:
- **Reliability**: LLMs can hallucinate intents, but never data. Catalog operations are 100% accurate.
- **Debuggability**: Can test ops independently with mock catalog.json
- **Performance**: Single LLM call per query (not agentic loops)
- **Transparency**: User sees detected intent and can override
- **Cost-effective**: Minimal token usage (classification only)

**Confidence-Based Proposals**: If intent confidence < 70%, system returns proposal for user confirmation instead of executing. This prevents acting on ambiguous intents.

### Data Flow Pipeline

1. **Input**: SQL Server catalog metadata (`../output/catalog.json`) + optional SQL DDL exports (`../output/sql_exports/`)
2. **Vectorization** (`vectorize_catalog.py`): Builds semantic index → `../output/vector_index/` (embeddings.npy, items.json, meta.json)
3. **Query Resolution** (3 modes):
   - **CLI** (`qcli/main.py`): Direct local queries using the index
   - **Web API** (`webapp.py`): FastAPI server with agentic intent resolution
   - **Remote CLI** (`qcli/server.py`): CLI client calling the web API

### Module Structure

```
qcat/           # Core operations and utilities
├── ops.py           # Deterministic graph/relation operations (finds, access checks, diffs)
├── formatters.py    # Markdown renderers for each intent type
├── intents.py       # Intent catalog and normalization helpers
├── llm_intent.py    # LLM-powered intent classification (OpenAI API)
├── agent.py         # Agentic dispatcher with confidence-based proposals
├── embeddings.py    # Embedding generation (LM Studio or sentence-transformers)
├── search.py        # Semantic search over vector index
├── paths.py         # Centralized path configuration
├── loader.py        # Index/catalog loading utilities
└── ...

qcli/           # CLI interface
├── main.py          # Entry point (local or remote execution)
├── args.py          # Argument parser
├── local.py         # Local query execution
├── server.py        # Remote API client
├── printers.py      # SQL resolution with fallbacks (exports → sources)
└── resolver.py      # Name resolution helpers

webapp.py            # FastAPI server with /api/ask endpoint
vectorize_catalog.py # Index builder (embeddings + metadata)
query_catalog.py     # Legacy standalone query tool (superseded by qcli)
```

### Key Design Patterns

**Agentic Intent Flow** (webapp.py → agent.py):
1. User sends natural language query to `/api/ask`
2. `agent_answer()` classifies intent using LLM (or accepts override/proposal)
3. If confidence < 0.70 → return proposal for user confirmation
4. If confident or accepted → dispatch to `formatters.py` renderer → `ops.py` operation

**SQL Resolution Chain** (qcli/printers.py):
1. Check item's embedded `sql` field (from index)
2. Check item's `sql_path` field (recorded during indexing)
3. Search `../output/sql_exports/{kind}/{safe_name}.sql` with robust variant matching (handles `·` vs `.`, spaces, etc.)
4. Fallback: scan `../sql_files/**/*.sql` for `CREATE/ALTER <object>` DDL

**Name Matching** (qcat/name_match.py + ops.py):
- Handles schema-qualified (`dbo.Order`) vs safe names (`dbo·Order`)
- Supports exact, word-boundary, substring, and fuzzy modes
- CamelCase tokenization for smarter partial matches

## Commands

### Environment Setup
```bash
# Create virtual environment (Python 3.10+)
./create_python_virtualenv.sh

# Activate
source .venv/bin/activate  # macOS/Linux
.venv\Scripts\activate     # Windows

# Install dependencies (if not automated)
pip install fastapi uvicorn sentence-transformers numpy openai requests
```

### Build Index (Required First Step)
```bash
# Build vector index from catalog.json
python vectorize_catalog.py

# Environment variables (optional):
# USE_LMSTUDIO=1              # Use LM Studio for embeddings (default)
# LMSTUDIO_BASE_URL=http://localhost:1234/v1
# EMBED_MODEL=text-embedding-nomic-embed-text-v1.5
# SQL_OUTPUT_DIR=/path/to/output  # Override default ../output location
```

**Output**: Creates `../output/vector_index/` with embeddings.npy, items.json, meta.json

### Run Web Server
```bash
# Start FastAPI server (port 8000)
./runwebapp.sh
# OR manually:
uvicorn webapp:app --host 0.0.0.0 --port 8000 --reload

# Web UI: http://localhost:8000
# API endpoint: POST /api/ask
```

**Server Dependencies**:
- Requires `../output/vector_index/items.json` (run `vectorize_catalog.py` first)
- Reads `.env` or environment for `OPENAI_API_KEY` (intent classification)

### CLI Usage

**Local queries** (uses index directly):
```bash
# Basic semantic search
python -m qcli.main "show me order tables"

# Relation queries with filters
python -m qcli.main "which procedures access Order" --table dbo.Order --kind procedure

# SQL retrieval
python -m qcli.main "Order" --sql-of --kind table --full

# List entities
python -m qcli.main --list-names --kind procedure --pattern "Order.*"

# Name matching modes
python -m qcli.main "..." --name-match exact    # exact match only
python -m qcli.main "..." --name-match fuzzy    # substring fallback
```

**Remote queries** (via web API):
```bash
python -m qcli.main "your query" --server http://localhost:8000
```

**Key flags**:
- `--k N`: Top N results (default 12)
- `--kind {table,view,procedure,function,column,any}`: Filter by entity type
- `--schema SCHEMA`: Filter by schema
- `--unused-only`: Only unused entities
- `--show-sql`: Print SQL DDL in results
- `--answer`: Use LLM to synthesize answer from results
- `--table NAME`: Force target table for relation queries
- `--include-via-views`: Include transitive reads via views

## Important Implementation Details

### Intent System

**Intents are the bridge between natural language and deterministic operations.** Each intent represents a specific query type that can be executed reliably against catalog.json.

Intents defined in `qcat/intents.py` map to:
- **Operations** (`qcat/ops.py`): Pure, deterministic logic (finds, filters, graph traversal) - **NO LLM calls**
- **Formatters** (`qcat/formatters.py`): Markdown rendering - **NO LLM calls**

**Critical**: Operations in `ops.py` must be:
- Pure functions (same input → same output)
- Deterministic (no randomness, no LLM calls)
- Testable with mock catalog data
- Graph-based (traverse relationships in catalog.json: Referenced_By, Reads, Writes, Calls)

**Adding a new intent**:
1. Add to `INTENTS` list in `intents.py` (e.g., `"tables_modified_after_date"`)
2. Implement **deterministic** operation in `ops.py` (return raw data, no LLM)
   ```python
   def tables_modified_after(items: List[Dict], date: str) -> List[Dict]:
       # Pure graph traversal/filtering logic only
       return [t for t in items if t.get("modified") > date]
   ```
3. Implement renderer in `formatters.py` (return markdown string)
4. Add dispatcher case in `agent.py` → `agent_answer()`
5. Update LLM prompt in `llm_intent.py` to teach it the new intent pattern

**Example Intent Execution** (procs_access_table):
```python
# 1. LLM classifies: "which procs touch Order" → {"intent": "procs_access_table", "name": "Order"}
# 2. Deterministic op:
def procs_access_table(items, table_name):
    table = _find_item(items, "table", table_name)  # exact match
    refs = table.get("Referenced_By") or []          # walk graph
    return [r for r in refs if r["kind"] == "procedure"]
# 3. Formatter renders list as markdown
```

### Embedding Backends
Two modes (controlled by `USE_LMSTUDIO` env var):

**LM Studio** (default, `USE_LMSTUDIO=1`):
- Requires running LM Studio server with embedding model
- API: `POST {LMSTUDIO_BASE_URL}/embeddings`
- Model: Configurable via `EMBED_MODEL` env var

**Sentence Transformers** (fallback, `USE_LMSTUDIO=0`):
- Uses `sentence-transformers/all-MiniLM-L6-v2` (or `LOCAL_EMBED_MODEL` env var)
- No external server needed

### Data Assumptions
- Catalog format: Expects `catalog.json` with `Tables`, `Views`, `Procedures`, `Functions` top-level keys
- Schema format: Case-insensitive key access (handles both `Schema`/`schema`, `Safe_Name`/`safe_name`)
- Safe names: Uses `.` (period) as schema separator (e.g., `dbo.Order`) — changed from `·` (middle dot) for cleaner display and clickability
- Referenced_By: Used for reverse lookups (which procs/views access a table)

### SQL Comparison & Diffing
`ops.py::compare_sql()` produces:
- **Similarity score**: Weighted blend of edit distance (45%), token overlap (35%), structural similarity for tables (20%)
- **Unified diff**: Git-style diff with full context (for diff2html rendering)
- **Structural summary**: Column adds/removes/type changes for table comparisons

Format normalization (`format_sql_for_diff`):
- Strips comments, normalizes whitespace
- Keywords on new lines (CREATE, SELECT, FROM, WHERE, JOIN, etc.)
- Parentheses indented (but protects numeric size specifiers like `(18)`, `(18, 4)`)

### Path Configuration
Centralized in `qcat/paths.py`:
```python
BASE = Path(__file__).resolve().parent.parent
OUTPUT_DIR = Path(os.getenv("SQL_OUTPUT_DIR") or BASE.parent / "output")
ITEMS_JSON = OUTPUT_DIR / "vector_index" / "items.json"
SQL_FILES_DIR = BASE.parent / "sql_files"  # Fallback DDL source
```

Override via `SQL_OUTPUT_DIR` environment variable.

## Testing & Development

**Run the vectorization pipeline**:
```bash
# Ensure catalog.json exists
ls -l ../output/catalog.json

# Build index
python vectorize_catalog.py

# Verify output
ls -l ../output/vector_index/
# Expected: embeddings.npy, items.json, meta.json
```

**Test CLI locally**:
```bash
# Should return markdown list of procedures
python -m qcli.main "list all procedures" --kind procedure

# Should show procedure call tree
python -m qcli.main "call tree of usp_GetOrder" --kind procedure
```

**Test web server**:
```bash
# Start server
./runwebapp.sh

# In another terminal:
curl -X POST http://localhost:8000/api/ask \
  -H "Content-Type: application/json" \
  -d '{"prompt": "which procedures access Order table"}'
```

## Known Edge Cases

1. **Name ambiguity**: If multiple entities have same base name in different schemas, use schema-qualified names (`dbo.Order`) or safe names (`dbo·Order`)

2. **Missing SQL exports**: System falls back to scanning `../sql_files/` recursively for CREATE/ALTER statements. Ensure your source DDL uses standard SQL Server syntax.

3. **Intent confidence**: Queries below 70% confidence trigger proposal mode (user must accept). Override via `intent_override` in API or use `--kind` CLI flag to force resolution.

4. **Embedding dimension mismatch**: If switching between LM Studio models or sentence-transformers, rebuild index (`python vectorize_catalog.py`) to ensure embedding dimensions match.

5. **Case sensitivity**: All name matching is case-insensitive, but display preserves original casing from catalog.

## Performance Notes

- Vector search is O(N) over all entities (no ANN index yet) — acceptable for catalogs up to ~10K entities
- LLM intent classification adds ~500ms-2s latency (can be bypassed with `--kind` flag or `intent_override`)
- Diff formatting is CPU-bound; very large procedures (>10K lines) may take 1-2s to format

## Web UI

Located in `static/`:
- `index.html`: Single-page app with chat interface
- Supports intent proposals, SQL diff rendering (via diff2html), markdown formatting
- Auto-connects to `/api/ask` endpoint

**Clickable Entities Feature**:
- All entity names in query results are rendered as clickable elements
- Click any entity (table, view, procedure, function) to append it to the prompt box
- Entities are automatically wrapped in backticks (e.g., `` `dbo.Order_Trx` ``) to handle names with spaces
- JavaScript pattern matches: `schema.object`, `object`, `schema.object.column`
- Supports entity names containing spaces (e.g., `dbo.BO Client Cash`)
- Visual feedback: blue color with hover effect

**Implementation**:
- Formatters output entity names in markdown backticks: `` `dbo.TableName` ``
- Backticks render as `<code>` elements in HTML
- JavaScript `makeEntitiesClickable()` function:
  - Scans all `<code>` elements after markdown rendering
  - Tests against entity pattern regex (allows spaces, dots, underscores)
  - Converts matching elements to clickable spans with event handlers
  - Appends wrapped entity names to prompt textarea on click

Static files served via FastAPI's `StaticFiles` at `/static` and root `/`.

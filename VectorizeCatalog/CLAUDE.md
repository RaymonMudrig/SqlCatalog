# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**VectorizeCatalog** is a unified SQL catalog analysis system for SQL Server databases. It combines three major capabilities:

1. **Catalog Queries (qcat)**: Graph-based relationship tracking with LLM-powered natural language interface
2. **Cluster Management**: Automated procedure grouping by table access patterns with interactive editing
3. **Unified Web UI**: Single-page application integrating both capabilities with natural language command routing

The system processes SQL Server catalog metadata (tables, views, procedures, functions) along with their relationships (reads/writes/calls, foreign keys, column references) and provides:
- LLM intent classification → deterministic graph operations (NO RAG, NO vector search)
- Relationship graph queries (e.g., "which procedures access table X", "call tree of procedure Y")
- Automated procedure clustering based on shared table access patterns
- Interactive cluster visualization and management (rename, move, delete, trash/restore)
- SQL comparison and diffing with text-based similarity scoring
- Detection of unused tables and columns, missing tables (referenced but don't exist)
- Unified web UI with natural language commands + traditional UI controls

**Note**: Vector embeddings infrastructure exists in the codebase (`embeddings.py`, `search.py`) but is **NOT used** in the current query flow. All queries go through LLM intent classification → deterministic catalog.json operations.

## Core Architecture

### Architectural Principle: LLM as Intent Classifier + Deterministic Operations

**Key Design Decision**: This system uses LLMs ONLY for intent classification, never for data retrieval or manipulation. All operations on catalog data are deterministic and reliable.

**Unified Command Flow** (webapp.py → webapp/agent.py):
```
Natural Language Query
    ↓
[LLM] Unified Intent Classification (webapp/llm_intent.py)
    → Single LLM call with ALL intents (cluster + qcat)
    → Returns: {"intent": "procs_access_table", "backend": "qcat", "name": "Order", "confidence": 0.95}
    ↓
[Router] Backend Dispatch (webapp/agent.py)
    → If backend == "qcat": Execute via qcat/ops.py functions
    → If backend == "cluster": Execute via cluster/ops.py functions
    ↓
[Deterministic] Operation Execution
    → Pure functions: procs_access_table(items, "Order") OR rename_cluster(state, "C1", "Orders")
    → Walks catalog.json graph (Referenced_By, Reads, Writes) OR modifies clusters.json state
    → Returns: Raw data objects
    ↓
[Deterministic] Formatting (formatters.py)
    → Renders to Markdown (qcat/formatters.py OR cluster/formatters.py)
```

**Why This Matters**:
- **Reliability**: LLMs can hallucinate intents, but never data. Catalog operations are 100% accurate.
- **Unified UX**: Single prompt box handles both catalog queries AND cluster management commands
- **Debuggability**: Can test ops independently with mock data
- **Performance**: Single LLM call per query (not agentic loops)
- **Transparency**: User sees detected intent and can override
- **Cost-effective**: Minimal token usage (classification only)

**Confidence-Based Proposals**: If intent confidence < 70%, system returns proposal for user confirmation instead of executing. This prevents acting on ambiguous intents.

**Backend Routing**: The unified agent knows about TWO backends:
- **qcat**: Catalog queries (search, relationships, SQL comparison) - read-only operations on catalog.json
- **cluster**: Cluster management (rename, move, delete, trash) - read/write operations on clusters.json

### Data Flow Pipeline

1. **Input**: SQL Server catalog metadata (`../output/catalog.json`) + optional SQL DDL exports (`../output/sql_exports/`)
2. **Index Loading**: Web server loads `../output/vector_index/items.json` (flattened catalog for faster access)
   - **Note**: items.json must be pre-built from external tooling
   - **Note**: Embeddings (embeddings.npy) may exist but are NOT used in current query flow
3. **Query Resolution**:
   - **Web UI** (`webapp.py`): Natural language → LLM intent classification → deterministic ops

### Module Structure

```
qcat/           # Catalog query operations and utilities
├── ops.py           # Deterministic graph/relation operations (finds, access checks, diffs)
├── formatters.py    # Markdown renderers for each intent type
├── intents.py       # Intent catalog and normalization helpers
├── llm_intent.py    # LLM-powered intent classification (OpenAI API)
├── agent.py         # Agentic dispatcher with confidence-based proposals
├── embeddings.py    # Embedding generation (LM Studio or sentence-transformers, legacy)
├── search.py        # Semantic search over vector index (legacy)
├── paths.py         # Centralized path configuration
├── loader.py        # Index/catalog loading utilities
├── backend.py       # QcatService (session state management)
├── printers.py      # SQL resolution with fallbacks (exports → sources)
├── name_match.py    # Name matching utilities (exact, fuzzy, CamelCase)
├── items.py         # Item data structures and utilities
├── relations.py     # Relationship graph utilities
├── graph.py         # Graph traversal utilities
├── dynamic_sql.py   # Dynamic SQL analysis utilities
├── llm.py           # LLM integration utilities
├── prompt.py        # Prompt templates and utilities
└── standalone_backend.py  # Standalone backend service

cluster/        # Cluster management operations and visualization
├── ops.py           # Cluster operations (rename, move, delete, trash/restore)
├── formatters.py    # Markdown renderers for cluster operations
├── intents.py       # Cluster intent catalog
├── backend.py       # ClusterService (state management, SVG generation, trash, snapshot persistence)
├── clustering.py    # Automated clustering algorithm (groups procedures by shared table access)
├── agent.py         # Cluster-specific agent dispatcher
└── llm_intent.py    # Cluster-specific intent classification

webapp/         # Unified web backend
├── agent.py         # Unified agent (routes to qcat or cluster backend)
└── llm_intent.py    # Unified LLM intent classification (all intents)

static/         # Unified web frontend
├── index.html       # Unified webapp (Entities + Clusters tabs)
├── app.js           # Main app logic, tab switching, command execution
├── cluster.js       # Cluster visualization and management
├── diagram.js       # Diagram rendering utilities
├── markdown_diff.js # Markdown and diff rendering
├── styles.css       # Main stylesheet
├── cluster.css      # Cluster-specific styles
├── diagram.css      # Diagram-specific styles
├── markdown_diff.css# Markdown/diff-specific styles
├── cluster/         # Standalone cluster editor (legacy)
│   ├── index.html   # Standalone cluster management UI
│   ├── app.js       # Standalone cluster app logic
│   └── styles.css   # Standalone cluster styles
└── qcat/            # Standalone qcat editor (legacy)
    ├── index.html   # Standalone qcat UI
    ├── app.js       # Standalone qcat app logic
    ├── ui.js        # Standalone qcat UI utilities
    └── styles.css   # Standalone qcat styles

webapp.py            # FastAPI unified server (port 8000)
cli.py               # CLI entry point
test_regression.py   # Regression test suite
```

### Key Design Patterns

**Agentic Intent Flow** (webapp.py → agent.py):
1. User sends natural language query to `/api/ask`
2. `agent_answer()` classifies intent using LLM (or accepts override/proposal)
3. If confidence < 0.70 → return proposal for user confirmation
4. If confident or accepted → dispatch to `formatters.py` renderer → `ops.py` operation

**SQL Resolution Chain** (qcat/printers.py):
1. Check item's embedded `sql` field (from index)
2. Check item's `sql_path` field (recorded during indexing)
3. Search `../output/sql_exports/{kind}/{safe_name}.sql` with robust variant matching (handles `·` vs `.`, spaces, etc.)
4. Fallback: scan `../sql_files/**/*.sql` for `CREATE/ALTER <object>` DDL

**Name Matching** (qcat/name_match.py + ops.py):
- Handles schema-qualified (`dbo.Order`) vs safe names (`dbo·Order`)
- Supports exact, word-boundary, substring, and fuzzy modes
- CamelCase tokenization for smarter partial matches

### Cluster Management Architecture

**Purpose**: Automatically group related procedures and provide interactive cluster editing for database organization.

**Data Model** (cluster/backend.py):
```python
class ClusterState:
    clusters: Dict[str, ClusterInfo]   # cluster_id → ClusterInfo
    groups: Dict[str, ProcedureGroup]  # group_id → ProcedureGroup
    cluster_order: List[str]           # Ordered list of cluster IDs
    group_order: List[str]             # Ordered list of group IDs
    global_tables: Set[str]            # Tables accessed by >= 2 clusters
    missing_tables: Set[str]           # Referenced tables not in catalog
    orphaned_tables: Set[str]          # Catalog tables not accessed by procedures
    similarity_edges: List[SimilarityEdge]  # SQL similarity edges
    parameters: Dict[str, Any]         # Clustering parameters
    catalog_path: Optional[str]        # Path to source catalog.json
    trash: List[TrashItem]             # Soft-deleted procedures and tables (uses TrashItem dataclass)
    # Computed fields:
    table_usage: Counter[str]          # Table access counts
    table_nodes: List[Dict[str, Any]]  # Table node metadata
    procedure_table_edges: List[ProcedureTableEdge]  # Procedure-table connections
    last_updated: datetime             # Last modification timestamp
```

**Key Concepts**:

1. **Cluster**: A logical group of related procedures that share table access patterns
   - Has unique `cluster_id` (C1, C2, ...)
   - Has `display_name` (user-customizable)
   - Contains multiple `ProcedureGroup` objects
   - Tracks all tables accessed by its procedures

2. **Procedure Group**: A collection of procedures that access exactly the same set of tables
   - Has unique `group_id` (G1, G2, ...)
   - Has `display_name` (user-customizable)
   - All procedures in the group share identical table access patterns
   - Created automatically by clustering algorithm

3. **Global Tables**: Tables accessed by procedures in multiple clusters (>= 2 clusters)
   - Rendered differently in visualizations (lighter color)
   - Indicate shared/cross-cutting tables in the database

4. **Missing Tables**: Virtual tables that are referenced by procedures but don't exist in catalog.json
   - Can happen when procedures reference dropped/external tables
   - Rendered in gray with special styling
   - Connected to the clusters whose procedures reference them

5. **Trash**: Soft-delete storage for procedures and tables
   - Procedures: Store procedure name, original cluster_id, and original group_id
   - Tables: Store table name and which clusters referenced it
   - Can be restored with restore_procedure() or restore_table()
   - Can be permanently deleted with empty_trash()

**Clustering Algorithm** (cluster/clustering.py):
```
1. Parse catalog.json to extract procedures and their table access (Reads + Writes)
2. Build procedure → table set mapping
3. Group procedures by identical table access sets (creates ProcedureGroups)
4. Cluster ProcedureGroups by shared tables (creates Clusters):
   - Groups that share tables go into same cluster
   - Use union-find algorithm for connected components
5. Identify global tables (accessed by >= 2 clusters)
6. Serialize to clusters.json
```

**State Synchronization Pattern**:
- **CRITICAL**: All operations immediately save state with `_save_snapshot()`
- Memory and file are ALWAYS synchronized
- Example flow:
  ```python
  def rename_cluster(state, cluster_id, new_name):
      cluster = state.clusters[cluster_id]
      cluster.display_name = new_name
      # Caller MUST call _save_snapshot() immediately after
  ```
- This eliminates the need for separate "save" commands
- User only needs one "Refresh" button (file → memory → display)

**SVG Diagram Generation** (cluster/backend.py):
- Uses Graphviz DOT language to generate interactive diagrams
- Two views:
  1. **Summary View**: All clusters + global tables
     - Each cluster rendered as box with cluster name
     - Tables rendered as ellipses
     - Global tables in lighter color
     - Missing tables in gray with dashed border
     - Edges connect clusters to their tables
  2. **Detail View**: Single cluster with all groups and procedures
     - Cluster at top
     - Groups below cluster
     - Procedures below groups
     - Tables at bottom
     - Edges show full hierarchy

**Operations** (cluster/ops.py - deterministic, NO LLM):
- `rename_cluster(state, cluster_id, new_name)` - Rename cluster
- `rename_group(state, group_id, new_name)` - Rename procedure group
- `move_group(state, group_id, cluster_id)` - Move group to different cluster
- `move_procedure(state, procedure_name, cluster_id)` - Move single procedure (creates new group)
- `delete_procedure(state, procedure_name)` - Soft delete (moves to trash)
- `delete_table(state, table_name)` - Soft delete table (moves to trash)
- `add_cluster(state, cluster_id, display_name)` - Create empty cluster
- `delete_cluster(state, cluster_id)` - Delete cluster (moves procedures to trash)
- `restore_procedure(state, procedure_name, target_cluster_id, force_new_group)` - Restore from trash
- `restore_table(state, trash_index)` - Restore table from trash
- `list_trash(state)` - List all trashed items
- `empty_trash(state)` - Permanently delete all trash
- `get_cluster_summary(state)` - Get summary stats
- `get_cluster_detail(state, cluster_id)` - Get single cluster details

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

### Index Requirements

The web server requires `../output/vector_index/items.json` to operate. This file should be pre-built from external tooling that processes your SQL Server catalog.

**Required file**: `../output/vector_index/items.json` (flattened catalog for fast loading)

**Optional files** (legacy, not used):
- `../output/vector_index/embeddings.npy` - Vector embeddings (legacy)
- `../output/vector_index/meta.json` - Embedding metadata (legacy)

**Environment variables**:
- `SQL_OUTPUT_DIR=/path/to/output` - Override default `../output` location
- `SQL_FILES_DIR=/path/to/sql_files` - Override default `../sql_files` location

### Build Clusters (Optional)
```bash
# Build initial clusters from catalog.json
python -m cluster.clustering

# Output: Creates ../output/cluster/clusters.json

# Or use the web UI "Reset Clusters" button (destructive rebuild)
```

**Clustering Output**:
- Creates `../output/cluster/clusters.json` with:
  - Clusters grouped by shared table access
  - Procedure groups (procedures with identical table sets)
  - Global tables (accessed by >= 2 clusters)
  - Empty trash

### Run Web Server
```bash
# Start unified FastAPI server (port 8000)
./runwebapp.sh
# OR manually:
uvicorn webapp:app --host 0.0.0.0 --port 8000 --reload

# Web UI: http://localhost:8000
# - Entities tab: Semantic search and catalog queries
# - Clusters tab: Interactive cluster visualization and management

# Unified API endpoint: POST /api/command
#   Routes to either qcat or cluster backend based on intent
```

**Server Dependencies**:
- Requires `../output/vector_index/items.json` (pre-built from external tooling)
- Requires `../output/cluster/clusters.json` (auto-created on first cluster command)
- Reads `.env` or environment for `OPENAI_API_KEY` (intent classification)

### Unified Web UI Commands

The unified web UI at http://localhost:8000 accepts natural language commands that route to either qcat or cluster backend:

**Catalog Query Commands** (qcat backend):
```
"which procedures access Order table"
"list all tables"
"show views that read Customer"
"what tables does usp_GetOrder access"
"call tree of usp_ProcessOrder"
"compare dbo.Order with dbo.Order_Archive"
"find similar SQL to usp_GetCustomer"
"show unused columns in Order table"
"list columns returned by usp_GetOrder"
```

**Cluster Management Commands** (cluster backend):
```
"rename cluster C1 to Orders"
"rename group G5 to Customer Management"
"move group G3 to cluster C2"
"move procedure usp_GetOrder to cluster C1"
"delete procedure usp_OldProc"
"delete table TempTable"
"add cluster C10 named Analytics"
"delete cluster C5"
"restore procedure usp_GetOrder to cluster C1"
"list trash"
"empty trash"
"show cluster summary"
"show cluster C1 details"
```

**Web UI Features**:
- **Two tabs**: Entities (qcat queries) and Clusters (cluster visualization)
- **Single prompt box**: Accepts both catalog queries and cluster commands
- **Entity memory**: Tracks recently mentioned entities for quick reference
- **Clickable entities**: Click any entity name to add it to prompt
- **Interactive diagrams**: Click clusters to see details, zoom in/out
- **Refresh button**: Reload clusters from file (file → memory → display)
- **Reset button**: Rebuild clusters from catalog.json (DESTRUCTIVE, with double confirmation)

## API Endpoints Reference

### Unified Command Endpoint
- `POST /api/command` - Unified command router (LLM classifies intent, routes to qcat or cluster)
  - Body: `{"command": "natural language query", "session_id": "optional"}`
  - Returns: `{"type": "qcat"|"cluster"|"error", "result": {...}}`

### Qcat Endpoints
- `POST /api/qcat/ask` - Direct qcat query (bypasses unified router)
  - Body: `{"prompt": "query", "k": 10, "schema": "dbo", "intent_override": "list_all_tables"}`
  - Returns: `{"answer": "...", "entities": [...], "session_id": "...", "memory": {...}}`
- `POST /api/qcat/clear_memory` - Clear session entity memory
  - Body: `{"session_id": "..."}`

### Cluster Endpoints
- `GET /api/cluster/summary` - Get cluster summary statistics
- `GET /api/cluster/{cluster_id}` - Get cluster details
- `GET /api/cluster/trash` - List trash contents
- `POST /api/cluster/trash/restore` - Restore item from trash
  - Body: `{"item_type": "procedure"|"table", "procedure_name": "...", "target_cluster_id": "...", "force_new_group": false}` (for procedure)
  - Body: `{"item_type": "table", "trash_index": 0}` (for table)
- `POST /api/cluster/trash/empty` - Empty trash permanently
- `GET /api/cluster/svg/summary` - Get summary diagram SVG
- `GET /api/cluster/svg/{cluster_id}` - Get cluster detail diagram SVG
- `POST /api/cluster/command` - Execute cluster command
  - Body: `{"command": "natural language"}` OR `{"action": "rename_cluster", "cluster_id": "C1", "new_name": "Orders"}`
- `POST /api/cluster/reload` - Reload clusters from snapshot file
- `POST /api/cluster/rebuild` - Rebuild clusters from catalog.json (DESTRUCTIVE)

### Health Check
- `GET /api/ping` - Health check, returns `{"ok": true, "backends": ["qcat", "cluster"]}`

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

### Embedding Backends (LEGACY - Not Used in Query Flow)

**Note**: Embedding generation still exists in the codebase but is **NOT used** for queries. All queries use LLM intent classification → deterministic catalog operations.

Two embedding modes (controlled by `USE_LMSTUDIO` env var):

**LM Studio** (default, `USE_LMSTUDIO=1`):
- Requires running LM Studio server with embedding model
- API: `POST {LMSTUDIO_BASE_URL}/embeddings`
- Model: Configurable via `EMBED_MODEL` env var

**Sentence Transformers** (fallback, `USE_LMSTUDIO=0`):
- Uses `sentence-transformers/all-MiniLM-L6-v2` (or `LOCAL_EMBED_MODEL` env var)
- No external server needed

**Why not used?**: The LLM intent classification + deterministic operations proved more reliable and transparent than vector similarity search. Embeddings infrastructure kept for potential future use.

### Data Assumptions
- Catalog format: Expects `catalog.json` with `Tables`, `Views`, `Procedures`, `Functions` top-level keys
- Schema format: Case-insensitive key access (handles both `Schema`/`schema`, `Safe_Name`/`safe_name`)
- Safe names: Uses `·` (middle dot) as schema separator internally (e.g., `dbo·Order`), converted to `.` (period) for file paths and exports
- Referenced_By: Used for reverse lookups (which procs/views access a table)

### SQL Comparison & Diffing (Text-Based, No Embeddings)

`qcat/ops.py::compare_sql()` produces:
- **Similarity score**: Weighted blend of edit distance (45%), token overlap (35%), structural similarity for tables (20%)
- **Unified diff**: Git-style diff with full context (for diff2html rendering)
- **Structural summary**: Column adds/removes/type changes for table comparisons

`qcat/ops.py::find_similar_sql()`:
- Finds entities with similar SQL using **text-based comparison** (not vector embeddings)
- Compares source entity against all entities of same kind
- Uses same `compute_similarity()` function as `compare_sql()`
- Returns entities above threshold (default 50%) sorted by similarity

Format normalization (`format_sql_for_diff`):
- Strips comments, normalizes whitespace
- Keywords on new lines (CREATE, SELECT, FROM, WHERE, JOIN, etc.)
- Parentheses indented (but protects numeric size specifiers like `(18)`, `(18, 4)`)

### Path Configuration
Centralized in `qcat/paths.py`:
```python
BASE = Path(__file__).resolve().parent.parent
ROOT_ABOVE = BASE.parent
OUTPUT_DIR = Path(os.getenv("SQL_OUTPUT_DIR") or (ROOT_ABOVE / "output")).resolve()
SQL_FILES_DIR = Path(os.getenv("SQL_FILES_DIR") or (ROOT_ABOVE / "sql_files")).resolve()

INDEX_DIR = OUTPUT_DIR / "vector_index"
CATALOG = OUTPUT_DIR / "catalog.json"
ITEMS_PATH = INDEX_DIR / "items.json"
EMB_PATH = INDEX_DIR / "embeddings.npy"
```

Override via environment variables:
- `SQL_OUTPUT_DIR` - Override output directory (default: `../output`)
- `SQL_FILES_DIR` - Override SQL files directory (default: `../sql_files`)

## Testing & Development

**Verify required files exist**:
```bash
# Ensure catalog.json exists
ls -l ../output/catalog.json

# Ensure items.json exists (pre-built from external tooling)
ls -l ../output/vector_index/items.json
```

**Test web server**:
```bash
# Start server
./runwebapp.sh

# In another terminal:
curl -X POST http://localhost:8000/api/command \
  -H "Content-Type: application/json" \
  -d '{"command": "list all tables"}'
```

## Known Edge Cases

### Catalog Query Edge Cases

1. **Name ambiguity**: If multiple entities have same base name in different schemas, use schema-qualified names (`dbo.Order`) or safe names (`dbo·Order`)

2. **Missing SQL exports**: System falls back to scanning `../sql_files/` recursively for CREATE/ALTER statements. Ensure your source DDL uses standard SQL Server syntax.

3. **Intent confidence**: Queries below 70% confidence trigger proposal mode (user must accept). Override via `intent_override` in API.

4. **Case sensitivity**: All name matching is case-insensitive, but display preserves original casing from catalog.

### Cluster Management Edge Cases

6. **LLM timeout errors**: If LM Studio times out during intent classification (>12s), the frontend displays error message with help text listing all available commands. This is graceful degradation - the system doesn't hang.

7. **Missing tables in diagrams**: Tables referenced by procedures but not in catalog.json appear as gray nodes labeled "missing-table". They ARE connected to their clusters in summary view (fixed in latest version). These indicate procedures referencing dropped/external tables.

8. **Global tables**: Tables accessed by procedures in >= 2 clusters appear in lighter color in diagrams. These are cross-cutting tables shared across multiple functional areas.

9. **State synchronization**: Memory and file (clusters.json) are ALWAYS synchronized because every operation calls `_save_snapshot()`. The only time they differ is if an external process modifies clusters.json. Use "Refresh" button to reload from file.

10. **Reset Clusters vs Refresh**:
    - **Refresh**: Reloads clusters.json from file → memory → display (non-destructive)
    - **Reset Clusters**: Rebuilds clusters.json from catalog.json (DESTRUCTIVE, requires double confirmation, loses all customizations)

11. **Trash restoration**: When restoring a procedure, you must specify target cluster. Use `force_new_group=True` if procedure's table access pattern no longer matches any existing groups.

12. **Empty clusters**: You can create empty clusters with "add cluster" command. They will appear in summary diagram even without procedures.

## Performance Notes

- **No vector search**: All queries are O(1) lookups or O(N) graph traversal on catalog.json (very fast)
- **LLM intent classification**: Adds ~500ms-2s latency (single LLM call per query)
- **find_similar_sql**: O(N) text comparison across all entities of same kind (acceptable for <1000 entities per type)
- **Diff formatting**: CPU-bound; very large procedures (>10K lines) may take 1-2s to format

## Web UI Architecture

### Unified Web Interface

Located in `static/`:
- **index.html**: Unified single-page app with two-tab interface
- **app.js**: Main application logic, tab switching, command execution
- **cluster.js**: Cluster visualization, diagram rendering, cluster management
- **diagram.js**: Diagram rendering utilities
- **markdown_diff.js**: Markdown rendering, syntax highlighting, SQL diff formatting

**Tab Structure**:
1. **Entities Tab** (qcat):
   - Semantic prompt box at top
   - Entity memory sidebar (tables, procedures, views, functions)
   - Content area for query results
   - Markdown rendering with syntax highlighting
   - SQL diff rendering (via diff2html)

2. **Clusters Tab** (cluster):
   - Same semantic prompt box (accepts cluster commands)
   - Cluster list sidebar (shows all clusters)
   - SVG diagram area (summary or detail view)
   - Interactive diagram controls (zoom, pan, click)
   - Refresh and Reset buttons

**State Management**:
- `lastContentState`: Preserves each tab's content when switching tabs
- `currentClusterId`: Tracks which cluster is being viewed in detail
- `sessionId`: Tracks qcat entity memory across queries
- `selectedEntity`: Tracks selected entity in diagrams

**Command Flow** (app.js):
```
User types command → executeCommand()
  ↓
POST /api/command with {"command": "...", "session_id": "..."}
  ↓
Backend classifies intent (qcat vs cluster)
  ↓
Response: {"type": "qcat"|"cluster"|"error", "result": {...}}
  ↓
Frontend routes to appropriate handler:
  - qcat: Render markdown, update entity memory, switch to Entities tab
  - cluster: Render markdown, reload cluster list, refresh diagram
  - error: Display error message with help text
```

**Clickable Entities Feature**:
- All entity names in query results are rendered as clickable elements
- Click any entity (table, view, procedure, function) to append it to the prompt box
- Entities are automatically wrapped in backticks (e.g., `` `dbo.Order_Trx` ``) to handle names with spaces
- JavaScript pattern matches: `schema.object`, `object`, `schema.object.column`
- Supports entity names containing spaces (e.g., `dbo.BO Client Cash`)
- Visual feedback: blue color with hover effect

**Implementation Details**:
- Formatters output entity names in markdown backticks: `` `dbo.TableName` ``
- Backticks render as `<code>` elements in HTML
- JavaScript `makeEntitiesClickable()` function:
  - Scans all `<code>` elements after markdown rendering
  - Tests against entity pattern regex (allows spaces, dots, underscores)
  - Converts matching elements to clickable spans with event handlers
  - Appends wrapped entity names to prompt textarea on click

**Interactive Diagrams** (cluster.js):
- SVG diagrams generated by Graphviz DOT on backend
- Frontend adds click handlers to nodes:
  - Click cluster node → load cluster detail view
  - Click procedure/group → show info (future enhancement)
  - Click table → show related procedures (future enhancement)
- Zoom controls: Fit, Zoom In, Zoom Out, Reset
- Diagram auto-updates after cluster operations

**Notification System**:
- Loading notifications (blue): "Processing query...", "Rebuilding clusters..."
- Success notifications (green): "Query completed", "Cluster renamed", auto-hide after 3s
- Error notifications (red): "Command failed", "LLM timeout", stay visible until dismissed

Static files served via FastAPI's `StaticFiles` at `/static` and root `/`.

## Troubleshooting Common Issues

### LLM-Related Issues

1. **LM Studio timeout errors (12s timeout)**:
   - **Symptom**: "LLM classification failed: Read timed out"
   - **Cause**: LM Studio inference taking >12 seconds
   - **Solution**: Use faster model, or bypass LLM with explicit flags
   - **Frontend**: Displays error message gracefully with help text

2. **Intent misclassification**:
   - **Symptom**: Wrong backend handles your command
   - **Example**: "which procedures access X" goes to cluster instead of qcat
   - **Solution**: Check webapp/llm_intent.py prompt, ensure intent examples are clear
   - **Workaround**: Use `/api/qcat/ask` endpoint directly with `intent_override`

### Cluster-Related Issues

3. **Missing tables not showing connections**:
   - **Fixed in latest version**: cluster/backend.py:1113-1122
   - Missing tables now properly connected to clusters in summary view

4. **"list all tables" showing no results**:
   - **Fixed in latest version**: webapp/agent.py:243-274
   - Now correctly calls qcat_ops.list_all_tables() instead of non-existent list_all_entities()

5. **Delete command hanging with "Processing query..." forever**:
   - **Fixed in latest version**: static/app.js:308-323
   - Now handles `{"type": "error"}` responses from backend

6. **Cluster operations not persisting**:
   - **Check**: Ensure _save_snapshot() is called after each operation
   - **Verify**: Check ../output/cluster/clusters.json modification timestamp
   - **Debug**: Enable debug logging in cluster/backend.py

### Index/Data Issues

7. **"Items not found" or "Index missing"**:
   - **Cause**: items.json not built or catalog.json missing
   - **Solution**: Build items.json using external tooling that processes your SQL Server catalog
   - **Check**: Verify ../output/vector_index/items.json exists

8. **Procedure not found in clusters**:
   - **Check trash**: Use "list trash" command
   - **Check catalog.json**: Ensure procedure exists there
   - **Rebuild**: Use "Reset Clusters" if all else fails

### Server Issues

9. **Port conflicts (port 8000 already in use)**:
    - **Check**: `lsof -i :8000` to see what's using the port
    - **Kill**: `kill -9 <PID>`
    - **Change port**: Edit webapp.py line 296 (NOT recommended, frontend expects 8000)

10. **Server not loading new routes**:
    - **Cause**: Server wasn't restarted after code changes
    - **Solution**: Stop server (Ctrl+C) and restart: `./runwebapp.sh`

11. **CORS errors in browser console**:
    - **Check**: FastAPI CORS middleware configured (webapp.py:28-34)
    - **Verify**: Browser DevTools → Network tab → Response headers include Access-Control-Allow-Origin

## Development Best Practices

### Adding New Cluster Operations

1. Implement pure, deterministic function in `cluster/ops.py`
2. Implement markdown renderer in `cluster/formatters.py`
3. Add intent to `cluster/intents.py`
4. Add dispatcher case in `webapp/agent.py` → `_execute_cluster_intent()`
5. Update LLM prompt in `webapp/llm_intent.py` with intent examples
6. **CRITICAL**: Ensure caller invokes `service._save_snapshot()` after operation

### Adding New Qcat Operations

1. Implement pure, deterministic function in `qcat/ops.py`
2. Implement markdown renderer in `qcat/formatters.py`
3. Add intent to `qcat/intents.py`
4. Add dispatcher case in `webapp/agent.py` → `_execute_qcat_intent()`
5. Update LLM prompt in `webapp/llm_intent.py` with intent examples

### Testing New Features

```bash
# Test catalog query
curl -X POST http://localhost:8000/api/command \
  -H "Content-Type: application/json" \
  -d '{"command": "list all tables"}'

# Test cluster operation
curl -X POST http://localhost:8000/api/command \
  -H "Content-Type: application/json" \
  -d '{"command": "show cluster summary"}'

# Test direct qcat endpoint
curl -X POST http://localhost:8000/api/qcat/ask \
  -H "Content-Type: application/json" \
  -d '{"prompt": "order tables", "k": 5, "intent_override": "list_all_tables"}'
```

### Debug Logging

- **Backend**: All operations print to console via `print()`
- **Frontend**: Check browser console (F12) for JavaScript logs
- **LLM calls**: webapp/llm_intent.py logs classified intents
- **Cluster ops**: cluster/backend.py logs state changes

## Project Maintenance

### Starting from Scratch

```bash
# 1. Ensure items.json exists (pre-built from external tooling)
ls -l ../output/vector_index/items.json

# 2. Build initial clusters (optional, auto-created on first use)
python -m cluster.clustering

# 3. Start server
./runwebapp.sh

# 4. Access unified UI
open http://localhost:8000
```

### Backup Important Data

- `../output/cluster/clusters.json` - Cluster state with customizations (CRITICAL - contains user edits!)
- `../output/catalog.json` - Source catalog metadata (critical, generated externally)
- `../output/vector_index/items.json` - Flattened catalog (can rebuild from catalog.json)
- `../output/vector_index/embeddings.npy` - Legacy embeddings (not used, can rebuild)

### Version Control

- **DO commit**: Source code, CLAUDE.md, package requirements
- **DON'T commit**: ../output/ directory (too large, generated data)
- **CONSIDER committing**: ../output/cluster/clusters.json IF you want to preserve cluster customizations
- **DON'T commit**: ../output/vector_index/ (generated, can rebuild)

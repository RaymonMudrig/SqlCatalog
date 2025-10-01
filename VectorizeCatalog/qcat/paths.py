from __future__ import annotations
from pathlib import Path
import os

# Directory layout:
#   .../SqlCatalog/SqlCatalog/
#       ├─ VectorizeCatalog/        <-- this file is in VectorizeCatalog/qcat/
#       ├─ output/                  <-- read/write here
#       └─ sql_files/               <-- raw SQL files live here
#
# BASE           -> .../SqlCatalog/SqlCatalog/VectorizeCatalog
# ROOT_ABOVE     -> .../SqlCatalog/SqlCatalog
BASE = Path(__file__).resolve().parent.parent
ROOT_ABOVE = BASE.parent

# Allow overrides via env vars if you ever need them
OUTPUT_DIR = Path(os.getenv("SQL_OUTPUT_DIR") or (ROOT_ABOVE / "output")).resolve()
SQL_FILES_DIR = Path(os.getenv("SQL_FILES_DIR") or (ROOT_ABOVE / "sql_files")).resolve()

INDEX_DIR  = OUTPUT_DIR / "vector_index"
CATALOG    = OUTPUT_DIR / "catalog.json"

ITEMS_PATH = INDEX_DIR / "items.json"
EMB_PATH   = INDEX_DIR / "embeddings.npy"

def ensure_dirs() -> None:
    """Create expected directories if they don't exist."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    INDEX_DIR.mkdir(parents=True, exist_ok=True)
    for sub in ("tables", "views", "procedures", "functions"):
        (OUTPUT_DIR / "sql_exports" / sub).mkdir(parents=True, exist_ok=True)

# Locations used by this tool
CATALOG_JSON = Path(os.getenv("CATALOG_JSON", str(OUTPUT_DIR / "catalog.json"))).resolve()
ITEMS_JSON   = Path(os.getenv("ITEMS_JSON",   str(BASE / "items.json"))).resolve()

# Exported SQL paths (produced by the .NET exporter)
SQL_EXPORTS_DIR = OUTPUT_DIR / "sql_exports"
SQL_EXPORTS_TABLES     = SQL_EXPORTS_DIR / "tables"
SQL_EXPORTS_PROCEDURES = SQL_EXPORTS_DIR / "procedures"
SQL_EXPORTS_VIEWS      = SQL_EXPORTS_DIR / "views"
SQL_EXPORTS_FUNCTIONS  = SQL_EXPORTS_DIR / "functions"

from __future__ import annotations
import argparse

def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    # Make query optional so --list-names can be used without it
    ap.add_argument("query", nargs="?", default=None,
                    help="For --sql-of, this is the entity name (e.g., dbo.RT_Order). Otherwise, the search/query text.")

    ap.add_argument("--k", type=int, default=12)
    ap.add_argument("--kind", choices=["any","table","view","procedure","function","column"], default="any")
    ap.add_argument("--schema")
    ap.add_argument("--unused-only", action="store_true")
    ap.add_argument("--show-sql", action="store_true")
    ap.add_argument("--answer", action="store_true")
    ap.add_argument("--answer-top", type=int, default=8)
    ap.add_argument("--name-match", choices=["smart","exact","word","substring"], default="smart")
    ap.add_argument("--table", help="Force the target table (e.g., 'Order' or 'dbo.Order')")
    ap.add_argument("--include-via-views", action="store_true", help="Include procs that read views which read the table")
    ap.add_argument("--server", help="Base URL of running FastAPI app, e.g. http://127.0.0.1:8000")

    # SQL printing + discovery
    ap.add_argument("--sql-of", action="store_true", help="Print SQL of the named entity (use with --kind table|view|procedure|function)")
    ap.add_argument("--full", action="store_true", help="Print full SQL instead of a head")
    ap.add_argument("--head", type=int, default=120, help="How many lines to print when not --full")
    ap.add_argument("--fuzzy", action="store_true", help="Allow fuzzy (non-exact) name matching for --sql-of")
    ap.add_argument("--list-names", action="store_true", help="List entity names by regex pattern (use with --kind and --pattern)")
    ap.add_argument("--pattern", help="Regex used with --list-names (matches safe_name or schema.name)")
    return ap

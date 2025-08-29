from __future__ import annotations
import re
from typing import Dict, Any, List

from qcat.loader import load_items, load_emb
from qcat.name_match import detect_kind
from qcat.relations import procs_accessing_table
from qcat.search import semantic_search

from qcli.printers import print_item, print_sql_blob
from qcli.resolver import resolve_items_by_name, list_names, _split_qualified

def via_local(args) -> List[Dict[str, Any]]:
    items = load_items()

    # Direct SQL print
    if args.sql_of:
        if args.kind == "any":
            print("--sql-of requires --kind table|view|procedure|function")
            return []
        # Strict first (with auto-unique-base fallback), then optional fuzzy
        matches = resolve_items_by_name(items, args.kind, args.query, strict=True)
        if not matches and args.fuzzy:
            matches = resolve_items_by_name(items, args.kind, args.query, strict=False)
        if not matches:
            _schema, _base = _split_qualified(args.query)
            needle = _base or args.query
            print("No exact match. Suggestions (safe_name):")
            sugg = list_names(items, args.kind, re.escape(needle))
            for s, d in sugg[:30]:
                print(f"  {d}    ({s})")
            print("Tip: use --list-names --kind", args.kind, f"--pattern '{re.escape(needle)}'")
            return []
        for it in matches:
            print_sql_blob(it, head=None if args.full else args.head, full=args.full)
        return []

    # List names (discovery)
    if args.list_names:
        hits = list_names(items, args.kind, args.pattern)
        if not hits:
            print("No names matched.")
        else:
            for safe, disp in hits:
                print(f"{disp}    ({safe})")
        return []

    # Relation (with dynamic SQL detection enabled inside procs_accessing_table)
    emb = load_emb()
    handled, picked, sections = procs_accessing_table(
        args.query, items,
        name_mode=args.name_match,
        forced_table=args.table,
        include_via_views=args.include_via_views,
        include_dynamic=True,
    )
    if handled:
        for sec in sections:
            print(f"\n=== {sec['title']} ===")
            if not sec["results"]:
                print("  (no procedures found)")
            for r in sec["results"]:
                it, how = r["item"], r["access"]
                print_item(it, None, show_sql=args.show_sql)
                print(f"    ACCESS: {how}")
        return picked

    # Semantic fallback
    auto_kind = detect_kind(args.query)
    kind = args.kind if args.kind != "any" else (auto_kind or "any")
    picked = semantic_search(args.query, items, emb, k=args.k, kind=kind,
                             schema=args.schema, unused_only=args.unused_only)
    if not picked:
        print("No items match the given filters.")
        return []
    for it in picked:
        print_item(it, it.get("_score"), show_sql=args.show_sql)
    return picked

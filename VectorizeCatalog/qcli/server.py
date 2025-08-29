from __future__ import annotations
import sys
from typing import Dict, Any, List, Tuple

from qcli.printers import print_item, print_sql_blob

def via_server(args) -> Tuple[bool, List[Dict[str, Any]]]:
    """
    Talks to the running FastAPI server when --server is provided.
    Returns (handled, picked_items_for_answer).
    """
    try:
        import requests
    except Exception as e:
        print(f"[remote error] requests not available: {e}", file=sys.stderr)
        sys.exit(2)

    base = args.server.rstrip("/")

    # Direct SQL print (optional endpoint)
    if args.sql_of:
        try:
            r = requests.post(
                f"{base}/api/sql_of",
                json={"kind": args.kind, "name": args.query, "head": None if args.full else args.head},
                timeout=60,
            )
            if r.status_code == 404:
                print("No matching entity found.")
                return True, []
            if r.status_code in (404, 405):
                pass  # endpoint not implemented — fall through to other endpoints
            r.raise_for_status()
            data = r.json()
            matches = data.get("matches", [])
            for it in matches:
                print_sql_blob(it, head=None if args.full else args.head, full=args.full)
            return True, []
        except Exception as e:
            print(f"[remote /api/sql_of] {e}", file=sys.stderr)

    # Relation endpoint first (same as web UI button)
    try:
        body = {
            "query": args.query,
            "name_mode": args.name_match,
            "forced_table": args.table,
            "include_via_views": args.include_via_views,
        }
        r = requests.post(f"{base}/api/procs_access_table", json=body, timeout=120)
        if r.status_code == 200:
            data = r.json()
            sections = data.get("sections") or []
            handled = bool(sections)
            picked = []
            if handled:
                for sec in sections:
                    print(f"\n=== {sec['title']} ===")
                    res = sec.get("results") or []
                    if not res:
                        print("  (no procedures found)")
                    for row in res:
                        it, how = row["item"], row["access"]
                        print_item(it, None, show_sql=args.show_sql)
                        print(f"    ACCESS: {how}")
                        picked.append(it)
                return True, picked
    except Exception as e:
        print(f"[remote /api/procs_access_table] {e}", file=sys.stderr)

    # Semantic endpoint
    try:
        sbody = {
            "query": args.query,
            "k": args.k,
            "kind": args.kind,
            "schema": args.schema,
            "unused_only": args.unused_only,
        }
        r = requests.post(f"{base}/api/semantic", json=sbody, timeout=120)
        r.raise_for_status()
        pdata = r.json()
        picked = pdata.get("picked") or []
        if not picked:
            print("No items match the given filters.")
            return True, []
        for it in picked:
            print_item(it, it.get("_score"), show_sql=args.show_sql)
        return True, picked
    except Exception as e:
        print(f"[remote /api/semantic] {e}", file=sys.stderr)
        return True, []

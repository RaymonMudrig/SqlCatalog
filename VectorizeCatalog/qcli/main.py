from __future__ import annotations
import sys

from qcli.args import build_parser
from qcli.server import via_server
from qcli.local import via_local
from qcat.llm import llm_answer

def main():
    ap = build_parser()
    args = ap.parse_args()

    # --list-names doesn't need a query
    if args.list_names:
        _ = via_local(args)
        return

    # Everything else needs a query text
    if not args.query:
        ap.print_usage(sys.stderr)
        print("error: the positional 'query' is required unless using --list-names", file=sys.stderr)
        sys.exit(2)

    # Server / local execution
    if args.server:
        handled, picked = via_server(args)
        if args.answer and picked:
            ans = llm_answer(args.query, picked[:max(1, min(args.answer_top, 8))])
            if ans:
                print("\n[LLM answer]")
                print(ans)
        return

    picked = via_local(args)
    if args.answer and picked:
        ans = llm_answer(args.query, picked[:max(1, min(args.answer_top, 8))])
        if ans:
            print("\n[LLM answer]")
            print(ans)

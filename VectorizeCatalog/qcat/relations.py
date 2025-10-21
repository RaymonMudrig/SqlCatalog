from typing import List, Dict, Any, Tuple, Optional

try:
    from .loader import load_catalog
    from .name_match import (
        split_safe,
        extract_quoted_names,
        choose_table_candidates,
        all_tables_matching_hints,
        choose_proc_candidates,
    )
    from .dynamic_sql import proc_hits_table
except ImportError:
    from loader import load_catalog
    from name_match import (
        split_safe,
        extract_quoted_names,
        choose_table_candidates,
        all_tables_matching_hints,
        choose_proc_candidates,
    )
    from dynamic_sql import proc_hits_table

def _refs_contains_table(lst, tbl_safe: str) -> bool:
    for r in lst:
        rsafe = r.get("Safe_Name")
        if rsafe and rsafe.lower() == tbl_safe.lower():
            return True
        rsch = r.get("Schema") or r.get("schema") or ""
        rnm  = r.get("Name") or r.get("name") or ""
        combo = (rsch + "·" + rnm) if rsch else rnm
        if combo.lower() == tbl_safe.lower():
            return True
    return False

def procs_accessing_table(
    query: str,
    items: List[Dict[str, Any]],
    *,
    name_mode: str = "smart",
    forced_table: Optional[str] = None,
    include_via_views: bool = False,
    include_dynamic: bool = True,
) -> Tuple[bool, List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns (handled, picked_procedure_items, sections)
    sections: list of {title, results: [{item, access}]}
    """
    catalog = load_catalog()
    procs  = catalog.get("Procedures") or catalog.get("procedures") or {}
    views  = catalog.get("Views") or catalog.get("views") or {}

    proc_item_by_safe = {it.get("safe_name"): it for it in items if it.get("kind") == "procedure"}
    table_items = [it for it in items if it.get("kind") == "table"]

    hints = [forced_table] if forced_table else extract_quoted_names(query)
    if forced_table and "·" in forced_table:
        candidates = [forced_table]
    else:
        candidates = choose_table_candidates(query, items, k=8, name_mode=name_mode)
        candidates = list(dict.fromkeys(candidates + all_tables_matching_hints(hints, items, name_mode)))

    if not candidates:
        return False, [], []

    # Views that read each table
    view_reads_map: Dict[str, List[str]] = {}
    if include_via_views:
        for vsafe, vobj in views.items():
            reads = vobj.get("Reads") or vobj.get("reads") or []
            for r in reads:
                rsafe = r.get("Safe_Name")
                if not rsafe:
                    s = r.get("Schema") or r.get("schema") or ""
                    n = r.get("Name") or r.get("name") or ""
                    rsafe = (s + "·" + n) if s else n
                if rsafe:
                    view_reads_map.setdefault(rsafe, []).append(vsafe)

    sections = []
    picked: List[Dict[str, Any]] = []

    for tbl_safe in candidates:
        schema, base = split_safe(tbl_safe)
        disp = f"{schema+'.' if schema else ''}{base}"
        title = f"Procedures that access table {disp} (safe: {tbl_safe})"
        if include_via_views:
            title += " [including via views]"
        if include_dynamic:
            title += " [dynamic-sql scan enabled]"

        via_views = set(view_reads_map.get(tbl_safe, [])) if include_via_views else set()
        results = []

        for psafe, pobj in procs.items():
            reads  = pobj.get("Reads") or pobj.get("reads") or []
            writes = pobj.get("Writes") or pobj.get("writes") or []
            state = []
            if _refs_contains_table(reads, tbl_safe):  state.append("READ")
            if _refs_contains_table(writes, tbl_safe): state.append("WRITE")

            if not state and include_via_views and reads:
                for r in reads:
                    rsafe = r.get("Safe_Name")
                    if not rsafe:
                        s = r.get("Schema") or r.get("schema") or ""
                        n = r.get("Name") or r.get("name") or ""
                        rsafe = (s + "·" + n) if s else n
                    if rsafe and rsafe in via_views:
                        state.append("READ(via view)")
                        break

            dyn_flag = False
            if not state and include_dynamic:
                it = proc_item_by_safe.get(psafe)
                if it and proc_hits_table(it, tbl_safe):
                    state.append("READ(dynamic)")
                    dyn_flag = True

            if state:
                it = proc_item_by_safe.get(psafe)
                if it:
                    results.append({"item": it, "access": "/".join(state), "dynamic": dyn_flag})

        results.sort(key=lambda r: (0 if "WRITE" in r["access"] else 1, (r["item"].get("safe_name") or "").lower()))
        sections.append({"title": title, "results": results})
        for r in results:
            picked.append(r["item"])

    # dedupe picked
    seen=set(); picked=[it for it in picked if not (it.get('id') in seen or seen.add(it.get('id')))]
    return True, picked, sections

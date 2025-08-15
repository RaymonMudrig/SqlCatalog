# detect_unaccessed_tables.py
import json, sys

CATALOG_PATH = "./output/catalog.json"

def load_catalog(path=CATALOG_PATH):
    with open(path, encoding="utf-8-sig") as f:
        root = json.load(f)
    return root.get("Catalog") or root  # supports wrapped export

def safe_name_of(obj_name: str) -> str:
    # our keys for tables are already the "safe" keys in Catalog.Tables (e.g., spaces -> ·)
    return obj_name

def main(schema_filter=None):
    cat = load_catalog()
    tables = cat.get("Tables") or cat.get("tables") or {}
    procs  = cat.get("Procedures") or cat.get("procedures") or {}

    # all table keys (safe keys), optionally filtered by schema
    all_tables = {
        k for k, t in tables.items()
        if schema_filter is None or (t.get("Schema") or t.get("schema") or "dbo").lower() == schema_filter.lower()
    }

    # gather every table name referenced by any procedure (reads or writes)
    referenced = set()
    for p in procs.values():
        for coll_name in ("Reads","reads","Writes","writes"):
            for ref in p.get(coll_name, []) or []:
                ref_schema = (ref.get("schema") or ref.get("Schema") or "dbo")
                ref_name   = ref.get("name") or ref.get("Name")
                if ref_name:
                    # Our catalog stores tables by safe key; functions/views keep plain names.
                    # For tables, the safe key is the original table's safe_name. Try both:
                    safe_key_guess = ref_name.replace(" ", "·")
                    if safe_key_guess in tables:
                        if schema_filter is None or ref_schema.lower() == schema_filter.lower():
                            referenced.add(safe_key_guess)

    unaccessed = sorted(all_tables - referenced, key=str.lower)
    print(f"Unaccessed tables{f' in schema {schema_filter}' if schema_filter else ''}: {len(unaccessed)}")
    for t in unaccessed:
        info = tables[t]
        schema = info.get("Schema") or info.get("schema") or "dbo"
        original = info.get("Original_Name") or info.get("original_name") or t
        print(f"- {schema}.{original}  (safe: {t})")

if __name__ == "__main__":
    # Usage:
    #   python detect_unaccessed_tables.py
    #   python detect_unaccessed_tables.py dbo
    schema = sys.argv[1] if len(sys.argv) > 1 else None
    main(schema_filter=schema)

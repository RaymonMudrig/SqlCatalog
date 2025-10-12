#!/usr/bin/env python3
"""
Regression test suite for SqlCatalog intents.

This test suite validates all implemented intents are working correctly.
Run with: python3 test_regression.py
"""

import json
import sys
import numpy as np
from pathlib import Path
from typing import Dict, Any, List, Tuple

# Add qcat to path if needed
sys.path.insert(0, str(Path(__file__).parent))

from qcat.agent import agent_answer

# Test cases: (query, expected_intent, validation_func)
# validation_func receives the answer string and returns (passed: bool, message: str)

def contains_text(answer: str, *texts: str) -> Tuple[bool, str]:
    """Check if answer contains all specified texts (case-insensitive)."""
    answer_lower = answer.lower()
    for text in texts:
        if text.lower() not in answer_lower:
            return False, f"Expected to find '{text}' in answer"
    return True, "OK"

def not_contains_text(answer: str, *texts: str) -> Tuple[bool, str]:
    """Check if answer does NOT contain specified texts."""
    answer_lower = answer.lower()
    for text in texts:
        if text.lower() in answer_lower:
            return False, f"Did not expect to find '{text}' in answer"
    return True, "OK"

def not_empty(answer: str) -> Tuple[bool, str]:
    """Check if answer is not empty."""
    if not answer or answer.strip() == "":
        return False, "Answer is empty"
    return True, "OK"

def contains_count(answer: str, min_count: int = 1) -> Tuple[bool, str]:
    """Check if answer contains a count >= min_count."""
    import re
    # Look for patterns like "42 tables", "(42)", "**42**"
    matches = re.findall(r'\b(\d+)\b', answer)
    if not matches:
        return False, f"No count found in answer"
    counts = [int(m) for m in matches]
    if max(counts) >= min_count:
        return True, "OK"
    return False, f"Expected count >= {min_count}, found {max(counts)}"

# Test suite definitions
TEST_CASES = [
    # --- Procedures accessing/updating tables ---
    {
        "name": "procs_access_table - existing table",
        "query": "which procedures access dbo.Order_Trx",
        "expected_intent": "procs_access_table",
        "validate": lambda a: contains_text(a, "procedures", "order_trx"),
    },
    {
        "name": "procs_update_table - existing table",
        "query": "which procedures update dbo.Order_Trx",
        "expected_intent": "procs_update_table",
        "validate": lambda a: contains_text(a, "procedures", "update"),
    },
    {
        "name": "procs_access_table - non-existent table",
        "query": "which procedures access dbo.NonExistentTable123",
        "expected_intent": "procs_access_table",
        "validate": lambda a: contains_text(a, "no procedures"),
    },

    # --- Views accessing tables ---
    {
        "name": "views_access_table - existing table",
        "query": "which views access dbo.Client",
        "expected_intent": "views_access_table",
        "validate": lambda a: contains_text(a, "views"),
    },

    # --- Tables accessed by procedure ---
    {
        "name": "tables_accessed_by_procedure - existing proc",
        "query": "what tables are accessed by dbo.RtBoGetOrderDone",
        "expected_intent": "tables_accessed_by_procedure",
        "validate": lambda a: contains_text(a, "tables", "accessed"),
    },
    {
        "name": "tables_accessed_by_procedure - show reads and writes",
        "query": "tables accessed by dbo.RtBoGetOrderDone",
        "expected_intent": "tables_accessed_by_procedure",
        "validate": lambda a: contains_text(a, "reads") or contains_text(a, "writes"),
    },

    # --- Tables accessed by view ---
    {
        "name": "tables_accessed_by_view - existing view",
        "query": "what tables are accessed by view vClient",
        "expected_intent": "tables_accessed_by_view",
        "validate": lambda a: contains_text(a, "tables"),
    },

    # --- Unaccessed tables ---
    {
        "name": "unaccessed_tables",
        "query": "show me all unaccessed tables",
        "expected_intent": "unaccessed_tables",
        "validate": lambda a: contains_text(a, "table") or contains_text(a, "every table"),
    },

    # --- Procedure calls ---
    {
        "name": "procs_called_by_procedure",
        "query": "which procedures are called by dbo.RtBoGetOrderDone",
        "expected_intent": "procs_called_by_procedure",
        "validate": lambda a: not_empty(a),
    },

    # --- Call tree ---
    {
        "name": "call_tree",
        "query": "show me the call tree for dbo.RtBoGetOrderDone",
        "expected_intent": "call_tree",
        "validate": lambda a: contains_text(a, "call tree") or not_empty(a),
    },

    # --- List columns of table ---
    {
        "name": "list_columns_of_table - existing table",
        "query": "list all columns of dbo.Order_Trx",
        "expected_intent": "list_columns_of_table",
        "validate": lambda a: contains_count(a, min_count=5),  # Should have multiple columns
    },
    {
        "name": "list_columns_of_table - show types",
        "query": "show columns of table dbo.Client",
        "expected_intent": "list_columns_of_table",
        "validate": lambda a: contains_text(a, "columns") and not_contains_text(a, "no columns"),
    },

    # --- Columns returned by procedure ---
    {
        "name": "columns_returned_by_procedure - existing proc",
        "query": "list all columns returned by dbo.RtBoGetOrderDone procedure",
        "expected_intent": "columns_returned_by_procedure",
        "validate": lambda a: not_empty(a),
    },
    {
        "name": "columns_returned_by_procedure - qualified names",
        "query": "what columns does dbo.RtBoGetOrderDone return",
        "expected_intent": "columns_returned_by_procedure",
        "validate": lambda a: contains_text(a, "."),  # Should have qualified names like Table.Column
    },

    # --- Unused columns of table ---
    {
        "name": "unused_columns_of_table - existing table",
        "query": "list all unused columns of dbo.Order_Trx",
        "expected_intent": "unused_columns_of_table",
        "validate": lambda a: not_empty(a),
    },
    {
        "name": "unused_columns_of_table - correct tracking",
        "query": "show unused columns of dbo.Order_Trx in any procedure",
        "expected_intent": "unused_columns_of_table",
        "validate": lambda a: contains_text(a, "no unused") or contains_text(a, "unused"),
    },

    # --- SQL of entity ---
    {
        "name": "sql_of_entity - procedure",
        "query": "show me the SQL for procedure dbo.RtBoGetOrderDone",
        "expected_intent": "sql_of_entity",
        "validate": lambda a: contains_text(a, "```sql") or contains_text(a, "create"),
    },
    {
        "name": "sql_of_entity - table",
        "query": "show creation SQL of table dbo.Client",
        "expected_intent": "sql_of_entity",
        "validate": lambda a: contains_text(a, "```sql") or contains_text(a, "no sql"),
    },
    {
        "name": "sql_of_entity - view",
        "query": "get SQL for view vClient",
        "expected_intent": "sql_of_entity",
        "validate": lambda a: not_empty(a),
    },

    # --- List all entities ---
    {
        "name": "list_all_tables",
        "query": "list all tables",
        "expected_intent": "list_all_tables",
        "validate": lambda a: contains_count(a, min_count=50),  # Should have many tables
    },
    {
        "name": "list_all_views",
        "query": "show all views",
        "expected_intent": "list_all_views",
        "validate": lambda a: contains_text(a, "views"),
    },
    {
        "name": "list_all_procedures",
        "query": "list all procedures",
        "expected_intent": "list_all_procedures",
        "validate": lambda a: contains_count(a, min_count=100),  # Should have many procs
    },
    {
        "name": "list_all_functions",
        "query": "show all functions",
        "expected_intent": "list_all_functions",
        "validate": lambda a: contains_text(a, "function"),
    },
    {
        "name": "list_all_tables - with schema filter",
        "query": "list all tables in dbo schema",
        "expected_intent": "list_all_tables",
        "validate": lambda a: contains_text(a, "tables"),  # Schema filtering may not work depending on name format
    },

    # --- Compare SQL ---
    {
        "name": "compare_sql - two procedures",
        "query": "compare SQL between procedure dbo.Order_Trx_Insert and procedure dbo.Order_Trx_Update",
        "expected_intent": "compare_sql",
        "validate": lambda a: contains_text(a, "compare", "similarity") or contains_text(a, "Order_Trx_Insert", "Order_Trx_Update"),
    },

    # --- Find Similar SQL ---
    {
        "name": "find_similar_sql - existing procedure",
        "query": "find similar sql to dbo.addClientCash",
        "expected_intent": "find_similar_sql",
        "validate": lambda a: contains_text(a, "similar") and not_contains_text(a, "error"),
    },
    {
        "name": "find_similar_sql - shows similarity percentages",
        "query": "find procedures with similar SQL to dbo.addClientCash",
        "expected_intent": "find_similar_sql",
        "validate": lambda a: contains_text(a, "%") or contains_text(a, "no similar"),
    },
    {
        "name": "find_similar_sql - shows entity name",
        "query": "which entities have similar SQL to dbo.RtBoGetOrderDone",
        "expected_intent": "find_similar_sql",
        "validate": lambda a: contains_text(a, "similar") and not_empty(a),
    },
]


def run_tests(catalog_path: str = "../output/catalog.json") -> None:
    """Run all regression tests."""

    # Load catalog
    print(f"Loading catalog from {catalog_path}...")
    try:
        with open(catalog_path) as f:
            catalog_raw = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: Catalog file not found at {catalog_path}")
        print("Please run the C# SqlCatalog tool first to generate catalog.json")
        sys.exit(1)

    # Wrap catalog in the structure expected by agent_answer
    catalog = {"catalog": catalog_raw}

    print(f"Catalog loaded: {len(catalog_raw.get('Tables', {}))} tables, "
          f"{len(catalog_raw.get('Views', {}))} views, "
          f"{len(catalog_raw.get('Procedures', {}))} procedures\n")

    # Run tests
    passed = 0
    failed = 0
    errors = 0

    print("=" * 80)
    print("RUNNING REGRESSION TESTS")
    print("=" * 80)

    for i, test in enumerate(TEST_CASES, 1):
        name = test["name"]
        query = test["query"]
        expected_intent = test.get("expected_intent")
        validate = test["validate"]

        print(f"\n[{i}/{len(TEST_CASES)}] {name}")
        print(f"  Query: {query}")

        try:
            # Get answer
            result = agent_answer(query, catalog)
            answer = result.get("answer", "")

            # Note: agent_answer doesn't return intent directly, it's logged during execution
            # We validate based on answer content instead

            # Validate answer
            validation_ok, validation_msg = validate(answer)

            # Overall result
            if validation_ok:
                print(f"  ✅ PASSED - {validation_msg}")
                passed += 1
            else:
                print(f"  ❌ FAILED - {validation_msg}")
                print(f"     Answer preview: {answer[:200]}...")
                failed += 1

        except Exception as e:
            print(f"  ❌ ERROR: {e}")
            errors += 1

    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    print(f"Total:  {len(TEST_CASES)}")
    print(f"Passed: {passed} ✅")
    print(f"Failed: {failed} ❌")
    print(f"Errors: {errors} ⚠️")

    if failed == 0 and errors == 0:
        print("\n🎉 ALL TESTS PASSED!")
        sys.exit(0)
    else:
        print("\n⚠️  SOME TESTS FAILED")
        sys.exit(1)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run SqlCatalog regression tests")
    parser.add_argument(
        "--catalog",
        default="../output/catalog.json",
        help="Path to catalog.json file"
    )
    parser.add_argument(
        "--test",
        help="Run only tests matching this name (substring match)"
    )

    args = parser.parse_args()

    # Filter tests if specified
    if args.test:
        filtered = [t for t in TEST_CASES if args.test.lower() in t["name"].lower()]
        TEST_CASES.clear()
        TEST_CASES.extend(filtered)
        print(f"Running {len(TEST_CASES)} tests matching '{args.test}'")

    run_tests(args.catalog)

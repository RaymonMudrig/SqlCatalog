# Restore Procedure Fix - Bug Fix Summary

## Problem

When trying to restore a procedure from trash, the operation failed with "Failed to restore procedure" error.

## Root Cause

**Location**: `cluster/backend.py` - `restore_procedure()` method (line 629)

The issue was in how the procedure's table dependencies were retrieved during restoration:

### Original (Buggy) Code:
```python
# Get procedure's tables
procedure_tables = set(trash_group.tables)  # ❌ WRONG - trash groups have empty tables!
```

### Why This Failed:

When a procedure is deleted (line 485), the trash group is created with **intentionally empty tables**:

```python
trash_group = ProcedureGroup(
    group_id=trash_group_id,
    cluster_id="trash",
    procedures=[procedure_name],
    tables=[],  # ← Empty! Trash procedures have no table connections
    is_singleton=True,
    display_name=procedure_name,
)
```

**Design rationale**: Trash procedures show as "disconnected nodes" in diagrams (no edges to tables).

However, the table information WAS being preserved - in the `trash` metadata list:

```python
trash_metadata = {
    "procedure_name": procedure_name,
    "original_group_id": original_group_id,
    "original_cluster_id": original_cluster_id,
    "tables": procedure_tables,  # ← Stored here!
}

trash_item = TrashItem(
    item_type="procedure",
    item_id=procedure_name,
    data=trash_metadata,  # ← Contains the tables!
    deleted_at=datetime.now(timezone.utc).isoformat(),
)
self.trash.append(trash_item)
```

So the bug was: **restore_procedure tried to read tables from trash_group.tables (empty), instead of from trash metadata (where they were actually stored).**

## Solution

### Fix #1: Read Tables from Trash Metadata

**File**: `cluster/backend.py`
**Line**: 628-643

```python
# Get procedure's tables from trash metadata (trash groups have empty tables)
# Find the trash metadata for this procedure
trash_metadata = None
for item in self.trash:
    if item.item_type == "procedure" and item.item_id == procedure_name:
        trash_metadata = item.data
        break

if not trash_metadata:
    raise ValueError(
        f"Cannot restore procedure '{procedure_name}': "
        f"Trash metadata not found. Procedure may have been permanently deleted."
    )

# Get table list from metadata (trash groups have empty tables list)
procedure_tables = set(trash_metadata.get("tables", []))
```

### Fix #2: Clean Up Trash Metadata After Restore

**File**: `cluster/backend.py`
**Line**: 740-744

```python
# Remove procedure from trash metadata list
self.trash = [
    item for item in self.trash
    if not (item.item_type == "procedure" and item.item_id == procedure_name)
]
```

This ensures the trash metadata is properly cleaned up after successful restoration.

## Test Results

Created `test_restore_procedure_fix.py` to verify the fix:

```
1. Initial State:
   ✓ Procedure 'usp_GetOrders' in cluster C1
   ✓ Tables: ['dbo.Orders', 'dbo.Customers']

2. Delete Procedure:
   ✓ Moved to trash
   ✓ Tables stored in trash metadata

3. Restore Procedure:
   ✓ SUCCESS!
   ✓ Tables restored: ['dbo.Customers', 'dbo.Orders']
   ✓ Procedure back in cluster C1
   ✓ Trash metadata cleaned up
```

## Impact

**Before Fix**:
- ❌ Restore procedure always failed
- ❌ Tables lost during restoration
- ❌ Error: "Failed to restore procedure"

**After Fix**:
- ✅ Restore procedure works correctly
- ✅ Table dependencies preserved
- ✅ Trash metadata properly managed
- ✅ Clean error message if metadata missing

## Files Modified

1. **cluster/backend.py**:
   - Lines 628-643: Read tables from trash metadata instead of trash_group.tables
   - Lines 740-744: Clean up trash metadata after successful restore

2. **test_restore_procedure_fix.py** (new):
   - Comprehensive test for restore functionality
   - Verifies tables are restored correctly
   - Verifies trash cleanup

## Related Code

The restore_procedure workflow now correctly:

1. **Find procedure in trash** ✅
2. **Get tables from trash metadata** ✅ (FIXED)
3. **Auto-group logic**:
   - Find groups with matching tables (100% similarity)
   - Join existing group OR create new singleton
4. **Un-orphan tables** ✅
5. **Clean up trash metadata** ✅ (FIXED)
6. **Rebuild indexes** ✅

## Backward Compatibility

- ✅ No API changes
- ✅ No data migration needed
- ✅ Existing clusters.json files work as-is
- ✅ Fix is transparent to users

## Future Enhancements

Possible improvements:
1. **Validate trash metadata** on startup (detect corruption)
2. **Add trash metadata to list_trash()** output (show tables for procedures)
3. **Restore to original cluster** button (use original_cluster_id from metadata)
4. **Bulk restore** operation (restore multiple procedures at once)

## Conclusion

The restore_procedure bug is now fixed. Procedures can be successfully restored from trash with their original table dependencies intact.

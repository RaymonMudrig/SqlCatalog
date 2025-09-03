using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SqlCatalog;

internal sealed class TableVisitor(Catalog catalog) : TSqlFragmentVisitor
{
    private readonly Catalog _catalog = catalog;
    
    private readonly HashSet<string> _idxSeen = []; // table|indexName

    public override void ExplicitVisit(CreateTableStatement node)
    {
        var (schema, name, _) = Helpers.NameOf(node.SchemaObjectName);
        var safe = Helpers.SafeKey(name);

        if (!_catalog.Tables.TryGetValue(safe, out var t))
        {
            t = new TableInfo { Schema = schema, Original_Name = name, Safe_Name = safe };
            _catalog.Tables[safe] = t;
        }

        var def = node.Definition;
        if (def == null) return;

        // Columns
        foreach (var c in def.ColumnDefinitions)
        {
            var colName = c.ColumnIdentifier.Value;
            var type = c.DataType is null ? "" : Helpers.SqlDataTypeToString(c.DataType);
            bool nullable = c.Constraints.OfType<NullableConstraintDefinition>().FirstOrDefault()?.Nullable ?? true;

            string? defVal = null;
            if (c.DefaultConstraint?.Expression is { } expr)
                defVal = Helpers.ScriptFragment(expr);

            t.Columns[colName] = new ColumnInfo(type, nullable, defVal);
        }

        // PK + FK
        foreach (var cons in def.TableConstraints)
        {
            switch (cons)
            {
                case UniqueConstraintDefinition u when u.IsPrimaryKey:
                {
                    var cols = u.Columns.Select(c => c.Column.MultiPartIdentifier.Identifiers.Last().Value);
                    foreach (var col in cols)
                        if (!t.Primary_Key.Contains(col)) t.Primary_Key.Add(col);
                    break;
                }
                case ForeignKeyConstraintDefinition fk:
                {
                    var (rs, rn, _) = Helpers.NameOf(fk.ReferenceTableName);
                    var refCol = fk.ReferencedTableColumns.FirstOrDefault()?.Value ?? "";
                    var localCol = fk.Columns.FirstOrDefault()?.Value ?? "";
                    t.Foreign_Keys.Add(new ForeignKeyRef(localCol, rs, Helpers.SafeKey(rn), refCol, rn));
                    break;
                }
            }
        }

        // Inline indexes
        foreach (var idx in def.Indexes ?? Enumerable.Empty<IndexDefinition>())
        {
            var idxName = idx.Name?.Value ?? "(unnamed)";
            var key = safe + "|" + idxName;
            if (_idxSeen.Add(key))
            {
                var cols = idx.Columns.Select(ic => ic.Column.MultiPartIdentifier.Identifiers.Last().Value).ToList();
                t.Indexes[idxName] = cols;
            }
        }
    }

    public override void ExplicitVisit(CreateIndexStatement node)
    {
        var (schema, tableName, _) = Helpers.NameOf(node.OnName);
        var safe = Helpers.SafeKey(tableName);
        if (!_catalog.Tables.TryGetValue(safe, out var t))
        {
            t = new TableInfo { Schema = schema, Original_Name = tableName, Safe_Name = safe };
            _catalog.Tables[safe] = t;
        }
        var idxName = node.Name?.Value ?? "(unnamed)";
        var key = safe + "|" + idxName;
        if (_idxSeen.Add(key))
        {
            var cols = node.Columns.Select(ic => ic.Column.MultiPartIdentifier.Identifiers.Last().Value).ToList();
            t.Indexes[idxName] = cols;
        }
    }

    public override void ExplicitVisit(AlterTableAddTableElementStatement node)
    {
        var (schema, tableName, _) = Helpers.NameOf(node.SchemaObjectName);
        var safe = Helpers.SafeKey(tableName);
        if (!_catalog.Tables.TryGetValue(safe, out var t))
        {
            t = new TableInfo { Schema = schema, Original_Name = tableName, Safe_Name = safe };
            _catalog.Tables[safe] = t;
        }

        foreach (var elem in node.Definition.TableConstraints)
        {
            switch (elem)
            {
                case UniqueConstraintDefinition u when u.IsPrimaryKey:
                {
                    var cols = u.Columns.Select(c => c.Column.MultiPartIdentifier.Identifiers.Last().Value);
                    foreach (var col in cols)
                        if (!t.Primary_Key.Contains(col)) t.Primary_Key.Add(col);
                    break;
                }
                case ForeignKeyConstraintDefinition fk:
                {
                    var (rs, rn, _) = Helpers.NameOf(fk.ReferenceTableName);
                    var refCol = fk.ReferencedTableColumns.FirstOrDefault()?.Value ?? "";
                    var localCol = fk.Columns.FirstOrDefault()?.Value ?? "";
                    t.Foreign_Keys.Add(new ForeignKeyRef(localCol, rs, Helpers.SafeKey(rn), refCol, rn));
                    break;
                }
            }
        }
    }
}

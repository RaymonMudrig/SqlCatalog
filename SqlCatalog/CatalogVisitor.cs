using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SqlCatalogApp;

internal sealed class CatalogVisitor : TSqlFragmentVisitor
{
    public readonly Catalog Catalog = new();
    private readonly HashSet<string> _idxSeen = new(); // table|indexName

    // --- CREATE TABLE ---
    public override void ExplicitVisit(CreateTableStatement node)
    {
        var (schema, name, _) = Helpers.NameOf(node.SchemaObjectName);
        var safe = Helpers.SafeKey(name);

        if (!Catalog.Tables.TryGetValue(safe, out var t))
        {
            t = new TableInfo { Schema = schema, Original_Name = name, Safe_Name = safe };
            Catalog.Tables[safe] = t;
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
            {
                defVal = Helpers.ScriptFragment(expr);
            }

            t.Columns[colName] = new ColumnInfo(type, nullable, defVal);
        }

        // Constraints (PK, FK)
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
                    var refName = fk.ReferenceTableName;
                    var (rs, rn, _) = Helpers.NameOf(refName);
                    var refCol = fk.ReferencedTableColumns.FirstOrDefault()?.Value ?? "";
                    var localCol = fk.Columns.FirstOrDefault()?.Value ?? "";
                    t.Foreign_Keys.Add(new ForeignKeyRef(
                        localCol, rs, Helpers.SafeKey(rn), refCol, rn
                    ));
                    break;
                }
            }
        }

        // Inline indexes (if any)
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

    // --- CREATE INDEX (separate) ---
    public override void ExplicitVisit(CreateIndexStatement node)
    {
        var (schema, tableName, _) = Helpers.NameOf(node.OnName);
        var safe = Helpers.SafeKey(tableName);
        if (!Catalog.Tables.TryGetValue(safe, out var t))
        {
            t = new TableInfo { Schema = schema, Original_Name = tableName, Safe_Name = safe };
            Catalog.Tables[safe] = t;
        }
        var idxName = node.Name?.Value ?? "(unnamed)";
        var key = safe + "|" + idxName;
        if (_idxSeen.Add(key))
        {
            var cols = node.Columns.Select(ic => ic.Column.MultiPartIdentifier.Identifiers.Last().Value).ToList();
            t.Indexes[idxName] = cols;
        }
    }

    // --- ALTER TABLE (PK/FK via ALTER) ---
    public override void ExplicitVisit(AlterTableAddTableElementStatement node)
    {
        var (schema, tableName, _) = Helpers.NameOf(node.SchemaObjectName);
        var safe = Helpers.SafeKey(tableName);
        if (!Catalog.Tables.TryGetValue(safe, out var t))
        {
            t = new TableInfo { Schema = schema, Original_Name = tableName, Safe_Name = safe };
            Catalog.Tables[safe] = t;
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
                    var refName = fk.ReferenceTableName;
                    var (rs, rn, _) = Helpers.NameOf(refName);
                    var refCol = fk.ReferencedTableColumns.FirstOrDefault()?.Value ?? "";
                    var localCol = fk.Columns.FirstOrDefault()?.Value ?? "";
                    t.Foreign_Keys.Add(new ForeignKeyRef(
                        localCol, rs, Helpers.SafeKey(rn), refCol, rn
                    ));
                    break;
                }
            }
        }
    }

    // --- CREATE VIEW ---
    public override void ExplicitVisit(CreateViewStatement node)
    {
        var (schema, name, _) = Helpers.NameOf(node.SchemaObjectName);
        if (!Catalog.Views.TryGetValue(name, out var v))
        {
            v = new ViewInfo { Schema = schema };
            Catalog.Views[name] = v;
        }

        // Columns (walk all select elements, also works with nested queries)
        foreach (var sse in node.GetDescendants<SelectScalarExpression>())
        {
            if (sse.ColumnName != null)
                v.Columns.Add(sse.ColumnName.Value);
            else if (sse.Expression != null)
                v.Columns.Add(Helpers.ScriptFragment(sse.Expression));
        }
        foreach (var star in node.GetDescendants<SelectStarExpression>())
        {
            v.Columns.Add("*");
        }

        // Reads (tables/views referenced)
        var seen = new HashSet<string>();
        foreach (var nt in node.GetDescendants<NamedTableReference>())
            Helpers.AddRead(seen, v.Reads, nt.SchemaObject);
    }

    // --- CREATE PROCEDURE ---
    public override void ExplicitVisit(CreateProcedureStatement node)
    {
        var (schema, name, _) = Helpers.NameOf(node.ProcedureReference.Name);
        if (!Catalog.Procedures.TryGetValue(name, out var p))
        {
            p = new ProcedureInfo { Schema = schema };
            Catalog.Procedures[name] = p;
        }

        foreach (var prm in node.Parameters)
        {
            var pname = prm.VariableName?.Value?.TrimStart('@') ?? "";
            var ptype = prm.DataType is null ? "" : Helpers.SqlDataTypeToString(prm.DataType);
            p.Params.Add(new ParamInfo(pname, ptype));
        }

        var seenR = new HashSet<string>();
        var seenW = new HashSet<string>();
        var seenC = new HashSet<string>();

        foreach (var nt in node.GetDescendants<NamedTableReference>())
            Helpers.AddRead(seenR, p.Reads, nt.SchemaObject);

        foreach (var ins in node.GetDescendants<InsertStatement>())
            Helpers.AddTargetWrite(ins.InsertSpecification?.Target, p.Writes, seenW);

        foreach (var up in node.GetDescendants<UpdateStatement>())
            Helpers.AddTargetWrite(up.UpdateSpecification?.Target, p.Writes, seenW);

        foreach (var del in node.GetDescendants<DeleteStatement>())
            Helpers.AddTargetWrite(del.DeleteSpecification?.Target, p.Writes, seenW);

        // MERGE target omitted for cross-version compatibility

        foreach (var exec in node.GetDescendants<ExecuteSpecification>())
        {
            var sobj = (exec.ExecutableEntity as ExecutableProcedureReference)
                       ?.ProcedureReference?.ProcedureReference?.Name;
            if (sobj?.BaseIdentifier != null)
            {
                var (ss, on, _) = Helpers.NameOf(sobj);
                var key = (ss ?? "") + "|" + on;
                if (seenC.Add(key)) p.Calls.Add(new ObjRef(ss, on));
            }
        }
    }

    // --- CREATE FUNCTION ---
    public override void ExplicitVisit(CreateFunctionStatement node)
    {
        var (schema, name, _) = Helpers.NameOf(node.Name);
        if (!Catalog.Functions.TryGetValue(name, out var f))
        {
            f = new FunctionInfo { Schema = schema };
            Catalog.Functions[name] = f;
        }

        foreach (var prm in node.Parameters)
        {
            var pname = prm.VariableName?.Value?.TrimStart('@') ?? "";
            var ptype = prm.DataType is null ? "" : Helpers.SqlDataTypeToString(prm.DataType);
            f.Params.Add(new ParamInfo(pname, ptype));
        }

        var seenR = new HashSet<string>();
        var seenW = new HashSet<string>();
        var seenC = new HashSet<string>();

        foreach (var nt in node.GetDescendants<NamedTableReference>())
            Helpers.AddRead(seenR, f.Reads, nt.SchemaObject);

        foreach (var ins in node.GetDescendants<InsertStatement>())
            Helpers.AddTargetWrite(ins.InsertSpecification?.Target, f.Writes, seenW);

        foreach (var up in node.GetDescendants<UpdateStatement>())
            Helpers.AddTargetWrite(up.UpdateSpecification?.Target, f.Writes, seenW);

        foreach (var del in node.GetDescendants<DeleteStatement>())
            Helpers.AddTargetWrite(del.DeleteSpecification?.Target, f.Writes, seenW);

        // MERGE target omitted

        foreach (var exec in node.GetDescendants<ExecuteSpecification>())
        {
            var sobj = (exec.ExecutableEntity as ExecutableProcedureReference)
                       ?.ProcedureReference?.ProcedureReference?.Name;
            if (sobj?.BaseIdentifier != null)
            {
                var (ss, on, _) = Helpers.NameOf(sobj);
                var key = (ss ?? "") + "|" + on;
                if (seenC.Add(key)) f.Calls.Add(new ObjRef(ss, on));
            }
        }
    }
}

using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SqlCatalog
{
    internal sealed class ProcFuncVisitor(Catalog cat) : TSqlFragmentVisitor
    {
        private readonly Catalog _cat = cat;

        public override void ExplicitVisit(CreateProcedureStatement node)
        {
            var (schema, name, safe) = Helpers.NameOf(node.ProcedureReference.Name);
            if (!_cat.Procedures.TryGetValue(safe, out var p))
            {
                p = new ProcedureInfo
                {
                    Schema = schema,
                    Original_Name = name,
                    Safe_Name = safe
                };
                _cat.Procedures[safe] = p;
            }

            // Params
            foreach (var prm in node.Parameters)
            {
                var pname = prm.VariableName?.Value;
                if (!string.IsNullOrWhiteSpace(pname))
                    p.Params.Add(pname!);
            }

            var seenR = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var seenW = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            // Build alias-to-table mapping for column resolution
            var aliasMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            // Reads (and collect aliases)
            foreach (var nt in DomExtensions.GetDescendants<NamedTableReference>(node))
            {
                if (nt.SchemaObject != null)
                {
                    Helpers.AddRead(_cat, seenR, p.Reads, nt.SchemaObject);

                    // Track alias -> table mapping
                    var (tschema, tname, tsafe) = Helpers.NameOf(nt.SchemaObject);
                    var alias = nt.Alias?.Value ?? tname; // Use alias if present, otherwise table name
                    if (!string.IsNullOrWhiteSpace(alias))
                        aliasMap[alias] = tsafe;
                }
            }

            // Writes: INSERT / UPDATE / DELETE
            foreach (var ins in DomExtensions.GetDescendants<InsertStatement>(node))
                Helpers.AddTargetWrite(_cat, ins.InsertSpecification?.Target, p.Writes, seenW);

            foreach (var upd in DomExtensions.GetDescendants<UpdateStatement>(node))
                Helpers.AddTargetWrite(_cat, upd.UpdateSpecification?.Target, p.Writes, seenW);

            foreach (var del in DomExtensions.GetDescendants<DeleteStatement>(node))
                Helpers.AddTargetWrite(_cat, del.DeleteSpecification?.Target, p.Writes, seenW);

            // Calls (EXEC ...)
            foreach (var ex in DomExtensions.GetDescendants<ExecuteStatement>(node))
            {
                var pref = ex.ExecuteSpecification?.ExecutableEntity as ExecutableProcedureReference;
                var pr = pref?.ProcedureReference?.ProcedureReference;
                if (pr?.Name != null)
                {
                    var (_, _, safeCall) = Helpers.NameOf(pr.Name);
                    p.Calls.Add(new ObjRef(null, safeCall));
                }
            }

            // Column references (best-effort with alias resolution)
            foreach (var cref in DomExtensions.GetDescendants<ColumnReferenceExpression>(node))
            {
                var ids = cref.MultiPartIdentifier?.Identifiers;
                if (ids == null || ids.Count == 0) continue;

                string? schemaName = null;
                string? tableName = null;
                string? colName;

                if (ids.Count >= 3)
                {
                    schemaName = ids[^3].Value;
                    tableName  = ids[^2].Value;
                    colName    = ids[^1].Value;
                }
                else if (ids.Count == 2)
                {
                    tableName = ids[0].Value;
                    colName   = ids[1].Value;
                }
                else
                {
                    continue; // unqualified; skip without deep binding
                }

                if (string.IsNullOrWhiteSpace(tableName) || string.IsNullOrWhiteSpace(colName))
                    continue;

                // Try to resolve alias to actual table name
                string resolvedTable;
                if (aliasMap.TryGetValue(tableName, out var mapped))
                {
                    resolvedTable = mapped;
                }
                else
                {
                    resolvedTable = Helpers.SafeTableKey(schemaName, tableName);
                }

                Helpers.AddColumnRef(p.Column_Refs, resolvedTable, colName);
            }

            // UPDATE SET columns (writes)
            foreach (var upd in DomExtensions.GetDescendants<UpdateStatement>(node))
            {
                var target = upd.UpdateSpecification?.Target;
                string? targetTable = null;

                // Get the target table name
                if (target is NamedTableReference ntr)
                {
                    var (_, tname, tsafe) = Helpers.NameOf(ntr.SchemaObject);
                    targetTable = tsafe;
                }

                if (string.IsNullOrWhiteSpace(targetTable))
                    continue;

                // Extract columns from SET clause
                foreach (var setClause in upd.UpdateSpecification?.SetClauses ?? Enumerable.Empty<SetClause>())
                {
                    if (setClause is AssignmentSetClause asc)
                    {
                        var colRef = asc.Column;
                        var ids = colRef?.MultiPartIdentifier?.Identifiers;
                        if (ids != null && ids.Count > 0)
                        {
                            var colName = ids[^1].Value; // Last identifier is the column name
                            if (!string.IsNullOrWhiteSpace(colName))
                                Helpers.AddColumnRef(p.Column_Refs, targetTable, colName);
                        }
                    }
                }
            }

            // INSERT column lists (writes)
            foreach (var ins in DomExtensions.GetDescendants<InsertStatement>(node))
            {
                var target = ins.InsertSpecification?.Target;
                string? targetTable = null;

                // Get the target table name
                if (target is NamedTableReference ntr)
                {
                    var (_, tname, tsafe) = Helpers.NameOf(ntr.SchemaObject);
                    targetTable = tsafe;
                }

                if (string.IsNullOrWhiteSpace(targetTable))
                    continue;

                // Extract columns from INSERT column list
                foreach (var col in ins.InsertSpecification?.Columns ?? Enumerable.Empty<ColumnReferenceExpression>())
                {
                    var ids = col.MultiPartIdentifier?.Identifiers;
                    if (ids != null && ids.Count > 0)
                    {
                        var colName = ids[^1].Value; // Last identifier is the column name
                        if (!string.IsNullOrWhiteSpace(colName))
                            Helpers.AddColumnRef(p.Column_Refs, targetTable, colName);
                    }
                }
            }

            // Header doc
            p.Doc ??= Helpers.ExtractHeaderDoc(Helpers.ScriptFragment(node));
        }
    }
}

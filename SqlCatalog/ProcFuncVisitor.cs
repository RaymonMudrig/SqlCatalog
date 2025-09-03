using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SqlCatalog
{
    internal sealed class ProcFuncVisitor : TSqlFragmentVisitor
    {
        private readonly Catalog _cat;

        public ProcFuncVisitor(Catalog cat) => _cat = cat;

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

            // Reads
            foreach (var nt in DomExtensions.GetDescendants<NamedTableReference>(node))
                Helpers.AddRead(seenR, p.Reads, nt.SchemaObject);

            // Writes: INSERT / UPDATE / DELETE
            foreach (var ins in DomExtensions.GetDescendants<InsertStatement>(node))
                Helpers.AddTargetWrite(ins.InsertSpecification?.Target, p.Writes, seenW);

            foreach (var upd in DomExtensions.GetDescendants<UpdateStatement>(node))
                Helpers.AddTargetWrite(upd.UpdateSpecification?.Target, p.Writes, seenW);

            foreach (var del in DomExtensions.GetDescendants<DeleteStatement>(node))
                Helpers.AddTargetWrite(del.DeleteSpecification?.Target, p.Writes, seenW);

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

            // Column references (best-effort)
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

                var safeTbl = Helpers.SafeTableKey(schemaName, tableName);
                Helpers.AddColumnRef(p.Column_Refs, safeTbl, colName);
            }

            // Header doc
            p.Doc ??= Helpers.ExtractHeaderDoc(Helpers.ScriptFragment(node));
        }
    }
}

using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SqlCatalog
{
    internal sealed class ViewVisitor : TSqlFragmentVisitor
    {
        private readonly Catalog _cat;

        public ViewVisitor(Catalog cat) => _cat = cat;

        public override void ExplicitVisit(CreateViewStatement node)
        {
            var (schema, name, safe) = Helpers.NameOf(node.SchemaObjectName);
            if (!_cat.Views.TryGetValue(safe, out var v))
            {
                v = new ViewInfo
                {
                    Schema = schema,
                    Original_Name = name,
                    Safe_Name = safe
                };
                _cat.Views[safe] = v;
            }

            // Header doc
            v.Doc ??= Helpers.ExtractHeaderDoc(Helpers.ScriptFragment(node));

            // Reads: walk the SELECT if present; otherwise the node
            TSqlFragment rootFrag = (node.SelectStatement as TSqlFragment) ?? node;
            foreach (var nt in DomExtensions.GetDescendants<NamedTableReference>(rootFrag))
            {
                if (nt.SchemaObject != null)
                {
                    var (ts, tn, tsafe) = Helpers.NameOf(nt.SchemaObject);
                    v.Reads.Add(new ObjRef(ts, tsafe));
                }
            }

            // Columns
            var cols = new List<string>();
            var select = node.SelectStatement?.QueryExpression as QuerySpecification;
            if (select != null)
            {
                foreach (var se in select.SelectElements)
                {
                    switch (se)
                    {
                        case SelectScalarExpression sse:
                            var alias = sse.ColumnName?.Value;
                            if (!string.IsNullOrEmpty(alias))
                                cols.Add(alias!);
                            else if (sse.Expression is ColumnReferenceExpression cre)
                            {
                                var id = cre.MultiPartIdentifier?.Identifiers?.LastOrDefault()?.Value;
                                if (!string.IsNullOrEmpty(id)) cols.Add(id!);
                            }
                            break;

                        case SelectStarExpression:
                            cols.Add("*");
                            break;
                    }
                }
            }
            if (cols.Count == 0 && node.Columns != null && node.Columns.Count > 0)
                cols.AddRange(node.Columns.Select(c => c.Value));

            foreach (var c in cols.Distinct(StringComparer.OrdinalIgnoreCase))
                v.Columns.Add(c);
        }
    }
}

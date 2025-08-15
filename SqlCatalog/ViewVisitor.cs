using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SqlCatalogApp;

internal sealed class ViewVisitor : TSqlFragmentVisitor
{
    private readonly Catalog _catalog;
    public ViewVisitor(Catalog catalog) => _catalog = catalog;

    public override void ExplicitVisit(CreateViewStatement node)
    {
        var (schema, name, _) = Helpers.NameOf(node.SchemaObjectName);
        if (!_catalog.Views.TryGetValue(name, out var v))
        {
            v = new ViewInfo { Schema = schema };
            _catalog.Views[name] = v;
        }

        // Output columns (aliased scalars / expressions / stars) — walk all elements
        foreach (var sse in node.GetDescendants<SelectScalarExpression>())
        {
            if (sse.ColumnName != null)
                v.Columns.Add(sse.ColumnName.Value);
            else if (sse.Expression != null)
                v.Columns.Add(Helpers.ScriptFragment(sse.Expression));
        }
        foreach (var star in node.GetDescendants<SelectStarExpression>())
            v.Columns.Add("*");

        // Reads (tables/views referenced)
        var seen = new HashSet<string>();
        foreach (var nt in node.GetDescendants<NamedTableReference>())
            Helpers.AddRead(seen, v.Reads, nt.SchemaObject);
    }
}

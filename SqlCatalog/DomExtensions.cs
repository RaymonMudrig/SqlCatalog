using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SqlCatalog;

internal static class DomExtensions
{
    public static IEnumerable<T> GetDescendants<T>(this TSqlFragment root) where T : TSqlFragment
    {
        var list = new List<T>();
        var v = new Collector<T>(list);
        root.Accept(v);
        return list;
    }

    private sealed class Collector<T> : TSqlFragmentVisitor where T : TSqlFragment
    {
        private readonly List<T> _sink;
        public Collector(List<T> sink) => _sink = sink;

        public override void Visit(TSqlFragment node)
        {
            if (node is T t) _sink.Add(t);
            base.Visit(node);
        }
    }
}

using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SqlCatalog
{
    /// <summary>
    /// Walks the script and re-exports each entity statement (table/view/proc/function)
    /// to output/sql_exports/<kind>/<schema·name>.sql (or SQL_EXPORT_DIR if set).
    /// </summary>
    internal sealed class ExportSqlVisitor : TSqlFragmentVisitor
    {
        public override void ExplicitVisit(CreateTableStatement node)
        {
            var (schema, name, _) = Helpers.NameOf(node.SchemaObjectName);
            var sql = Helpers.ScriptFragment(node);
            Helpers.WriteEntitySql("tables", schema, name, sql);
        }

        public override void ExplicitVisit(CreateViewStatement node)
        {
            var (schema, name, _) = Helpers.NameOf(node.SchemaObjectName);
            var sql = Helpers.ScriptFragment(node);
            Helpers.WriteEntitySql("views", schema, name, sql);
        }

        public override void ExplicitVisit(CreateProcedureStatement node)
        {
            var (schema, name, _) = Helpers.NameOf(node.ProcedureReference.Name);
            var sql = Helpers.ScriptFragment(node);
            Helpers.WriteEntitySql("procedures", schema, name, sql);
        }

        public override void ExplicitVisit(CreateFunctionStatement node)
        {
            // Scalar/table-valued functions both derive from CreateFunctionStatement
            var (schema, name, _) = Helpers.NameOf(node.Name);
            var sql = Helpers.ScriptFragment(node);
            Helpers.WriteEntitySql("functions", schema, name, sql);
        }
    }
}

using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SqlCatalogApp;

internal class Program
{
    static void Main()
    {
        Directory.CreateDirectory(Config.OutputDir);

        var parser = Helpers.CreateBestParser(initialQuotedIdentifiers: true);
        var catalog = new Catalog();

        var tableVisitor = new TableVisitor(catalog);
        var viewVisitor  = new ViewVisitor(catalog);
        var pfVisitor    = new ProcFuncVisitor(catalog);

        foreach (var path in Directory.GetFiles(Config.SqlDir, "*.sql", SearchOption.TopDirectoryOnly))
        {
            Console.WriteLine($"Processing: {Path.GetFileName(path)}");

            IList<ParseError> errors;
            using (var reader = new StreamReader(path, Encoding.UTF8))
            {
                var fragment = parser.Parse(reader, out errors);

                // Walk with specialized visitors
                fragment.Accept(tableVisitor);
                fragment.Accept(viewVisitor);
                fragment.Accept(pfVisitor);
            }

            if (errors?.Count > 0)
            {
                foreach (var e in errors.Take(5))
                    Console.WriteLine($"  ⚠ L{e.Line}:{e.Column} {e.Message}");
                if (errors.Count > 5) Console.WriteLine($"  ... {errors.Count - 5} more");
            }
        }

        // ---------- Build schema-based clustering index ----------
        var schemaIndex = new Dictionary<string, SchemaGroup>(StringComparer.OrdinalIgnoreCase);

        // Tables (use safe key for IDs)
        foreach (var (safeKey, t) in catalog.Tables)
        {
            var schema = t.Schema ?? "dbo";
            if (!schemaIndex.TryGetValue(schema, out var grp))
                schemaIndex[schema] = grp = new SchemaGroup();

            grp.Tables.Add(safeKey);
        }

        // Views
        foreach (var (name, v) in catalog.Views)
        {
            var schema = v.Schema ?? "dbo";
            if (!schemaIndex.TryGetValue(schema, out var grp))
                schemaIndex[schema] = grp = new SchemaGroup();

            grp.Views.Add(name);
        }

        // Procedures (split by Access)
        foreach (var (name, p) in catalog.Procedures)
        {
            var schema = p.Schema ?? "dbo";
            if (!schemaIndex.TryGetValue(schema, out var grp))
                schemaIndex[schema] = grp = new SchemaGroup();

            if (p.Access == "read") grp.Procedures_Read.Add(name);
            else grp.Procedures_Write.Add(name);
        }

        // Functions (split by Access)
        foreach (var (name, f) in catalog.Functions)
        {
            var schema = f.Schema ?? "dbo";
            if (!schemaIndex.TryGetValue(schema, out var grp))
                schemaIndex[schema] = grp = new SchemaGroup();

            if (f.Access == "read") grp.Functions_Read.Add(name);
            else grp.Functions_Write.Add(name);
        }

        // Sort lists for consistency
        foreach (var grp in schemaIndex.Values)
        {
            grp.Tables.Sort(StringComparer.OrdinalIgnoreCase);
            grp.Views.Sort(StringComparer.OrdinalIgnoreCase);
            grp.Procedures_Read.Sort(StringComparer.OrdinalIgnoreCase);
            grp.Procedures_Write.Sort(StringComparer.OrdinalIgnoreCase);
            grp.Functions_Read.Sort(StringComparer.OrdinalIgnoreCase);
            grp.Functions_Write.Sort(StringComparer.OrdinalIgnoreCase);
        }

        // ---------- Export combined object ----------
        var export = new CatalogExport
        {
            Catalog = catalog,
            Schema_Index = schemaIndex
        };

        var json = JsonSerializer.Serialize(export, new JsonSerializerOptions
        {
            WriteIndented = true,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        });
        File.WriteAllText(Config.CatalogPath, json, Encoding.UTF8);

        Console.WriteLine($"\n✅ Catalog exported to {Config.CatalogPath}");
    }
}

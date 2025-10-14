using System.Text.Json;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SqlCatalog
{
    internal static class Program
    {
        static int Main(string[] args)
        {
            try
            {
                var sqlRoot = Helpers.ResolveSqlRoot();          // ../sql_files by default
                var outDir  = Helpers.ResolveOutputRoot();       // ../output by default
                var outPath = Path.Combine(outDir, "catalog.json");

                Directory.CreateDirectory(outDir);

                var files = Directory.Exists(sqlRoot)
                    ? Directory.EnumerateFiles(sqlRoot, "*.sql", SearchOption.AllDirectories).ToList()
                    : new List<string>();

                Console.WriteLine($"[SqlCatalog] Using SQL root: {Path.GetFullPath(sqlRoot)}");
                Console.WriteLine($"[SqlCatalog] Found {files.Count} .sql files");
                Console.WriteLine($"[SqlCatalog] Output dir: {Path.GetFullPath(outDir)}");

                var cat = new Catalog();

                foreach (var file in files)
                {
                    var text = File.ReadAllText(file);
                    var parser = new TSql150Parser(initialQuotedIdentifiers: true);
                    var fragment = parser.Parse(new StringReader(text), out IList<ParseError> errors);

                    if (errors is { Count: > 0 })
                    {
                        Console.Error.WriteLine($"[WARN] Parse errors in {file} : {errors.Count}");
                        foreach (var e in errors.Take(3))
                            Console.Error.WriteLine($"  L{e.Line},{e.Column}: {e.Message}");
                    }

                    // Your existing + new visitors
                    var tv = new TableVisitor(cat);     // your class
                    var vv = new ViewVisitor(cat);      // ours
                    var pv = new ProcFuncVisitor(cat);  // ours
                    var xv = new ExportSqlVisitor();    // re-export .sql files

                    fragment.Accept(tv);
                    fragment.Accept(vv);
                    fragment.Accept(pv);
                    fragment.Accept(xv);
                }

                // ---------- Post-pass: propagate usage ----------
                foreach (var p in cat.Procedures.Values)
                {
                    DeduplicateRefs(p.Reads);
                    DeduplicateRefs(p.Writes);
                    DeduplicateRefs(p.Calls);

                    // Process reads
                    foreach (var r in p.Reads)
                    {
                        // Try exact match first, then try with procedure's schema if unqualified
                        if (!cat.Tables.TryGetValue(r.Safe_Name, out var t))
                        {
                            // If the reference lacks schema, try with the procedure's schema
                            if (!r.Safe_Name.Contains("·") && !string.IsNullOrEmpty(p.Schema))
                            {
                                var qualifiedKey = Helpers.SafeName(p.Schema, r.Safe_Name);
                                cat.Tables.TryGetValue(qualifiedKey, out t);
                            }
                        }
                        if (t != null)
                            t.Referenced_By.Add(new ObjRef(p.Schema, p.Safe_Name, "read"));
                    }

                    // Process writes
                    foreach (var w in p.Writes)
                    {
                        // Try exact match first, then try with procedure's schema if unqualified
                        if (!cat.Tables.TryGetValue(w.Safe_Name, out var t))
                        {
                            // If the reference lacks schema, try with the procedure's schema
                            if (!w.Safe_Name.Contains("·") && !string.IsNullOrEmpty(p.Schema))
                            {
                                var qualifiedKey = Helpers.SafeName(p.Schema, w.Safe_Name);
                                cat.Tables.TryGetValue(qualifiedKey, out t);
                            }
                        }
                        if (t != null)
                            t.Referenced_By.Add(new ObjRef(p.Schema, p.Safe_Name, "write"));
                    }

                    foreach (var kv in p.Column_Refs)
                    {
                        // Try exact match first, then with procedure's schema
                        if (!cat.Tables.TryGetValue(kv.Key, out var t))
                        {
                            if (!kv.Key.Contains("·") && !string.IsNullOrEmpty(p.Schema))
                            {
                                var qualifiedKey = Helpers.SafeName(p.Schema, kv.Key);
                                cat.Tables.TryGetValue(qualifiedKey, out t);
                            }
                        }
                        if (t != null)
                        {
                            foreach (var col in kv.Value)
                                if (t.Columns.TryGetValue(col, out var ci))
                                    ci.Referenced_In.Add(new UsageRef("procedure", p.Safe_Name, "unknown"));
                        }
                    }
                }

                foreach (var v in cat.Views.Values)
                {
                    foreach (var r in v.Reads)
                    {
                        // Try exact match first, then try with view's schema if unqualified
                        if (!cat.Tables.TryGetValue(r.Safe_Name, out var t))
                        {
                            // If the reference lacks schema, try with the view's schema
                            if (!r.Safe_Name.Contains("·") && !string.IsNullOrEmpty(v.Schema))
                            {
                                var qualifiedKey = Helpers.SafeName(v.Schema, r.Safe_Name);
                                cat.Tables.TryGetValue(qualifiedKey, out t);
                            }
                        }
                        if (t != null)
                            t.Referenced_By.Add(new ObjRef(v.Schema, v.Safe_Name, "read"));
                    }

                    if (v.Columns.Count > 0 && v.Reads.Count == 1)
                    {
                        var baseSafe = v.Reads[0].Safe_Name;
                        if (!cat.Tables.TryGetValue(baseSafe, out var t))
                        {
                            // Try with view's schema if unqualified
                            if (!baseSafe.Contains("·") && !string.IsNullOrEmpty(v.Schema))
                            {
                                var qualifiedKey = Helpers.SafeName(v.Schema, baseSafe);
                                cat.Tables.TryGetValue(qualifiedKey, out t);
                            }
                        }
                        if (t != null)
                        {
                            foreach (var c in v.Columns.Where(cn => cn != "*"))
                                if (t.Columns.TryGetValue(c, out var ci))
                                    ci.Referenced_In.Add(new UsageRef("view", v.Safe_Name, "select"));
                        }
                    }
                }

                // ---------- Unused ----------
                foreach (var t in cat.Tables.Values)
                {
                    t.Is_Unused = t.Referenced_By.Count == 0;
                    if (t.Is_Unused) cat.Unused_Tables.Add(t.Safe_Name);

                    foreach (var kv in t.Columns)
                        if (kv.Value.Referenced_In.Count == 0)
                            cat.Unused_Columns.Add(new UnusedColumn(t.Safe_Name, kv.Key));
                }

                // ---------- Save ----------
                var json = JsonSerializer.Serialize(cat, new JsonSerializerOptions { WriteIndented = true });
                File.WriteAllText(outPath, json);
                Console.WriteLine($"[SqlCatalog] Wrote: {Path.GetFullPath(outPath)}");
                Console.WriteLine($"[SqlCatalog] Tables={cat.Tables.Count}, Views={cat.Views.Count}, Procs={cat.Procedures.Count}");
                return 0;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex);
                return 1;
            }
        }

        static void DeduplicateRefs(List<ObjRef> refs)
        {
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            for (int i = refs.Count - 1; i >= 0; i--)
            {
                var key = refs[i]?.Safe_Name ?? "";
                if (!seen.Add(key))
                    refs.RemoveAt(i);
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SqlCatalogApp
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
                    IList<ParseError> errors;
                    var fragment = parser.Parse(new StringReader(text), out errors);

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
                    foreach (var r in p.Reads.Concat(p.Writes))
                        if (cat.Tables.TryGetValue(r.Safe_Name, out var t))
                            t.Referenced_By.Add(new ObjRef(p.Schema, p.Safe_Name));

                    foreach (var kv in p.Column_Refs)
                    {
                        if (!cat.Tables.TryGetValue(kv.Key, out var t)) continue;
                        foreach (var col in kv.Value)
                            if (t.Columns.TryGetValue(col, out var ci))
                                ci.Referenced_In.Add(new UsageRef("procedure", p.Safe_Name, "unknown"));
                    }
                }

                foreach (var v in cat.Views.Values)
                {
                    foreach (var r in v.Reads)
                        if (cat.Tables.TryGetValue(r.Safe_Name, out var t))
                            t.Referenced_By.Add(new ObjRef(v.Schema, v.Safe_Name));

                    if (v.Columns.Count > 0 && v.Reads.Count == 1)
                    {
                        var baseSafe = v.Reads[0].Safe_Name;
                        if (cat.Tables.TryGetValue(baseSafe, out var t))
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
    }
}

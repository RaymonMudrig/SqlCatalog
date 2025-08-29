using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SqlCatalogApp
{
    internal static class Helpers
    {
        // ---------- Names & Keys ----------
        public static string SafeSpace => Config.SafeSpace.ToString(); // Config.SafeSpace is a char

        public static string SafeKey(string? name)
        {
            if (string.IsNullOrWhiteSpace(name)) return "";
            var n = name.Trim().Trim('[', ']', '"');
            n = Regex.Replace(n, @"\s+", " ");
            return n;
        }

        public static string SafeName(string? schema, string name)
        {
            schema ??= "";
            var core = SafeKey(name);
            return schema.Length == 0 ? core : $"{schema}{SafeSpace}{core}";
        }

        public static string SafeTableKey(string? schema, string name) => SafeName(schema, name);

        public static (string schema, string name, string safe) NameOf(SchemaObjectName on)
        {
            string schema = on.SchemaIdentifier?.Value ?? "";
            string name   = on.BaseIdentifier?.Value ?? "";
            return (schema, name, SafeName(schema, name));
        }

        public static (string schema, string name, string safe) NameOf(Identifier baseId, Identifier? schemaId)
        {
            string schema = schemaId?.Value ?? "";
            string name   = baseId.Value;
            return (schema, name, SafeName(schema, name));
        }

        // ---------- Script fragments ----------
        public static string ScriptFragment(TSqlFragment frag)
        {
            if (frag.ScriptTokenStream is null || frag.FirstTokenIndex < 0 || frag.LastTokenIndex < 0)
                return frag.ToString() ?? string.Empty;

            var tokens = frag.ScriptTokenStream;
            var sb = new StringBuilder();
            for (int i = frag.FirstTokenIndex; i <= frag.LastTokenIndex && i < tokens.Count; i++)
                sb.Append(tokens[i].Text);
            return sb.ToString();
        }

        // ---------- Reads / Writes helpers ----------
        public static void AddRead(HashSet<string> seen, List<ObjRef> reads, SchemaObjectName schemaObject)
        {
            var (_, _, safe) = NameOf(schemaObject);
            if (safe.Length == 0 || !seen.Add(safe)) return;
            reads.Add(new ObjRef(schemaObject.SchemaIdentifier?.Value, safe));
        }

        public static void AddTargetWrite(TableReference? target, List<ObjRef> writes, HashSet<string> seen)
        {
            if (target is NamedTableReference ntr && ntr.SchemaObject != null)
            {
                var (s, _, safe) = NameOf(ntr.SchemaObject);
                if (seen.Add(safe))
                    writes.Add(new ObjRef(s, safe));
            }
        }

        // ---------- Types & defaults ----------
        public static string SqlDataTypeToString(DataTypeReference type)
        {
            switch (type)
            {
                case SqlDataTypeReference s:
                    var t = s.SqlDataTypeOption.ToString().ToLowerInvariant();
                    if (s.Parameters is { Count: > 0 })
                        t += $"({string.Join(",", s.Parameters.Select(p => p is Literal l ? l.Value : ScriptFragment(p)))})";
                    return t;
                default:
                    return ScriptFragment(type);
            }
        }

        public static string? ColumnDefault(ColumnDefinition c)
        {
            var def = c.Constraints?.OfType<DefaultConstraintDefinition>().FirstOrDefault();
            if (def?.Expression != null) return ScriptFragment(def.Expression);

            if (c.IdentityOptions is { } io)
            {
                var seed = io.IdentitySeed is Literal s ? s.Value : null;
                var inc  = io.IdentityIncrement is Literal i ? i.Value : null;
                return $"IDENTITY({seed ?? "1"},{inc ?? "1"})";
            }
            return null;
        }

        public static bool ColumnIsNullable(ColumnDefinition c)
        {
            var n = c.Constraints?.OfType<NullableConstraintDefinition>().FirstOrDefault();
            return n?.Nullable ?? true;
        }

        // ---------- Lightweight "docs" ----------
        public static string? ExtractHeaderDoc(string sqlText)
        {
            var mCreate = Regex.Match(sqlText, @"\bCREATE\b", RegexOptions.IgnoreCase);
            int cut = mCreate.Success ? mCreate.Index : Math.Min(2000, sqlText.Length);
            var header = sqlText[..cut];

            var mBlock = Regex.Match(header, @"/\*+(?<doc>[\s\S]*?)\*/", RegexOptions.Multiline);
            if (mBlock.Success) return CleanDoc(mBlock.Groups["doc"].Value);

            var lines = new List<string>();
            using (var sr = new StringReader(header))
            {
                string? line;
                while ((line = sr.ReadLine()) != null)
                {
                    var trimmed = line.TrimStart();
                    if (trimmed.StartsWith("--") || trimmed.StartsWith("//"))
                        lines.Add(trimmed.TrimStart('-', '/').Trim());
                    else if (lines.Count > 0) break;
                }
            }
            return lines.Count > 0 ? CleanDoc(string.Join("\n", lines)) : null;

            static string CleanDoc(string s) =>
                Regex.Replace(s, @"^\s*\* ?", "", RegexOptions.Multiline).Trim();
        }

        public static string? ExtractColumnTrailingDoc(string line)
        {
            var idx = line.IndexOf("--", StringComparison.Ordinal);
            return idx >= 0 ? line[(idx + 2)..].Trim() : null;
        }

        // ---------- Column usage aggregation ----------
        public static void AddColumnRef(Dictionary<string, HashSet<string>> dst, string safeTable, string column)
        {
            if (!dst.TryGetValue(safeTable, out var set))
            {
                set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                dst[safeTable] = set;
            }
            set.Add(column);
        }

        // ---------- Path resolution (one level above the .csproj) ----------
        public static string ResolveSqlRoot()
        {
            var env = Environment.GetEnvironmentVariable("SQL_FILES_DIR");
            if (!string.IsNullOrWhiteSpace(env) && Directory.Exists(env))
                return Path.GetFullPath(env);

            var repoRootAbove = RepoRootAboveCsproj();
            return Path.Combine(repoRootAbove, "sql_files");
        }

        public static string ResolveOutputRoot()
        {
            var env = Environment.GetEnvironmentVariable("SQL_OUTPUT_DIR");
            if (!string.IsNullOrWhiteSpace(env))
                return Path.GetFullPath(env);

            var repoRootAbove = RepoRootAboveCsproj();
            return Path.Combine(repoRootAbove, "output");
        }

        public static string ResolveExportRoot()
        {
            var env = Environment.GetEnvironmentVariable("SQL_EXPORT_DIR");
            if (!string.IsNullOrWhiteSpace(env))
                return Path.GetFullPath(env);

            return Path.Combine(ResolveOutputRoot(), "sql_exports");
        }

        private static string RepoRootAboveCsproj()
        {
            // Find the nearest dir upward that contains a .csproj; return its parent.
            string? probe = AppContext.BaseDirectory;
            for (int i = 0; i < 10 && probe != null; i++)
            {
                if (Directory.EnumerateFiles(probe, "*.csproj").Any())
                {
                    var parent = Directory.GetParent(probe);
                    if (parent != null) return parent.FullName;
                    break;
                }
                probe = Directory.GetParent(probe)?.FullName;
            }

            // Fallback to current dir's parent
            var cur = Directory.GetCurrentDirectory();
            for (int i = 0; i < 10 && cur != null; i++)
            {
                if (Directory.EnumerateFiles(cur, "*.csproj").Any())
                {
                    var parent = Directory.GetParent(cur);
                    if (parent != null) return parent.FullName;
                    break;
                }
                cur = Directory.GetParent(cur)?.FullName;
            }

            // Last resort: go one level up from current dir
            return Directory.GetParent(Directory.GetCurrentDirectory())?.FullName ?? Directory.GetCurrentDirectory();
        }

        // ---------- Export (re-write each entity to .sql) ----------
        public static void WriteEntitySql(string kind, string? schema, string name, string sql)
        {
            var root = ResolveExportRoot();
            var dir  = Path.Combine(root, kind);
            Directory.CreateDirectory(dir);

            var fname = MakeSafeFileName(SafeName(schema, name)) + ".sql";
            var path  = Path.Combine(dir, fname);

            File.WriteAllText(path, sql);
        }

        private static string MakeSafeFileName(string s)
        {
            var invalid = Path.GetInvalidFileNameChars();
            var cleaned = new string(Array.FindAll(s.ToCharArray(), ch => Array.IndexOf(invalid, ch) < 0));
            cleaned = cleaned.Trim().TrimEnd('.', ' ');
            cleaned = Regex.Replace(cleaned, @"\s+", " ");
            return cleaned.Length == 0 ? "unnamed" : cleaned;
        }
    }
}

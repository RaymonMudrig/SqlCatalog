// SqlCatalog/CatalogVisitor.cs
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SqlCatalogApp
{
    internal sealed class CatalogVisitor : TSqlFragmentVisitor
    {
        private readonly Catalog _cat;
        private readonly string _fileSql;

        public CatalogVisitor(Catalog cat, string fileSql)
        {
            _cat = cat;
            _fileSql = fileSql;
        }

        public override void ExplicitVisit(CreateTableStatement node)
        {
            var (schema, name, safe) = Helpers.NameOf(node.SchemaObjectName);
            if (!_cat.Tables.TryGetValue(safe, out var t))
            {
                t = new TableInfo
                {
                    Schema = schema,
                    Original_Name = name,
                    Safe_Name = safe
                };
                _cat.Tables[safe] = t;
            }

            // Header doc (from the whole file fragment of the statement)
            var sql = Helpers.ScriptFragment(node);
            t.Doc ??= Helpers.ExtractHeaderDoc(sql);

            var def = node.Definition;
            if (def == null) return;

            // Columns (type/null/default + trailing "--" docs if present)
            var lines = sql.Split('\n');
            foreach (var c in def.ColumnDefinitions)
            {
                var cn = c.ColumnIdentifier?.Value ?? "";
                var type = c.DataType is null ? "" : Helpers.SqlDataTypeToString(c.DataType);
                bool nullable = Helpers.ColumnIsNullable(c);
                string? defVal = Helpers.ColumnDefault(c);

                string? colDoc = null;
                var maybeLine = lines.FirstOrDefault(l =>
                    l.IndexOf(cn, StringComparison.OrdinalIgnoreCase) >= 0);
                if (maybeLine != null) colDoc = Helpers.ExtractColumnTrailingDoc(maybeLine);

                t.Columns[cn] = new ColumnInfo(type, nullable, defVal, colDoc);
            }

            // PK
            foreach (var uc in def.TableConstraints.OfType<UniqueConstraintDefinition>())
            {
                if (!uc.IsPrimaryKey) continue;
                foreach (var c in uc.Columns)
                {
                    var cn = c.Column?.MultiPartIdentifier?.Identifiers?.LastOrDefault()?.Value;
                    if (!string.IsNullOrEmpty(cn))
                        t.Primary_Key.Add(cn!);
                }
            }

            // FKs
            foreach (var fk in def.TableConstraints.OfType<ForeignKeyConstraintDefinition>())
            {
                var refName = fk.ReferenceTableName;
                var (rs, rn, rsafe) = Helpers.NameOf(refName);
                var localCols = fk.Columns;
                var refCols = fk.ReferencedTableColumns;

                for (int i = 0; i < Math.Min(localCols.Count, refCols.Count); i++)
                {
                    var lc = localCols[i].Value;
                    var rc = refCols[i].Value;
                    t.Foreign_Keys.Add(new ForeignKeyRef(
                        lc, rs, rn, rc, rn));
                }
            }
        }

        // Capture CREATE INDEX (outside CREATE TABLE)
        public override void ExplicitVisit(CreateIndexStatement node)
        {
            if (node.OnName == null) return;
            var (schema, name, safe) = Helpers.NameOf(node.OnName);
            if (!_cat.Tables.TryGetValue(safe, out var t)) return;

            var idxName = node.Name?.Value ?? "(unnamed)";
            var cols = node.Columns?.Select(c => c.Column?.MultiPartIdentifier?.Identifiers?.LastOrDefault()?.Value ?? "")
                                   .Where(x => !string.IsNullOrWhiteSpace(x))
                                   .ToList() ?? new List<string>();
            if (!t.Indexes.TryGetValue(idxName, out var list))
                t.Indexes[idxName] = list = new List<string>();
            list.AddRange(cols);
        }
    }
}

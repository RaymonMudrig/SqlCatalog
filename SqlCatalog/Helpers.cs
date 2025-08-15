using System.Reflection;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SqlCatalogApp;

internal static class Helpers
{
    // ---------- Names / Keys ----------
    public static string SafeKey(string name) => name.Replace(' ', Config.SafeSpace);

    public static (string? schema, string name, string originalComposite) NameOf(SchemaObjectName n)
    {
        string? schema = n.SchemaIdentifier?.Value;
        string name = n.BaseIdentifier?.Value ?? "";
        string original = schema is null ? name : $"{schema}.{name}";
        return (schema, name, original);
    }

    // ---------- Types ----------
    public static string SqlDataTypeToString(DataTypeReference dt)
    {
        var sb = new StringBuilder();
        switch (dt)
        {
            case SqlDataTypeReference sref:
                sb.Append(sref.SqlDataTypeOption.ToString().ToLowerInvariant());
                if (sref.Parameters is { Count: > 0 })
                {
                    sb.Append('(');
                    sb.Append(string.Join(", ", sref.Parameters.Select(p => p.LiteralType == LiteralType.Max ? "max" : p.Value)));
                    sb.Append(')');
                }
                break;

            case ParameterizedDataTypeReference pref:
                sb.Append(pref.Name.BaseIdentifier.Value);
                if (pref.Parameters is { Count: > 0 })
                {
                    sb.Append('(');
                    sb.Append(string.Join(", ", pref.Parameters.Select(p => p.LiteralType == LiteralType.Max ? "max" : p.Value)));
                    sb.Append(')');
                }
                break;

            default:
                if (dt.Name?.BaseIdentifier != null)
                    sb.Append(dt.Name.BaseIdentifier.Value);
                break;
        }
        return sb.ToString();
    }

    // ---------- Script fragment (for defaults and expressions) ----------
    public static string ScriptFragment(TSqlFragment frag)
    {
        var candidates = new[]
        {
            "Microsoft.SqlServer.TransactSql.ScriptDom.Sql170ScriptGenerator",
            "Microsoft.SqlServer.TransactSql.ScriptDom.Sql160ScriptGenerator",
            "Microsoft.SqlServer.TransactSql.ScriptDom.Sql150ScriptGenerator",
            "Microsoft.SqlServer.TransactSql.ScriptDom.Sql140ScriptGenerator",
            "Microsoft.SqlServer.TransactSql.ScriptDom.Sql130ScriptGenerator",
        };

        foreach (var fullName in candidates)
        {
            var t = Type.GetType(fullName);
            if (t == null) continue;

            var opts = new SqlScriptGeneratorOptions { KeywordCasing = KeywordCasing.Uppercase };

            object? gen = t.GetConstructor(new[] { typeof(SqlScriptGeneratorOptions) })?.Invoke(new object[] { opts })
                           ?? t.GetConstructor(Type.EmptyTypes)?.Invoke(null);

            if (gen != null)
            {
                var mi = t.GetMethod("GenerateScript", new[] { typeof(TSqlFragment), typeof(string).MakeByRefType() });
                if (mi != null)
                {
                    object?[] args = { frag, "" };
                    mi.Invoke(gen, args);
                    return args[1]?.ToString() ?? "";
                }
            }
        }

        // Fallback: token concat (best-effort)
        return string.Join("",
            frag.ScriptTokenStream!.Skip(frag.FirstTokenIndex)
                .Take(frag.LastTokenIndex - frag.FirstTokenIndex + 1)
                .Select(t => t.Text));
    }

    // ---------- Read/Write edges ----------
    public static void AddRead(HashSet<string> seen, List<ObjRef> list, SchemaObjectName? name)
    {
        if (name is null || name.BaseIdentifier is null) return;
        var (schema, obj, _) = NameOf(name);
        var key = (schema ?? "") + "|" + obj;
        if (seen.Add(key)) list.Add(new ObjRef(schema, obj));
    }

    public static void AddTargetWrite(TableReference? target, List<ObjRef> list, HashSet<string> seen)
    {
        if (target is null) return;

        SchemaObjectName? name = target switch
        {
            NamedTableReference nt => nt.SchemaObject,
            TableReferenceWithAlias ta when ta is NamedTableReference ntr => ntr.SchemaObject,
            _ => null
        };

        if (name is null) return;
        var (schema, obj, _) = NameOf(name);
        var key = (schema ?? "") + "|" + obj;
        if (seen.Add(key)) list.Add(new ObjRef(schema, obj));
    }

    // ---------- Parser factory (pick newest available parser) ----------
    public static TSqlParser CreateBestParser(bool initialQuotedIdentifiers = true)
    {
        var candidates = new[]
        {
            "Microsoft.SqlServer.TransactSql.ScriptDom.TSql170Parser",
            "Microsoft.SqlServer.TransactSql.ScriptDom.TSql160Parser",
            "Microsoft.SqlServer.TransactSql.ScriptDom.TSql150Parser",
            "Microsoft.SqlServer.TransactSql.ScriptDom.TSql140Parser",
            "Microsoft.SqlServer.TransactSql.ScriptDom.TSql130Parser",
        };

        foreach (var fullName in candidates)
        {
            var t = Type.GetType(fullName);
            if (t == null) continue;
            var ctor = t.GetConstructor(new[] { typeof(bool) });
            if (ctor != null) return (TSqlParser)ctor.Invoke(new object[] { initialQuotedIdentifiers });
        }

        return new TSql150Parser(initialQuotedIdentifiers);
    }
}

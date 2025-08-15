using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SqlCatalogApp;

internal sealed class ProcFuncVisitor : TSqlFragmentVisitor
{
    private readonly Catalog _catalog;
    public ProcFuncVisitor(Catalog catalog) => _catalog = catalog;

    public override void ExplicitVisit(CreateProcedureStatement node)
    {
        var (schema, name, _) = Helpers.NameOf(node.ProcedureReference.Name);
        if (!_catalog.Procedures.TryGetValue(name, out var p))
        {
            p = new ProcedureInfo { Schema = schema };
            _catalog.Procedures[name] = p;
        }

        // Params
        foreach (var prm in node.Parameters)
        {
            var pname = prm.VariableName?.Value?.TrimStart('@') ?? "";
            var ptype = prm.DataType is null ? "" : Helpers.SqlDataTypeToString(prm.DataType);
            p.Params.Add(new ParamInfo(pname, ptype));
        }

        var seenR = new HashSet<string>();
        var seenW = new HashSet<string>();
        var seenC = new HashSet<string>();

        foreach (var nt in node.GetDescendants<NamedTableReference>())
            Helpers.AddRead(seenR, p.Reads, nt.SchemaObject);

        foreach (var ins in node.GetDescendants<InsertStatement>())
            Helpers.AddTargetWrite(ins.InsertSpecification?.Target, p.Writes, seenW);

        foreach (var up in node.GetDescendants<UpdateStatement>())
            Helpers.AddTargetWrite(up.UpdateSpecification?.Target, p.Writes, seenW);

        foreach (var del in node.GetDescendants<DeleteStatement>())
            Helpers.AddTargetWrite(del.DeleteSpecification?.Target, p.Writes, seenW);

        // MERGE targets omitted for cross-version compatibility

        foreach (var exec in node.GetDescendants<ExecuteSpecification>())
        {
            var sobj = (exec.ExecutableEntity as ExecutableProcedureReference)
                       ?.ProcedureReference?.ProcedureReference?.Name;
            if (sobj?.BaseIdentifier != null)
            {
                var (ss, on, _) = Helpers.NameOf(sobj);
                var key = (ss ?? "") + "|" + on;
                if (seenC.Add(key)) p.Calls.Add(new ObjRef(ss, on));
            }
        }
    }

    public override void ExplicitVisit(CreateFunctionStatement node)
    {
        var (schema, name, _) = Helpers.NameOf(node.Name);
        if (!_catalog.Functions.TryGetValue(name, out var f))
        {
            f = new FunctionInfo { Schema = schema };
            _catalog.Functions[name] = f;
        }

        foreach (var prm in node.Parameters)
        {
            var pname = prm.VariableName?.Value?.TrimStart('@') ?? "";
            var ptype = prm.DataType is null ? "" : Helpers.SqlDataTypeToString(prm.DataType);
            f.Params.Add(new ParamInfo(pname, ptype));
        }

        var seenR = new HashSet<string>();
        var seenW = new HashSet<string>();
        var seenC = new HashSet<string>();

        foreach (var nt in node.GetDescendants<NamedTableReference>())
            Helpers.AddRead(seenR, f.Reads, nt.SchemaObject);

        foreach (var ins in node.GetDescendants<InsertStatement>())
            Helpers.AddTargetWrite(ins.InsertSpecification?.Target, f.Writes, seenW);

        foreach (var up in node.GetDescendants<UpdateStatement>())
            Helpers.AddTargetWrite(up.UpdateSpecification?.Target, f.Writes, seenW);

        foreach (var del in node.GetDescendants<DeleteStatement>())
            Helpers.AddTargetWrite(del.DeleteSpecification?.Target, f.Writes, seenW);

        foreach (var exec in node.GetDescendants<ExecuteSpecification>())
        {
            var sobj = (exec.ExecutableEntity as ExecutableProcedureReference)
                       ?.ProcedureReference?.ProcedureReference?.Name;
            if (sobj?.BaseIdentifier != null)
            {
                var (ss, on, _) = Helpers.NameOf(sobj);
                var key = (ss ?? "") + "|" + on;
                if (seenC.Add(key)) f.Calls.Add(new ObjRef(ss, on));
            }
        }
    }
}

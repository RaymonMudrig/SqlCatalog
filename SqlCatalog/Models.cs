using System.Text.Json.Serialization;

namespace SqlCatalogApp;

internal record ColumnInfo(string Type, bool Nullable, string? Default);

internal class TableInfo
{
    public string? Schema { get; set; }
    public string Original_Name { get; set; } = "";
    public string Safe_Name { get; set; } = "";
    public Dictionary<string, ColumnInfo> Columns { get; } = new();
    public List<string> Primary_Key { get; } = new();
    public Dictionary<string, List<string>> Indexes { get; } = new();
    public List<ForeignKeyRef> Foreign_Keys { get; } = new();
}

internal record ForeignKeyRef(
    string Column,
    string? Referenced_Schema,
    string Referenced_Table,
    string Referenced_Column,
    string Referenced_Table_Original
);

internal class ProcFuncBase
{
    public string? Schema { get; set; }
    public List<ParamInfo> Params { get; } = new();
    public List<ObjRef> Reads { get; } = new();
    public List<ObjRef> Writes { get; } = new();
    public List<ObjRef> Calls { get; } = new();

    [JsonIgnore]
    public bool Is_Read_Only => Writes.Count == 0;

    // Serialized convenience field: "read" (no writes) or "write" (has writes)
    public string Access => Is_Read_Only ? "read" : "write";
}

internal record ParamInfo(string Name, string Type);
internal record ObjRef(string? Schema, string Name);

internal class ProcedureInfo : ProcFuncBase { }
internal class FunctionInfo : ProcFuncBase { }

internal class ViewInfo
{
    public string? Schema { get; set; }
    public List<string> Columns { get; } = new();
    public List<ObjRef> Reads { get; } = new();
}

internal class Catalog
{
    public Dictionary<string, TableInfo> Tables { get; } = new();
    public Dictionary<string, ViewInfo> Views { get; } = new();
    public Dictionary<string, ProcedureInfo> Procedures { get; } = new();
    public Dictionary<string, FunctionInfo> Functions { get; } = new();

    [JsonIgnore]
    public Dictionary<string, List<string>> Dependencies =>
        Tables.ToDictionary(
            kv => kv.Key,
            kv => kv.Value.Foreign_Keys
                  .Select(f => f.Referenced_Table)
                  .Distinct()
                  .OrderBy(x => x)
                  .ToList()
        );
}

/* ---------- New: schema clustering index & export root ---------- */

internal class SchemaGroup
{
    public List<string> Tables { get; } = new();
    public List<string> Views { get; } = new();
    public List<string> Procedures_Read { get; } = new();
    public List<string> Procedures_Write { get; } = new();
    public List<string> Functions_Read { get; } = new();
    public List<string> Functions_Write { get; } = new();
}

internal class CatalogExport
{
    public Catalog Catalog { get; set; } = new();
    public Dictionary<string, SchemaGroup> Schema_Index { get; set; } = new();
}

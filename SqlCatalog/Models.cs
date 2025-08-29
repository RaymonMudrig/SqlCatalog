using System;
using System.Collections.Generic;

namespace SqlCatalogApp
{
    public sealed class Catalog
    {
        public Dictionary<string, TableInfo> Tables { get; } =
            new Dictionary<string, TableInfo>(StringComparer.OrdinalIgnoreCase);

        public Dictionary<string, ViewInfo> Views { get; } =
            new Dictionary<string, ViewInfo>(StringComparer.OrdinalIgnoreCase);

        public Dictionary<string, ProcedureInfo> Procedures { get; } =
            new Dictionary<string, ProcedureInfo>(StringComparer.OrdinalIgnoreCase);

        // Computed at the end of Program.cs
        public List<string> Unused_Tables { get; } = new List<string>();
        public List<UnusedColumn> Unused_Columns { get; } = new List<UnusedColumn>();
    }

    public sealed class TableInfo
    {
        public string Schema { get; set; } = "";
        public string Original_Name { get; set; } = "";
        public string Safe_Name { get; set; } = "";
        public string? Doc { get; set; }

        public Dictionary<string, ColumnInfo> Columns { get; } =
            new Dictionary<string, ColumnInfo>(StringComparer.OrdinalIgnoreCase);

        public List<string> Primary_Key { get; } = new List<string>();

        public List<ForeignKeyRef> Foreign_Keys { get; } = new List<ForeignKeyRef>();

        public Dictionary<string, List<string>> Indexes { get; } =
            new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

        public List<ObjRef> Referenced_By { get; } = new List<ObjRef>(); // who reads/writes this table
        public bool Is_Unused { get; set; }
    }

    public sealed record ColumnInfo(string Type, bool Nullable, string? Default, string? Doc = null)
    {
        public List<UsageRef> Referenced_In { get; } = new List<UsageRef>(); // column-level usage
    }

    public sealed class ViewInfo
    {
        public string Schema { get; set; } = "";
        public string Original_Name { get; set; } = "";
        public string Safe_Name { get; set; } = "";

        public List<string> Columns { get; } = new List<string>(); // select list (may include "*")
        public List<ObjRef> Reads { get; } = new List<ObjRef>();    // tables it reads
        public string? Doc { get; set; }
    }

    public sealed class ProcedureInfo
    {
        public string Schema { get; set; } = "";
        public string Original_Name { get; set; } = "";
        public string Safe_Name { get; set; } = "";

        public List<string> Params { get; } = new List<string>();
        public List<ObjRef> Reads { get; } = new List<ObjRef>();
        public List<ObjRef> Writes { get; } = new List<ObjRef>();   // <-- fixed
        public List<ObjRef> Calls { get; } = new List<ObjRef>();

        // key = safe table ("schema·name"), values = set of column names referenced
        public Dictionary<string, HashSet<string>> Column_Refs { get; } =
            new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

        public string? Doc { get; set; }
    }

    public sealed record ForeignKeyRef(
        string Local_Column,
        string Ref_Schema,
        string Ref_Table,
        string Ref_Column,
        string Ref_Table_Original
    );

    public sealed record ObjRef(string? Schema, string Safe_Name);

    public sealed record UsageRef(string Kind, string Safe_Name, string Context);

    public sealed record UnusedColumn(string Table, string Column);
}

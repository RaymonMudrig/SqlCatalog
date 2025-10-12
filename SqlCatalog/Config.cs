namespace SqlCatalog;

internal static class Config
{
    public static readonly string SqlDir = "../sql_files";
    public static readonly string OutputDir = "../output";
    public static readonly string CatalogPath = Path.Combine(OutputDir, "catalog.json");
    public const char SafeSpace = '.'; // Changed from '·' (U+00B7) to '.' for cleaner display
}

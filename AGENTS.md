# Repository Guidelines

## Project Structure & Module Organization
- `SqlCatalog/` (C#) implements the SQL DOM visitors that parse schemas and emit catalog assets. The entry point is `SqlCatalog/Program.cs`; runtime settings live in `Config.cs`.
- `VectorizeCatalog/` (Python) hosts the FastAPI web UI, CLI, and agent logic under `qcat/` and `qcli/`; front-end assets sit in `VectorizeCatalog/static/`.
- `sql_files/` collects source DDL scripts that seed the catalog. Generated artifacts surface in `output/` (`catalog.json`, `sql_exports/`), while ad-hoc comparisons live next to the repo root as `compare_*.sql`.

## Build, Test, and Development Commands
- `dotnet build SqlCatalog/SqlCatalog.sln` builds the C# cataloger; run it after changing any visitor or model.
- `python3 VectorizeCatalog/webapp.py` starts the FastAPI server for the agent UI at `http://localhost:8000`.
- `python3 VectorizeCatalog/cli.py "what tables..."` exercises the deterministic CLI end to end.
- `python3 VectorizeCatalog/vectorize_catalog.py` refreshes optional semantic embeddings when you add new SQL files.

## Coding Style & Naming Conventions
- C#: match existing 4-space indentation, PascalCase types, camelCase locals, and keep visitors side-effect free; place helper extensions in `DomExtensions.cs`.
- Python: follow PEP 8 with type hints; prefer pure functions inside `qcat/*` and keep FastAPI routes thin. Use f-strings for formatting and guard top-level scripts with `if __name__ == "__main__":`.
- SQL snippets placed in `sql_files/` should be formatted with uppercase keywords and schema-qualified names.

## Testing Guidelines
- Run `python3 VectorizeCatalog/test_regression.py` to validate agent intents; add cases covering new operations or answer formats.
- For C# changes, add targeted console smoke checks (e.g., `dotnet run --project SqlCatalog/SqlCatalog.csproj`) before regenerating `catalog.json`.
- Capture new expected outputs in `output/` only after verifying they reflect intentional catalog changes.

## Commit & Pull Request Guidelines
- Write short, imperative commit subjects (e.g., “Add memory and sql highlighting”); avoid stacking unrelated edits.
- PRs should summarize behavioral changes, link the motivating issue, and include before/after snippets or screenshots for UI or catalog diffs.
- Note any manual steps (re-running catalog, refreshing embeddings) so reviewers can reproduce your environment.

## Environment & Configuration Tips
- Update `Config.cs` or `state.json` only when defaults change; otherwise keep local secrets in a `.env` outside version control.
- Use `write_state.py` to script reproducible catalog states instead of editing JSON by hand.

#!/usr/bin/env python3
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# === CONFIG (edit to match your repo layout) ===
BASE_DIR = Path(__file__).parent.resolve()

# Exact files to track
FILES_TO_TRACK = [
    "VectorizeCatalog/vectorize_catalog.py",
    "VectorizeCatalog/query_catalog.py",
    "VectorizeCatalog/rag_chat.py",
    "VectorizeCatalog/detect_unaccessed_table.py"
]

# Folders to track as a single "artifact" (hash = all file contents combined)
FOLDERS_TO_TRACK = [
    "SqlCatalog"
]

OUTPUT_FILE = BASE_DIR / "state.json"
REQUIREMENTS_FILE = BASE_DIR / "requirements.txt"  # optional but preferred


# === HASH HELPERS ===
def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def folder_sha256(path: Path) -> str:
    h = hashlib.sha256()
    for root, _, files in os.walk(path):
        # Sort for determinism
        for fname in sorted(files):
            fpath = Path(root) / fname
            # Skip typical build outputs
            if any(part in {"bin", "obj", ".git"} for part in fpath.parts):
                continue
            try:
                h.update(file_sha256(fpath).encode())
            except Exception:
                # If something is unreadable, skip but keep going
                pass
    return h.hexdigest()


# === PIP ENV HELPERS ===
def pip_freeze_from_requirements(req_path: Path) -> list[str]:
    """
    Parse requirements.txt lines (ignoring comments/empty lines).
    """
    lines = []
    for line in req_path.read_text().splitlines():
        s = line.strip()
        if not s or s.startswith("#") or s.startswith("-e ") or s.startswith("--"):
            continue
        lines.append(s)
    return lines


def pip_list_freeze() -> list[str]:
    """
    Use the current interpreter's pip to list installed packages in freeze format.
    This respects your active venv automatically.
    """
    # Safer than calling "pip": uses the same interpreter running this script
    cmd = [sys.executable, "-m", "pip", "list", "--format=freeze"]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    pkgs = [ln.strip() for ln in res.stdout.splitlines() if ln.strip()]
    # Optional: remove pip/setuptools/wheel noise
    noisy = {"pip", "setuptools", "wheel"}
    filtered = [p for p in pkgs if p.split("==", 1)[0].lower() not in noisy]
    return sorted(filtered, key=str.lower)


def get_pip_packages(prefer_requirements: bool = True) -> list[str]:
    """
    If requirements.txt exists and prefer_requirements=True, use it.
    Otherwise fall back to full environment freeze.
    """
    if prefer_requirements and REQUIREMENTS_FILE.exists():
        pkgs = pip_freeze_from_requirements(REQUIREMENTS_FILE)
        if pkgs:
            return sorted(pkgs, key=str.lower)
    return pip_list_freeze()


# === MAIN ===
def main():
    artifacts = []

    # Add file artifacts
    for rel_path in FILES_TO_TRACK:
        full_path = BASE_DIR / rel_path
        if full_path.exists():
            artifacts.append({
                "name": full_path.name,
                "path": str(rel_path),
                "language": "python" if full_path.suffix == ".py" else "unknown",
                "sha256": file_sha256(full_path),
            })
        else:
            print(f"⚠ Missing file: {rel_path}")

    # Add folder artifacts
    for rel_folder in FOLDERS_TO_TRACK:
        full_path = BASE_DIR / rel_folder
        if full_path.exists():
            artifacts.append({
                "name": rel_folder,
                "path": str(rel_folder),
                "language": "C#",
                "sha256": folder_sha256(full_path),
            })
        else:
            print(f"⚠ Missing folder: {rel_folder}")

    pip_packages = get_pip_packages(prefer_requirements=True)

    state = {
        "updated_at": datetime.now().astimezone().isoformat(),
        "artifacts": artifacts,
        "environment": {
            "dotnet": ">= 8.0",
            "python": f">= {sys.version_info.major}.{sys.version_info.minor}",
            "pip_packages": pip_packages,
        },
        "next_steps": [
            "Describe your next tasks here so future sessions can resume instantly."
        ],
    }

    OUTPUT_FILE.write_text(json.dumps(state, indent=2))
    print(f"✅ State saved to {OUTPUT_FILE}")
    print(f"📦 Packages recorded: {len(pip_packages)} "
          f"({'requirements.txt' if REQUIREMENTS_FILE.exists() else 'pip freeze'})")


if __name__ == "__main__":
    main()

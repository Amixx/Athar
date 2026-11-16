"""Build a persistent demo Git repository from real IFC revision files.

Creates a standalone repository whose history is real model revisions
committed as successive versions of one ``model.ifc``, with the Athar diff
driver and difftool installed repo-locally. Point any Git client at it:
``git diff``, ``git log -p --ext-diff``, and ``git difftool -t athar`` all
show semantic IFC output.

Two version sets:

- default: the four Building-Landscaping revisions (~1.2 MB each, fast);
- ``--revit``: the real Revit round-trip IFC4 chain r2..r10 from
  ``corpus/roundtrip-revit-2026-07-06`` (~46-49 MB each, nine revisions whose
  edits are documented in that corpus's MANIFEST; sources are decompressed
  from ``.zst`` on demand).

Also reports how well Git's own packfile delta compression stores the lineage,
as a baseline against the athar_store delta pack.

Usage: python scripts/build_git_demo_repo.py [--dest DIR] [--force] [--revit]
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

LANDSCAPING_VERSIONS = [
    ("real-world-test/Building-Landscaping-v0-original.ifc", "v0: original landscaping model"),
    ("real-world-test/Building-Landscaping-v1.ifc", "v1: revised landscaping"),
    ("real-world-test/Building-Landscaping-v2.ifc", "v2: revised landscaping"),
    ("real-world-test/Building-Landscaping-v3.ifc", "v3: revised landscaping"),
]

_REVIT_EXPORTS = "corpus/roundtrip-revit-2026-07-06/exports"

REVIT_VERSIONS = [
    (f"{_REVIT_EXPORTS}/r2.ifc", "r2: IFC4 Reference View [architecture] export, no edits"),
    (f"{_REVIT_EXPORTS}/r3.ifc", "r3: + keep GUIDs export option, no edits"),
    (f"{_REVIT_EXPORTS}/r4.ifc", "r4: + export Revit property sets, no edits"),
    (f"{_REVIT_EXPORTS}/r5.ifc", "r5: moved whole extruded facade away from building"),
    (f"{_REVIT_EXPORTS}/r6.ifc", "r6: deleted a row of windows (one merged proxy)"),
    (f"{_REVIT_EXPORTS}/r7.ifc", 'r7: one door instance property "Frame type" = "Test frame type"'),
    (f"{_REVIT_EXPORTS}/r8.ifc", "r8: duplicated that door twice onto other walls, same level"),
    (f"{_REVIT_EXPORTS}/r9.ifc", "r9: re-export with no changes (determinism pair with r8)"),
    (f"{_REVIT_EXPORTS}/r10.ifc", "r10: same door, one type property value changed"),
]

DRIVER_COMMAND = f'PYTHONPATH="{REPO_ROOT}" "{sys.executable}" -m athar git diff --external'
DIFFTOOL_COMMAND = f'PYTHONPATH="{REPO_ROOT}" "{sys.executable}" -m athar view "$LOCAL" "$REMOTE"'


def _run(args: list[str], cwd: Path) -> str:
    result = subprocess.run(args, cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        raise SystemExit(f"{' '.join(args)} failed ({result.returncode}): {result.stderr.strip()}")
    return result.stdout


def _mb(size_bytes: int) -> str:
    return f"{size_bytes / 1_000_000:.1f} MB"


def _dir_size(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def _resolve_source(rel: str) -> Path:
    src = REPO_ROOT / rel
    if src.is_file() and src.stat().st_size > 10_000:
        return src
    zst = src.with_name(src.name + ".zst")
    if zst.is_file() and zst.stat().st_size > 10_000:
        print(f"decompressing {zst.name}")
        result = subprocess.run(
            ["zstd", "-d", "-k", "-q", "-f", str(zst), "-o", str(src)],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise SystemExit(f"zstd -d {zst} failed: {result.stderr.strip()}")
        return src
    raise SystemExit(f"missing or unfetched source file: {src} (run `git lfs pull`?)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dest", default=str(Path.home() / "athar-git-demo"), help="Demo repo directory")
    parser.add_argument("--force", action="store_true", help="Recreate the directory if it exists")
    parser.add_argument(
        "--revit",
        action="store_true",
        help="Use the nine-revision Revit round-trip IFC4 chain instead of Building-Landscaping",
    )
    args = parser.parse_args()

    versions = REVIT_VERSIONS if args.revit else LANDSCAPING_VERSIONS
    sources = [(_resolve_source(rel), message) for rel, message in versions]

    dest = Path(args.dest).expanduser().resolve()
    if dest.exists():
        if not args.force:
            raise SystemExit(f"{dest} already exists; pass --force to recreate it")
        shutil.rmtree(dest)
    dest.mkdir(parents=True)

    _run(["git", "init", "-q"], cwd=dest)
    _run(["git", "config", "diff.athar.command", DRIVER_COMMAND], cwd=dest)
    _run(["git", "config", "diff.athar.binary", "true"], cwd=dest)
    _run(["git", "config", "difftool.athar.cmd", DIFFTOOL_COMMAND], cwd=dest)
    (dest / ".gitattributes").write_text("*.ifc diff=athar -merge\n", encoding="utf-8")

    logical_bytes = 0
    for src, message in sources:
        shutil.copyfile(src, dest / "model.ifc")
        logical_bytes += src.stat().st_size
        _run(["git", "add", "-A"], cwd=dest)
        _run(["git", "commit", "-q", "-m", message], cwd=dest)
        print(f"committed {message} ({_mb(src.stat().st_size)})")

    gc_args = ["git", "gc", "-q"]
    if logical_bytes < 50_000_000:
        gc_args.append("--aggressive")
    _run(gc_args, cwd=dest)
    packed = _dir_size(dest / ".git" / "objects")
    print()
    print(f"history: {len(sources)} versions, {_mb(logical_bytes)} of IFC bytes")
    print(f"git object store after gc: {_mb(packed)} ({packed / logical_bytes:.0%} of logical size)")
    print()
    print(f"demo repo ready at {dest}")
    print("try:")
    print(f"  git -C {dest} log --oneline")
    print(f"  git -C {dest} diff HEAD~1 HEAD")
    print(f"  git -C {dest} log -p --ext-diff")
    print(f"  git -C {dest} difftool -t athar -y HEAD~1 HEAD")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

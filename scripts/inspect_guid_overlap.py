#!/usr/bin/env python3
"""Show entity GUID overlap between IFC files.

Helps validate whether files are versions of the same model (high overlap)
or unrelated models (low overlap). Used to test grouping heuristics.

Usage:
    python scripts/inspect_guid_overlap.py data/ real-world-test/
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import ifcopenshell


def get_guids(path: Path) -> set[str]:
    """Extract all IfcProduct GlobalIds from an IFC file."""
    ifc = ifcopenshell.open(str(path))
    return {e.GlobalId for e in ifc.by_type("IfcProduct")}


def collect_ifc_files(paths: list[str]) -> list[Path]:
    result = []
    for p in paths:
        p = Path(p)
        if p.is_dir():
            result.extend(
                f for f in sorted(p.iterdir())
                if f.suffix.lower() == ".ifc" and f.is_file()
            )
        elif p.is_file() and p.suffix.lower() == ".ifc":
            result.append(p)
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Show entity GUID overlap between IFC files",
    )
    parser.add_argument("paths", nargs="+", help="IFC files or directories")
    args = parser.parse_args()

    files = collect_ifc_files(args.paths)
    if len(files) < 2:
        print("Need at least 2 IFC files.")
        sys.exit(1)

    # Collect GUIDs per file
    file_guids: list[tuple[Path, set[str]]] = []
    for f in files:
        print(f"Reading {f.name}...", file=sys.stderr)
        file_guids.append((f, get_guids(f)))

    print(f"\nGUID overlap matrix ({len(files)} files):\n")

    # Header
    names = [f.name[:25] for f, _ in file_guids]
    print(f"{'':>28}", end="")
    for i, n in enumerate(names):
        print(f" [{i}]{n:>22}", end="")
    print()

    for i, (fi, gi) in enumerate(file_guids):
        print(f"[{i}] {names[i]:>25}", end="")
        for j, (fj, gj) in enumerate(file_guids):
            if i == j:
                count = len(gi)
                print(f" {count:>25}", end="")
            else:
                overlap = len(gi & gj)
                total = len(gi | gj)
                pct = (overlap / total * 100) if total else 0
                print(f" {overlap:>5}/{total:<5} {pct:>5.0f}%   ", end="")
        print()


if __name__ == "__main__":
    main()

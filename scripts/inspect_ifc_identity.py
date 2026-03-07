#!/usr/bin/env python3
"""Inspect IFC files to show their project identity and timestamps.

Useful for understanding how files would be grouped by Athar's folder diff.
Shows IfcProject name/GlobalId + header timestamp for each file.

Usage:
    python scripts/inspect_ifc_identity.py data/ real-world-test/
    python scripts/inspect_ifc_identity.py some_file.ifc
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import ifcopenshell


def inspect_file(path: Path) -> dict:
    """Extract lightweight identity info from an IFC file."""
    ifc = ifcopenshell.open(str(path))
    projects = ifc.by_type("IfcProject")
    project_name = projects[0].Name if projects else None
    project_guid = projects[0].GlobalId if projects else None
    timestamp = ifc.header.file_name.time_stamp or None
    schema = ifc.schema
    return {
        "file": path.name,
        "project_name": project_name,
        "project_guid": project_guid,
        "timestamp": timestamp,
        "schema": schema,
    }


def collect_ifc_files(paths: list[str]) -> list[Path]:
    """Collect .ifc files from a mix of files and directories."""
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
        else:
            print(f"Skipping {p} (not an IFC file or directory)", file=sys.stderr)
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Show IFC project identity and timestamps for grouping analysis",
    )
    parser.add_argument("paths", nargs="+", help="IFC files or directories to inspect")
    args = parser.parse_args()

    files = collect_ifc_files(args.paths)
    if not files:
        print("No IFC files found.")
        sys.exit(1)

    for f in files:
        info = inspect_file(f)
        print(f"{info['file']}")
        print(f"  Project:   {info['project_name']!r}  (GlobalId: {info['project_guid']})")
        print(f"  Timestamp: {info['timestamp']}")
        print(f"  Schema:    {info['schema']}")
        print()


if __name__ == "__main__":
    main()

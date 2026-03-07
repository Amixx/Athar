#!/usr/bin/env python3
"""Diff all IFC file versions in a folder, oldest to newest.

Sorts files by modification time and diffs each consecutive pair,
printing a step-by-step textual summary.

Usage:
    python scripts/diff_folder.py real-world-test/
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Allow running as script from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from athar.parser import parse
from athar.differ import diff


def find_ifc_files(folder: str) -> list[Path]:
    """Find all .ifc files in folder, sorted by modification time (oldest first)."""
    files = [
        p for p in Path(folder).iterdir()
        if p.suffix.lower() == ".ifc" and p.is_file()
    ]
    files.sort(key=lambda p: p.stat().st_mtime)
    return files


def format_summary(result: dict, old_path: Path, new_path: Path) -> str:
    """Format a single diff result as readable text."""
    lines = []

    lines.append(f"  {old_path.name}  →  {new_path.name}")

    s = result["summary"]
    lines.append(f"  {s['added']} added, {s['deleted']} deleted, "
                 f"{s['changed']} changed, {s['unchanged']} unchanged")

    if result["added"]:
        for e in result["added"]:
            lines.append(f"    + [{e['ifc_class']}] {e['name'] or '(unnamed)'}")

    if result["deleted"]:
        for e in result["deleted"]:
            lines.append(f"    - [{e['ifc_class']}] {e['name'] or '(unnamed)'}")

    if result["changed"]:
        for e in result["changed"]:
            lines.append(f"    ~ [{e['ifc_class']}] {e['name'] or '(unnamed)'}")
            for c in e["changes"]:
                if c["field"] == "placement":
                    desc = c.get("description", "(matrix changed)")
                    lines.append(f"        {desc}")
                else:
                    lines.append(f"        {c['field']}: {c['old']!r} → {c['new']!r}")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Diff all IFC versions in a folder, oldest to newest",
    )
    parser.add_argument("folder", help="Folder containing IFC file versions")
    args = parser.parse_args()

    files = find_ifc_files(args.folder)

    if len(files) < 2:
        print(f"Need at least 2 IFC files in {args.folder}, found {len(files)}.")
        sys.exit(1)

    print(f"Found {len(files)} IFC files in {args.folder}/\n")
    for i, f in enumerate(files):
        print(f"  [{i}] {f.name}")
    print()

    # Parse all files upfront
    parsed = []
    for f in files:
        print(f"Parsing {f.name}...", file=sys.stderr)
        parsed.append(parse(str(f)))

    print(f"\n{'='*60}")
    print(f"STEP-BY-STEP DIFF ({len(files) - 1} transitions)")
    print(f"{'='*60}\n")

    for i in range(len(files) - 1):
        print(f"Step {i + 1}: ", end="")
        result = diff(parsed[i], parsed[i + 1])
        print(format_summary(result, files[i], files[i + 1]))
        print()

    # Cumulative summary if more than 2 files
    if len(files) > 2:
        print(f"{'='*60}")
        print(f"CUMULATIVE (first → last)")
        print(f"{'='*60}\n")
        result = diff(parsed[0], parsed[-1])
        print(format_summary(result, files[0], files[-1]))
        print()


if __name__ == "__main__":
    main()

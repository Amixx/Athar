"""Scan a folder of IFC files, group by model identity, sort by timestamp."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import ifcopenshell


# Files sharing more than this fraction of entity GUIDs are considered
# versions of the same model.
_OVERLAP_THRESHOLD = 0.50

# Content-based fallback: files sharing more than this fraction of
# (ifc_class, name) fingerprints are considered versions of the same model.
_CONTENT_OVERLAP_THRESHOLD = 0.60


def scan_folder(folder: str) -> list[list[Path]]:
    """Find IFC files in a folder, group by model, sort each group by timestamp.

    Returns a list of groups. Each group is a list of Paths (oldest first)
    containing at least 2 files that are versions of the same model.
    Groups are identified by entity GUID overlap — files that share >50%
    of their IfcProduct GlobalIds are considered versions of the same model.
    When GUID overlap is low (suggesting regenerated GUIDs), falls back to
    content fingerprint overlap.

    Files that don't belong to any multi-file group are silently skipped.
    """
    ifc_files = [
        p for p in Path(folder).iterdir()
        if p.suffix.lower() == ".ifc" and p.is_file()
    ]

    if len(ifc_files) < 2:
        return []

    # Lightweight scan: extract GUIDs, content fingerprints, and timestamp per file
    file_info: list[tuple[Path, set[str], Counter, str | None]] = []
    for path in ifc_files:
        ifc = ifcopenshell.open(str(path))
        guids = {e.GlobalId for e in ifc.by_type("IfcProduct")}
        # Content fingerprint: multiset of (ifc_class, name) — survives GUID regeneration
        fingerprints = Counter(
            (e.is_a(), e.Name) for e in ifc.by_type("IfcProduct")
        )
        timestamp = ifc.header.file_name.time_stamp or None
        file_info.append((path, guids, fingerprints, timestamp))

    # Group by GUID overlap using union-find, with content fallback
    n = len(file_info)
    parent = list(range(n))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for i in range(n):
        for j in range(i + 1, n):
            gi = file_info[i][1]
            gj = file_info[j][1]
            total = len(gi | gj)

            # Try GUID overlap first
            if total > 0 and len(gi & gj) / total >= _OVERLAP_THRESHOLD:
                union(i, j)
                continue

            # Fallback: content fingerprint overlap (multiset intersection)
            fi = file_info[i][2]
            fj = file_info[j][2]
            shared = sum((fi & fj).values())
            total_fp = sum((fi | fj).values())
            if total_fp > 0 and shared / total_fp >= _CONTENT_OVERLAP_THRESHOLD:
                union(i, j)

    # Collect groups
    groups: dict[int, list[int]] = {}
    for i in range(n):
        root = find(i)
        groups.setdefault(root, []).append(i)

    # Build result: only groups with 2+ files, sorted by timestamp
    result = []
    for indices in groups.values():
        if len(indices) < 2:
            continue
        group = [(file_info[i][0], file_info[i][3]) for i in indices]
        # Sort by IFC header timestamp (None sorts first)
        group.sort(key=lambda x: x[1] or "")
        result.append([path for path, _ in group])

    # Sort groups by their first file's name for stable output
    result.sort(key=lambda g: g[0].name)
    return result

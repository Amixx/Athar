#!/usr/bin/env python3
"""Run Speckle's IFC diff semantics on two IFC files, fully locally.

Conversion uses ``speckleifc`` — Speckle's production IFC importer, shipped
inside ``specklepy`` and itself built on ifcopenshell. Object ids are the
content hashes computed by specklepy's ``BaseObjectSerializer``; a Speckle
server only stores those hashes, so no server is involved here.

Classification mirrors Speckle's version-diff semantics (viewer and the
official "Version Diffing with Python" tutorial): per object,
``applicationId`` (= IFC GlobalId for elements) is the correlation key and the
content hash decides the verdict — same id on both sides is unchanged, same
applicationId with a different id is modified, the rest is added/deleted.
Child hashes bubble into parents (Merkle-style), so a container whose child
changed also counts as modified — that is Speckle behavior, not an artifact.

Counts cover DataObjects only — the visible elements Speckle's viewer diff
colors. Meshes, chunks, and instance proxies influence their parents' hashes
but are not counted. Collections (project/spatial grouping) are excluded:
speckleifc's proxy lists (render materials, instance definitions) have
nondeterministic ordering, so Collection hashes differ even between two
imports of the identical file. Objects without an applicationId are excluded.

Usage:
    python scripts/explore/speckle_diff_runner.py old.ifc new.ifc
"""

from __future__ import annotations

import argparse
import contextlib
import importlib.metadata
import json
import sys
import time
from collections import Counter

_ELEMENT_TYPES = ("Objects.Data.DataObject",)


class _NoProgress:
    def report(self, *args: object, **kwargs: object) -> None:
        pass

    def should_report_progress(self) -> bool:
        return False


def _convert_and_hash(path: str) -> dict[str, list[str]]:
    """Convert one IFC file and return {applicationId: [object ids]} for elements."""
    from speckleifc.ifc_geometry_processing import open_ifc
    from speckleifc.importer import ImportJob
    from specklepy.serialization.base_object_serializer import BaseObjectSerializer
    from specklepy.transports.memory import MemoryTransport

    # speckleifc prints progress to stdout; stdout must stay JSON-only.
    with contextlib.redirect_stdout(sys.stderr):
        ifc_file = open_ifc(path)
        root = ImportJob(ifc_file, _NoProgress()).convert()

    transport = MemoryTransport()
    serializer = BaseObjectSerializer(write_transports=[transport])
    serializer.traverse_base(root)

    elements: dict[str, list[str]] = {}
    for object_id in transport.objects:
        payload = json.loads(transport.get_object(object_id))
        if payload.get("speckle_type") not in _ELEMENT_TYPES:
            continue
        application_id = payload.get("applicationId")
        if not application_id:
            continue
        elements.setdefault(application_id, []).append(object_id)
    return elements


def _classify(old: dict[str, list[str]], new: dict[str, list[str]]) -> dict[str, int]:
    counts = {"added": 0, "deleted": 0, "modified": 0, "unchanged": 0}
    for application_id in old.keys() | new.keys():
        old_ids = Counter(old.get(application_id, []))
        new_ids = Counter(new.get(application_id, []))
        identical = sum((old_ids & new_ids).values())
        remaining_old = sum(old_ids.values()) - identical
        remaining_new = sum(new_ids.values()) - identical
        paired = min(remaining_old, remaining_new)
        counts["unchanged"] += identical
        counts["modified"] += paired
        counts["deleted"] += remaining_old - paired
        counts["added"] += remaining_new - paired
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("old_path")
    parser.add_argument("new_path")
    args = parser.parse_args()

    try:
        start = time.perf_counter()
        old_elements = _convert_and_hash(args.old_path)
        old_done = time.perf_counter()
        new_elements = _convert_and_hash(args.new_path)
        new_done = time.perf_counter()
    except Exception as exc:  # noqa: BLE001 - single-purpose CLI, report and exit
        print(json.dumps({"error": f"{type(exc).__name__}: {exc}"}))
        return 1

    result = {
        "tool": "speckle_local",
        "specklepy_version": importlib.metadata.version("specklepy"),
        "counts": _classify(old_elements, new_elements),
        "elements": {
            "old": sum(len(ids) for ids in old_elements.values()),
            "new": sum(len(ids) for ids in new_elements.values()),
        },
        "seconds": {
            "convert_old": round(old_done - start, 3),
            "convert_new": round(new_done - old_done, 3),
        },
    }
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

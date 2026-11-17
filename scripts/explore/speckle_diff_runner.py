#!/usr/bin/env python3
"""Diff two IFC files by converting them with Speckle's importer and comparing an Athar-defined digest.

Conversion uses ``speckleifc`` — Speckle's production IFC importer, shipped
inside ``specklepy`` and itself built on ifcopenshell. The importer writes a
Parquet bundle (EAV property rows, instance/definition/material nodes,
geometry blobs and the relations between them) that a Speckle server ingests;
no server is involved here.

Each object's content digest is a SHA-256 over its own property rows, its type
rows, and everything reachable through its outgoing relations: geometry bytes,
placement transforms, materials, levels and containers resolved by content,
other objects by applicationId. The digest is defined here, not by Speckle:
Speckle itself compares serialized object IDs, which this runner does not
compute. Per object, ``applicationId`` (= IFC GlobalId) is the correlation
key and the digest decides the verdict — same digest on both sides is
unchanged, same applicationId with a different digest is modified, the rest
is added/deleted. Objects without an applicationId are excluded.

Usage:
    python scripts/explore/speckle_diff_runner.py old.ifc new.ifc
"""

from __future__ import annotations

import argparse
import contextlib
import fnmatch
import hashlib
import importlib.metadata
import json
import os
import sys
import tempfile
import time
from collections import Counter, defaultdict

import pyarrow.parquet as pq

_BUNDLE_NAME = "bundle"


class _NoProgress:
    def report(self, *args: object, **kwargs: object) -> None:
        pass

    def should_report_progress(self) -> bool:
        return False


def _convert_and_hash(path: str) -> dict[str, list[str]]:
    """Convert one IFC file and return {applicationId: [content digests]}."""
    from speckleifc.ifc_geometry_processing import open_ifc
    from speckleifc.importer import ImportJob

    # speckleifc prints progress to stdout; stdout must stay JSON-only.
    with contextlib.redirect_stdout(sys.stderr), tempfile.TemporaryDirectory(prefix="speckle-bundle-") as bundle_dir:
        ImportJob(open_ifc(path), bundle_dir, _BUNDLE_NAME, _NoProgress()).run()
        return _object_digests(bundle_dir)


def _rows(directory: str, pattern: str) -> list[dict]:
    names = sorted(name for name in os.listdir(directory) if fnmatch.fnmatch(name, pattern))
    return [row for name in names for row in pq.read_table(os.path.join(directory, name)).to_pylist()]


def _object_digests(directory: str) -> dict[str, list[str]]:
    paths = {row["path_index"]: row["path"] for row in _rows(directory, "*.eav.paths.parquet")}

    def property_rows(pattern: str, key: str) -> dict[int, list]:
        grouped: dict[int, list] = defaultdict(list)
        for row in _rows(directory, pattern):
            value = (paths[row["path_index"]], row["value_string"], row["value_double"], row["value_boolean"], row["unit"])
            grouped[row[key]].append(value)
        return {k: sorted(values, key=repr) for k, values in grouped.items()}

    instance_properties = property_rows("*.eav.eav.parquet", "object_index")
    type_properties = property_rows("*.eav.type_eav.parquet", "type_index")
    type_of = {row["object_index"]: row["type_index"] for row in _rows(directory, "*.eav.object_type.parquet")}
    app_ids = {row["object_index"]: row["application_id"] for row in _rows(directory, "*.eav.objects.parquet")}
    nodes = {row["id"]: row for row in _rows(directory, "*.envelope.nodes.parquet")}
    geometry_bytes = {
        row["geometryIndex"]: (row["type"], hashlib.sha256(row["content"] or b"").hexdigest())
        for row in _rows(directory, "*.geometries*.parquet")
    }
    namespaces = {row["rel"]: (row["src_ns"], row["dst_ns"]) for row in _rows(directory, "*.envelope.rel_types.parquet")}
    edges: dict[tuple[str, int], list[tuple[int, int, str, int]]] = defaultdict(list)
    for row in _rows(directory, "*.envelope.relations.parquet"):
        src_ns, dst_ns = namespaces[row["rel"]]
        edges[(src_ns, row["src"])].append((row["rel"], row["ord"], dst_ns, row["dst"]))

    memo: dict[tuple[str, int], str] = {}

    def digest(namespace: str, key: int) -> str:
        if namespace == "object":
            return f"object:{app_ids[key]}"
        if (namespace, key) in memo:
            return memo[(namespace, key)]
        memo[(namespace, key)] = "cycle"
        if namespace == "node":
            node = nodes[key]
            definition = digest("node", node["def_ref"]) if node["def_ref"] is not None else None
            own = ([(name, value) for name, value in sorted(node.items()) if name not in ("id", "def_ref")], definition)
        else:
            own = geometry_bytes[key]
        out = sorted((rel, ordinal, digest(dst_ns, dst)) for rel, ordinal, dst_ns, dst in edges[(namespace, key)])
        result = hashlib.sha256(repr((own, out)).encode()).hexdigest()
        memo[(namespace, key)] = result
        return result

    elements: dict[str, list[str]] = {}
    for key, application_id in app_ids.items():
        if not application_id:
            continue
        content = (
            instance_properties.get(key, []),
            type_properties.get(type_of.get(key), []),
            sorted((rel, ordinal, digest(dst_ns, dst)) for rel, ordinal, dst_ns, dst in edges[("object", key)]),
        )
        elements.setdefault(application_id, []).append(hashlib.sha256(repr(content).encode()).hexdigest())
    return elements


def _classify(
    old: dict[str, list[str]], new: dict[str, list[str]]
) -> tuple[dict[str, int], dict[str, list[str]]]:
    counts = {"added": 0, "deleted": 0, "modified": 0, "unchanged": 0}
    reported_guids: dict[str, list[str]] = {"added": [], "deleted": [], "modified": []}
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
        if paired:
            reported_guids["modified"].extend([application_id] * paired)
        if remaining_new - paired:
            reported_guids["added"].extend([application_id] * (remaining_new - paired))
        if remaining_old - paired:
            reported_guids["deleted"].extend([application_id] * (remaining_old - paired))
    return counts, reported_guids


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

    counts, reported_guids = _classify(old_elements, new_elements)
    result = {
        "tool": "speckle_digest",
        "specklepy_version": importlib.metadata.version("specklepy"),
        "counts": counts,
        "reported_guids": reported_guids,
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

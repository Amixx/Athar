#!/usr/bin/env python3
"""Profile the current bottom-layer signature bundle stages.

This is intentionally small and mirrors ``build_signature_bundle`` so native
ports can compare parse/edge/Merkle/WL/spatial costs one function at a time.

Usage:
    python scripts/explore/profile_signature_bundle.py data/BasicHouse.ifc --out profile.json
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Callable, TypeVar

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

T = TypeVar("T")


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    results = {
        "command": " ".join(sys.argv),
        "repeat": args.repeat,
        "files": [_profile_file(Path(path), repeat=args.repeat) for path in args.files],
    }
    payload = json.dumps(results, indent=2, sort_keys=True)
    if args.out:
        Path(args.out).write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="+", help="IFC files to profile")
    parser.add_argument("--repeat", type=int, default=1, help="runs per file; reports every run and the fastest total")
    parser.add_argument("--out", help="optional JSON output path")
    return parser.parse_args(argv)


def _profile_file(path: Path, *, repeat: int) -> dict[str, Any]:
    runs = [_profile_once(path) for _ in range(max(1, repeat))]
    best = min(runs, key=lambda run: run["total_seconds"])
    return {
        "path": str(path),
        "size_mb": round(path.stat().st_size / 1_000_000, 3),
        "runs": runs,
        "best": best,
    }


def _profile_once(path: Path) -> dict[str, Any]:
    from athar.bottom.edge_policy import build_edge_set, edge_stats
    from athar.bottom.merkle import compute_merkle_hashes
    from athar.bottom.parser import parse_ifc
    from athar.bottom.spatial import build_spatial_features
    from athar.bottom.wl_gossip import compute_topology_hashes

    timings: dict[str, float] = {}
    started = time.perf_counter()

    parsed = _time_stage(timings, "parse_ifc", lambda: parse_ifc(str(path)))
    edges = _time_stage(timings, "build_edge_set", lambda: build_edge_set(parsed))
    merkle_hashes = _time_stage(timings, "compute_merkle_hashes", lambda: compute_merkle_hashes(parsed, edges))
    topology_hashes = _time_stage(
        timings,
        "compute_topology_hashes",
        lambda: compute_topology_hashes(parsed, edges, merkle_hashes),
    )
    spatial_features = _time_stage(timings, "build_spatial_features", lambda: build_spatial_features(parsed, edges))
    signatures = _time_stage(
        timings,
        "assemble_signatures",
        lambda: _assemble_signatures(parsed.entities, merkle_hashes, topology_hashes, spatial_features),
    )

    total_seconds = time.perf_counter() - started
    return {
        "schema": parsed.schema,
        "entities": len(parsed.entities),
        "edges": len(edges),
        "signatures": len(signatures),
        "edge_stats": edge_stats(edges),
        "diagnostics": {
            "dangling_refs": parsed.diagnostics.dangling_refs,
            "cycle_breaks": parsed.diagnostics.cycle_breaks,
            "warnings": len(parsed.diagnostics.warnings),
        },
        "stage_seconds": {key: round(value, 6) for key, value in timings.items()},
        "total_seconds": round(total_seconds, 6),
    }


def _time_stage(timings: dict[str, float], name: str, func: Callable[[], T]) -> T:
    started = time.perf_counter()
    try:
        return func()
    finally:
        timings[name] = time.perf_counter() - started


def _assemble_signatures(entities, merkle_hashes, topology_hashes, spatial_features) -> dict[int, Any]:
    from athar.bottom.constants import CANON_VERSION
    from athar.bottom.edge_policy import DOMAIN_DATA, DOMAIN_GEOMETRY
    from athar.bottom.types import SignatureVector

    signatures: dict[int, Any] = {}
    for step_id, entity in sorted(entities.items()):
        if not (entity.is_product or entity.is_spatial):
            continue
        spatial = spatial_features.get(step_id)
        signatures[step_id] = SignatureVector(
            step_id=step_id,
            guid=entity.global_id,
            name=entity.name,
            entity_type=entity.entity_type,
            canonical_class=entity.canonical_class,
            vh_geometry=merkle_hashes.get(step_id, {}).get(DOMAIN_GEOMETRY, ""),
            vh_data=merkle_hashes.get(step_id, {}).get(DOMAIN_DATA, ""),
            vh_topology=topology_hashes.get(step_id, ""),
            placement=spatial.placement if spatial else None,
            centroid=spatial.centroid if spatial else None,
            aabb=spatial.aabb if spatial else None,
            canon_version=CANON_VERSION,
        )
    return signatures


if __name__ == "__main__":
    raise SystemExit(main())

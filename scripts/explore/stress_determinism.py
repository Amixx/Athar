"""Stress-check deterministic output stability across repeated runs."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import sys
import time

from athar.graph.determinism import canonical_json
from athar.diff.engine import diff_graphs, stream_diff_graphs


def _graph(entities: dict[int, dict]) -> dict:
    return {"metadata": {"schema": "IFC4"}, "entities": entities}


def _fixture_graphs() -> tuple[dict, dict]:
    old_graph = _graph({
        1: {
            "entity_type": "IfcWall",
            "global_id": "WALL",
            "attributes": {"Name": {"kind": "string", "value": "Wall A"}},
            "refs": [],
        },
        2: {
            "entity_type": "IfcBuildingStorey",
            "global_id": "OLD_PARENT",
            "attributes": {},
            "refs": [],
        },
        3: {
            "entity_type": "IfcBuildingStorey",
            "global_id": "NEW_PARENT",
            "attributes": {},
            "refs": [],
        },
        100: {
            "entity_type": "IfcRelContainedInSpatialStructure",
            "attributes": {},
            "refs": [
                {"path": "/RelatingStructure", "target": 2, "target_type": "IfcBuildingStorey"},
                {"path": "/RelatedElements/0", "target": 1, "target_type": "IfcWall"},
            ],
        },
        200: {
            "entity_type": "IfcDirection",
            "attributes": {
                "DirectionRatios": {
                    "kind": "list",
                    "items": [{"kind": "real", "value": "1"}],
                },
            },
            "refs": [],
        },
    })
    new_graph = _graph({
        11: {
            "entity_type": "IfcWall",
            "global_id": "WALL",
            "attributes": {"Name": {"kind": "string", "value": "Wall A v2"}},
            "refs": [],
        },
        12: {
            "entity_type": "IfcBuildingStorey",
            "global_id": "OLD_PARENT",
            "attributes": {},
            "refs": [],
        },
        13: {
            "entity_type": "IfcBuildingStorey",
            "global_id": "NEW_PARENT",
            "attributes": {},
            "refs": [],
        },
        101: {
            "entity_type": "IfcRelContainedInSpatialStructure",
            "attributes": {},
            "refs": [
                {"path": "/RelatingStructure", "target": 13, "target_type": "IfcBuildingStorey"},
                {"path": "/RelatedElements/0", "target": 11, "target_type": "IfcWall"},
            ],
        },
        201: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {
                "Coordinates": {
                    "kind": "list",
                    "items": [{"kind": "real", "value": "9"}],
                },
            },
            "refs": [],
        },
    })
    return old_graph, new_graph


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description="Stress deterministic output stability.")
    parser.add_argument("--rounds", type=int, default=25, help="Number of repeated runs.")
    parser.add_argument("--out", default=None, help="Optional output JSON path.")
    parser.add_argument(
        "--progress-every",
        type=int,
        default=1,
        help="Report progress every N rounds (0 disables intermediate logs).",
    )
    args = parser.parse_args()

    if args.rounds < 1:
        raise ValueError("--rounds must be >= 1")
    if args.progress_every < 0:
        raise ValueError("--progress-every must be >= 0")

    old_graph, new_graph = _fixture_graphs()

    diff_hashes: list[str] = []
    ndjson_hashes: list[str] = []
    chunked_hashes: list[str] = []
    suite_started = time.perf_counter()

    for round_no in range(1, args.rounds + 1):
        result = diff_graphs(old_graph, new_graph, profile="semantic_stable")
        diff_hashes.append(_sha256_text(canonical_json(result)))

        ndjson = "\n".join(stream_diff_graphs(old_graph, new_graph, profile="semantic_stable", mode="ndjson"))
        ndjson_hashes.append(_sha256_text(ndjson))

        chunked = "\n".join(
            stream_diff_graphs(
                old_graph,
                new_graph,
                profile="semantic_stable",
                mode="chunked_json",
                chunk_size=2,
            )
        )
        chunked_hashes.append(_sha256_text(chunked))

        if args.progress_every and (
            round_no == 1 or round_no == args.rounds or round_no % args.progress_every == 0
        ):
            elapsed_s = round(time.perf_counter() - suite_started, 3)
            print(
                f"[determinism] round {round_no}/{args.rounds} elapsed={elapsed_s}s",
                file=sys.stderr,
                flush=True,
            )

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rounds": args.rounds,
        "diff_graphs": {
            "unique_hashes": sorted(set(diff_hashes)),
            "stable": len(set(diff_hashes)) == 1,
        },
        "stream_diff_graphs_ndjson": {
            "unique_hashes": sorted(set(ndjson_hashes)),
            "stable": len(set(ndjson_hashes)) == 1,
        },
        "stream_diff_graphs_chunked_json": {
            "unique_hashes": sorted(set(chunked_hashes)),
            "stable": len(set(chunked_hashes)) == 1,
        },
    }

    payload = canonical_json(report) + "\n"
    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(payload, encoding="utf-8")
        print(f"Wrote determinism stress report to {out_path}")
    else:
        print(payload, end="")
    print(
        f"[determinism] completed rounds={args.rounds} elapsed={round(time.perf_counter() - suite_started, 3)}s",
        file=sys.stderr,
        flush=True,
    )


if __name__ == "__main__":
    main()

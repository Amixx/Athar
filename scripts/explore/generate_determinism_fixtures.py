"""Generate deterministic golden fixtures for low-level diff output."""

from __future__ import annotations

from pathlib import Path

from athar.determinism import canonical_json, environment_fingerprint
from athar.diff_engine import diff_graphs, stream_diff_graphs


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


def main() -> None:
    root = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "determinism"
    root.mkdir(parents=True, exist_ok=True)

    old_graph, new_graph = _fixture_graphs()
    result = diff_graphs(old_graph, new_graph, profile="semantic_stable")
    ndjson_lines = list(stream_diff_graphs(old_graph, new_graph, profile="semantic_stable", mode="ndjson"))
    chunked_lines = list(
        stream_diff_graphs(
            old_graph,
            new_graph,
            profile="semantic_stable",
            mode="chunked_json",
            chunk_size=2,
        )
    )

    (root / "diff_result.json").write_text(canonical_json(result) + "\n", encoding="utf-8")
    (root / "stream_ndjson.ndjson").write_text("\n".join(ndjson_lines) + "\n", encoding="utf-8")
    (root / "stream_chunked_json.ndjson").write_text("\n".join(chunked_lines) + "\n", encoding="utf-8")
    (root / "environment_fingerprint.json").write_text(
        canonical_json(environment_fingerprint()) + "\n",
        encoding="utf-8",
    )

    print(f"Wrote determinism fixtures to {root}")


if __name__ == "__main__":
    main()

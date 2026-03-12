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


def _fixture_dir() -> Path:
    return Path(__file__).resolve().parent / "fixtures" / "determinism"


def test_diff_result_matches_frozen_golden_fixture():
    old_graph, new_graph = _fixture_graphs()
    result = diff_graphs(old_graph, new_graph, profile="semantic_stable")
    actual = canonical_json(result) + "\n"
    expected = (_fixture_dir() / "diff_result.json").read_text(encoding="utf-8")
    assert actual == expected


def test_ndjson_stream_matches_frozen_golden_fixture():
    old_graph, new_graph = _fixture_graphs()
    lines = list(stream_diff_graphs(old_graph, new_graph, profile="semantic_stable", mode="ndjson"))
    actual = "\n".join(lines) + "\n"
    expected = (_fixture_dir() / "stream_ndjson.ndjson").read_text(encoding="utf-8")
    assert actual == expected


def test_chunked_stream_matches_frozen_golden_fixture():
    old_graph, new_graph = _fixture_graphs()
    lines = list(
        stream_diff_graphs(
            old_graph,
            new_graph,
            profile="semantic_stable",
            mode="chunked_json",
            chunk_size=2,
        )
    )
    actual = "\n".join(lines) + "\n"
    expected = (_fixture_dir() / "stream_chunked_json.ndjson").read_text(encoding="utf-8")
    assert actual == expected


def test_repeated_runs_are_byte_identical():
    old_graph, new_graph = _fixture_graphs()
    first = canonical_json(diff_graphs(old_graph, new_graph, profile="semantic_stable"))
    for _ in range(5):
        assert canonical_json(diff_graphs(old_graph, new_graph, profile="semantic_stable")) == first


def test_environment_fingerprint_matches_frozen_fixture():
    actual = canonical_json(environment_fingerprint()) + "\n"
    expected = (_fixture_dir() / "environment_fingerprint.json").read_text(encoding="utf-8")
    assert actual == expected

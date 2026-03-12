import json

from athar.diff.engine import diff_graphs, stream_diff_graphs
from athar.diff.streaming import stream_diff_result


def _graph_with_entities(entities: dict[int, dict]) -> dict:
    return {"metadata": {"schema": "IFC4"}, "entities": entities}


def _sample_diff() -> dict:
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Name": {"kind": "string", "value": "Wall A"}},
            "refs": [],
        },
    })
    new_graph = _graph_with_entities({
        2: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Name": {"kind": "string", "value": "Wall A v2"}},
            "refs": [],
        },
    })
    return diff_graphs(old_graph, new_graph)


def test_stream_diff_result_ndjson_emits_header_items_end():
    result = _sample_diff()
    lines = list(stream_diff_result(result, mode="ndjson"))
    records = [json.loads(line) for line in lines]

    assert records[0]["record_type"] == "header"
    assert records[-1]["record_type"] == "end"
    assert records[-1]["op_counts"]["MODIFY"] == 1
    item_types = [record["record_type"] for record in records]
    assert "base_change" in item_types


def test_stream_diff_result_chunked_json_honors_chunk_size():
    result = _sample_diff()
    lines = list(stream_diff_result(result, mode="chunked_json", chunk_size=1))
    records = [json.loads(line) for line in lines]

    assert records[0]["chunk_type"] == "header"
    assert records[-1]["chunk_type"] == "end"
    assert records[-1]["op_counts"]["MODIFY"] == 1
    base_chunks = [record for record in records if record["chunk_type"] == "base_changes"]
    assert all(chunk["count"] <= 1 for chunk in base_chunks)


def test_stream_diff_result_rejects_invalid_mode_and_chunk_size():
    result = _sample_diff()
    try:
        list(stream_diff_result(result, mode="weird"))
        assert False, "expected ValueError for invalid mode"
    except ValueError:
        pass

    try:
        list(stream_diff_result(result, mode="ndjson", chunk_size=0))
        assert False, "expected ValueError for invalid chunk_size"
    except ValueError:
        pass


def test_stream_diff_graphs_ndjson_matches_diff_graphs_payload():
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Name": {"kind": "string", "value": "Wall A"}},
            "refs": [],
        },
    })
    new_graph = _graph_with_entities({
        2: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Name": {"kind": "string", "value": "Wall A v2"}},
            "refs": [],
        },
    })
    full = diff_graphs(old_graph, new_graph)
    records = [json.loads(line) for line in stream_diff_graphs(old_graph, new_graph, mode="ndjson")]
    streamed_changes = [r["change"] for r in records if r.get("record_type") == "base_change"]
    streamed_markers = [r["marker"] for r in records if r.get("record_type") == "derived_marker"]
    assert streamed_changes == full["base_changes"]
    assert streamed_markers == full["derived_markers"]


def test_stream_diff_graphs_chunked_json_honors_chunk_size():
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Name": {"kind": "string", "value": "Wall A"}},
            "refs": [],
        },
        2: {
            "entity_type": "IfcWall",
            "global_id": "BBB",
            "attributes": {"Name": {"kind": "string", "value": "Wall B"}},
            "refs": [],
        },
    })
    new_graph = _graph_with_entities({
        3: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Name": {"kind": "string", "value": "Wall A v2"}},
            "refs": [],
        },
    })
    records = [json.loads(line) for line in stream_diff_graphs(old_graph, new_graph, mode="chunked_json", chunk_size=1)]
    base_chunks = [r for r in records if r.get("chunk_type") == "base_changes"]
    assert base_chunks
    assert all(chunk["count"] <= 1 for chunk in base_chunks)


def test_stream_diff_graphs_omits_add_remove_snapshots():
    old_graph = _graph_with_entities({})
    new_graph = _graph_with_entities({
        2: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Name": {"kind": "string", "value": "Wall A"}},
            "refs": [],
        },
    })
    records = [json.loads(line) for line in stream_diff_graphs(old_graph, new_graph, mode="ndjson")]
    change = next(r["change"] for r in records if r.get("record_type") == "base_change")
    assert change["op"] == "ADD"
    assert change["new_snapshot"] is None
    end = records[-1]
    assert end["record_type"] == "end"
    assert end["op_counts"]["ADD"] == 1


def test_diff_graphs_keeps_add_remove_snapshots():
    old_graph = _graph_with_entities({})
    new_graph = _graph_with_entities({
        2: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Name": {"kind": "string", "value": "Wall A"}},
            "refs": [],
        },
    })
    result = diff_graphs(old_graph, new_graph)
    change = result["base_changes"][0]
    assert change["op"] == "ADD"
    assert change["new_snapshot"] is not None


def test_stream_diff_graphs_chunked_end_includes_op_counts():
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Name": {"kind": "string", "value": "Wall A"}},
            "refs": [],
        },
    })
    new_graph = _graph_with_entities({
        2: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Name": {"kind": "string", "value": "Wall A v2"}},
            "refs": [],
        },
    })
    records = [json.loads(line) for line in stream_diff_graphs(old_graph, new_graph, mode="chunked_json", chunk_size=1)]
    end = records[-1]
    assert end["chunk_type"] == "end"
    assert end["op_counts"]["MODIFY"] == 1


def test_stream_diff_graphs_ndjson_end_counts_match_payload():
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Name": {"kind": "string", "value": "Wall A"}},
            "refs": [],
        },
        2: {
            "entity_type": "IfcWall",
            "global_id": "BBB",
            "attributes": {"Name": {"kind": "string", "value": "Wall B"}},
            "refs": [],
        },
    })
    new_graph = _graph_with_entities({
        3: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Name": {"kind": "string", "value": "Wall A v2"}},
            "refs": [],
        },
        4: {
            "entity_type": "IfcWall",
            "global_id": "CCC",
            "attributes": {"Name": {"kind": "string", "value": "Wall C"}},
            "refs": [],
        },
    })
    records = [json.loads(line) for line in stream_diff_graphs(old_graph, new_graph, mode="ndjson")]
    changes = [record["change"] for record in records if record.get("record_type") == "base_change"]
    end = records[-1]
    assert end["record_type"] == "end"
    assert end["base_change_count"] == len(changes)
    expected_op_counts: dict[str, int] = {}
    for change in changes:
        op = change["op"]
        expected_op_counts[op] = expected_op_counts.get(op, 0) + 1
    assert end["op_counts"] == expected_op_counts


def test_stream_diff_graphs_header_matches_diff_graphs_with_custom_matcher_policy():
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Name": {"kind": "string", "value": "Wall A"}},
            "refs": [],
        },
        20: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {
                "Coordinates": {
                    "kind": "list",
                    "items": [
                        {"kind": "real", "value": "1"},
                        {"kind": "real", "value": "2"},
                        {"kind": "real", "value": "3"},
                    ],
                }
            },
            "refs": [],
        },
    })
    new_graph = _graph_with_entities({
        2: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Name": {"kind": "string", "value": "Wall A"}},
            "refs": [],
        },
        21: {
            "entity_type": "IfcCartesianPoint",
            "attributes": {
                "Coordinates": {
                    "kind": "list",
                    "items": [
                        {"kind": "real", "value": "1"},
                        {"kind": "real", "value": "2"},
                        {"kind": "real", "value": "4"},
                    ],
                }
            },
            "refs": [],
        },
    })
    matcher_policy = {"secondary_match": {"score_threshold": 0.99}}
    full = diff_graphs(old_graph, new_graph, matcher_policy=matcher_policy)
    records = [json.loads(line) for line in stream_diff_graphs(old_graph, new_graph, mode="ndjson", matcher_policy=matcher_policy)]
    header = records[0]
    assert header["record_type"] == "header"
    assert header["identity_policy"] == full["identity_policy"]


def test_stream_diff_graphs_timings_are_opt_in_in_header_stats():
    old_graph = _graph_with_entities({
        1: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Name": {"kind": "string", "value": "Wall A"}},
            "refs": [],
        },
    })
    new_graph = _graph_with_entities({
        2: {
            "entity_type": "IfcWall",
            "global_id": "AAA",
            "attributes": {"Name": {"kind": "string", "value": "Wall A v2"}},
            "refs": [],
        },
    })

    default_header = json.loads(next(iter(stream_diff_graphs(old_graph, new_graph, mode="ndjson"))))
    assert "timings_ms" not in default_header["stats"]

    timed_header = json.loads(
        next(iter(stream_diff_graphs(old_graph, new_graph, mode="ndjson", timings=True)))
    )
    timings = timed_header["stats"].get("timings_ms")
    assert isinstance(timings, dict)
    assert "prepare_context" in timings

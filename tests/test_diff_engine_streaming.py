import json

from athar.diff_engine import diff_graphs, stream_diff_graphs, stream_diff_result


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
    item_types = [record["record_type"] for record in records]
    assert "base_change" in item_types


def test_stream_diff_result_chunked_json_honors_chunk_size():
    result = _sample_diff()
    lines = list(stream_diff_result(result, mode="chunked_json", chunk_size=1))
    records = [json.loads(line) for line in lines]

    assert records[0]["chunk_type"] == "header"
    assert records[-1]["chunk_type"] == "end"
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
